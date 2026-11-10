"""Bounded-memory fused ingest, immutable parts, and externally spilled consolidation."""
import hashlib
import json
import os
import resource
import shutil
import time
from collections import Counter
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import psutil

from .download import atomic_json
from .features import COLUMNS, KEYS, partial, merge
from .storage import Manifest
from . import REVISION, REPOSITORY

ENGINE_SOURCE=Path(__file__).read_text()
FEATURE_SOURCE=Path(__file__).with_name('features.py').read_text()


def usage():
    p=psutil.Process()
    io=p.io_counters()
    return dict(rss=p.memory_info().rss,peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                cpu_seconds=sum(p.cpu_times()[:2]),disk_read_bytes=io.read_bytes,disk_write_bytes=io.write_bytes)


def _process(entry, source, root, *, mode='local', batch_size=100288, groups_per_unit=64, max_groups=None, stream_groups=False, deadline=None,dictionary_read=False):
    root=Path(root); root.mkdir(parents=True,exist_ok=True)
    m=Manifest(root/'parts')
    run_meta=root/'source.json'
    (root/'engine_ingest_snapshot.py').write_text(ENGINE_SOURCE)
    (root/'features_snapshot.py').write_text(FEATURE_SOURCE)
    code_hash=hashlib.sha256(FEATURE_SOURCE.encode()).hexdigest()
    identity=dict(revision=REVISION,source=entry,feature_code_sha256=code_hash,groups_per_unit=groups_per_unit)
    if run_meta.exists() and json.loads(run_meta.read_text())!=identity:
        raise ValueError('Source or feature code identity changed; use a new output directory')
    atomic_json(run_meta,identity)
    m.verify()  # never trust a confirmed unit with missing or damaged objects
    started=time.perf_counter(); before=usage(); times=Counter(); total=0; requests=[]
    io_log=root/'original_reads.jsonl'
    attempt=f'{os.getpid()}-{time.time_ns()}'
    if mode=='remote':
        from huggingface_hub import HfFileSystem
        f=HfFileSystem().open(f'datasets/{REPOSITORY}@{REVISION}/'+entry['path'],'rb',
                              block_size=4<<20,cache_type='readahead')
        fetch=f._fetch_range
        def counted(start,end):
            t=time.perf_counter(); data=fetch(start,end)
            requests.append(dict(start=start,end=end,bytes=len(data),seconds=time.perf_counter()-t))
            return data
        f._fetch_range=counted
        f.cache.fetcher=counted  # cache captured the original bound method during construction
    else:
        f=open(source,'rb')
    try:
        t=time.perf_counter(); p=pq.ParquetFile(f,read_dictionary=['asset_id','condition_id','event_type','source_archive','side'] if dictionary_read else None); times['footer_seconds']+=time.perf_counter()-t
        if set(COLUMNS)-set(p.schema_arrow.names): raise ValueError('Required columns missing')
        for name in COLUMNS:
            dtype=p.schema_arrow.field(name).type
            if name=='event_ts_ms':valid=pa.types.is_integer(dtype)
            elif name in ['best_bid','best_ask','price','size']:valid=pa.types.is_floating(dtype) or pa.types.is_integer(dtype)
            else:valid=pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or (pa.types.is_dictionary(dtype) and (pa.types.is_string(dtype.value_type) or pa.types.is_large_string(dtype.value_type)))
            if not valid:raise TypeError(f'Unsupported original type {name}: {dtype}')
        declared=p.metadata.num_rows; ng=p.num_row_groups
        selected=min(ng,max_groups) if max_groups is not None else ng
        atomic_json(root/'parquet.json',dict(rows=declared,row_groups=ng,footer_bytes=p.metadata.serialized_size,
            schema=str(p.schema_arrow),selected_groups=selected))
        offsets=[0]
        for g in range(ng):offsets.append(offsets[-1]+p.metadata.row_group(g).num_rows)
        for first in range(0,selected,groups_per_unit):
            last=min(first+groups_per_unit,selected); unit=f'{entry["ordinal"]}:{first}:{last}'
            if m.confirmed(unit): continue
            if deadline is not None and time.time()>=deadline:raise TimeoutError('Approved execution budget reached at unit boundary')
            if shutil.disk_usage(root).free<100*1024**3:raise OSError('Disk reserve reached')
            features=[]; relations=[]; quality=Counter(); types=Counter(); rows=0; missing=0
            with io_log.open('a') as log:
                log.write(json.dumps(dict(attempt=attempt,operation='reader_requested',groups=list(range(first,last)),mode=mode))+'\n')
            if stream_groups:
                prior=len(requests);rg_start=time.perf_counter();n=0
                it=p.iter_batches(batch_size=batch_size,row_groups=list(range(first,last)),columns=COLUMNS,use_threads=True)
                while True:
                    t=time.perf_counter();b=next(it,None);times['parquet_read_decode_seconds']+=time.perf_counter()-t
                    if b is None:break
                    t=time.perf_counter();feat,rel,audit=partial(b,entry['ordinal'],offsets[first]+n)
                    times['transform_catalog_group_seconds']+=time.perf_counter()-t
                    for phase,value in audit.get('timings',{}).items():times[phase]+=value
                    features.append(feat);relations.append(rel);n+=b.num_rows
                    quality.update(audit['quality']);types.update(audit['types']);missing+=audit['missing_key']
                    if psutil.Process().memory_info().rss>10*1024**3:raise MemoryError('RSS limit reached')
                if n!=offsets[last]-offsets[first]:raise ValueError('Incomplete group range')
                rows=n
                with io_log.open('a') as log:
                    for g in range(first,last):
                        log.write(json.dumps(dict(attempt=attempt,group=g,rows=offsets[g+1]-offsets[g],decoder_passes=1,mode=mode,
                            shared_reader_first_group=first,shared_reader_last_group=last))+'\n')
                    log.write(json.dumps(dict(attempt=attempt,operation='shared_reader_ranges',first_group=first,last_group=last,
                        seconds=time.perf_counter()-rg_start,range_fetches=requests[prior:]))+'\n')
            else:
                for g in range(first,last):
                    start=offsets[g]; n=0; prior=len(requests); rg_start=time.perf_counter()
                    it=p.iter_batches(batch_size=batch_size,row_groups=[g],columns=COLUMNS,use_threads=True)
                    while True:
                        t=time.perf_counter(); b=next(it,None); times['parquet_read_decode_seconds']+=time.perf_counter()-t
                        if b is None:break
                        t=time.perf_counter(); feat,rel,audit=partial(b,entry['ordinal'],start+n)
                        times['transform_catalog_group_seconds']+=time.perf_counter()-t
                        for phase,value in audit.get('timings',{}).items():times[phase]+=value
                        features.append(feat);relations.append(rel);n+=b.num_rows
                        quality.update(audit['quality']);types.update(audit['types']);missing+=audit['missing_key']
                        if psutil.Process().memory_info().rss>10*1024**3:raise MemoryError('RSS limit reached')
                    expected=p.metadata.row_group(g).num_rows
                    if n!=expected:raise ValueError('Incomplete original row group')
                    rows+=n
                    with io_log.open('a') as log:
                        log.write(json.dumps(dict(attempt=attempt,group=g,rows=n,decoder_passes=1,mode=mode,
                             seconds=time.perf_counter()-rg_start,range_fetches=requests[prior:]))+'\n')
            t=time.perf_counter()
            feats=pl.concat(features).with_columns(pl.col('asset_id').cast(pl.String),pl.col('condition_id').cast(pl.String))  # partial keys may repeat; consolidate exactly once downstream
            rels=pl.concat(relations).group_by(['asset_id','condition_id']).agg(
                pl.col('first_ts').min(),pl.col('last_ts').max(),pl.col('events').sum())
            rels=rels.with_columns(pl.col('asset_id').cast(pl.String),pl.col('condition_id').cast(pl.String))
            times['partial_merge_seconds']+=time.perf_counter()-t
            if sum(quality.values())!=rows or sum(types.values())!=rows:raise ValueError('Audit does not reconcile')
            metadata=dict(first_group=first,last_group_exclusive=last,rows=rows,missing_key=missing,
                          quality=dict(quality),types=dict(types),attempt=attempt)
            t=time.perf_counter();m.publish(unit,{'features':feats.to_arrow(),'relations':rels.to_arrow()},metadata)
            times['write_hash_commit_seconds']+=time.perf_counter()-t
            total+=rows
            progress=dict(source=entry['path'],last_group=last,total_groups=ng,rows_this_attempt=total,
                          seconds=time.perf_counter()-started,resources=usage(),timings=dict(times),pid=os.getpid())
            atomic_json(root/'progress.json',progress)
            print(json.dumps(progress),flush=True)
    except BaseException as error:
        with (root/'ingest_attempts.jsonl').open('a') as history:history.write(json.dumps(dict(attempt=attempt,seconds=time.perf_counter()-started,failed=True))+'\n')
        with io_log.open('a') as log:log.write(json.dumps(dict(attempt=attempt,operation='attempt_failed',error=repr(error),range_payload_bytes=sum(x['bytes'] for x in requests)))+'\n')
        raise
    finally:
        f.close()
        m.db.close()
    attempt_seconds=time.perf_counter()-started
    journal=root/'ingest_attempts.jsonl'
    if not journal.exists() and (root/'ingest_report.json').exists():
        previous=json.loads((root/'ingest_report.json').read_text())
        with journal.open('a') as history:history.write(json.dumps(dict(attempt='legacy_recorded_attempts',seconds=previous['seconds']))+'\n')
    with journal.open('a') as history:history.write(json.dumps(dict(attempt=attempt,seconds=attempt_seconds,failed=False,rows=total))+'\n')
    cumulative_seconds=sum(json.loads(line)['seconds'] for line in journal.read_text().splitlines())
    m=Manifest(root/'parts')
    commits=[json.loads(x[0]) for x in m.db.execute('SELECT metadata FROM commits')]
    counted_rows=sum(x['rows'] for x in commits)
    complete=selected==ng and counted_rows==declared
    report=dict(source=entry['path'],revision=REVISION,mode=mode,complete=complete,
        declared_rows=declared,rows_confirmed=counted_rows,row_groups=ng,groups_selected=selected,
        seconds=cumulative_seconds,current_attempt_seconds=attempt_seconds,rows_this_attempt=total,
        events_per_second=total/max(1e-9,time.perf_counter()-started),timings=dict(times),
        range_payload_bytes=sum(x['bytes'] for x in requests),range_requests=len(requests),
        resources_before=before,resources_after=usage(),engine_code_sha256=hashlib.sha256(ENGINE_SOURCE.encode()).hexdigest(),
        derived_bytes=sum(x.stat().st_size for x in (root/'parts').glob('*.parquet')),
        dictionary_read=dictionary_read,logical_source_passes=int(total>0),metadata_opens=1,stream_groups=stream_groups,batch_size=batch_size,orphan_objects=m.orphans())
    atomic_json(root/'ingest_report.json',report);m.db.close()
    return report


def _consolidate(root, buckets=None, deadline=None,sparse_merge=False):
    import duckdb
    import math
    from .storage import digest
    root=Path(root);(root/'engine_consolidation_snapshot.py').write_text(ENGINE_SOURCE);m=Manifest(root/'parts')
    entries=m.db.execute("SELECT path,rows FROM objects WHERE path LIKE '%_features.parquet'").fetchall()
    parts=[str(root/'parts'/x[0]) for x in entries]
    expected=sum(json.loads(x[0])['rows']-json.loads(x[0])['missing_key'] for x in m.db.execute('SELECT metadata FROM commits'))
    if not parts:raise ValueError('No confirmed features')
    marker=root/'consolidated.json'
    if marker.exists():
        saved=json.loads(marker.read_text());verify_series(saved);m.db.close();return saved
    buckets=buckets or max(8,2**math.ceil(math.log2(max(1,sum(x[1] for x in entries)/1000000))))
    soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE,(min(hard,max(soft,int(buckets)*3+256)),hard))
    db=duckdb.connect();db.execute("SET memory_limit='3GB'");db.execute('SET threads=4')
    db.execute('SET preserve_insertion_order=false')
    spill=root/'spill_partitioned';spill.mkdir(exist_ok=True)
    db.execute('SET temp_directory=?',[str(spill)])
    db.from_parquet(parts).create_view('parts')
    columns=[x[0] for x in db.execute('DESCRIBE parts').fetchall()];expr=[]
    for edge,func in [('first','arg_min'),('last','arg_max')]:
        names=[c for c in columns if c.endswith('_'+edge)]
        fields=','.join('"'+n+'" := "'+n+'"' for n in names)
        order=','.join(c+'_'+edge for c in ['event_ts_ms','file_ordinal','row_position'])
        expr.append(f'{func}(struct_pack({fields}), ({order})) FILTER (WHERE event_ts_ms_{edge} IS NOT NULL) AS _{edge}')
    for name in columns:
        q='"'+name+'"'
        if name in KEYS or name.endswith(('_first','_last')):continue
        if name.endswith('_min'):expr.append(f'min({q}) AS {q}')
        elif name.endswith('_max'):expr.append(f'max({q}) AS {q}')
        elif name=='reported_trade_size_sum':expr.append(f'sum({q}) AS {q}')
        else:expr.append(f'sum({q})::UBIGINT AS {q}')
    t=time.perf_counter();partition=root/'merge_partitioned';stages={};io_start=usage();reused_partition=(root/'partitioned.json').exists()
    if not (root/'partitioned.json').exists():
        # Exactly one scan routes every record to one physical bucket. Subsequent bucket
        # queries read disjoint files, never re-filter the full original part list.
        if partition.exists():shutil.rmtree(partition)
        # Arrow streaming writes each bucket immediately; no all-partition DuckDB
        # writer buffers or repartition-sort spill. Every source part is scanned once.
        from concurrent.futures import ThreadPoolExecutor
        writers={};buffers={};buffer_rows={};cycle=0
        def flush_bucket(bucket):
            if not buffer_rows.get(bucket,0):return
            table=pa.concat_tables(buffers[bucket]);folder=partition/f'bucket={bucket}';folder.mkdir(parents=True,exist_ok=True)
            if bucket not in writers:
                writers[bucket]=pq.ParquetWriter(folder/f'data_{cycle:05d}.parquet',table.schema,compression='zstd',compression_level=1)
            writers[bucket].write_table(table,row_group_size=8192)
            buffers[bucket]=[];buffer_rows[bucket]=0
        def write_bucket(item):
            key,sub=item;bucket=int(key[0]);table=sub.drop('_bucket').to_arrow()
            buffers.setdefault(bucket,[]).append(table);buffer_rows[bucket]=buffer_rows.get(bucket,0)+table.num_rows
            if buffer_rows[bucket]>=8192:flush_bucket(bucket)
        def close_cycle():
            for bucket in list(buffers):flush_bucket(bucket)
            for writer in writers.values():writer.close()
            writers.clear();buffers.clear();buffer_rows.clear()
        complete=False
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for part_number,part in enumerate(parts):
                    if deadline is not None and time.time()>=deadline:raise TimeoutError('Approved budget reached during derived partitioning')
                    for batch in pq.ParquetFile(part).iter_batches(batch_size=1_000_000):
                        frame=pl.from_arrow(batch).with_columns((pl.col('asset_id').hash(seed=2026)%int(buckets)).alias('_bucket'))
                        list(pool.map(write_bucket,frame.partition_by('_bucket',as_dict=True).items()))
                    if (part_number+1)%16==0:close_cycle();cycle+=1
            close_cycle();complete=True
        finally:
            for writer in writers.values():writer.close()
            writers.clear();buffers.clear();buffer_rows.clear()
        import gc
        gc.collect();pa.default_memory_pool().release_unused()
        atomic_json(root/'partitioned.json',dict(buckets=buckets,input_passes=1,seconds=time.perf_counter()-t))
    if (root/'partitioned.json').exists():buckets=json.loads((root/'partitioned.json').read_text())['buckets']
    stages['partition_seconds']=time.perf_counter()-t;stages['partition_io']=usage()
    db.execute('SET threads=4');aggregate_started=time.perf_counter()
    dest=root/'series';dest.mkdir(exist_ok=True)
    folders=sorted(partition.glob('bucket=*'))
    for bucket_number,folder in enumerate(folders):
        if deadline is not None and time.time()>=deadline:raise TimeoutError('Approved budget reached at bucket boundary')
        out=dest/(folder.name+'.parquet')
        if out.exists():continue
        db.from_parquet(str(folder/'*.parquet')).create_view('bucket_parts',replace=True)
        if sparse_merge:
            db.execute('CREATE OR REPLACE TEMP TABLE duplicate_keys AS SELECT asset_id,condition_id,second FROM bucket_parts GROUP BY asset_id,condition_id,second HAVING count(*)>1').fetchall()
            projected=','.join('b."'+c+'"' for c in columns)
            join='b.asset_id=d.asset_id AND b.condition_id IS NOT DISTINCT FROM d.condition_id AND b.second=d.second'
            db.execute('CREATE OR REPLACE TEMP TABLE marked AS SELECT '+projected+',d.asset_id IS NOT NULL AS _duplicate FROM bucket_parts b LEFT JOIN duplicate_keys d ON '+join).fetchall()
            quoted=','.join('"'+c+'"' for c in columns)
            grouped='SELECT asset_id,condition_id,second,'+','.join(expr)+' FROM marked WHERE _duplicate GROUP BY asset_id,condition_id,second'
            flattened='SELECT * EXCLUDE(_first,_last),_first.*,_last.* FROM ('+grouped+')'
            query='SELECT '+quoted+' FROM marked WHERE NOT _duplicate UNION ALL SELECT '+quoted+' FROM ('+flattened+') ORDER BY asset_id,condition_id,second'
        else:
            grouped='SELECT asset_id,condition_id,second,'+','.join(expr)+' FROM bucket_parts GROUP BY asset_id,condition_id,second'
            query='SELECT * EXCLUDE(_first,_last),_first.*,_last.* FROM ('+grouped+') ORDER BY asset_id,condition_id,second'
        tmp=out.with_suffix('.tmp')
        from .columnar import options
        reader=db.sql(query).to_arrow_reader(batch_size=65536);writer=None
        try:
            for batch in reader:
                table=pa.Table.from_batches([batch])
                if writer is None:writer=pq.ParquetWriter(tmp,table.schema,**options(table.schema))
                writer.write_table(table,row_group_size=65536)
        finally:
            reader.close()
            if writer is not None:writer.close()
        with tmp.open('rb') as f:os.fsync(f.fileno())
        os.replace(tmp,out)
        atomic_json(root/'consolidation_progress.json',dict(buckets_completed=bucket_number+1,buckets_total=len(folders),seconds_current_attempt=time.perf_counter()-t,resources=usage()))
    stages['aggregate_seconds']=time.perf_counter()-aggregate_started;stages['aggregate_io']=usage()
    files=sorted(str(p) for p in dest.glob('*.parquet'));db.from_parquet(files).create_view('result')
    check=db.execute('SELECT sum(events),count(*),count(DISTINCT asset_id),count(DISTINCT condition_id) FROM result').fetchall()[0]
    if int(check[0] or 0)!=expected:raise ValueError(f'Feature event reconciliation failed: {check[0]} != {expected}')
    r=dict(seconds=time.perf_counter()-t+(json.loads((root/'partitioned.json').read_text())['seconds'] if reused_partition else 0),current_attempt_seconds=time.perf_counter()-t,reused_partition=reused_partition,events=int(check[0]),series_rows=check[1],assets=check[2],conditions=check[3],
           sparse_merge=sparse_merge,key_only_scan_passes=int(sparse_merge),materialized_memory_passes=2 if sparse_merge else 0,stage_timings=stages,resources_before=io_start,input_files=len(parts),derived_scan_passes=2,validation_output_scan_passes=1,
           output_bytes=sum(Path(p).stat().st_size for p in files),outputs=files,buckets=buckets,integrity_output_scan_passes=1,
           sha256={Path(p).name:digest(p) for p in files},resources=usage())
    atomic_json(marker,r);db.close();m.db.close();return r


def consolidate(root,buckets=None,deadline=None,sparse_merge=False):
    import fcntl
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with (root/'consolidate.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        return _consolidate(root,buckets,deadline,sparse_merge)


def process(entry,source,root,**kwargs):
    import fcntl
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with (root/'ingest.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        return _process(entry,source,root,**kwargs)


def verify_series(report):
    """Validate canonical objects before reusing a completed partition."""
    from .storage import digest
    rows=0
    for name in report['outputs']:
        path=Path(name)
        if digest(path)!=report['sha256'][path.name]:raise ValueError(f'Canonical series checksum mismatch: {path}')
        rows+=pq.ParquetFile(path).metadata.num_rows
    if rows!=report['series_rows']:raise ValueError('Canonical row count mismatch')
    return rows
