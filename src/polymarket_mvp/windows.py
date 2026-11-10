"""Complete sparse window census, strictly separated from observable extraction."""
import hashlib
import json
import os
import time
from pathlib import Path
import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from .download import atomic_json

SPLITS={'train':(1771718400,1782000000),'validation':(1782000000,1784160000),'test':(1784160000,1786406400)}


def build_census(series, root, splits=None, buckets=128):
    root=Path(root);root.mkdir(parents=True,exist_ok=True);splits=splits or SPLITS
    fingerprint=hashlib.sha256(json.dumps(dict(series=[str(Path(p).resolve()) for p in series],splits=splits,buckets=buckets),sort_keys=True).encode()).hexdigest()
    marker=root/'census.json'
    if marker.exists():
        saved=json.loads(marker.read_text())
        if saved['identity']!=fingerprint:raise ValueError('Census configuration mismatch')
        return saved
    t=time.perf_counter();db=duckdb.connect();db.execute("SET memory_limit='4GB'");db.execute('SET threads=4')
    db.execute('SET temp_directory=?',[str(root/'spill')])
    db.from_parquet([str(p) for p in series]).create_view('series')
    normalized=root/'normalized'
    if not (root/'normalized.json').exists():
        # Global merging handles late events and crossings between original files before windowing.
        select='''SELECT asset_id,condition_id,second, (second//512)*512 AS block_start,
          (arg_max(struct_pack(v:=spread_last),(event_ts_ms_last,file_ordinal_last,row_position_last))
            FILTER (WHERE event_ts_ms_last IS NOT NULL)).v AS spread,
          sum(events) AS activity,sum(n_v1) AS v1,sum(n_v2) AS v2
          FROM series GROUP BY asset_id,condition_id,second'''
        db.execute(f"COPY (SELECT *,count(DISTINCT condition_id) OVER (PARTITION BY asset_id,block_start) AS n_conditions,hash(asset_id,block_start)%{int(buckets)} AS bucket FROM ({select})) TO '{normalized}' (FORMAT PARQUET,PARTITION_BY(bucket),COMPRESSION ZSTD)").fetchall()
        atomic_json(root/'normalized.json',dict(identity=fingerprint,original_derived_scan_passes=1,seconds=time.perf_counter()-t))
    total=0;eligible=0;excluded={};split_counts={k:0 for k in splits};index_writer=None
    schema=pa.schema([('window_id',pa.string()),('asset_id',pa.string()),('condition_id',pa.string()),
        ('start',pa.int64()),('split',pa.string()),('source',pa.string()),('activity',pa.int64()),
        ('observed',pa.int32()),('target',pa.int32()),('reason',pa.string()),
        ('file',pa.string()),('row_group',pa.int32()),('row',pa.int32())])
    index_tmp=root/'index.partial.parquet';index_writer=pq.ParquetWriter(index_tmp,schema,compression='zstd')
    try:
        for folder in sorted(normalized.glob('bucket=*')):
            outfile=root/(folder.name+'.parquet')
            db.from_parquet(str(folder/'*.parquet')).create_view('bucket_input',replace=True)
            q='''SELECT asset_id,condition_id,block_start,list(second-block_start ORDER BY second) AS offsets,
                list(spread ORDER BY second) AS values,sum(activity)::BIGINT AS activity,
                sum(v1)::BIGINT AS v1,sum(v2)::BIGINT AS v2,max(n_conditions) AS n_conditions
                FROM bucket_input GROUP BY asset_id,condition_id,block_start ORDER BY asset_id,condition_id,block_start'''
            if not outfile.exists():
                tmp=outfile.with_suffix('.tmp')
                db.execute(f"COPY ({q}) TO '{tmp}' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 16384)").fetchall()
                os.replace(tmp,outfile)
            p=pq.ParquetFile(outfile)
            for rg in range(p.num_row_groups):
                records=[]
                for row,r in enumerate(p.read_row_group(rg).to_pylist()):
                    total+=1;start=r['block_start'];split=next((s for s,(a,b) in splits.items() if a<=start and start+512<=b),None)
                    offsets=np.asarray(r['offsets'],dtype=np.int32);values=np.asarray(r['values'],dtype=np.float64)
                    good=np.isfinite(values);obs=np.zeros(512,dtype=bool);obs[offsets[good]]=True
                    patches=np.flatnonzero(obs.reshape(64,8).all(1));target=int(patches[-1])*8 if len(patches)>=2 else -1
                    reason='eligible'
                    if split is None:reason='split_boundary_or_outside'
                    elif r['condition_id'] is None:reason='missing_condition'
                    elif r['n_conditions']>1:reason='conflicting_condition'
                    elif target<0:reason='insufficient_complete_patches'
                    if reason=='eligible':eligible+=1;split_counts[split]+=1
                    else:excluded[reason]=excluded.get(reason,0)+1
                    source='mixed' if r['v1'] and r['v2'] else 'v1' if r['v1'] else 'v2' if r['v2'] else 'unknown'
                    wid=hashlib.sha256(f'{r["asset_id"]}|{r["condition_id"]}|{start}'.encode()).hexdigest()
                    records.append(dict(window_id=wid,asset_id=r['asset_id'],condition_id=r['condition_id'],start=start,
                        split=split,source=source,activity=r['activity'],observed=int(obs.sum()),target=target,reason=reason,
                        file=str(outfile.resolve()),row_group=rg,row=row))
                index_writer.write_table(pa.Table.from_pylist(records,schema=schema))
    finally:index_writer.close();db.close()
    os.replace(index_tmp,root/'index.parquet')
    report=dict(identity=fingerprint,total_windows=total,eligible=eligible,excluded=excluded,split_counts=split_counts,
        seconds=time.perf_counter()-t,source_derived_passes=1,normalized_passes=1,window_records_passes=1,
        splits=splits,index=str((root/'index.parquet').resolve()),final_independent_test=splits==SPLITS)
    atomic_json(marker,report);return report


def select_training(census_root, budget, seed=2026):
    root=Path(census_root)
    from .storage import digest
    if (root/'selection.json').exists():
        saved=json.loads((root/'selection.json').read_text())
        if saved.get('seed')!=seed:raise ValueError('Frozen selection seed mismatch')
        if saved.get('selection_sha256')!=digest(root/'selection.parquet'):raise ValueError('Frozen selection integrity mismatch; use a new experiment for legacy selections')
        return saved
    db=duckdb.connect();db.execute("SET memory_limit='3GB'")
    db.from_parquet(str(root/'index.parquet')).create_view('census')
    edges=db.execute("SELECT quantile_cont(activity,[0.25,0.5,0.75]) FROM census WHERE split='train' AND reason='eligible'").fetchall()[0][0]
    if edges is None:raise ValueError('No eligible training windows')
    q=f'''SELECT *,CAST(start//86400 AS BIGINT) AS day,
       CASE WHEN activity<={edges[0]} THEN 0 WHEN activity<={edges[1]} THEN 1 WHEN activity<={edges[2]} THEN 2 ELSE 3 END AS activity_bin,
       sha256(window_id||'{int(seed)}') AS draw FROM census WHERE split='train' AND reason='eligible' '''
    db.execute('CREATE TEMP TABLE candidates AS '+q).fetchall()
    db.execute('''CREATE TEMP TABLE ranked AS SELECT *,
       row_number() OVER(PARTITION BY day,condition_id,source,activity_bin ORDER BY draw,window_id) AS stratum_rank,
       row_number() OVER(PARTITION BY asset_id ORDER BY draw,window_id) AS asset_rank
       FROM candidates''').fetchall()
    minimum,total=db.execute('SELECT count(*) FILTER(WHERE stratum_rank=1 OR asset_rank=1),count(*) FROM ranked').fetchall()[0]
    report=dict(minimum_windows=minimum,eligible_train=total,budget_windows=int(budget),seed=seed,activity_edges=edges,
                cardinality_gate=minimum<=budget)
    atomic_json(root/'selection_budget.json',report)
    if minimum>budget:
        alternative=db.execute('''SELECT count(*) FROM (
          SELECT *,row_number() OVER(PARTITION BY day ORDER BY draw,window_id) AS d,
            row_number() OVER(PARTITION BY condition_id ORDER BY draw,window_id) AS c,
            row_number() OVER(PARTITION BY source,activity_bin ORDER BY draw,window_id) AS s
          FROM ranked) WHERE d=1 OR c=1 OR s=1 OR asset_rank=1''').fetchall()[0][0]
        report.update(conflict_windows=minimum-budget,
          alternative_policy='deterministic marginal coverage of every date, condition, asset and source/activity bin; no condition-day cross-product guarantee',
          alternative_minimum_windows=alternative,alternative_fits=alternative<=budget,
          requires_user_decision=True)
        atomic_json(root/'selection_budget.json',report)
        db.close();raise RuntimeError(f'Minimum coverage exceeds measured budget: {report}')
    # Mandatory coverage first; remaining quota uses sqrt(stratum size), then deterministic hash draws.
    db.execute('''CREATE TEMP TABLE mandatory AS SELECT * FROM ranked WHERE stratum_rank=1 OR asset_rank=1''').fetchall()
    db.execute('''CREATE TEMP TABLE remaining AS SELECT * FROM ranked WHERE stratum_rank<>1 AND asset_rank<>1''').fetchall()
    extra=min(total,budget)-minimum
    db.execute('''CREATE TEMP TABLE allocation AS SELECT day,condition_id,source,activity_bin,count(*) AS n,sqrt(count(*)) AS weight
                  FROM remaining GROUP BY ALL''').fetchall()
    # Weighted deterministic priority gives balanced representation without replacement.
    db.execute('''CREATE TEMP TABLE chosen AS SELECT * FROM mandatory''').fetchall()
    if extra:
        db.execute(f'''INSERT INTO chosen SELECT r.* FROM remaining r JOIN allocation a USING(day,condition_id,source,activity_bin)
          ORDER BY r.stratum_rank/a.weight,r.draw,r.window_id LIMIT {int(extra)}''').fetchall()
    dest=root/'selection.parquet';db.execute(f"COPY (SELECT * FROM chosen ORDER BY draw,window_id) TO '{dest}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    db.execute(f"COPY (SELECT day,condition_id,source,activity_bin,count(*) AS selected,count(DISTINCT asset_id) AS assets FROM chosen GROUP BY ALL) TO '{root/'selection_distribution.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    distribution=db.execute('SELECT source,activity_bin,count(*),count(DISTINCT asset_id),count(DISTINCT condition_id) FROM chosen GROUP BY ALL').fetchall()
    report.update(selection_sha256=digest(root/'selection.parquet'),selected=min(total,budget),excluded_budget=total-min(total,budget),distribution=distribution,epoch_definition='one pass over frozen selection')
    atomic_json(root/'selection.json',report);db.close();return report


def pack_windows(census_root, output, split, selected=False):
    """Stream compact locators and write memmaps; never materialize the index in RAM."""
    from .storage import digest
    import fcntl
    root=Path(census_root);output=Path(output);output.mkdir(parents=True,exist_ok=True)
    with (output/'pack.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        index_path=root/('selection.parquet' if selected else 'index.parquet')
        index_sha=digest(index_path)
        receipt=output/'pack.json'
        if receipt.exists():
            old=json.loads(receipt.read_text())
            if old.get('input_index_sha256')!=index_sha or old.get('split')!=split:
                raise ValueError('Pack input changed; use a new output directory')
            for name,sha in old['hashes'].items():
                if digest(output/name)!=sha:raise ValueError(f'Corrupt pack {name}')
            return old['windows']
        db=duckdb.connect();db.execute("SET memory_limit='2GB'")
        db.execute('SET temp_directory=?',[str(output/'spill')]);db.from_parquet(str(index_path)).create_view('idx')
        n=db.execute("SELECT count(*) FROM idx WHERE reason='eligible' AND split=?",[split]).fetchall()[0][0]
        if not n:db.close();raise ValueError('No windows to pack')
        x=np.lib.format.open_memmap(output/'x.npy',mode='w+',dtype=np.float32,shape=(n,512))
        observed=np.lib.format.open_memmap(output/'observed.npy',mode='w+',dtype=np.uint8,shape=(n,512))
        target=np.lib.format.open_memmap(output/'target.npy',mode='w+',dtype=np.int16,shape=(n,))
        query="SELECT window_id,asset_id,condition_id,start,target,source,activity,observed,split,file,row_group,row FROM idx WHERE reason='eligible' AND split=? ORDER BY file,row_group,row"
        reader=db.sql(query,params=[split]).to_arrow_reader(batch_size=16384)
        writer=None;cached_key=None;cached=None;i=0;rg_reads=0
        try:
            for batch in reader:
                table=pa.Table.from_batches([batch])
                if writer is None:writer=pq.ParquetWriter(output/'metadata.partial.parquet',table.schema,compression='zstd')
                writer.write_table(table)
                for r in table.to_pylist():
                    key=(r['file'],r['row_group'])
                    if key!=cached_key:
                        cached=pq.ParquetFile(key[0]).read_row_group(key[1],columns=['offsets','values']).to_pylist();cached_key=key;rg_reads+=1
                    record=cached[r['row']];offset=np.asarray(record['offsets']);v=np.asarray(record['values'],dtype=np.float32);good=np.isfinite(v)
                    x[i]=0;observed[i]=0;x[i,offset[good]]=v[good];observed[i,offset[good]]=1;target[i]=r['target'];i+=1
        finally:
            reader.close()
            if writer:writer.close()
            db.close()
        if i!=n:raise ValueError('Pack cardinality mismatch')
        x.flush();observed.flush();target.flush();os.replace(output/'metadata.partial.parquet',output/'metadata.parquet')
        hashes={name:digest(output/name) for name in ['x.npy','observed.npy','target.npy','metadata.parquet']}
        for name in hashes:
            with (output/name).open('rb') as f:os.fsync(f.fileno())
        atomic_json(receipt,dict(windows=n,split=split,selected=selected,input_index_sha256=index_sha,
            index_passes=2,window_row_groups_read=rg_reads,window_row_groups_read_once=True,hashes=hashes,
            pack_integrity_scan_passes=1,input_integrity_scan_passes=1,max_locator_batch=16384))
        return n


def repartition_census(source_root,destination,splits):
    """Change experimental temporal splits using the index, without rereading features."""
    source_root=Path(source_root);destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    db=duckdb.connect();db.from_parquet(str(source_root/'index.parquet')).create_view('old')
    cases=' '.join(f"WHEN start>={int(a)} AND start+512<={int(b)} THEN '{s}'" for s,(a,b) in splits.items())
    q=f'''SELECT * EXCLUDE(split,reason),CASE {cases} END AS split,
      CASE WHEN NOT ({' OR '.join(f'(start>={a} AND start+512<={b})' for a,b in splits.values())}) THEN 'split_boundary_or_outside'
        WHEN condition_id IS NULL THEN 'missing_condition'
        WHEN count(DISTINCT condition_id) OVER(PARTITION BY asset_id,start)>1 THEN 'conflicting_condition'
        WHEN target<0 THEN 'insufficient_complete_patches' ELSE 'eligible' END AS reason FROM old'''
    path=destination/'index.parquet';db.execute(f"COPY ({q}) TO '{path}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    db.from_parquet(str(path)).create_view('new')
    excluded=dict(db.execute("SELECT reason,count(*) FROM new WHERE reason<>'eligible' GROUP BY reason").fetchall())
    counts=dict(db.execute("SELECT split,count(*) FROM new WHERE reason='eligible' GROUP BY split").fetchall())
    r=dict(total_windows=db.execute('SELECT count(*) FROM new').fetchall()[0][0],eligible=sum(counts.values()),excluded=excluded,
        split_counts=counts,splits=splits,source_derived_passes=0,index_relabel_passes=1,final_independent_test=False,
        source_census=str(source_root.resolve()),index=str(path.resolve()))
    atomic_json(destination/'census.json',r);db.close();return r

# Keep the previous implementation available for measured comparisons; new runs use
# the streaming calendar frontier and vectorized SQL census below.
build_census_legacy = build_census

def build_census(series, root, splits=None, buckets=16,deadline=None,canonical_unique=False):
    import shutil
    import pyarrow.compute as pc
    root=Path(root);root.mkdir(parents=True,exist_ok=True);splits=splits or SPLITS
    import fcntl
    lock=(root/'census.lock').open('a');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    marker=root/'census.json'
    identity=hashlib.sha256(json.dumps(dict(series=[str(Path(p).resolve()) for p in series],splits=splits,buckets=buckets,canonical_unique=canonical_unique,version=4),sort_keys=True).encode()).hexdigest()
    if marker.exists():
        saved=json.loads(marker.read_text())
        if saved.get('identity')!=identity:raise ValueError('Census configuration mismatch')
        return saved
    started=time.perf_counter();db=duckdb.connect();db.execute("SET memory_limit='3GB'");db.execute('SET threads=4')
    db.execute('SET temp_directory=?',[str(root/'spill')]);stage=root/'normalized_v2'
    if stage.exists():shutil.rmtree(stage)  # only unconfirmed, regenerable census staging
    stage.mkdir(exist_ok=True)
    schema=pa.schema([('window_id',pa.string()),('asset_id',pa.string()),('condition_id',pa.string()),
        ('start',pa.int64()),('split',pa.string()),('source',pa.string()),('activity',pa.int64()),
        ('observed',pa.int32()),('target',pa.int32()),('reason',pa.string()),
        ('file',pa.string()),('row_group',pa.int32()),('row',pa.int32())])
    writer=pq.ParquetWriter(root/'index.partial.parquet',schema,compression='zstd')
    # Footer ranges determine a safe completion frontier, even if nominal file dates
    # are wrong. A future file cannot contribute seconds earlier than its own minimum.
    sources=[]
    wanted=['asset_id','condition_id','second','spread_last','event_ts_ms_last','file_ordinal_last',
            'row_position_last','events','n_v1','n_v2']
    for path in series:
        p=pq.ParquetFile(path);j=p.schema_arrow.names.index('second');mins=[]
        for g in range(p.num_row_groups):
            stat=p.metadata.row_group(g).column(j).statistics
            if stat is None or not stat.has_min_max:raise ValueError('Generated series require timestamp statistics')
            mins.append(stat.min)
        if mins:sources.append((min(mins),str(path)))
    sources.sort();total=0;eligible=0;excluded={};split_counts={s:0 for s in splits};completed_days=set()
    split_sql='CASE '+ ' '.join(f"WHEN block_start>={int(a)} AND block_start+512<={int(b)} THEN '{s}'" for s,(a,b) in splits.items())+' END'
    reads=root/'derived_reads.jsonl';pending=set();chunk=0
    def finalize(day):
        nonlocal total,eligible
        folders=sorted(stage.glob(f'day={day}/bucket=*'))
        for folder in folders:
            if deadline is not None and time.time()>=deadline:raise TimeoutError('Approved budget reached at census bucket boundary')
            db.from_parquet(str(folder/'*.parquet')).create_view('raw',replace=True)
            # Only compact observations for this day/bucket are grouped; no global
            # list aggregation and no rereading the full feature list per bucket.
            merged='''SELECT asset_id,condition_id,second,(second//512)*512 AS block_start,
                (arg_max(struct_pack(v:=spread_last),(event_ts_ms_last,file_ordinal_last,row_position_last))
                 FILTER(WHERE event_ts_ms_last IS NOT NULL)).v AS spread,
                sum(events)::BIGINT AS activity,sum(n_v1)::BIGINT AS v1,sum(n_v2)::BIGINT AS v2
                FROM raw GROUP BY asset_id,condition_id,second'''
            if canonical_unique:
                merged='''SELECT asset_id,condition_id,second,(second//512)*512 AS block_start,
                    spread_last AS spread,events::BIGINT AS activity,n_v1::BIGINT AS v1,n_v2::BIGINT AS v2 FROM raw'''
            db.execute('CREATE OR REPLACE TEMP TABLE merged AS '+merged).fetchall()
            db.execute('''CREATE OR REPLACE TEMP TABLE patches AS
                SELECT asset_id,condition_id,block_start,second//8*8-block_start AS patch,
                    count(*) FILTER(WHERE spread IS NOT NULL AND isfinite(spread)) AS observed
                FROM merged GROUP BY ALL''').fetchall()
            db.execute('''CREATE OR REPLACE TEMP TABLE targets AS
                SELECT asset_id,condition_id,block_start,
                   CASE WHEN count(*) FILTER(WHERE observed=8)>=2 THEN max(patch) FILTER(WHERE observed=8) ELSE -1 END::INTEGER AS target
                FROM patches GROUP BY asset_id,condition_id,block_start''').fetchall()
            # Census metadata covers every observed block. Arrays are materialized
            # only for eligible windows; all other channels remain in feature Parquet.
            windows='''SELECT asset_id,condition_id,block_start,
                sum(activity)::BIGINT AS activity,sum(v1) AS v1,sum(v2) AS v2,
                count(*) FILTER(WHERE spread IS NOT NULL AND isfinite(spread))::INTEGER AS observed
                FROM merged GROUP BY asset_id,condition_id,block_start'''
            q=f'''WITH w AS ({windows}), j AS (
                SELECT w.*,t.target,count(DISTINCT w.condition_id) OVER(PARTITION BY w.asset_id,w.block_start) AS n_conditions
                FROM w JOIN targets t ON w.asset_id=t.asset_id AND w.condition_id IS NOT DISTINCT FROM t.condition_id AND w.block_start=t.block_start),
                s AS (SELECT *,{split_sql} AS split FROM j)
                SELECT *,sha256(asset_id||'|'||coalesce(condition_id,'None')||'|'||CAST(block_start AS VARCHAR)) AS window_id,
                  CASE WHEN v1>0 AND v2>0 THEN 'mixed' WHEN v1>0 THEN 'v1' WHEN v2>0 THEN 'v2' ELSE 'unknown' END AS source,
                  CASE WHEN split IS NULL THEN 'split_boundary_or_outside' WHEN condition_id IS NULL THEN 'missing_condition'
                       WHEN n_conditions>1 THEN 'conflicting_condition' WHEN target<0 THEN 'insufficient_complete_patches' ELSE 'eligible' END AS reason
                FROM s'''
            db.execute('CREATE OR REPLACE TEMP TABLE window_meta AS '+q).fetchall()
            data_query='''SELECT w.asset_id,w.condition_id,w.block_start,w.window_id,
                list(m.second-m.block_start ORDER BY m.second) AS offsets,list(m.spread ORDER BY m.second) AS values
                FROM merged m JOIN window_meta w ON m.asset_id=w.asset_id AND m.condition_id IS NOT DISTINCT FROM w.condition_id
                    AND m.block_start=w.block_start WHERE w.reason='eligible'
                GROUP BY w.asset_id,w.condition_id,w.block_start,w.window_id ORDER BY w.asset_id,w.condition_id,w.block_start'''
            output=root/f'day={day}_{folder.name}.parquet';tmp=output.with_suffix('.tmp')
            db.execute(f"COPY ({data_query}) TO '{tmp}' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 16384)").fetchall();os.replace(tmp,output)
            db.execute('CREATE OR REPLACE TEMP TABLE locs(window_id VARCHAR,file VARCHAR,rg INTEGER,pos INTEGER)').fetchall()
            p=pq.ParquetFile(output)
            for rg in range(p.num_row_groups):
                loc=p.read_row_group(rg,columns=['window_id']);n=loc.num_rows
                loc=loc.append_column('file',pa.array([str(output.resolve())]*n)).append_column('rg',pa.array(np.full(n,rg,dtype=np.int32))).append_column('pos',pa.array(np.arange(n,dtype=np.int32)))
                db.from_arrow(loc).create_view('location_chunk',replace=True)
                db.execute('INSERT INTO locs SELECT * FROM location_chunk').fetchall()
            index_query='''SELECT w.window_id,w.asset_id,w.condition_id,w.block_start AS start,w.split,w.source,w.activity,
                w.observed,w.target,w.reason,l.file,coalesce(l.rg,-1)::INTEGER AS row_group,coalesce(l.pos,-1)::INTEGER AS row
                FROM window_meta w LEFT JOIN locs l USING(window_id) ORDER BY w.asset_id,w.condition_id,w.block_start'''
            index_reader=db.sql(index_query).to_arrow_reader(batch_size=16384)
            try:
                for batch in index_reader:
                    table=pa.Table.from_batches([batch]).cast(schema);n=table.num_rows;total+=n
                    for reason in pc.value_counts(table['reason']).to_pylist():
                        if reason['values']=='eligible':eligible+=reason['counts']
                        else:excluded[reason['values']]=excluded.get(reason['values'],0)+reason['counts']
                    for split in splits:
                        split_counts[split]+=int(pc.sum(pc.cast(pc.and_(pc.equal(table['reason'],'eligible'),pc.fill_null(pc.equal(table['split'],split),False)),pa.int64())).as_py() or 0)
                    writer.write_table(table)
            finally:index_reader.close()
            # This staging directory is generated by this function, never an original.
            shutil.rmtree(folder)
        completed_days.add(day)
    try:
        for number,(minimum,path) in enumerate(sources):
            if deadline is not None and time.time()>=deadline:raise TimeoutError('Approved budget reached at census source boundary')
            p=pq.ParquetFile(path)
            for batch in p.iter_batches(batch_size=500000,columns=wanted):
                d=__import__('polars').from_arrow(batch)
                import polars as pl
                d=d.with_columns(((pl.col('second')//512*512)//86400).alias('_day'),(pl.col('asset_id').hash(seed=2026)%buckets).alias('_bucket'))
                for (day,bucket),part in d.partition_by(['_day','_bucket'],as_dict=True).items():
                    day=int(day);bucket=int(bucket)
                    if day in completed_days:raise ValueError('Invalid timestamp frontier')
                    pending.add(day);folder=stage/f'day={day}'/f'bucket={bucket}';folder.mkdir(parents=True,exist_ok=True)
                    pq.write_table(part.drop('_day','_bucket').to_arrow(),folder/f'chunk_{chunk:09d}.parquet',compression='zstd',compression_level=1)
                    chunk+=1
            with reads.open('a') as f:f.write(json.dumps(dict(operation='census_source',path=path,pass_number=1,rows=p.metadata.num_rows))+'\n')
            frontier=(sources[number+1][0]//512*512)//86400 if number+1<len(sources) else float('inf')
            for day in sorted(x for x in pending if x<frontier):finalize(day);pending.remove(day)
        writer.close();os.replace(root/'index.partial.parquet',root/'index.parquet')
    except BaseException:
        writer.close();raise
    finally:db.close();lock.close()
    r=dict(canonical_unique=canonical_unique,identity=identity,total_windows=total,eligible=eligible,excluded=excluded,split_counts=split_counts,
        splits=splits,seconds=time.perf_counter()-started,source_derived_passes=1,normalized_passes=1,
        index_projection_passes=1,source_footer_passes=1,window_universe='observed asset-condition blocks only; blocks without events are not enumerated',staging_policy='safe timestamp frontier; normalized days reclaimed',
        index=str((root/'index.parquet').resolve()),final_independent_test=splits==SPLITS)
    atomic_json(marker,r);return r
