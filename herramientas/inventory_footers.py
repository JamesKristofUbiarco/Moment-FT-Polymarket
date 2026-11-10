"""Concurrent footer-only census, independent of fused event ingestion."""
import json,time,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa,pyarrow.parquet as pq
from huggingface_hub import HfFileSystem
from polymarket_mvp import REVISION,REPOSITORY
from polymarket_mvp.features import COLUMNS
from polymarket_mvp.download import inventory,atomic_json
root=Path('runs/mvp');dest=root/'metadata_inventory';dest.mkdir(exist_ok=True);started=time.perf_counter()
def one(entry):
    day=entry['path'].split('date=')[1][:10];summary=dest/(day+'.json')
    if summary.exists():return json.loads(summary.read_text())
    fetched=[];t=time.perf_counter()
    with HfFileSystem().open(f'datasets/{REPOSITORY}@{REVISION}/'+entry['path'],'rb',block_size=65536,cache_type='none') as f:
        original=f._fetch_range
        def fetch(a,b):
            t=time.perf_counter();data=original(a,b);fetched.append(dict(start=a,end=b,bytes=len(data),seconds=time.perf_counter()-t));return data
        f._fetch_range=fetch;f.cache.fetcher=fetch;p=pq.ParquetFile(f)
        indices={p.schema.column(i).path:i for i in range(len(p.schema))};rg=[];compressed={c:0 for c in COLUMNS}
        for g in range(p.num_row_groups):
            m=p.metadata.row_group(g);stat=m.column(indices['event_ts_ms']).statistics
            amounts={c:m.column(indices[c]).total_compressed_size for c in COLUMNS}
            for c,n in amounts.items():compressed[c]+=n
            rg.append(dict(row_group=g,rows=m.num_rows,min_ts_ms=stat.min if stat and stat.has_min_max else None,
                max_ts_ms=stat.max if stat and stat.has_min_max else None,selected_compressed_bytes=sum(amounts.values())))
        pq.write_table(pa.Table.from_pylist(rg),dest/(day+'_row_groups.parquet'),compression='zstd')
        r=dict(entry,revision=REVISION,rows=p.metadata.num_rows,row_groups=p.num_row_groups,footer_bytes=p.metadata.serialized_size,
            schema=str(p.schema_arrow),selected_compressed_bytes=compressed,metadata_payload_bytes=sum(x['bytes'] for x in fetched),
            metadata_requests=fetched,seconds=time.perf_counter()-t,original_data_decoder_passes=0)
        if sum(x['rows'] for x in rg)!=r['rows']:raise ValueError('Metadata row sum mismatch')
    atomic_json(summary,r);print(day,r['rows'],r['metadata_payload_bytes'],flush=True);return r
entries=inventory(root)['files']
with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(one,entries))
r=dict(revision=REVISION,files=results,file_count=len(results),original_bytes=sum(x['bytes'] for x in results),
    declared_rows=sum(x['rows'] for x in results),row_groups=sum(x['row_groups'] for x in results),
    metadata_payload_bytes=sum(x['metadata_payload_bytes'] for x in results),seconds=time.perf_counter()-started,
    scope='footers only; does not claim original events processed',data_decoder_passes=0)
atomic_json(root/'inventory_enriched.json',r);print('INVENTORY_COMPLETE',r['declared_rows'],r['row_groups'],flush=True)
