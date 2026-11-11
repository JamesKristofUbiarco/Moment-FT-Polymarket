"""Disprove quickly or certify completely; never infer ordering from a sample."""
import json,time
from pathlib import Path
import polars as pl
import pyarrow.parquet as pq
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/order_certificates');root.mkdir(exist_ok=True)
for day in ['2026-06-06','2026-02-26','2026-04-26','2026-06-09']:
    p=pq.ParquetFile(f'runs/mvp/staging/{day}.parquet');start=time.perf_counter();rows=0
    candidates={'condition_second':True,'condition_asset_second':True,'asset_second':True,'second':True};last={k:None for k in candidates}
    with (root/f'{day}_original_reads.jsonl').open('a') as log:
        for g in range(p.num_row_groups):
            for b in p.iter_batches(row_groups=[g],columns=['condition_id','asset_id','event_ts_ms'],batch_size=262144):
                d=pl.from_arrow(b).with_columns((pl.col('event_ts_ms')//1000).alias('second'));rows+=b.num_rows
                for name,cols in [('condition_second',['condition_id','second']),('condition_asset_second',['condition_id','asset_id','second']),('asset_second',['asset_id','second']),('second',['second'])]:
                    if not candidates[name]:continue
                    keys=d.select(pl.struct(cols).alias('key')).to_series()
                    if any(d[c].null_count() for c in cols):candidates[name]=False;continue
                    first=tuple(keys[0][c] for c in cols);tail=tuple(keys[-1][c] for c in cols)
                    candidates[name]=keys.is_sorted() and (last[name] is None or last[name]<=first)
                    last[name]=tail
            log.write(json.dumps(dict(group=g,rows=b.num_rows,decoder_passes=1,purpose='ordering certificate'))+'\n')
            if not any(candidates.values()):break
    r=dict(day=day,rows_checked=rows,total_rows=p.metadata.num_rows,complete=rows==p.metadata.num_rows,candidates=candidates,seconds=time.perf_counter()-start)
    atomic_json(root/f'{day}.json',r);print(r,flush=True)
