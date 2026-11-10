import time,json
from pathlib import Path
import polars as pl,pyarrow as pa,pyarrow.parquet as pq
from polars.testing import assert_frame_equal
from polymarket_mvp.features import partial,KEYS,COLUMNS
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/dictionary_kernel_complete_day');root.mkdir(exist_ok=True)
p=pq.ParquetFile('runs/mvp/staging/2026-06-06.parquet',read_dictionary=['asset_id','condition_id','event_type','source_archive','side'])
plain_schema=pa.schema([pa.field(f.name,f.type.value_type if pa.types.is_dictionary(f.type) else f.type) for f in p.schema_arrow if f.name in COLUMNS])
times={'plain':0.,'dictionary':0.};rows=0;start=time.perf_counter()
def normalized(d):return d.with_columns(pl.col('asset_id').cast(pl.String),pl.col('condition_id').cast(pl.String)).sort(KEYS)
with (root/'original_reads.jsonl').open('a') as log:
    for g in range(p.num_row_groups):
        for b in p.iter_batches(row_groups=[g],columns=COLUMNS,batch_size=262144):
            table=pa.Table.from_batches([b]);fields=[pa.field(f.name,f.type.value_type if pa.types.is_dictionary(f.type) else f.type) for f in table.schema]
            plain=table.cast(pa.schema(fields)).to_batches()[0]
            inputs={'plain':plain,'dictionary':b};results={}
            for name in (['plain','dictionary'] if g%2==0 else ['dictionary','plain']):
                t=time.perf_counter();results[name]=partial(inputs[name],104,rows);times[name]+=time.perf_counter()-t
            assert_frame_equal(normalized(results['plain'][0]),normalized(results['dictionary'][0]),check_column_order=False,rel_tol=1e-12,abs_tol=1e-12)
            for key in ['rows','missing_key','quality','types']:
                if results['plain'][2][key]!=results['dictionary'][2][key]:raise ValueError(f'Audit mismatch {key}')
            rows+=b.num_rows
        log.write(json.dumps(dict(group=g,decoder_passes=1,purpose='dictionary versus plain kernel equivalence'))+'\n')
    r=dict(complete=rows==p.metadata.num_rows,rows=rows,wall_seconds=time.perf_counter()-start,kernel_seconds=times,
        all_aggregate_fields_compared=True,all_audit_counts_compared=True,alternating_execution_order=True)
    atomic_json(root/'benchmark.json',r);print(r,flush=True)
