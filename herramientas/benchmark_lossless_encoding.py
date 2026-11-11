import json,time
from pathlib import Path
import pyarrow as pa,pyarrow.parquet as pq
from polymarket_mvp.columnar import options
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');source=root/'benchmarks/2026-06-06_local_v2';out=root/'lossless_encoding_complete_day';out.mkdir(exist_ok=True)
r=json.loads((source/'consolidated.json').read_text());started=time.perf_counter();before=0;after=0;rows=0
for i,file in enumerate(r['outputs']):
    p=pq.ParquetFile(file);writer=None;target=out/f'part_{i:04d}.parquet';before+=Path(file).stat().st_size
    for batch in p.iter_batches(batch_size=65536):
        table=pa.Table.from_batches([batch])
        fields=[];arrays=[]
        for field,col in zip(table.schema,table.columns):
            if field.name in ['events','quotes'] or field.name.startswith(('n_','updates_')):
                col=col.cast(pa.uint64(),safe=True);field=pa.field(field.name,pa.uint64())
            fields.append(field);arrays.append(col)
        table=pa.Table.from_arrays(arrays,schema=pa.schema(fields))
        if writer is None:writer=pq.ParquetWriter(target,table.schema,**options(table.schema))
        writer.write_table(table);rows+=table.num_rows
    if writer:writer.close()
    after+=target.stat().st_size
atomic_json(out/'benchmark.json',dict(complete=True,rows=rows,input_bytes=before,output_bytes=after,
    ratio=after/before,seconds=time.perf_counter()-started,encoding='lossless delta integer positions/timestamps; exact uint64 counters',
    source_feature_passes=1))
print(json.loads((out/'benchmark.json').read_text()),flush=True)
