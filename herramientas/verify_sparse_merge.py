import json,sys
from pathlib import Path
import numpy as np
import pyarrow as pa,pyarrow.parquet as pq
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/benchmarks')
old,new=(map(Path,sys.argv[1:]) if len(sys.argv)==3 else (root/'2026-06-06_local_v7',root/'2026-06-06_consolidation_sparse'))
rows=0;max_volume_delta=0.
a=json.loads((old/'consolidated.json').read_text());b=json.loads((new/'consolidated.json').read_text())
assert a['events']==b['events'] and a['series_rows']==b['series_rows']
for target in b['outputs']:
    target=Path(target);source=old/'series'/target.name
    schema=pq.ParquetFile(target).schema_arrow
    left=pq.ParquetFile(source).iter_batches(batch_size=65536,columns=schema.names)
    right=pq.ParquetFile(target).iter_batches(batch_size=65536)
    for x,y in zip(left,right,strict=True):
        xx=pa.Table.from_batches([x]).select(schema.names).cast(schema,safe=True);yy=pa.Table.from_batches([y])
        for name in schema.names:
            if pa.types.is_floating(schema.field(name).type):
                u=xx[name].to_numpy();v=yy[name].to_numpy()
                if name=='reported_trade_size_sum':
                    if not np.allclose(u,v,rtol=1e-12,atol=1e-12,equal_nan=True):raise ValueError(name)
                    if len(u):max_volume_delta=max(max_volume_delta,float(np.nanmax(np.abs(u-v))))
                elif not np.array_equal(u,v,equal_nan=True):raise ValueError(f'Exact field mismatch {name}')
            elif not xx[name].equals(yy[name]):raise ValueError(f'Exact field mismatch {name}')
        rows+=xx.num_rows
assert rows==b['series_rows']
atomic_json(new/'equivalence.json',dict(passed=True,all_rows_compared=rows,all_fields_compared=True,max_reported_volume_delta=max_volume_delta))
print('SPARSE_EQUIVALENCE',rows,max_volume_delta,flush=True)
