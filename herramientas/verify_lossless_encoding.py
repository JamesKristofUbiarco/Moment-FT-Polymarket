import json
from pathlib import Path
import pyarrow as pa,pyarrow.parquet as pq
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');data=root/'lossless_encoding_complete_day'
source=json.loads((root/'benchmarks/2026-06-06_local_v2/consolidated.json').read_text())['outputs'];rows=0
for i,path in enumerate(source):
    a=pq.ParquetFile(path).iter_batches(batch_size=65536);b=pq.ParquetFile(data/f'part_{i:04d}.parquet').iter_batches(batch_size=65536)
    for x,y in zip(a,b,strict=True):
        old=pa.Table.from_batches([x]);new=pa.Table.from_batches([y])
        old=old.cast(new.schema,safe=True)
        if not old.equals(new):raise ValueError(f'Lossless encoding mismatch at file {i}')
        rows+=old.num_rows
atomic_json(data/'verification.json',dict(passed=True,all_rows_compared=rows,all_columns_compared=True))
print('LOSSLESS_VERIFIED',rows,flush=True)
