import time,json,resource,os
from pathlib import Path
import pyarrow.parquet as pq
from polymarket_mvp.features import COLUMNS
from polymarket_mvp.download import atomic_json
class Metered:
    def __init__(self,f):self.f=f;self.bytes=0;self.read_seconds=0.;self.read_calls=0
    def __getattr__(self,k):return getattr(self.f,k)
    def read(self,n=-1):
        t=time.perf_counter();b=self.f.read(n);self.read_seconds+=time.perf_counter()-t;self.bytes+=len(b);self.read_calls+=1;return b
    def readinto(self,b):
        t=time.perf_counter();n=self.f.readinto(b);self.read_seconds+=time.perf_counter()-t;self.bytes+=n;self.read_calls+=1;return n
root=Path('runs/mvp/decode_complete_day');root.mkdir(exist_ok=True)
f=Metered(open('runs/mvp/staging/2026-06-06.parquet','rb'));start=time.perf_counter();cpu=time.process_time();p=pq.ParquetFile(f);rows=0
with (root/'original_reads.jsonl').open('a') as log:
    for g in range(p.num_row_groups):
        n=0
        for batch in p.iter_batches(row_groups=[g],columns=COLUMNS,batch_size=262144,use_threads=False):n+=batch.num_rows
        if n!=p.metadata.row_group(g).num_rows:raise ValueError('Incomplete group')
        rows+=n;log.write(json.dumps(dict(group=g,rows=n,decoder_passes=1,purpose='isolated decode measurement'))+'\n')
wall=time.perf_counter()-start;cpu=time.process_time()-cpu;f.close()
atomic_json(root/'benchmark.json',dict(complete=rows==p.metadata.num_rows,rows=rows,row_groups=p.num_row_groups,
 wall_seconds=wall,cpu_seconds=cpu,source_read_wall_seconds=f.read_seconds,logical_local_bytes=f.bytes,
 decode_and_arrow_construction_residual_seconds=wall-f.read_seconds,read_calls=f.read_calls,
 peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
 limitation='Single-thread timing separates Python file reads from decode/allocation; residual is not pure codec time'))
print(json.loads((root/'benchmark.json').read_text()),flush=True)
