import json,os,shutil
from pathlib import Path
from polymarket_mvp.engine import consolidate
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/benchmarks');source=root/'2026-06-06_local_v7';out=root/'2026-06-06_consolidation_sparse';out.mkdir(exist_ok=True)
for name in ['parts','merge_partitioned']:
    if not (out/name).exists():(out/name).symlink_to((source/name).resolve(),target_is_directory=True)
for name in ['partitioned.json','ingest_report.json']:
    if not (out/name).exists():shutil.copyfile(source/name,out/name)
r=consolidate(out,sparse_merge=True)
atomic_json(out/'benchmark.json',dict(complete=True,consolidation=r,source_benchmark=str(source),original_input_reads=0))
print({k:v for k,v in r.items() if k not in ['sha256','outputs']},flush=True)
