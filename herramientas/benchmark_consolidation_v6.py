import json,os
from pathlib import Path
from polymarket_mvp.engine import consolidate
from polymarket_mvp.download import atomic_json
source=Path('runs/mvp/benchmarks/2026-06-06_local_v2').resolve()
dest=Path('runs/mvp/benchmarks/2026-06-06_consolidation_v6').resolve();dest.mkdir(exist_ok=True)
if not (dest/'parts').exists():(dest/'parts').symlink_to(source/'parts',target_is_directory=True)
r=consolidate(dest,buckets=32)
original=json.loads((source/'benchmark.json').read_text())
atomic_json(dest/'benchmark.json',dict(complete=True,source_benchmark=str(source),consolidation=r,
    original_source_reads=0,original_ingest_seconds=original['ingest']['seconds'],
    pipeline_phase_seconds=original['ingest']['seconds']+r['seconds']+original['download']['seconds']))
print(json.dumps({k:v for k,v in r.items() if k not in ['sha256','outputs']}),flush=True)
