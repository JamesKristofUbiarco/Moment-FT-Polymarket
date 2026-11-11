import json,os,time
from pathlib import Path
import psutil
from polymarket_mvp.download import atomic_json
from polymarket_mvp.engine import consolidate
root=Path('runs/mvp/benchmarks/2026-06-06_local_v1')
status=json.loads((root/'status.json').read_text());pid=status['pid']
# Preserve a bounded, explicitly censored observation of the original spill strategy.
while psutil.pid_exists(pid) and time.time()-status['started_utc']<500:
    time.sleep(5)
if psutil.pid_exists(pid):
    proc=psutil.Process(pid)
    if 'polymarket_mvp.runner' not in ' '.join(proc.cmdline()):raise RuntimeError('PID identity mismatch')
    atomic_json(root/'baseline_consolidation_censored.json',dict(elapsed_seconds=time.time()-status['started_utc'],
        io=proc.io_counters()._asdict(),rss=proc.memory_info().rss,reason='Global aggregate spill amplification; replaced by disjoint physical buckets'))
    proc.terminate();proc.wait(30)
    atomic_json(root/'status.json',dict(state='ingest_complete_original_consolidation_censored',pid=pid))
r=consolidate(root)
ingest=json.loads((root/'ingest_report.json').read_text());download=json.loads(Path('runs/mvp/staging/2026-06-06.download.json').read_text())
atomic_json(root/'benchmark_optimized_consolidation.json',dict(ingest=ingest,consolidation=r,download=download,
    complete=ingest['complete'],pipeline_seconds=ingest['seconds']+r['seconds']+download['seconds'],
    note='Pipeline phase sum; censored failed consolidation separately retained'))
print(json.dumps(r),flush=True)
