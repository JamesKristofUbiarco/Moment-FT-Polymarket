import json,time,os,shutil
from pathlib import Path
import psutil
from polymarket_mvp.engine import consolidate
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');old=root/'benchmarks/2026-02-26_local_v2'
while not (old/'ingest_report.json').exists():time.sleep(5)
status=json.loads((old/'status.json').read_text());pid=status['pid']
if psutil.pid_exists(pid):
    p=psutil.Process(pid)
    if 'polymarket_mvp.runner' not in ' '.join(p.cmdline()):raise RuntimeError('PID mismatch')
    atomic_json(old/'consolidation_censored.json',dict(seconds=time.time()-p.create_time(),io=p.io_counters()._asdict(),
        reason='Replace old COPY partitioning with measured streaming Arrow alternative'))
    p.terminate()
    try:p.wait(5)
    except psutil.TimeoutExpired:
        if p.status()!=psutil.STATUS_ZOMBIE:p.kill()
    atomic_json(old/'status.json',dict(state='ingest_complete_consolidation_censored',pid=pid))
new=root/'benchmarks/2026-02-26_local_v5';new.mkdir(exist_ok=True)
if not (new/'parts').exists():(new/'parts').symlink_to((old/'parts').resolve(),target_is_directory=True)
for name in ['source.json','ingest_report.json','parquet.json','progress.json','original_reads.jsonl']:
    if not (new/name).exists():shutil.copyfile(old/name,new/name)
atomic_json(new/'status.json',dict(state='consolidation',pid=os.getpid()))
t=time.perf_counter();r=consolidate(new)
ingest=json.loads((old/'ingest_report.json').read_text());download=json.loads((root/'staging/2026-02-26.download.json').read_text())
atomic_json(new/'benchmark.json',dict(complete=True,source_bytes=download['original_bytes'],ingest=ingest,download=download,
    consolidation=r,wall_seconds=ingest['seconds']+r['seconds'],timing_scope='sum of selected strategy phases; censored earlier attempt retained separately'))
atomic_json(new/'status.json',dict(state='complete',pid=os.getpid()))
print('V1_COMPLETE',r['events'],r['seconds'],flush=True)
