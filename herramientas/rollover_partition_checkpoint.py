import json,time
from pathlib import Path
import psutil
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/benchmarks/2026-02-26_local_v5')
pid=json.loads((root/'status.json').read_text())['pid']
while not (root/'partitioned.json').exists():
    if not psutil.pid_exists(pid):raise RuntimeError('Worker ended before partition checkpoint')
    time.sleep(2)
p=psutil.Process(pid)
if 'finish_representatives.py' not in ' '.join(p.cmdline()):raise RuntimeError('PID mismatch')
atomic_json(root/'partition_worker_rollover.json',dict(pid=pid,rss=p.memory_info().rss,
    io=p.io_counters()._asdict(),reason='Release retained Arrow writer metadata at a completed derived-stage checkpoint'))
p.terminate()
try:p.wait(5)
except psutil.TimeoutExpired:
    if p.status()!=psutil.STATUS_ZOMBIE:p.kill()
