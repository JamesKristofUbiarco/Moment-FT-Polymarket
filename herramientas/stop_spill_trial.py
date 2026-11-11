import psutil,time
from pathlib import Path
from polymarket_mvp.download import atomic_json
for p in psutil.process_iter(['pid','cmdline']):
    cmd=' '.join(p.info['cmdline'] or [])
    if cmd.endswith('python -u herramientas/benchmark_consolidation_v4.py'):
        atomic_json(Path('runs/mvp/benchmarks/2026-06-06_consolidation_v4/censored.json'),
            dict(seconds=time.time()-p.create_time(),io=p.io_counters()._asdict(),reason='Partitioned COPY buffering still amplifies IO; replacing with streaming Arrow writers'))
        p.terminate()
        try:p.wait(5)
        except psutil.TimeoutExpired:p.kill()
