"""Append-only resource evidence for owned project workers; no environment secrets."""
import json,os,time,subprocess
from pathlib import Path
import psutil
root=Path('runs/mvp');project=Path.cwd().resolve();log=root/'resources.jsonl'
while True:
    workers=[]
    for p in psutil.process_iter(['pid','cmdline','memory_info','cpu_times']):
        try:
            if p.pid==os.getpid() or Path(p.cwd()).resolve()!=project:continue
            cmd=' '.join(p.info['cmdline'] or [])
            if not any(x in cmd for x in ['polymarket_mvp.runner','polymarket_mvp.worker','herramientas/finish_one_benchmark.py','herramientas/finish_representatives.py','herramientas/benchmark_','herramientas/pilot_','herramientas/streamed_trial.py']):continue
            workers.append(dict(pid=p.pid,args=p.info['cmdline'],rss=p.info['memory_info'].rss,
                cpu_seconds=sum(p.info['cpu_times'][:2]),io=p.io_counters()._asdict(),status=p.status()))
        except (psutil.NoSuchProcess,psutil.AccessDenied,FileNotFoundError):continue
    record=dict(utc=time.time(),workers=workers,system_memory=psutil.virtual_memory()._asdict(),
                system_swap=psutil.swap_memory()._asdict(),system_disk_shared=psutil.disk_io_counters()._asdict())
    with log.open('a') as f:f.write(json.dumps(record)+'\n')
    time.sleep(5)
