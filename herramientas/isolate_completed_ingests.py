import json,time,os,subprocess,sys
from pathlib import Path
import psutil
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');done=set()
while len(done)<2:
    for day in ['2026-04-26','2026-06-09']:
        if day in done:continue
        out=root/'benchmarks'/f'{day}_local_v5'
        if not (out/'ingest_report.json').exists():continue
        status=json.loads((out/'status.json').read_text());pid=status['pid']
        if (out/'benchmark.json').exists():done.add(day);continue
        if psutil.pid_exists(pid):
            p=psutil.Process(pid)
            if 'polymarket_mvp.runner' not in ' '.join(p.cmdline()):raise RuntimeError('PID identity mismatch')
            atomic_json(out/'ingest_worker_rollover.json',dict(pid=pid,rss=p.memory_info().rss,io=p.io_counters()._asdict(),reason='Release ingest allocator; use bounded, rotating bucket writers'))
            p.terminate()
            try:p.wait(5)
            except psutil.TimeoutExpired:
                if p.status()!=psutil.STATUS_ZOMBIE:p.kill()
        log=(out/'isolated_consolidation.log').open('a')
        process=subprocess.Popen([sys.executable,'-u','herramientas/finish_one_benchmark.py',str(out)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        atomic_json(out/'status.json',dict(state='consolidation',pid=process.pid))
        done.add(day)
    time.sleep(3)
