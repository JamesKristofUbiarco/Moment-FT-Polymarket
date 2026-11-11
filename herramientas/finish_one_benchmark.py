import json,sys,time,os
from pathlib import Path
from polymarket_mvp.engine import consolidate
from polymarket_mvp.download import atomic_json
out=Path(sys.argv[1]);root=Path('runs/mvp')
try:
    r=consolidate(out);ingest=json.loads((out/'ingest_report.json').read_text());day=ingest['source'].split('date=')[1][:10]
    download=json.loads((root/f'staging/{day}.download.json').read_text())
    atomic_json(out/'benchmark.json',dict(complete=True,source_bytes=download['original_bytes'],ingest=ingest,download=download,
        consolidation=r,wall_seconds=ingest['seconds']+r['seconds'],timing_scope='sum of isolated ingest and consolidation phase times'))
    atomic_json(out/'status.json',dict(state='complete',pid=os.getpid(),finished_utc=time.time()))
except BaseException as e:
    atomic_json(out/'status.json',dict(state='failed',pid=os.getpid(),error=repr(e)));raise
