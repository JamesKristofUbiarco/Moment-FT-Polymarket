import json,os,time
from pathlib import Path
import psutil
root=Path('runs/mvp');states=[]
for p in sorted(root.rglob('status.json')):
    r=json.loads(p.read_text());pid=r.get('pid');alive=False
    if pid and psutil.pid_exists(pid):
        try:alive=psutil.Process(pid).status()!=psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:pass
    r.update(file=str(p),pid_alive=alive);states.append(r)
print(json.dumps(dict(utc=time.time(),memory=psutil.virtual_memory()._asdict(),swap=psutil.swap_memory()._asdict(),states=states),indent=2))
