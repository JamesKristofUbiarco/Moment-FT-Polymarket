import json,shutil,time
from pathlib import Path
import psutil
from polymarket_mvp.download import atomic_json
base=Path('runs/mvp/benchmarks').resolve()
targets=[base/'2026-06-06_local_v1/spill',base/'2026-06-06_local_v2/spill_partitioned',
         base/'2026-06-06_consolidation_v3/spill_partitioned',base/'2026-06-06_consolidation_v4/spill_partitioned',
         base/'2026-02-26_local_v2/spill_partitioned']
opened=set()
for p in psutil.process_iter():
    try:
        if str(Path.cwd())==p.cwd():opened.update(Path(x.path).resolve() for x in p.open_files())
    except (psutil.AccessDenied,psutil.NoSuchProcess,FileNotFoundError):pass
for target in targets:
    if not target.exists():continue
    if target.is_symlink() or not target.resolve().is_relative_to(base):raise ValueError('Unowned cleanup path')
    if any(p.is_relative_to(target) for p in opened):raise RuntimeError(f'Spill still open: {target}')
    files=[dict(path=str(p.relative_to(target)),bytes=p.stat().st_size) for p in target.rglob('*') if p.is_file()]
    atomic_json(target.parent/'reclaimed_spill.json',dict(utc=time.time(),files=files,bytes=sum(x['bytes'] for x in files),
        reason='Disposable failed-engine scratch only; originals, feature parts and benchmark reports preserved'))
    shutil.rmtree(target)
    print('RECLAIMED',target,sum(x['bytes'] for x in files),flush=True)
