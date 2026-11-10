import json,time,os
from pathlib import Path
from polymarket_mvp.reporting import projection
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp')
atomic_json(root/'gate_watcher.json',dict(state='waiting_complete_benchmarks',pid=os.getpid()))
while True:
    import importlib,polymarket_mvp.reporting as reporting
    importlib.reload(reporting)
    r=reporting.projection(root)
    preferred=root/'benchmarks/2026-02-26_local_optimized_v9'
    if r.get('full_file_benchmarks',0)>=3 and (preferred/'benchmark.json').exists():break
    time.sleep(15)
print(json.dumps(r,indent=2),flush=True)
verification=root/'implementation_verification.json'
verified=verification.exists() and json.loads(verification.read_text()).get('passed',False)
if r['performance_gate'] and r['storage_gate'] and verified:
    atomic_json(root/'gate_watcher.json',dict(state='starting_authorized_full',pid=os.getpid()))
    from polymarket_mvp.full import execute
    execute(root,root/'projection.json')
else:
    atomic_json(root/'decision_required.json',dict(state='full_not_started_budget_gate',projection=r,
        reason='Measured conservative cost exceeds approved time or available storage; no multi-day run started'))
    atomic_json(root/'gate_watcher.json',dict(state='budget_decision_required',pid=os.getpid()))
