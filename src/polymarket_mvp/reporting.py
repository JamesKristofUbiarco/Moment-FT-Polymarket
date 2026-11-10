"""Coverage and measured cost reports; estimates remain explicitly labelled."""
import json
import time
from pathlib import Path
from .download import inventory,atomic_json


def progress(root):
    root=Path(root);result=[]
    for path in sorted((root/'benchmarks').glob('*/status.json')):
        r=json.loads(path.read_text());r['run']=path.parent.name
        import psutil
        pid=r.get('pid');r['pid_alive']=bool(pid and psutil.pid_exists(pid))
        if r['pid_alive']:
            try:r['pid_alive']=psutil.Process(pid).status()!=psutil.STATUS_ZOMBIE
            except psutil.NoSuchProcess:r['pid_alive']=False
        if not r['pid_alive'] and r.get('state') in ['running','consolidation']:r['observed_state']='stale_process_not_running'
        p=path.parent/'progress.json'
        if p.exists():r['progress']=json.loads(p.read_text())
        result.append(r)
    return result


def projection(root):
    root=Path(root);inv=inventory(root);by_date={}
    for path in (root/'benchmarks').glob('*/benchmark.json'):
        r=json.loads(path.read_text())
        if not r.get('complete') or 'ingest' not in r:continue
        if r['ingest']['mode']!='local':continue
        date=r['ingest']['source'].split('date=')[1][:10]
        # Latest benchmark for each date; all alternatives remain stored.
        if date not in by_date or path.stat().st_mtime>by_date[date][0]:by_date[date]=(path.stat().st_mtime,r)
    required=['2026-02-26','2026-04-26','2026-06-09'];missing=[d for d in required if d not in by_date]
    if missing:
        r=dict(performance_gate=False,storage_gate=False,full_file_benchmarks=len([d for d in required if d in by_date]),
               missing_complete_benchmarks=missing,state='benchmarking',estimated_not_executed=True)
        atomic_json(root/'projection.json',r);return r
    reps=[by_date[d][1] for d in required]
    v1=reps[0];v2=reps[1:]
    def cost(r):return (r['ingest']['seconds']+r['consolidation']['seconds'])/r['source_bytes']
    # Conservative within-era scaling by original compressed bytes; source size is measured exactly.
    cpu=sum(e['bytes']*(cost(v1) if e['path'].split('date=')[1][:10]<'2026-04-13' else max(map(cost,v2))) for e in inv['files'])
    transfer=[r['download'] for r in reps if r['download'] and r['download'].get('workers')==4 and not r['download'].get('recovered_prefix_bytes',0)]
    if not transfer:transfer=[r['download'] for r in reps if r['download']]
    net_rate=min(r['original_bytes']/r['network_seconds'] for r in transfer)
    network=sum(e['bytes'] for e in inv['files'])/net_rate
    experiment=root/'integration_complete_day_v2';census=json.loads((experiment/'census/census.json').read_text())
    new_census=root/'census_vectorized_complete_day/census.json'
    original_census=json.loads((new_census if new_census.exists() else root/'integration_complete_day/census/census.json').read_text())
    gpu=json.loads((experiment/'gpu_benchmarks.json').read_text());best=max(gpu,key=lambda x:x['windows_per_second'])
    day=by_date['2026-06-06'][1];scale=sum(e['bytes'] for e in inv['files'])/day['source_bytes']
    census_seconds=original_census['seconds']*scale
    eligible=int(census['eligible']*scale);training=int(eligible*.7)
    training_seconds=training/best['windows_per_second']
    evaluation_windows=int(eligible*.3)
    val=json.loads((experiment/'evaluation_validation/evaluation.json').read_text())
    test=json.loads((experiment/'evaluation_test/evaluation.json').read_text())
    evaluation_seconds=evaluation_windows*(val['seconds']+test['seconds'])/(val['windows']+test['windows'])
    export_seconds=.1*(census_seconds+training_seconds+evaluation_seconds)
    elapsed=time.time()-(root/'inventory.json').stat().st_mtime
    # Sequential conservative budget; no unmeasured overlap or parallel speedup is assumed.
    total=elapsed+1.15*cpu+network+census_seconds+training_seconds+evaluation_seconds+export_seconds
    final_bytes=sum(e['bytes']*max(r['consolidation']['output_bytes']/r['source_bytes'] for r in reps) for e in inv['files'])
    import shutil
    storage=final_bytes+max(r['source_bytes'] for r in reps)*5+100*1024**3
    r=dict(estimated_not_executed=True,performance_gate=total<=48*3600,storage_gate=storage<=shutil.disk_usage(root).free,
        full_file_benchmarks=3,approved_hours=48,projected_hours=total/3600,processing_seconds=cpu,processing_verification_reserve_seconds=.15*cpu,network_seconds=network,
        census_seconds=census_seconds,training_seconds=training_seconds,evaluation_seconds=evaluation_seconds,
        export_seconds=export_seconds,elapsed_seconds=elapsed,estimated_eligible_windows=eligible,
        estimated_training_windows=training,training_windows_per_second=best['windows_per_second'],
        microbatch=best['identity']['microbatch'],estimated_final_bytes=final_bytes,estimated_storage_requirement=storage,
        assumptions=['Conservative per-era scaling by compressed original bytes','No unmeasured concurrency speedup',
          'Census and eligible-window scaling from one complete sparse day: high uncertainty',
          'Minimum training cardinality must be recalculated from the complete census before training'])
    atomic_json(root/'projection.json',r);return r
