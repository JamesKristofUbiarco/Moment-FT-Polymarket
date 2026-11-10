"""Full execution behind measured performance and storage gates."""
import argparse
import json
import os
import shutil
import time
from pathlib import Path
from .download import inventory,download,atomic_json
from .engine import process,consolidate,verify_series
from .storage import Manifest
from .windows import build_census,select_training,pack_windows
from .model import train,evaluate,verify_adapter


def verify_full(root):
    root=Path(root);inv=inventory(root);coverage=[]
    for entry in inv['files']:
        day=entry['path'].split('date=')[1][:10];out=root/'full'/day
        p=out/'ingest_report.json'
        if not p.exists():continue
        report=json.loads(p.read_text())
        identity=json.loads((out/'source.json').read_text())
        from . import REVISION
        if identity['revision']!=REVISION or identity['source']!=entry:raise ValueError('Coverage source identity mismatch')
        if (root/'inventory_enriched.json').exists():
            expected={x['path']:x['rows'] for x in json.loads((root/'inventory_enriched.json').read_text())['files']}
            if report['declared_rows']!=expected[entry['path']]:raise ValueError('Declared original row count mismatch')
        m=Manifest(out/'parts')
        m.verify()
        commits=[json.loads(r[0]) for r in m.db.execute('SELECT metadata FROM commits')]
        rows=sum(c['rows'] for c in commits)
        groups=sum(c['last_group_exclusive']-c['first_group'] for c in commits)
        if report['complete'] and rows==report['declared_rows'] and groups==report['row_groups']:
            coverage.append(dict(path=entry['path'],bytes=entry['bytes'],rows=rows,row_groups=groups,
                invalid_or_missing_key=sum(c['missing_key'] for c in commits)))
        m.db.close()
    r=dict(files_confirmed=len(coverage),files_expected=164,bytes_confirmed=sum(x['bytes'] for x in coverage),
        rows=sum(x['rows'] for x in coverage),coverage=coverage,data_complete=len(coverage)==164)
    atomic_json(root/'coverage.json',r);return r


def execute(root,projection_path):
    root=Path(root);projection=json.loads(Path(projection_path).read_text())
    if not projection.get('performance_gate') or projection.get('full_file_benchmarks',0)<3:
        raise RuntimeError('FULL blocked: complete representative benchmarks and <=48h projection required')
    if not projection.get('storage_gate',False):raise RuntimeError('FULL blocked: storage budget not verified')
    if projection.get('approved_hours',48)>48:raise RuntimeError('An over-budget run requires explicit updated authorization')
    inv=inventory(root);mission=root/'mission.json'
    if not mission.exists():atomic_json(mission,dict(started_utc=(root/'inventory.json').stat().st_mtime,scope='from first instrumented inventory'))
    started=json.loads(mission.read_text())['started_utc'];series=[];deadline=started+48*3600
    atomic_json(root/'full_status.json',dict(state='ingestion',pid=os.getpid(),started_utc=started,projection=projection))
    from .parallel import run_partitions
    pool=run_partitions(inv['files'],root,tag='full',workers=projection.get('workers',4),consolidators=2,
        deadline=deadline,output_group='full',reclaim=True,reuse=True)
    if not pool['complete']:raise RuntimeError(f'Incomplete originals; committed results retained: {pool["errors"]}')
    for entry in inv['files']:
        day=entry['path'].split('date=')[1][:10]
        r=json.loads((root/'full'/day/'consolidated.json').read_text());verify_series(r);series.extend(r['outputs'])
    coverage=verify_full(root)
    if not coverage['data_complete']:raise RuntimeError('Full input coverage not verified')
    from .catalog import build_catalog
    catalog=build_catalog([root/'full'/e['path'].split('date=')[1][:10] for e in inv['files']],root/'catalog')
    atomic_json(root/'full_status.json',dict(state='census',pid=os.getpid(),started_utc=started))
    certificate=root/'canonical_day_certificate.json'
    canonical_unique=certificate.exists() and json.loads(certificate.read_text()).get('disjoint_utc_day_timestamp_ranges',False)
    census=build_census(series,root/'full_census',deadline=deadline,canonical_unique=canonical_unique)
    prototype=root/'integration_complete_day_v2'
    val_bench=json.loads((prototype/'evaluation_validation/evaluation.json').read_text())
    test_bench=json.loads((prototype/'evaluation_test/evaluation.json').read_text())
    eval_rate=.8*min(val_bench['windows']/val_bench['seconds'],test_bench['windows']/test_bench['seconds'])
    evaluation_seconds=(census['split_counts']['validation']+census['split_counts']['test'])/eval_rate
    remaining=max(0,48*3600-(time.time()-started)-evaluation_seconds-projection['export_seconds'])/1.2
    atomic_json(root/'full_status.json',dict(state='selection_and_packing',pid=os.getpid(),evaluation_seconds=evaluation_seconds,started_utc=started))
    budget=int(remaining*projection['training_windows_per_second'])
    # A conflicting minimum blocks training and records its exact size; no silent downsampling.
    selection=select_training(root/'full_census',budget)
    for split in ['train','validation','test']:
        pack_windows(root/'full_census',root/'full_packs'/split,split,selected=split=='train')
    import torch
    if not torch.cuda.is_available():
        atomic_json(root/'decision_required.json',dict(state='gpu_unavailable_before_training'))
        raise RuntimeError('Measured GPU throughput requires an available CUDA device')
    atomic_json(root/'full_status.json',dict(state='training',pid=os.getpid(),started_utc=started))
    model,trained=train(root/'full_packs/train',root/'full_model',microbatch=projection['microbatch'],
                        resume=(root/'full_model/checkpoint.json').exists(),deadline=deadline)
    if not trained['complete']:
        atomic_json(root/'decision_required.json',dict(state='training_time_budget_reached',training=trained))
        raise TimeoutError('Training budget reached; model checkpoint retained')
    adapter_check=verify_adapter(root/'full_model')
    atomic_json(root/'full_status.json',dict(state='evaluation',pid=os.getpid(),started_utc=started))
    validation=evaluate(root/'full_packs/validation',model,root/'full_evaluation/validation',deadline=deadline)
    test=evaluate(root/'full_packs/test',model,root/'full_evaluation/test',calibration=validation['calibration'],deadline=deadline)
    if not trained['complete']:raise RuntimeError('Training incomplete')
    atomic_json(root/'full_result.json',dict(complete=True,coverage=coverage,catalog=catalog,census=census,selection=selection,
                training=trained,adapter_verification=adapter_check,validation=validation,test=test,elapsed_seconds=time.time()-started))
    atomic_json(root/'full_status.json',dict(state='complete',pid=os.getpid(),finished_utc=time.time()))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',default='runs/mvp');p.add_argument('--projection',required=True)
    a=p.parse_args()
    try:execute(a.root,a.projection)
    except BaseException as error:
        atomic_json(Path(a.root)/'full_status.json',dict(state='budget_stopped' if isinstance(error,TimeoutError) else 'failed',pid=os.getpid(),error=repr(error)))
        raise
