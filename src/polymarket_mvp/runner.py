"""Persistent benchmark runner; FULL cannot bypass measured feasibility gates."""
import argparse
import json
import os
import time
from pathlib import Path
from .download import inventory,download,atomic_json
from .engine import process,consolidate


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',default='runs/mvp');p.add_argument('--date',required=True)
    p.add_argument('--mode',choices=['local','remote'],default='local');p.add_argument('--batch-size',type=int,default=100288)
    p.add_argument('--groups-per-unit',type=int,default=64);p.add_argument('--tag',default='v1');p.add_argument('--stream-groups',action='store_true');p.add_argument('--dictionary-read',action='store_true');p.add_argument('--sparse-merge',action='store_true')
    a=p.parse_args();root=Path(a.root);out=root/'benchmarks'/f'{a.date}_{a.mode}_{a.tag}'
    out.mkdir(parents=True,exist_ok=True)
    import fcntl
    lock=(out/'run.lock').open('a');fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
    if (out/'benchmark.json').exists() and json.loads((out/'benchmark.json').read_text()).get('complete'):
        from .engine import verify_series
        cached=json.loads((out/'benchmark.json').read_text());verify_series(cached['consolidation'])
        print('Verified complete:',out,flush=True);return
    t=time.perf_counter()
    atomic_json(out/'status.json',dict(state='running',pid=os.getpid(),started_utc=time.time(),arguments=vars(a)))
    try:
        inv=inventory(root);entry=next(x for x in inv['files'] if a.date in x['path'])
        transfer=None;source=None
        if a.mode=='local':source,transfer=download(entry,root/'staging')
        report=process(entry,source,out,mode=a.mode,batch_size=a.batch_size,groups_per_unit=a.groups_per_unit,stream_groups=a.stream_groups,dictionary_read=a.dictionary_read)
        merged=consolidate(out,sparse_merge=a.sparse_merge)
        atomic_json(out/'benchmark.json',dict(ingest=report,download=transfer,consolidation=merged,
            wall_seconds=time.perf_counter()-t,source_bytes=entry['bytes'],complete=report['complete']))
        atomic_json(out/'status.json',dict(state='complete',pid=os.getpid(),finished_utc=time.time()))
    except BaseException as e:
        atomic_json(out/'status.json',dict(state='failed',pid=os.getpid(),error=repr(e),utc=time.time()))
        raise

if __name__=='__main__':main()
