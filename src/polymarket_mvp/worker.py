"""Isolated stages release allocator and writer metadata between large files."""
import argparse,json
from pathlib import Path
from .engine import process,consolidate
from .download import atomic_json

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('stage',choices=['ingest','consolidate']);p.add_argument('--out',required=True)
    p.add_argument('--source');p.add_argument('--entry');p.add_argument('--stream-groups',action='store_true');p.add_argument('--deadline',type=float);p.add_argument('--groups-per-unit',type=int,default=16)
    p.add_argument('--dictionary-read',action='store_true');p.add_argument('--sparse-merge',action='store_true')
    a=p.parse_args();root=Path(a.out)
    if a.stage=='ingest':
        r=process(json.loads(a.entry),a.source,root,batch_size=262144,groups_per_unit=a.groups_per_unit,stream_groups=a.stream_groups,deadline=a.deadline,dictionary_read=a.dictionary_read)
    else:r=consolidate(root,deadline=a.deadline,sparse_merge=a.sparse_merge)
    atomic_json(root/(a.stage+'_worker_result.json'),r)
