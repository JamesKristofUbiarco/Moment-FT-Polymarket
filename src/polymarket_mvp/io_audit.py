"""Summarize actual completed decoder passes, keeping unknown measurements explicit."""
import json
from collections import Counter
from pathlib import Path
from .download import atomic_json


def audit(root):
    root=Path(root);completed=Counter();requested=Counter();runs=[];missing=[]
    for folder in sorted((root/'benchmarks').iterdir()):
        identity=folder/'source.json';log=folder/'original_reads.jsonl'
        if not identity.exists() or not log.exists():continue
        source=json.loads(identity.read_text())['source']['path'];local=Counter();seen_attempts=set()
        for line in log.open():
            r=json.loads(line)
            if r.get('operation')=='reader_requested':
                for group in r['groups']:requested[(source,group)]+=1
            elif 'group' in r and 'decoder_passes' in r:
                local[r['group']]+=r['decoder_passes'];seen_attempts.add(r.get('attempt'))
        for group,count in local.items():completed[(source,group)]+=count
        runs.append(dict(run=folder.name,source=source,groups_seen=len(local),completed_decoder_visits=sum(local.values()),
            max_passes_per_group=max(local.values(),default=0),attempts=len(seen_attempts)))
    # Cloned consolidation benchmarks may carry a copied read log for lineage; do
    # not sum these as fresh physical input work. Their source reference is retained.
    unique=[]
    for r in runs:
        if 'consolidation_' not in r['run'] and not (r['run']=='2026-02-26_local_v5'):unique.append(r)
    downloads=[]
    for p in (root/'staging').glob('*.download.json'):downloads.append(json.loads(p.read_text()))
    remote=[]
    for p in (root/'benchmarks').glob('*/benchmark.json'):
        r=json.loads(p.read_text());ingest=r.get('ingest',{})
        if ingest.get('mode')=='remote':
            if ingest.get('range_payload_bytes') is None:missing.append(str(p))
            else:remote.append(dict(run=p.parent.name,bytes=ingest['range_payload_bytes'],requests=ingest['range_requests']))
    result=dict(runs=unique,decoder_visits_logged=sum(r['completed_decoder_visits'] for r in unique),
        original_download_payload_bytes=sum(r['payload_bytes'] for r in downloads),remote_measured=remote,
        unknown_legacy_transfer_measurements=missing,
        caveats=['Original decoder passes are distinct from physical disk reads served by cache',
                 'Standalone decode and previous-architecture benchmarks have separate ledgers',
                 'HTTP counters measure response payload, excluding protocol overhead'])
    atomic_json(root/'io_audit.json',result);return result
