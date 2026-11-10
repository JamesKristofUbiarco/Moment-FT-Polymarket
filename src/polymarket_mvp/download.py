"""Pinned inventory and resumable HTTP staging with payload byte accounting."""
import hashlib
import json
import os
import time
from pathlib import Path
import httpx
from huggingface_hub import HfApi
from huggingface_hub.hf_api import RepoFile
from . import REVISION, REPOSITORY


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def inventory(root):
    path = Path(root) / 'inventory.json'
    if path.exists():
        data = json.loads(path.read_text())
    else:
        files = [f for f in HfApi().list_repo_tree(REPOSITORY, repo_type='dataset', revision=REVISION,
                 path_in_repo='orderbook', recursive=True) if isinstance(f, RepoFile) and f.path.endswith('.parquet')]
        data = {'repository': REPOSITORY, 'revision': REVISION, 'files': [
            {'path': f.path, 'bytes': f.size, 'oid': f.blob_id,
             'sha256': getattr(f.lfs, 'sha256', None)} for f in sorted(files, key=lambda f: f.path)]}
        for i, f in enumerate(data['files']): f['ordinal'] = i
        atomic_json(path, data)
    if data['revision'] != REVISION or len(data['files']) != 164 or sum(f['bytes'] for f in data['files']) != 1177799623341:
        raise ValueError('Pinned inventory mismatch')
    return data


def download(entry, root):
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    dest = root / (entry['path'].split('date=')[1][:10] + '.parquet')
    report = dest.with_suffix('.download.json')
    if dest.exists() and report.exists():
        r = json.loads(report.read_text())
        if dest.stat().st_size == entry['bytes'] and r['verified_sha256'] == entry['sha256']:
            return dest, r
        raise ValueError('Staging identity mismatch')
    part = dest.with_suffix('.partial')
    if entry['bytes']>=1024**3:
        return parallel_download(entry,root)
    history = dest.with_suffix('.transfers.jsonl')
    start = time.perf_counter(); transferred = 0
    url = f'https://huggingface.co/datasets/{REPOSITORY}/resolve/{REVISION}/{entry["path"]}'
    for attempt in range(12):
        offset = part.stat().st_size if part.exists() else 0
        got = 0; t = time.perf_counter(); error = None
        try:
            with httpx.stream('GET', url, headers={'Range': f'bytes={offset}-', 'Accept-Encoding':'identity'},
                              follow_redirects=True, timeout=httpx.Timeout(120, connect=30)) as response:
                response.raise_for_status()
                if offset and response.status_code != 206:
                    raise ValueError('Server ignored resume range')
                with part.open('ab' if offset else 'wb') as f:
                    for chunk in response.iter_bytes(8 << 20):
                        f.write(chunk); got += len(chunk)
                    f.flush(); os.fsync(f.fileno())
        except Exception as exc:
            error = repr(exc)
        transferred += got
        with history.open('a') as f:
            f.write(json.dumps(dict(offset=offset, bytes=got, seconds=time.perf_counter()-t,
                                    error=error, attempt=attempt, utc=time.time()))+'\n')
        if error is None and part.stat().st_size == entry['bytes']: break
        if error: print('DOWNLOAD_RETRY', entry['path'], error, flush=True)
        time.sleep(min(30, attempt+1))
    else: raise RuntimeError('Download retries exhausted')
    t = time.perf_counter(); h = hashlib.sha256()
    with part.open('rb') as f:
        for block in iter(lambda: f.read(16 << 20), b''): h.update(block)
    if h.hexdigest() != entry['sha256']: raise ValueError('Original SHA256 mismatch')
    os.replace(part, dest)
    records = [json.loads(line) for line in history.read_text().splitlines()]
    r = dict(path=entry['path'], revision=REVISION, original_bytes=entry['bytes'],
             payload_bytes=sum(x['bytes'] for x in records), network_seconds=sum(x['seconds'] for x in records),
             verification_seconds=time.perf_counter()-t, seconds=time.perf_counter()-start,
             requests=len(records), verified_sha256=h.hexdigest())
    atomic_json(report, r)
    return dest, r


def parallel_download(entry, root, workers=4):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    root=Path(root);dest=root/(entry['path'].split('date=')[1][:10]+'.parquet')
    url=f'https://huggingface.co/datasets/{REPOSITORY}/resolve/{REVISION}/{entry["path"]}'
    size=entry['bytes'];prefix=dest.with_suffix('.partial');prefix_size=prefix.stat().st_size if prefix.exists() else 0
    chunk=(size-prefix_size+workers-1)//workers;lock=Lock();history=dest.with_suffix('.parallel_transfers.jsonl')
    started=time.perf_counter()
    def fetch(i):
        first=prefix_size+i*chunk;last=min(size,prefix_size+(i+1)*chunk)-1;part=dest.with_suffix(f'.range{i}')
        for attempt in range(12):
            offset=part.stat().st_size if part.exists() else 0
            if offset==last-first+1:return part
            got=0;t=time.perf_counter();error=None
            try:
                with httpx.stream('GET',url,headers={'Range':f'bytes={first+offset}-{last}','Accept-Encoding':'identity'},
                                  follow_redirects=True,timeout=httpx.Timeout(120,connect=30)) as response:
                    response.raise_for_status()
                    if response.status_code!=206 or not response.headers.get('content-range','').startswith(f'bytes {first+offset}-'):
                        raise ValueError('Invalid range response')
                    with part.open('ab' if offset else 'wb') as f:
                        for data in response.iter_bytes(4<<20):f.write(data);got+=len(data)
                        f.flush();os.fsync(f.fileno())
            except Exception as exc:error=repr(exc)
            with lock:
                with history.open('a') as f:f.write(json.dumps(dict(worker=i,offset=first+offset,bytes=got,seconds=time.perf_counter()-t,error=error))+'\n')
            if error is None and part.stat().st_size==last-first+1:return part
            time.sleep(min(30,attempt+1))
        raise RuntimeError('Range transfer exhausted retries')
    with ThreadPoolExecutor(max_workers=workers) as pool:parts=list(pool.map(fetch,range(workers)))
    network=time.perf_counter()-started;t=time.perf_counter();h=hashlib.sha256();tmp=dest.with_suffix('.assembled')
    with tmp.open('wb') as out:
        for part in ([prefix] if prefix_size else [])+parts:
            with part.open('rb') as f:
                for block in iter(lambda:f.read(16<<20),b''):out.write(block);h.update(block)
        out.flush();os.fsync(out.fileno())
    if tmp.stat().st_size!=size or h.hexdigest()!=entry['sha256']:raise ValueError('Parallel download integrity failure')
    os.replace(tmp,dest)
    records=[json.loads(x) for x in history.read_text().splitlines()]
    r=dict(path=entry['path'],revision=REVISION,original_bytes=size,payload_bytes=prefix_size+sum(x['bytes'] for x in records),
        recovered_prefix_bytes=prefix_size,network_time_scope='current resumed attempt; prior prefix time excluded' if prefix_size else 'complete file transfer',prior_prefix_requests_unknown=bool(prefix_size),network_seconds=network,request_seconds_sum=sum(x['seconds'] for x in records),verification_seconds=time.perf_counter()-t,
        seconds=time.perf_counter()-started,requests=len(records),verified_sha256=h.hexdigest(),workers=workers)
    atomic_json(dest.with_suffix('.download.json'),r)
    for part in parts:part.unlink()
    if prefix_size:prefix.unlink()
    return dest,r
