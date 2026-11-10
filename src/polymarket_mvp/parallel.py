"""Measured file-level concurrency with independent manifests and stage admission."""
import json,os,subprocess,sys,time,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
import psutil
from .download import download,atomic_json
from .engine import verify_series


def run_partitions(entries,root,tag='parallel4',workers=4,consolidators=2,deadline=None,output_group='benchmarks',reclaim=False,reuse=False):
    root=Path(root);start=time.perf_counter();sem=threading.Semaphore(consolidators)
    admission=threading.Lock();running={};records=[]
    def wait_memory(need):
        if deadline and time.time()>=deadline:raise TimeoutError('Approved execution budget reached')
        while psutil.virtual_memory().available<need:
            if deadline and time.time()>=deadline:raise TimeoutError('Approved budget reached while waiting for RAM')
            time.sleep(2)
    def run_child(args,log_path):
        env=dict(os.environ,POLARS_MAX_THREADS='4',OMP_NUM_THREADS='4')
        with log_path.open('a') as log:
            process=subprocess.Popen([sys.executable,'-u','-m','polymarket_mvp.worker',*args],stdout=log,stderr=subprocess.STDOUT,env=env)
            with admission:running[process.pid]=dict(arguments=args,started_utc=time.time())
            code=process.wait()
            with admission:running.pop(process.pid,None)
        if code:raise RuntimeError(f'Worker failed ({code}); committed progress retained: {log_path}')
    def one(entry):
        if deadline and time.time()>=deadline:raise TimeoutError('Approved budget reached before downloading next original')
        day=entry['path'].split('date=')[1][:10]
        out=root/'full'/day if output_group=='full' else root/'benchmarks'/f'{day}_local_{tag}'
        if reuse and not out.exists():
            import hashlib
            feature_hash=hashlib.sha256(Path(__file__).with_name('features.py').read_bytes()).hexdigest()
            candidates=sorted((root/'benchmarks').glob(day+'_local_*'),key=lambda p:p.stat().st_mtime,reverse=True)
            for candidate in candidates:
                if not (candidate/'benchmark.json').exists():continue
                identity=json.loads((candidate/'source.json').read_text())
                if identity['source']==entry and identity['feature_code_sha256']==feature_hash:
                    out.parent.mkdir(parents=True,exist_ok=True);out.symlink_to(candidate.resolve(),target_is_directory=True);break
        out.mkdir(parents=True,exist_ok=True)
        done=out/'benchmark.json'
        if done.exists():
            from . import REVISION
            identity=json.loads((out/'source.json').read_text())
            if identity['revision']!=REVISION or identity['source']!=entry:raise ValueError('Completed partition source identity mismatch')
            r=json.loads(done.read_text());verify_series(r['consolidation']);return r
        began=time.perf_counter();atomic_json(out/'status.json',dict(state='waiting_memory',pid=os.getpid(),started_utc=time.time()))
        cache_existed=(root/'staging'/(day+'.parquet')).exists()
        source,network=download(entry,root/'staging')
        if not (out/'ingest_report.json').exists():
            wait_memory(5*1024**3)
            atomic_json(out/'status.json',dict(state='ingestion',pid=os.getpid()))
            args=['ingest','--out',str(out),'--source',str(source),'--entry',json.dumps(entry),'--groups-per-unit','16']
            if deadline:args+=['--deadline',str(deadline)]
            run_child(args,out/'ingest.log')
        ingest=json.loads((out/'ingest_report.json').read_text())
        if not ingest['complete']:raise RuntimeError('Original file incomplete')
        with sem:
            wait_memory(6*1024**3)
            atomic_json(out/'status.json',dict(state='consolidation',pid=os.getpid()))
            args=['consolidate','--out',str(out)]
            if deadline:args+=['--deadline',str(deadline)]
            run_child(args,out/'consolidation.log')
        merged=json.loads((out/'consolidated.json').read_text())
        r=dict(complete=True,source_bytes=entry['bytes'],ingest=ingest,download=network,consolidation=merged,
            wall_seconds=time.perf_counter()-began,network_cache_reused=cache_existed,
            concurrency=dict(ingesters=workers,consolidators=consolidators),timing_scope='actual queued job wall; component stage timings separate')
        atomic_json(done,r)
        if reclaim:
            from .storage import Manifest
            import shutil
            m=Manifest(out/'parts');m.compact(merged['outputs'],merged,delete_originals=True);m.db.close()
            source.unlink(missing_ok=True)
            for name in ['merge_partitioned','spill','spill_partitioned']:
                if (out/name).exists():shutil.rmtree(out/name)
        atomic_json(out/'status.json',dict(state='complete',finished_utc=time.time(),pid=os.getpid()))
        print('PARTITION_COMPLETE',day,r['wall_seconds'],flush=True);return r
    errors=[]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(one,e) for e in entries]
        while futures:
            ready=[f for f in futures if f.done()]
            for f in ready:
                try:records.append(f.result())
                except BaseException as error:errors.append(repr(error))
                futures.remove(f)
            with admission:active=dict(running)
            atomic_json(root/f'{tag}_progress.json',dict(pid=os.getpid(),complete_files=len(records),errors=errors,total_files=len(entries),active_workers=active,
                seconds=time.perf_counter()-start,memory_available=psutil.virtual_memory().available))
            if futures:time.sleep(2)
    stage_seconds=sum(r['ingest']['seconds']+r['consolidation']['seconds'] for r in records)
    report=dict(complete=not errors and len(records)==len(entries),errors=errors,files=len(records),rows=sum(r['ingest']['rows_confirmed'] for r in records),
        original_bytes=sum(r['source_bytes'] for r in records),wall_seconds=time.perf_counter()-start,
        component_seconds_sum=stage_seconds,observed_overlap_factor=stage_seconds/(time.perf_counter()-start),
        workers=workers,consolidators=consolidators,benchmark_only=output_group=='benchmarks',records=[r['ingest']['source'] for r in records])
    atomic_json(root/f'{tag}_benchmark.json',report);return report
