"""Real MOMENT LoRA optimization, resume, throughput, and paired evaluation."""
import contextlib
import copy
import json
import math
import os
import random
import time
import uuid
import inspect
import hashlib
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from torch.utils.data import Dataset,DataLoader
from momentfm import MOMENTPipeline
from peft import LoraConfig,get_peft_model
from . import MODEL_REVISION
from .download import atomic_json
from .storage import digest


class WindowDataset(Dataset):
    def __init__(self,root):
        self.root=Path(root);self.x=np.load(self.root/'x.npy',mmap_mode='r')
        self.observed=np.load(self.root/'observed.npy',mmap_mode='r');self.target=np.load(self.root/'target.npy',mmap_mode='r')
    def __len__(self):return len(self.target)
    def __getitem__(self,i):
        target=int(self.target[i]);values=np.array(self.x[i],copy=True);observed=np.array(self.observed[i],copy=True)
        y=values[target:target+8].copy();values[target:]=0;observed[target+8:]=0
        visible=observed.copy();visible[target:]=0
        return dict(x=torch.from_numpy(values).unsqueeze(0),input_mask=torch.from_numpy(observed).long(),
          mask=torch.from_numpy(visible).long(),y=torch.from_numpy(y),target=target,index=i)


def load_base(device):
    model=MOMENTPipeline.from_pretrained('AutonLab/MOMENT-1-small',revision=MODEL_REVISION,
            model_kwargs={'task_name':'reconstruction'})
    model.init();return model.to(device)


def adapter(base):
    return get_peft_model(base,LoraConfig(r=8,lora_alpha=16,lora_dropout=0.,target_modules=['q','v'],
                                        modules_to_save=['head'],bias='none'))


def prediction(model,batch,device):
    positions=batch['target'].to(device)[:,None]+torch.arange(8,device=device)[None,:]
    with torch.autocast(device_type=device.type,dtype=torch.bfloat16,enabled=device.type=='cuda'):
        out=model(x_enc=batch['x'].to(device),input_mask=batch['input_mask'].to(device),mask=batch['mask'].to(device)).reconstruction[:,0]
    return out.gather(1,positions).float()


def sync(device):
    if device.type=='cuda':torch.cuda.synchronize()


def checkpoint(root,model,optimizer,state):
    root=Path(root);name=f'checkpoint_{state["step"]:09d}_{uuid.uuid4().hex}.pt';tmp=root/(name+'.tmp')
    payload=dict(state,trainable={n:p.detach().cpu() for n,p in model.named_parameters() if p.requires_grad},
        optimizer=optimizer.state_dict(),torch_rng=torch.get_rng_state(),numpy_rng=np.random.get_state(),python_rng=random.getstate(),
        cuda_rng=torch.cuda.get_rng_state_all() if torch.cuda.is_available() else [])
    with tmp.open('wb') as f:torch.save(payload,f);f.flush();os.fsync(f.fileno())
    sha=digest(tmp);os.replace(tmp,root/name);atomic_json(root/'checkpoint.json',dict(file=name,sha256=sha,step=state['step']))


def train(pack,root,microbatch=32,effective_batch=256,resume=False,max_steps=None,benchmark=False,deadline=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    pack_receipt=json.loads((Path(pack)/'pack.json').read_text())
    if 'hashes' not in pack_receipt:raise ValueError('Rebuild legacy pack to establish tensor integrity')
    for filename,sha in pack_receipt['hashes'].items():
        if digest(Path(pack)/filename)!=sha:raise ValueError(f'Corrupt training pack: {filename}')
    device=torch.device('cuda' if torch.cuda.is_available() else 'cpu');torch.manual_seed(2026)
    np.random.seed(2026);random.seed(2026);data=WindowDataset(pack)
    code_hash=hashlib.sha256('\n'.join(inspect.getsource(x) for x in [WindowDataset,adapter,prediction,train]).encode()).hexdigest()
    identity=dict(pack_hashes=pack_receipt['hashes'],code_sha256=code_hash,model_revision=MODEL_REVISION,pack_sha256=digest(Path(pack)/'metadata.parquet'),
          microbatch=microbatch,effective_batch=effective_batch,lr=1e-4,seed=2026,windows=len(data))
    model=adapter(load_base(device));params={n:p for n,p in model.named_parameters() if p.requires_grad}
    opt=torch.optim.AdamW(params.values(),lr=1e-4,weight_decay=.01)
    permutation=np.random.default_rng(2026).permutation(len(data));cursor=0;step=0;history=[];prior_wall=0.
    if resume:
        meta=json.loads((root/'checkpoint.json').read_text());path=root/meta['file']
        if digest(path)!=meta['sha256']:raise ValueError('Checkpoint integrity failure')
        saved=torch.load(path,map_location='cpu',weights_only=False) # only locally created verified checkpoint
        if saved['identity']!=identity:raise ValueError('Incompatible checkpoint')
        for n,p in params.items():p.data.copy_(saved['trainable'][n].to(device))
        opt.load_state_dict(saved['optimizer']);cursor=saved['cursor'];step=saved['step'];history=saved['history']
        prior_wall=saved.get('training_wall_seconds',sum(x['seconds'] for x in history))
        torch.set_rng_state(saved['torch_rng']);np.random.set_state(saved['numpy_rng']);random.setstate(saved['python_rng'])
        if saved['cuda_rng'] and device.type=='cuda':torch.cuda.set_rng_state_all(saved['cuda_rng'])
    elif (root/'checkpoint.json').exists():raise FileExistsError('Checkpoint exists; request resume')
    start=time.perf_counter();saved_at=start;trained_start=cursor
    if device.type=='cuda':torch.cuda.reset_peak_memory_stats()
    # Check both LoRA and head updates; frozen tensors are checked by version counters.
    before={n:p.detach().clone() for n,p in params.items()}
    frozen={n:p._version for n,p in model.named_parameters() if not p.requires_grad}
    grad_verified=False;model.train()
    while cursor<len(data):
        if deadline is not None and time.time()>=deadline:break
        if max_steps is not None and step>=max_steps:break
        indices=permutation[cursor:min(cursor+effective_batch,len(data))]
        opt.zero_grad(set_to_none=True);loss_sum=0.;sync(device);t=time.perf_counter()
        for offset in range(0,len(indices),microbatch):
            ids=indices[offset:offset+microbatch]
            batch=torch.utils.data.default_collate([data[int(i)] for i in ids])
            pred=prediction(model,batch,device);loss=(pred-batch['y'].to(device)).square().mean()
            if not torch.isfinite(loss):raise ValueError('Nonfinite loss')
            (loss*len(ids)/len(indices)).backward();loss_sum+=float(loss.detach())*len(ids)
        finite=all(p.grad is None or bool(torch.isfinite(p.grad).all()) for p in params.values())
        lora_grad=sum(float(p.grad.float().abs().sum()) for n,p in params.items() if 'lora_' in n and p.grad is not None)
        if not finite or not lora_grad>0:raise ValueError('Missing or invalid LoRA gradients')
        grad_verified=True;torch.nn.utils.clip_grad_norm_(params.values(),1.);opt.step();cursor+=len(indices);step+=1
        sync(device);seconds=time.perf_counter()-t
        rec=dict(step=step,windows=len(indices),mse=loss_sum/len(indices),seconds=seconds,windows_per_second=len(indices)/seconds)
        history.append(rec)
        with (root/'history.jsonl').open('a') as f:f.write(json.dumps(rec)+'\n')
        if step%500==0 or time.perf_counter()-saved_at>=300:
            checkpoint(root,model,opt,dict(identity=identity,cursor=cursor,step=step,history=history,training_wall_seconds=prior_wall+time.perf_counter()-start));saved_at=time.perf_counter()
    checkpoint(root,model,opt,dict(identity=identity,cursor=cursor,step=step,history=history,training_wall_seconds=prior_wall+time.perf_counter()-start))
    model.save_pretrained(root/'adapter',safe_serialization=True)
    changes={n:float((p.detach()-before[n]).abs().max()) for n,p in params.items()}
    if cursor>trained_start:
        if not any(v>0 for n,v in changes.items() if 'lora_' in n):raise ValueError('LoRA did not update')
        if not any(v>0 for n,v in changes.items() if 'head' in n):raise ValueError('Head did not update')
    if any(p._version!=frozen[n] for n,p in model.named_parameters() if n in frozen):raise ValueError('Frozen weights changed')
    measured=history[min(5,max(0,len(history)-1)):]
    throughput=sum(x['windows'] for x in measured)/max(1e-9,sum(x['seconds'] for x in measured))
    report=dict(identity=identity,complete=cursor==len(data),benchmark=benchmark,trained_windows=cursor,optimizer_steps=step,
        elapsed_seconds=prior_wall+time.perf_counter()-start,current_attempt_seconds=time.perf_counter()-start,optimizer_step_seconds=sum(x['seconds'] for x in history),windows_per_second=throughput,gradient_verified=grad_verified,
        parameter_changes=changes,trainable_parameters=sum(p.numel() for p in params.values()),
        vram_peak=torch.cuda.max_memory_allocated() if device.type=='cuda' else 0,
        epoch_definition='one pass over frozen selection; not over the eligible census')
    atomic_json(root/'training.json',report);return model,report


def evaluate(pack,trained,root,batch_size=64,calibration=None,deadline=None):
    receipt=json.loads((Path(pack)/'pack.json').read_text())
    if 'hashes' not in receipt:raise ValueError('Rebuild legacy evaluation pack to establish tensor integrity')
    for filename,sha in receipt['hashes'].items():
        if digest(Path(pack)/filename)!=sha:raise ValueError(f'Corrupt evaluation pack: {filename}')
    root=Path(root);root.mkdir(parents=True,exist_ok=True);device=next(trained.parameters()).device
    base=load_base(device);base.eval();trained.eval();data=WindowDataset(pack)
    metadata_rows=(row for batch in pq.ParquetFile(Path(pack)/'metadata.parquet').iter_batches(batch_size=4096) for row in batch.to_pylist())
    metadata_cursor=0
    t=time.perf_counter();records=[];writer=None;output=root/'errors.parquet'
    with torch.inference_mode():
        for batch in DataLoader(data,batch_size=batch_size,shuffle=False,num_workers=0):
            if deadline is not None and time.time()>=deadline:
                if writer:writer.close()
                raise TimeoutError('Approved budget reached during evaluation; no complete evaluation published')
            original=prediction(base,batch,device).cpu();adjusted=prediction(trained,batch,device).cpu()
            # Last visible observation, not padded zero or a hidden target.
            positions=torch.arange(512)[None,:].expand(len(batch['x']),-1)
            last=torch.where(batch['mask'].bool(),positions,-1).max(1).values
            persistence=batch['x'][:,0].gather(1,last[:,None]).expand(-1,8)
            result=[]
            for i,index in enumerate(batch['index'].tolist()):
                if index!=metadata_cursor:raise ValueError('Evaluation metadata order mismatch')
                meta=next(metadata_rows);metadata_cursor+=1
                row={k:meta[k] for k in ['window_id','asset_id','condition_id','start','target']}
                row.update(channel='spread',source=meta.get('source','not_recorded'),activity=meta.get('activity'),observed=meta.get('observed'),window_file=meta.get('file'))
                for name,pred in [('persistence',persistence),('moment',original),('lora',adjusted)]:
                    err=pred[i]-batch['y'][i];row[name+'_mae']=float(err.abs().mean());row[name+'_mse']=float(err.square().mean())
                row.update(observed_spread=float(batch['y'][i].mean()),last_context=float(persistence[i,0]),
                           direction=float(batch['y'][i].mean()-persistence[i,0]))
                result.append(row)
            table=pa.Table.from_pylist(result)
            if writer is None:writer=pq.ParquetWriter(output,table.schema,compression='zstd')
            writer.write_table(table)
    if writer:writer.close()
    del base
    import duckdb
    db=duckdb.connect();db.execute("SET memory_limit='2GB'");db.execute('SET temp_directory=?',[str(root/'spill')]);db.from_parquet(str(output)).create_view('errors')
    aggregate=','.join(f'avg({n}_mae) AS {n}_mae,sqrt(avg({n}_mse)) AS {n}_rmse' for n in ['persistence','moment','lora'])
    overall=db.execute('SELECT count(*) AS windows,'+aggregate+' FROM errors').fetchdf().to_dict('records')[0]
    for key in ['asset_id','condition_id']:
        db.execute(f"COPY (SELECT {key},count(*) AS windows,{aggregate} FROM errors GROUP BY {key}) TO '{root/(key+'.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    macro=db.execute('SELECT '+','.join(f'avg({n}_mae) AS macro_mae_{n}' for n in ['persistence','moment','lora'])+' FROM read_parquet(?)',[str(root/'condition_id.parquet')]).fetchdf().to_dict('records')[0]
    overall.update(macro)
    if calibration is None:calibration=float(db.execute('SELECT quantile_cont(lora_mae,0.995) FROM errors').fetchall()[0][0])
    episodes=root/'episodes.parquet'
    db.execute(f"COPY (SELECT condition_id,start,max(lora_mae) AS score,count(*) AS assets,min(start+target) AS target_start,max(start+target+8) AS target_end FROM errors WHERE lora_mae>={float(calibration)} GROUP BY condition_id,start ORDER BY score DESC) TO '{episodes}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    overall.update(seconds=time.perf_counter()-t,calibration=calibration,episodes=str(episodes))
    atomic_json(root/'evaluation.json',overall);db.close();return overall


def verify_adapter(root,device=None):
    """Reload the exported PEFT adapter/head and compare every trained tensor."""
    from peft import PeftModel
    root=Path(root);meta=json.loads((root/'checkpoint.json').read_text());path=root/meta['file']
    if digest(path)!=meta['sha256']:raise ValueError('Checkpoint integrity failure')
    saved=torch.load(path,map_location='cpu',weights_only=False)
    device=torch.device(device or ('cuda' if torch.cuda.is_available() else 'cpu'))
    reloaded=PeftModel.from_pretrained(load_base(device),root/'adapter',is_trainable=False)
    actual=dict(reloaded.named_parameters());difference=0.
    for name,tensor in saved['trainable'].items():
        if name not in actual:raise ValueError(f'Exported trained tensor missing: {name}')
        difference=max(difference,float((actual[name].detach().cpu()-tensor).abs().max()))
    if difference!=0:raise ValueError(f'Adapter reload differs from checkpoint: {difference}')
    result=dict(passed=True,trained_tensors=len(saved['trainable']),max_parameter_difference=difference,
        model_revision=MODEL_REVISION,checkpoint_sha256=meta['sha256'])
    atomic_json(root/'adapter_verification.json',result);return result
