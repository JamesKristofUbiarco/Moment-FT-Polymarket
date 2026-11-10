"""Integration on a complete processed day; explicitly exploratory, not final FULL training."""
import json,time
from pathlib import Path
from polymarket_mvp.download import atomic_json
from polymarket_mvp.windows import build_census,select_training,pack_windows,repartition_census
from polymarket_mvp.model import train,evaluate
root=Path('runs/mvp');source=root/'benchmarks/2026-06-06_local_v2'
while not (source/'consolidated.json').exists():
    status=json.loads((source/'status.json').read_text()) if (source/'status.json').exists() else {}
    if status.get('state')=='failed':raise RuntimeError(status)
    time.sleep(5)
series=[Path(x) for x in json.loads((source/'consolidated.json').read_text())['outputs']]
start=1780704000 # 2026-06-06 UTC; exploratory splits within this day only.
splits={'train':(start,1780788224),'validation':(1780788224,1780789248),'test':(1780789248,start+24*3600)}
exp=root/'integration_complete_day_v2';exp.mkdir(exist_ok=True)
atomic_json(exp/'status.json',dict(state='census',pid=__import__('os').getpid(),final_mvp=False))
r=repartition_census(root/'integration_complete_day/census',exp/'census',splits);print('CENSUS',r,flush=True)
# This is a throughput/integration experiment, never the final training selection.
s=select_training(exp/'census',max(32768,1_000_000));print('SELECTION',s,flush=True)
for split in ['train','validation','test']:
    print('PACK',split,pack_windows(exp/'census',exp/split,split,selected=split=='train'),flush=True)
atomic_json(exp/'status.json',dict(state='training',pid=__import__('os').getpid(),final_mvp=False))
model,trained=train(exp/'train',exp/'model',microbatch=32,max_steps=100,benchmark=True)
print('TRAIN',trained,flush=True)
v=evaluate(exp/'validation',model,exp/'evaluation_validation')
t=evaluate(exp/'test',model,exp/'evaluation_test',calibration=v['calibration'])
atomic_json(exp/'result.json',dict(census=r,selection=s,training=trained,validation=v,test=t,final_mvp=False,
    limitation='Complete-day development benchmark; within-day exploratory splits; not an independent final test or full-dataset training'))
atomic_json(exp/'status.json',dict(state='complete_exploratory_integration',pid=__import__('os').getpid(),final_mvp=False))
