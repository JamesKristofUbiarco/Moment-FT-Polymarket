import json
from pathlib import Path
from polymarket_mvp.model import train
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/integration_complete_day_v2')
results=[]
for micro in [64,128,256]:
    _,report=train(root/'train',root/f'throughput_micro{micro}',microbatch=micro,max_steps=20,benchmark=True)
    results.append({k:v for k,v in report.items() if k!='parameter_changes'})
    print(micro,report['windows_per_second'],report['vram_peak'],flush=True)
atomic_json(root/'gpu_benchmarks.json',results)
