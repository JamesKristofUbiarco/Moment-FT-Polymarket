from pathlib import Path
import json
import torch
from polymarket_mvp.model import train
from polymarket_mvp.download import atomic_json
r=Path('runs/mvp/integration_complete_day_v2')
_,resumed=train(r/'train',r/'throughput_micro256',microbatch=256,resume=True)
_,continuous=train(r/'train',r/'continuous_micro256',microbatch=256)
def weights(path):
    m=json.loads((path/'checkpoint.json').read_text())
    return torch.load(path/m['file'],map_location='cpu',weights_only=False)['trainable']
a=weights(r/'throughput_micro256');b=weights(r/'continuous_micro256')
delta=max(float((a[k]-b[k]).abs().max()) for k in a)
atomic_json(r/'real_resume_verification.json',dict(max_parameter_difference=delta,
    resumed_windows=resumed['trained_windows'],continuous_windows=continuous['trained_windows'],
    resumed_steps=resumed['optimizer_steps'],continuous_steps=continuous['optimizer_steps'],passed=delta<=1e-6))
print('RESUME_MAX_DIFFERENCE',delta,flush=True)
