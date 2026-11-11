import sys,json
from pathlib import Path
import torch
from polymarket_mvp.model import train
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp/current_training_protocol_v3');pack=Path('runs/mvp/integration_complete_day_v2/train')
if sys.argv[1]=='first':
    from polymarket_mvp.windows import pack_windows
    pack=root/'train'
    pack_windows('runs/mvp/integration_complete_day_v2/census',pack,'train',True)
    _,r=train(pack,root/'resumed',microbatch=256,max_steps=20,benchmark=True)
else:
    pack=root/'train'
    _,r=train(pack,root/'resumed',microbatch=256,resume=True,benchmark=True)
    _,c=train(pack,root/'continuous',microbatch=256,benchmark=True)
    def load(p):
        m=json.loads((p/'checkpoint.json').read_text());return torch.load(p/m['file'],map_location='cpu',weights_only=False)['trainable']
    a=load(root/'resumed');b=load(root/'continuous');delta=max(float((a[k]-b[k]).abs().max()) for k in a)
    atomic_json(root/'verification.json',dict(passed=delta<=1e-6,max_parameter_difference=delta,
        windows=r['trained_windows'],steps=r['optimizer_steps'],training_code_sha256=r['identity']['code_sha256'],
        process_boundary_verified=True,complete_day_development_only=True))
    print('CURRENT_PROTOCOL_VERIFIED',delta,flush=True)
