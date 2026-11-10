import json,platform,sys,os,shutil
from importlib.metadata import version
from pathlib import Path
import torch,psutil
from polymarket_mvp.download import atomic_json
report=dict(python=sys.version,platform=platform.platform(),cpu=platform.processor(),logical_cpus=os.cpu_count(),
 memory=psutil.virtual_memory()._asdict(),disk=shutil.disk_usage('.')._asdict(),cuda_available=torch.cuda.is_available(),cuda_build=torch.version.cuda,
 versions={p:version(p) for p in ['torch','pyarrow','polars','duckdb','momentfm','peft','transformers','huggingface-hub','psutil']})
if torch.cuda.is_available():report.update(gpu=torch.cuda.get_device_name(0),bf16=torch.cuda.is_bf16_supported(),vram=torch.cuda.get_device_properties(0).total_memory)
atomic_json('runs/mvp/environment.json',report)
print(json.dumps(report,indent=2))
