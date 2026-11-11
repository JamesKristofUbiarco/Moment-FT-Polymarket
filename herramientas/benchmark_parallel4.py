import time,psutil
from polymarket_mvp.download import inventory
from polymarket_mvp.parallel import run_partitions
# Avoid competing with the earlier complete-file consolidation comparisons.
from pathlib import Path
required=['2026-06-06','2026-02-26','2026-04-26','2026-06-09']
while not (Path('runs/mvp/benchmarks/2026-06-09_local_v5/benchmark.json').exists() and Path('runs/mvp/benchmarks/2026-02-26_local_optimized_v9/benchmark.json').exists()):time.sleep(5)
while psutil.virtual_memory().available<12*1024**3:time.sleep(5)
entries=[e for e in inventory('runs/mvp')['files'] if any(d in e['path'] for d in required)]
print(run_partitions(entries,'runs/mvp'),flush=True)
