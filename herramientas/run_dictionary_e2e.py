import json,time,sys
from pathlib import Path
while not Path('runs/mvp/dictionary_kernel_complete_day/benchmark.json').exists():
    time.sleep(3)
r=json.loads(Path('runs/mvp/dictionary_kernel_complete_day/benchmark.json').read_text())
if r['kernel_seconds']['dictionary']>=r['kernel_seconds']['plain']*.95:
    print('No meaningful dictionary kernel gain; no additional end-to-end run',r,flush=True)
else:
    from polymarket_mvp.runner import main
    sys.argv=['runner','--root','runs/mvp','--date','2026-06-06','--tag','dictionary_sparse_v10','--groups-per-unit','16','--batch-size','262144','--dictionary-read','--sparse-merge']
    main()
