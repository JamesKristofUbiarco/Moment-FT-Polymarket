import json
from pathlib import Path
from polymarket_mvp.windows import build_census
root=Path('runs/mvp');r=json.loads((root/'benchmarks/2026-06-06_local_v2/consolidated.json').read_text())
splits=json.loads((root/'integration_complete_day_v2/census/census.json').read_text())['splits']
print(build_census(r['outputs'],root/'census_vectorized_complete_day',splits),flush=True)
