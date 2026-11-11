import json
from pathlib import Path
from polymarket_mvp.windows import build_census
root=Path('runs/mvp');report=json.loads((root/'benchmarks/2026-06-06_local_v7/consolidated.json').read_text())
splits=json.loads((root/'integration_complete_day_v2/census/census.json').read_text())['splits']
print(build_census(report['outputs'],root/'census_aligned_hash_complete_day',splits),flush=True)
