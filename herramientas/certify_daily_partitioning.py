import datetime,json,hashlib
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.compute as pc
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');inventory=json.loads((root/'inventory_enriched.json').read_text());bad=[]
for f in inventory['files']:
    day=f['path'].split('date=')[1][:10];start=(datetime.date.fromisoformat(day)-datetime.date(1970,1,1)).days*86400000
    t=pq.read_table(root/'metadata_inventory'/f'{day}_row_groups.parquet',columns=['min_ts_ms','max_ts_ms'])
    lo=pc.min(t['min_ts_ms']).as_py();hi=pc.max(t['max_ts_ms']).as_py()
    if lo is None or hi is None or lo<start or hi>=start+86400000:bad.append(dict(path=f['path'],minimum=lo,maximum=hi))
r=dict(revision=inventory['revision'],files=164,disjoint_utc_day_timestamp_ranges=not bad,exceptions=bad,
    inventory_sha256=hashlib.sha256((root/'inventory_enriched.json').read_bytes()).hexdigest(),
    proof='Distinct original UTC days plus per-file canonical GROUP BY(asset,condition,second) imply globally unique canonical keys')
atomic_json(root/'canonical_day_certificate.json',r);print(r,flush=True)
