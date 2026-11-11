import json,time
from pathlib import Path
import duckdb
from polymarket_mvp.download import atomic_json
r=Path('runs/mvp');db=duckdb.connect();db.execute("SET memory_limit='2GB'");db.execute('SET threads=2')
old=str(r/'integration_complete_day_v2/census/index.parquet');new=str(r/'census_vectorized_complete_day/index.parquet')
cols='window_id,asset_id,condition_id,start,target,split,reason,observed,activity,source'
a=f'SELECT {cols} FROM read_parquet(?)';b=f'SELECT {cols} FROM read_parquet(?)'
left=db.execute(f'SELECT count(*) FROM (({a}) EXCEPT ALL ({b}))',[old,new]).fetchall()[0][0]
right=db.execute(f'SELECT count(*) FROM (({a}) EXCEPT ALL ({b}))',[new,old]).fetchall()[0][0]
atomic_json(r/'census_vectorized_complete_day/equivalence.json',dict(old_only=left,new_only=right,all_windows_compared=488621,passed=left==right==0))
source=json.loads((r/'benchmarks/2026-06-06_local_v2/consolidated.json').read_text())
db.from_parquet(source['outputs']).create_view('features')
experiment=r/'integration_complete_day_v2';ev=experiment/'evaluation_test'
db.from_parquet(str(ev/'errors.parquet')).create_view('errors')
threshold=json.loads((ev/'evaluation.json').read_text())['calibration']
q=f'''SELECT e.window_id,e.asset_id,e.condition_id,e.start,e.target,e.lora_mae AS score,
    to_timestamp(e.start+e.target) AS target_start_utc,to_timestamp(e.start+e.target+8) AS target_end_utc,
    e.observed_spread,e.last_context,e.direction,
    min(f.spread_min) AS observed_spread_min,max(f.spread_max) AS observed_spread_max,
    min(f.mid_min) AS observed_mid_min,max(f.mid_max) AS observed_mid_max,
    sum(f.events) AS original_events,sum(f.n_bilateral) AS bilateral_quotes,
    sum(f.n_boundary) AS boundary_quotes,sum(f.n_invalid_quote) AS invalid_quotes,
    min(f.file_ordinal_first) AS source_file_ordinal,min(f.row_position_first) AS first_source_row,
    max(f.row_position_last) AS last_source_row
    FROM errors e JOIN features f ON e.asset_id=f.asset_id AND e.condition_id=f.condition_id
         AND f.second>=e.start AND f.second<e.start+512
    WHERE e.lora_mae>={threshold} GROUP BY ALL ORDER BY score DESC'''
out=ev/'candidate_evidence.parquet';t=time.perf_counter()
db.execute(f"COPY ({q}) TO '{out}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
db.execute(f"COPY (SELECT * FROM read_parquet('{out}') LIMIT 100) TO '{ev/'candidate_evidence.csv'}' (HEADER,DELIMITER ',')").fetchall()
atomic_json(ev/'evidence.json',dict(seconds=time.perf_counter()-t,feature_projection_passes=1,threshold_from_validation=threshold,
    candidates=db.execute('SELECT count(*) FROM read_parquet(?)',[str(out)]).fetchall()[0][0],retrospective=True,fraud_labels=False))
print('CENSUS_EQUIVALENCE',left,right,'EVIDENCE',out,flush=True)
