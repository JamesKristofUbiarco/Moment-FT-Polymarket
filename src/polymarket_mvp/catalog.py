"""Post-ingest catalog audit; never a dependency for producing early features."""
from pathlib import Path
import json
import duckdb
from .storage import Manifest
from .download import atomic_json


def build_catalog(partition_roots,output):
    output=Path(output);output.mkdir(parents=True,exist_ok=True);paths=[]
    for root in partition_roots:
        root=Path(root);m=Manifest(root/'parts')
        paths.extend(str((root/'parts'/r[0]).resolve()) for r in m.db.execute("SELECT path FROM objects WHERE path LIKE '%_relations.parquet'"))
        m.db.close()
    db=duckdb.connect();db.execute("SET memory_limit='2GB'");db.from_parquet(paths).create_view('relations')
    q='''SELECT asset_id,condition_id,min(first_ts) AS first_observed_ms,max(last_ts) AS last_observed_ms,
         sum(events)::UBIGINT AS original_events FROM relations GROUP BY asset_id,condition_id'''
    path=output/'observed_relations.parquet';db.execute(f"COPY ({q}) TO '{path}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    db.from_parquet(str(path)).create_view('catalog')
    conflicts=output/'multiple_observed_conditions.parquet'
    db.execute(f"COPY (SELECT asset_id,count(*) AS observed_conditions,min(first_observed_ms) AS first_ms,max(last_observed_ms) AS last_ms FROM catalog GROUP BY asset_id HAVING count(*)>1) TO '{conflicts}' (FORMAT PARQUET,COMPRESSION ZSTD)").fetchall()
    count=db.execute('SELECT count(DISTINCT asset_id),count(DISTINCT condition_id),count(*) FROM catalog').fetchall()[0]
    r=dict(assets=count[0],conditions=count[1],observed_pairs=count[2],relation_derived_scan_passes=1,
        conflicts=db.execute('SELECT count(*) FROM read_parquet(?)',[str(conflicts)]).fetchall()[0][0],
        policy='Preserve reported historical IDs; exclude ambiguous asset/window identities; no global reassignment')
    atomic_json(output/'catalog.json',r);db.close();return r
