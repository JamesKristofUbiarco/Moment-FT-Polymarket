"""Coverage and measured cost reports; estimates remain explicitly labelled."""
import json
import time
from pathlib import Path
from .download import inventory,atomic_json


def progress(root):
    root=Path(root);result=[]
    for path in sorted((root/'benchmarks').glob('*/status.json')):
        r=json.loads(path.read_text());r['run']=path.parent.name
        import psutil
        pid=r.get('pid');r['pid_alive']=bool(pid and psutil.pid_exists(pid))
        if r['pid_alive']:
            try:r['pid_alive']=psutil.Process(pid).status()!=psutil.STATUS_ZOMBIE
            except psutil.NoSuchProcess:r['pid_alive']=False
        if not r['pid_alive'] and r.get('state') in ['running','consolidation']:r['observed_state']='stale_process_not_running'
        p=path.parent/'progress.json'
        if p.exists():r['progress']=json.loads(p.read_text())
        result.append(r)
    return result


REPRESENTATIVE_DAYS = ("2026-02-26", "2026-04-26", "2026-06-06", "2026-06-09")
DISK_RESERVE = 32 * 1024**3


def audit_benchmark(root):
    """Read the exact cohort used for the measured concurrent wall time."""
    root = Path(root)
    cohort_path = root / "parallel4_benchmark.json"
    if not cohort_path.exists():
        raise ValueError("Missing completed concurrent benchmark: parallel4_benchmark.json")
    cohort = json.loads(cohort_path.read_text())
    if not cohort.get("complete"):
        raise ValueError("Concurrent benchmark is incomplete")
    files = []
    for day in REPRESENTATIVE_DAYS:
        folder = root / "benchmarks" / f"{day}_local_parallel4"
        path = folder / "benchmark.json"
        if not path.exists():
            raise ValueError(f"Missing complete-file report for {day}: {path}")
        r = json.loads(path.read_text())
        i, c = r["ingest"], r["consolidation"]
        if not r["complete"] or not i["complete"] or i["rows_confirmed"] != i["declared_rows"]:
            raise ValueError(f"Unreconciled benchmark rows for {day}")
        timing, stages = i["timings"], c["stage_timings"]
        local_reads = folder / "original_reads.jsonl"
        visits = {}
        if local_reads.exists():
            for line in local_reads.read_text().splitlines():
                event = json.loads(line)
                if event.get("decoder_passes") == 1:
                    group = event["group"]
                    visits[group] = visits.get(group, 0) + 1
        writes = i["resources_after"]["disk_write_bytes"] - i["resources_before"]["disk_write_bytes"]
        writes += c["resources"]["disk_write_bytes"] - c["resources_before"]["disk_write_bytes"]
        files.append(dict(date=day, report=str(path), rows=i["rows_confirmed"], original_bytes=r["source_bytes"],
            ingest_seconds=i["seconds"], read_decode_seconds=timing["parquet_read_decode_seconds"],
            transform_seconds=timing["transform_catalog_group_seconds"], catalog_seconds=timing["catalog_seconds"],
            publish_seconds=timing["write_hash_commit_seconds"], repartition_seconds=stages["partition_seconds"],
            aggregate_seconds=stages["aggregate_seconds"], consolidation_seconds=c["seconds"],
            verification_seconds=None, ingest_events_per_second=i["events_per_second"],
            peak_worker_rss=max(i["resources_after"]["peak_rss"], c["resources"]["peak_rss"]),
            partial_bytes=i["derived_bytes"], intermediate_bytes=sum(
                p.stat().st_size for p in (folder / "merge_partitioned").rglob("*.parquet")),
            final_bytes=c["output_bytes"], series_rows=c["series_rows"], observed_disk_write_bytes=writes,
            write_amplification=writes/r["source_bytes"], decoded_row_group_passes=i["logical_source_passes"],
            duplicate_decodes=sum(n-1 for n in visits.values()) if visits else None,
            download=r["download"], derived_scan_passes=c["derived_scan_passes"],
            integrity_output_scan_passes=c["integrity_output_scan_passes"]))
    if sum(f["rows"] for f in files) != cohort["rows"] or sum(f["original_bytes"] for f in files) != cohort["original_bytes"]:
        raise ValueError("Per-file benchmark totals do not match concurrent cohort")
    result = dict(cohort=cohort, files=files, notes=[
        "Catalog is included in Polars transformation; aggregate includes output writing.",
        "Verification is not isolated in these historical reports.",
        "Process disk writes include final output and metadata; not the live temporary peak.",
        "Overlap is accumulated phase time divided by cohort wall time, not a serial speedup."])
    atomic_json(root / "benchmark_audit.json", result)
    return result


def projection(root, budget_hours=120, preparation=None):
    import shutil
    from . import REVISION
    root = Path(root)
    inv = inventory(root)
    preparation = Path(preparation) if preparation else root / "preflight_full" / "result.json"
    free = shutil.disk_usage(root).free
    try:
        audit = audit_benchmark(root)
        if not preparation.exists():
            raise ValueError(f"Missing end-to-end preparation receipt: {preparation}")
        pilot = json.loads(preparation.read_text())
        if not pilot["complete"] or not pilot["training"]["complete"] or pilot["source"] not in inv["files"]:
            raise ValueError(f"Incompatible preparation receipt: {preparation}")
        if not pilot["validation"]["windows"] or not pilot["test"]["windows"]:
            raise ValueError("Preparation must evaluate both validation and test")
    except (ValueError, KeyError, TypeError) as error:
        result = dict(performance_gate=False, storage_gate=False, approved_hours=budget_hours, error=str(error))
        atomic_json(root / "projection.json", result)
        return result
    cohort, files = audit["cohort"], audit["files"]
    total_bytes = sum(e["bytes"] for e in inv["files"])
    processing = cohort["wall_seconds"] * total_bytes / cohort["original_bytes"]
    final_bytes = 0
    series_rows = 0
    for e in inv["files"]:
        day = e["path"].split("date=")[1][:10]
        if day < "2026-04-13":
            reps = files[:1]
        elif e["bytes"] < 1024**3:
            reps = files[2:3]
        else:
            reps = [files[1], files[3]]
        final_bytes += e["bytes"] * max(f["final_bytes"]/f["original_bytes"] for f in reps)
        series_rows += e["bytes"] * max(f["series_rows"]/f["original_bytes"] for f in reps)
    transfer = [f["download"] for f in files if f["download"].get("workers") == 4
                and not f["download"].get("recovered_prefix_bytes", 0)]
    if not transfer:
        result = dict(performance_gate=False, storage_gate=False, error="Missing fresh parallel download measurement")
        atomic_json(root / "projection.json", result)
        return result
    network = total_bytes / min(t["original_bytes"]/t["seconds"] for t in transfer)
    # Model storage scales with observed series density, with a separate density margin.
    scale = series_rows / pilot["consolidated"]["series_rows"]
    census_seconds = pilot["census"]["seconds"] * scale
    eligible = int(pilot["census"]["eligible"] * scale)
    observed = int(pilot["census"]["total_windows"] * scale)
    estimated_splits = dict(train=int(eligible*.7), validation=int(eligible*.15), test=int(eligible*.15))
    tensor_bytes = {k: v * 2562 for k, v in estimated_splits.items()}
    prepared_census = Path(pilot["census"]["index"]).parent
    census_measured_bytes = sum(p.stat().st_size for p in prepared_census.glob("*.parquet"))
    census_bytes = int(1.15 * census_measured_bytes * scale)
    pack_seconds = sum(pilot["packing_seconds"].values()) * scale
    train_rate = .8 * pilot["training"]["windows_per_second"]
    eval_rate = .8 * min(pilot[s]["windows"]/pilot[s]["seconds"] for s in ("validation", "test"))
    training = estimated_splits["train"] / train_rate
    evaluation = (estimated_splits["validation"] + estimated_splits["test"]) / eval_rate
    largest = max(e["bytes"] for e in inv["files"])
    # Admission limits the sum of active per-file reservations, allowing fewer
    # concurrent files when free space falls as verified outputs accumulate.
    scratch_factor = 8.5
    max_scratch = int(largest * scratch_factor)
    manifest_bytes = int(total_bytes * .01)
    evaluation_spill_and_results = int(1.2 * max(tensor_bytes["validation"], tensor_bytes["test"])) + 2*1024**3
    disk_for_training = free - 10*1024**3 - final_bytes - manifest_bytes - DISK_RESERVE - census_bytes - evaluation_spill_and_results
    estimated_training_cap = max(0, min(estimated_splits["train"], int(disk_for_training / (2818 * 1.1))))
    model_bytes = int(estimated_training_cap * 2818 * 1.1) + evaluation_spill_and_results
    training = estimated_training_cap / train_rate
    projected_storage = int(final_bytes + manifest_bytes + DISK_RESERVE + max(max_scratch, census_bytes + model_bytes))
    reserve_time = .20 * (processing + census_seconds + pack_seconds)
    total = processing + network + census_seconds + pack_seconds + training + evaluation + reserve_time
    result = dict(revision=REVISION, estimated_not_executed=True, approved_hours=budget_hours,
        projected_hours=total/3600, processing_target_hours=72, performance_gate=total<=budget_hours*3600,
        storage_gate=projected_storage<=free and estimated_training_cap>0, full_file_benchmarks=len(files),
        processing_seconds=processing, network_seconds=network, census_seconds=census_seconds,
        packing_seconds=pack_seconds, training_seconds=training, evaluation_seconds=evaluation,
        export_seconds=reserve_time, training_windows_per_second=train_rate, evaluation_windows_per_second=eval_rate,
        microbatch=pilot["training"]["identity"]["microbatch"], workers=cohort["workers"],
        consolidators=cohort["consolidators"], estimated_eligible_windows=eligible, estimated_split_windows=estimated_splits,
        estimated_tensor_bytes_by_split=tensor_bytes, estimated_training_disk_cap=estimated_training_cap,
        estimated_observed_windows=observed, measured_preflight_census_bytes=census_measured_bytes, estimated_final_bytes=int(final_bytes),
        estimated_manifest_bytes=manifest_bytes, estimated_census_bytes=census_bytes, estimated_model_stage_bytes=model_bytes,
        estimated_storage_requirement=projected_storage, disk_available_bytes=free, disk_reserve_bytes=DISK_RESERVE,
        scratch_factor=scratch_factor, maximum_single_partition_reservation_bytes=max_scratch,
        max_staging_original_bytes=2*sum(sorted((e["bytes"] for e in inv["files"]),reverse=True)[:cohort["workers"]]),
        preflight_receipt=str(preparation), benchmark_audit=str(root/"benchmark_audit.json"),
        assumptions=["Runtime scales by measured concurrent cohort bytes; mixed sizes and disk pressure may change throughput.",
                     "Network conservatively added without overlap.", "Census density and split sizes are estimates from one exploratory day.",
                     "Train selection uses a disk cap; actual minimum is checked against it after the complete census.",
                     "Validation and test remain exhaustive; at most one evaluation split is packed at a time."])
    atomic_json(root / "projection.json", result)
    return result
