"""Full execution behind measured performance and storage gates."""
import argparse
import fcntl
from contextlib import contextmanager
import json
import os
import shutil
import time
from pathlib import Path
from .download import inventory,atomic_json
from .engine import verify_series
from .storage import Manifest
from .windows import build_census,select_training,pack_windows
from .model import train,evaluate,verify_adapter


def verify_full(root):
    root=Path(root);inv=inventory(root);coverage=[]
    enriched=root/'inventory_enriched.json'
    expected={x['path']:x for x in json.loads(enriched.read_text())['files']} if enriched.exists() else {}
    for entry in inv['files']:
        day=entry['path'].split('date=')[1][:10];out=root/'full'/day
        p=out/'ingest_report.json'
        if not p.exists():continue
        report=json.loads(p.read_text())
        identity=json.loads((out/'source.json').read_text())
        from . import REVISION
        if identity['revision']!=REVISION or identity['source']!=entry:raise ValueError('Coverage source identity mismatch')
        if entry['path'] in expected:
            row=expected[entry['path']]
            if report['declared_rows']!=row['rows'] or report['row_groups']!=row['row_groups']:
                raise ValueError('Original footer count mismatch')
        m=Manifest(out/'parts')
        m.verify()
        commits=[json.loads(r[0]) for r in m.db.execute('SELECT metadata FROM commits')]
        rows=sum(c['rows'] for c in commits)
        groups=sum(c['last_group_exclusive']-c['first_group'] for c in commits)
        if report['complete'] and rows==report['declared_rows'] and groups==report['row_groups']:
            coverage.append(dict(path=entry['path'],bytes=entry['bytes'],rows=rows,row_groups=groups,
                invalid_or_missing_key=sum(c['missing_key'] for c in commits)))
        m.db.close()
    r=dict(files_confirmed=len(coverage),files_expected=164,bytes_confirmed=sum(x['bytes'] for x in coverage),
        rows=sum(x['rows'] for x in coverage),coverage=coverage,data_complete=len(coverage)==164)
    atomic_json(root/'coverage.json',r);return r


class Mission:
    def __init__(self, root, plan, hours, extra_hours):
        self.root = root
        self.path = root / "mission.json"
        now = time.time()
        if self.path.exists():
            self.data = json.loads(self.path.read_text())
            if self.data["budget_hours"] != hours:
                raise ValueError("Use the original budget and --extra-hours to extend a mission")
            previous = self.data["attempts"][-1]
            if "ended_utc" not in previous:
                # Last durable heartbeat is a lower bound on activity before a hard interruption.
                previous.update(ended_utc=previous["heartbeat_utc"], interrupted=True)
            if extra_hours:
                self.data["extensions"].append(dict(hours=extra_hours, utc=now))
        else:
            if extra_hours:
                raise ValueError("--extra-hours requires an existing mission")
            self.data = dict(started_utc=now, budget_hours=hours, extensions=[], attempts=[],
                             revision=plan["revision"], benchmark=plan["benchmark_audit"],
                             preparation=plan["preflight_receipt"])
        attempts = self.data["attempts"]
        used = sum(a["ended_utc"] - a["started_utc"] for a in attempts)
        self.pause_seconds = sum(max(0, b["started_utc"] - a["ended_utc"])
                                 for a, b in zip(attempts, attempts[1:]))
        if attempts:
            self.pause_seconds += max(0, now - attempts[-1]["ended_utc"])
        self.deadline = now + 3600 * (hours + sum(x["hours"] for x in self.data["extensions"])) - used
        self.attempt = dict(started_utc=now, heartbeat_utc=now, pid=os.getpid(), stages=[])
        attempts.append(self.attempt)
        self.save()

    def save(self):
        self.attempt["heartbeat_utc"] = time.time()
        self.data["paused_seconds"] = self.pause_seconds
        atomic_json(self.path, self.data)

    @contextmanager
    def stage(self, name):
        if time.time() >= self.deadline:
            raise TimeoutError(f"Execution budget exhausted before {name}")
        stage = dict(name=name, started_utc=time.time())
        self.attempt["stages"].append(stage)
        self.save()
        atomic_json(self.root / "full_status.json", dict(state=name, pid=os.getpid(),
            started_utc=self.data["started_utc"], deadline=self.deadline))
        try:
            yield
            stage["complete"] = True
        finally:
            stage["ended_utc"] = time.time()
            stage["seconds"] = stage["ended_utc"] - stage["started_utc"]
            self.save()

    def heartbeat(self):
        self.save()


def check_inventory(root, entries):
    meta = json.loads((root / "inventory_enriched.json").read_text())
    from . import REVISION
    if meta["revision"] != REVISION or meta["file_count"] != 164:
        raise ValueError("Missing complete pinned footer inventory")
    fields = ("path", "bytes", "sha256", "ordinal", "oid")
    normalized = [{k: e[k] for k in fields} for e in meta["files"]]
    if normalized != entries or sum(e["rows"] for e in meta["files"]) != meta["declared_rows"]:
        raise ValueError("Pinned inventory and Parquet footers disagree")


def evaluate_split(root, split, model, mission, reserve_bytes, calibration=None):
    from .storage import digest
    output = root / "full_evaluation" / split
    pack = root / "full_packs" / split
    receipt = output / "verified.json"
    model_identity = digest(root / "full_model" / "adapter" / "adapter_model.safetensors")
    index_identity = digest(root / "full_census" / "index.parquet")
    if receipt.exists():
        saved = json.loads(receipt.read_text())
        if saved["adapter_sha256"] != model_identity or saved["index_sha256"] != index_identity:
            raise ValueError("Evaluation inputs changed")
        if any(digest(output / name) != sha for name, sha in saved["outputs"].items()):
            raise ValueError(f"Corrupt completed {split} evaluation")
    else:
        with mission.stage(f"packing_{split}"):
            pack_windows(root / "full_census", pack, split, deadline=mission.deadline, reserve_bytes=reserve_bytes)
        with mission.stage(f"evaluation_{split}"):
            result = evaluate(pack, model, output, calibration=calibration, deadline=mission.deadline)
            files = ["errors.parquet", "asset_id.parquet", "condition_id.parquet", "episodes.parquet", "evaluation.json"]
            hashes = {}
            for name in files:
                with (output / name).open("rb") as stream:
                    os.fsync(stream.fileno())
                hashes[name] = digest(output / name)
            saved = dict(result=result, adapter_sha256=model_identity, index_sha256=index_identity,
                         pack=json.loads((pack / "pack.json").read_text()), outputs=hashes)
            atomic_json(receipt, saved)
    if pack.exists():
        shutil.rmtree(pack)
    return saved["result"]


def execute(root, projection_path, budget_hours=120, extra_hours=0):
    import math
    from . import REVISION
    root = Path(root).resolve()
    if not math.isfinite(budget_hours) or not math.isfinite(extra_hours) or budget_hours <= 0 or extra_hours < 0:
        raise ValueError("Execution budgets must be finite, with a positive initial budget")
    root.mkdir(parents=True, exist_ok=True)
    with (root / "full.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = json.loads(Path(projection_path).read_text())
        if plan.get("revision") != REVISION or plan.get("full_file_benchmarks") != 4:
            raise ValueError("FULL requires a current pinned projection using four complete benchmarks")
        existing = (root / "mission.json").exists()
        if not existing and (not plan["performance_gate"] or plan["projected_hours"] > budget_hours
                             or not plan["storage_gate"] or plan["estimated_storage_requirement"] > shutil.disk_usage(root).free):
            raise RuntimeError("FULL time or storage projection exceeds the supplied budget")
        entries = inventory(root)["files"]
        check_inventory(root, entries)
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError("The projected MOMENT throughput requires CUDA")
        if (root / "full_result.json").exists():
            return json.loads((root / "full_result.json").read_text())
        mission = Mission(root, plan, budget_hours, extra_hours)
        try:
            return run_full(root, entries, plan, mission)
        except BaseException as error:
            atomic_json(root / "full_status.json", dict(state="budget_stopped" if isinstance(error, TimeoutError) else "failed",
                pid=os.getpid(), error=repr(error)))
            raise
        finally:
            mission.attempt["ended_utc"] = time.time()
            mission.save()


def run_full(root, entries, plan, mission):
    from .parallel import run_partitions
    from .catalog import build_catalog
    with mission.stage("download_ingestion_consolidation"):
        pool = run_partitions(entries, root, tag="full", workers=plan["workers"],
            consolidators=plan["consolidators"], deadline=mission.deadline, output_group="full",
            reclaim=True, reuse=True, disk_reserve=plan["disk_reserve_bytes"],
            scratch_factor=plan["scratch_factor"], optimized=True, heartbeat=mission.heartbeat)
        if not pool["complete"]:
            if time.time() >= mission.deadline:
                raise TimeoutError("Execution budget reached; committed partitions retained")
            raise RuntimeError(f'Incomplete originals; committed results retained: {pool["errors"]}')
    with mission.stage("coverage_verification"):
        coverage = verify_full(root)
        if not coverage["data_complete"]:
            raise RuntimeError("Full input coverage not verified")
        partition_roots = [root / "full" / e["path"].split("date=")[1][:10] for e in entries]
        series = []
        for part in partition_roots:
            r = json.loads((part / "consolidated.json").read_text())
            series.extend(r["outputs"])
    with mission.stage("catalog"):
        catalog = build_catalog(partition_roots, root / "catalog")
    with mission.stage("census"):
        certificate = root / "canonical_day_certificate.json"
        from .storage import digest
        proof = json.loads(certificate.read_text()) if certificate.exists() else {}
        canonical_unique = (proof.get("disjoint_utc_day_timestamp_ranges", False)
                            and proof.get("inventory_sha256") == digest(root / "inventory_enriched.json"))
        census = build_census(series, root / "full_census", deadline=mission.deadline, canonical_unique=canonical_unique)
    counts = census["split_counts"]
    eval_seconds = (counts["validation"] + counts["test"]) / plan["evaluation_windows_per_second"]
    packing_seconds = plan["packing_seconds"] * census["eligible"] / max(1, plan["estimated_eligible_windows"])
    budget = max(0, int((mission.deadline - time.time() - eval_seconds - packing_seconds) * plan["training_windows_per_second"] / 1.2))
    free = shutil.disk_usage(root).free
    existing_train_bytes = sum(p.stat().st_size for p in (root / "full_packs/train").glob("*.npy"))
    evaluation_reserve = int(1.2 * max(counts["validation"], counts["test"]) * 2562) + 2*1024**3
    disk_cap = max(0, (free + existing_train_bytes - plan["disk_reserve_bytes"] - evaluation_reserve) // 3100)
    budget = min(budget, disk_cap, counts["train"])
    with mission.stage("selection"):
        selection = select_training(root / "full_census", budget)
        if selection["selected"] > disk_cap:
            raise OSError(f"Frozen selection needs {selection['selected']} windows; disk cap is {disk_cap}")
        tensor_bytes = dict(train=selection["selected"] * 2562, validation=counts["validation"] * 2562,
                            test=counts["test"] * 2562)
        atomic_json(root / "full_pack_budget.json", dict(windows=counts, selected=selection["selected"],
            tensor_bytes=tensor_bytes, disk_cap_windows=disk_cap, policy="train plus one evaluation split at a time",
            disk_available_bytes=shutil.disk_usage(root).free, reserve_bytes=plan["disk_reserve_bytes"]))
    with mission.stage("packing_train"):
        pack_windows(root / "full_census", root / "full_packs/train", "train", selected=True,
                     deadline=mission.deadline, reserve_bytes=plan["disk_reserve_bytes"])
    with mission.stage("training"):
        model, trained = train(root / "full_packs/train", root / "full_model", microbatch=plan["microbatch"],
            resume=(root / "full_model/checkpoint.json").exists(), deadline=mission.deadline)
        if not trained["complete"]:
            raise TimeoutError("Training budget reached; checkpoint retained")
        adapter_check = verify_adapter(root / "full_model")
    validation = evaluate_split(root, "validation", model, mission, plan["disk_reserve_bytes"])
    test = evaluate_split(root, "test", model, mission, plan["disk_reserve_bytes"], calibration=validation["calibration"])
    result = dict(complete=True, coverage=coverage, catalog=catalog, census=census, selection=selection,
        training=trained, adapter_verification=adapter_check, validation=validation, test=test,
        elapsed_seconds=time.time()-mission.data["started_utc"])
    atomic_json(root / "full_result.json", result)
    atomic_json(root / "full_status.json", dict(state="complete", pid=os.getpid(), finished_utc=time.time()))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", default="runs/mvp")
    p.add_argument("--projection", required=True)
    p.add_argument("--budget-hours", type=float, default=120)
    p.add_argument("--extra-hours", type=float, default=0)
    args = p.parse_args()
    execute(args.root, args.projection, args.budget_hours, args.extra_hours)
