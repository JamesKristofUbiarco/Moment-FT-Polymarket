"""Run the release pipeline on a complete original with exploratory UTC splits."""
import json
import sys
import time
from pathlib import Path

from polymarket_mvp.download import atomic_json, download, inventory
from polymarket_mvp.engine import consolidate, process, verify_series
from polymarket_mvp.model import WindowDataset, evaluate, train, verify_adapter
from polymarket_mvp.storage import digest
from polymarket_mvp.windows import build_census, pack_windows, select_training


def main(root):
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required for the measured preflight")
    output = root / "preflight_full"
    output.mkdir(parents=True, exist_ok=True)
    entry = next(e for e in inventory(root)["files"] if "date=2026-06-06" in e["path"])
    source, network = download(entry, root / "staging")
    if digest(source) != entry["sha256"]:
        raise ValueError("Original SHA256 mismatch")
    ingested = process(entry, source, output / "partition", groups_per_unit=16,
                       batch_size=262144, dictionary_read=True)
    consolidated = consolidate(output / "partition", sparse_merge=True)
    verify_series(consolidated)
    if ingested["rows_confirmed"] != consolidated["events"] + ingested.get("missing_key", 0):
        from polymarket_mvp.storage import Manifest
        manifest = Manifest(output / "partition" / "parts")
        missing = sum(json.loads(r[0])["missing_key"] for r in manifest.db.execute("SELECT metadata FROM commits"))
        manifest.db.close()
        assert ingested["rows_confirmed"] == consolidated["events"] + missing
    splits = {"train": (1780704000, 1780788224), "validation": (1780788224, 1780789248),
              "test": (1780789248, 1780790400)}
    census = build_census(consolidated["outputs"], output / "census", splits=splits, canonical_unique=True)
    selection = select_training(output / "census", census["split_counts"]["train"])
    timings = {}
    for split in splits:
        start = time.perf_counter()
        pack_windows(output / "census", output / split, split, selected=split == "train")
        timings[split] = time.perf_counter() - start
    import numpy as np
    data = WindowDataset(output / "train")
    for i in range(len(data)):
        row = data[i]
        target = int(data.target[i])
        if (row["x"].shape != (1, 512) or row["x"][..., target:].any()
                or row["mask"][target:].any()
                or not np.array_equal(row["y"].numpy(), data.x[i, target:target+8])):
            raise ValueError(f"Invalid target masking in window {i}")
    model_dir = output / "model"
    if not (model_dir / "checkpoint.json").exists():
        _, first = train(output / "train", model_dir, microbatch=256, max_steps=20)
        assert not first["complete"]
    model, training = train(output / "train", model_dir, microbatch=256, resume=True)
    assert training["complete"]
    adapter = verify_adapter(model_dir)
    validation = evaluate(output / "validation", model, output / "evaluation_validation")
    test = evaluate(output / "test", model, output / "evaluation_test", calibration=validation["calibration"])
    atomic_json(output / "result.json", dict(complete=True, exploratory=True, source=entry, download=network,
        ingest=ingested, consolidated=consolidated, census=census, selection=selection, packing_seconds=timings,
        training=training, adapter=adapter, validation=validation, test=test))


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
