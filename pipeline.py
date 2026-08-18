"""End-to-end pipeline orchestrator — what `make demo` runs."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from acquisition.config import load_sources
from acquisition.logging_utils import log_event
from acquisition.storage_guard import StorageGuard
from catalog import CatalogWriter, build_record
from catalog.record import compute_task_distribution
from data_generator import augment_dataset
from ingest.config import load_quality_gates
from ingest.curate import run_validation
from spark.jobs.signal_gates import Calibration


def _git_commit() -> Optional[str]:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return None


def merge_datasets(base_curated: str | Path, synth_curated: str | Path, out_root: str | Path) -> dict:
    """Concatenate a synthetic curated dataset onto a base one (renumbering episodes)."""
    base, synth, out_root = Path(base_curated), Path(synth_curated), Path(out_root)
    if out_root.exists():
        shutil.rmtree(out_root)
    (out_root / "meta").mkdir(parents=True)
    (out_root / "data" / "chunk-000").mkdir(parents=True)

    base_tbl = pa.concat_tables([pq.read_table(f) for f in sorted((base / "data").rglob("*.parquet"))])
    tables = [base_tbl]
    synth_files = sorted((synth / "data").rglob("*.parquet"))
    if synth_files:
        synth_tbl = pa.concat_tables([pq.read_table(f) for f in synth_files])
        offset = int(max(base_tbl.column("episode_index").to_pylist())) + 1
        new_ep = np.asarray(synth_tbl.column("episode_index").to_pylist()) + offset
        synth_tbl = synth_tbl.set_column(
            synth_tbl.column_names.index("episode_index"),
            "episode_index",
            pa.array(new_ep.tolist(), type=pa.int64()),
        )
        tables.append(synth_tbl)
    # Real DROID parquet carries extra columns (next.reward/next.done/index) that
    # synthetic episodes lack — union the schemas, null-filling the missing columns.
    merged = pa.concat_tables(tables, promote_options="permissive")
    pq.write_table(merged, out_root / "data" / "chunk-000" / "file-000.parquet")

    info = json.loads((base / "meta" / "info.json").read_text(encoding="utf-8"))
    info["total_episodes"] = len(set(merged.column("episode_index").to_pylist()))
    info["total_frames"] = merged.num_rows
    (out_root / "meta" / "info.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    for name in ("tasks.parquet", "tasks.jsonl", "stats.json"):
        if (base / "meta" / name).exists():
            shutil.copy2(base / "meta" / name, out_root / "meta" / name)
    if (base / "meta" / "episodes").exists():
        shutil.copytree(base / "meta" / "episodes", out_root / "meta" / "episodes")
    if (base / "videos").exists():
        shutil.copytree(base / "videos", out_root / "videos")
    return {"episodes": info["total_episodes"], "frames": info["total_frames"]}


def run_pipeline(
    source: str,
    *,
    data_root: str | Path = "./data",
    engine: str = "spark",
    catalog_backend: str = "sqlite",
    catalog_dsn: str | None = None,
    do_augment: bool = True,
    target_per_task: int = 3,
    max_new_episodes: int = 20,
    config_path: str = "config/sources.yaml",
    gates_path: str = "config/quality_gates.yaml",
) -> dict:
    root = Path(data_root)
    cfg = load_sources(config_path)
    src = cfg.get(source)
    gates = load_quality_gates(gates_path)
    guard = StorageGuard(root, budget_gb=cfg.storage_budget_gb)
    git = _git_commit()
    writer = CatalogWriter(catalog_backend, catalog_dsn)
    log_event("pipeline_start", source=source, engine=engine, catalog_backend=catalog_backend, git_commit=git)

    raw_root = root / "raw" / source
    if not raw_root.exists():
        from acquisition.downloader import acquire  # lazy: needs huggingface_hub
        acquire(src, guard)
    else:
        log_event("acquire_skipped", source=source, reason="raw already present")

    vres = run_validation(source, guard, data_root=root, gates=gates, engine=engine)
    if vres.schema_action != "ready":
        log_event("pipeline_stopped", source=source, schema_action=vres.schema_action)
        return {"schema_action": vres.schema_action}
    curated = root / "curated" / source

    rec_real = build_record(
        f"v0.1.0-{source}", source, curated,
        hf_repo=src.hf_repo, license=src.license, gate_pass_rate=vres.gate_pass_rate,
        git_commit=git, notes="real",
    )
    writer.record_version(rec_real)
    versions = [rec_real.dataset_version]

    if do_augment:
        calib_path = root / "calibration" / f"{gates.signal.calibrate_from or source}.json"
        calibration = Calibration.from_file(calib_path)
        synth_id = f"{source}-synth"
        info = augment_dataset(
            curated, root / "raw" / synth_id, calibration,
            target_per_task=target_per_task, max_new_episodes=max_new_episodes,
        )
        if info["generated"] > 0:
            vres_s = run_validation(synth_id, guard, data_root=root, gates=gates, engine=engine)
            aug_root = root / "curated" / f"{source}-aug"
            merge_datasets(curated, root / "curated" / synth_id, aug_root)
            total = vres.total_episodes + vres_s.total_episodes
            combined_rate = round((vres.passed_episodes + vres_s.passed_episodes) / total, 4) if total else None
            rec_aug = build_record(
                f"v0.2.0-{source}-aug", source, aug_root,
                hf_repo=src.hf_repo, license=src.license, gate_pass_rate=combined_rate,
                git_commit=git, notes=f"real+synthetic ({info['generated']} synthetic episodes)",
            )
            writer.record_version(rec_aug)
            versions.append(rec_aug.dataset_version)
        else:
            log_event("augment_skipped", source=source, reason="no under-represented tasks under target")

    log_event("pipeline_done", source=source, versions=versions)
    return {"versions": versions}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeline", description=__doc__)
    p.add_argument("--source", default="droid-100")
    p.add_argument("--engine", choices=["spark", "local"], default="spark")
    p.add_argument("--catalog-backend", choices=["postgres", "sqlite"], default="sqlite")
    p.add_argument("--catalog-dsn", default=None)
    p.add_argument("--no-augment", action="store_true")
    p.add_argument("--target-per-task", type=int, default=3)
    p.add_argument("--max-new-episodes", type=int, default=20)
    args = p.parse_args(argv)
    run_pipeline(
        args.source,
        engine=args.engine,
        catalog_backend=args.catalog_backend,
        catalog_dsn=args.catalog_dsn,
        do_augment=not args.no_augment,
        target_per_task=args.target_per_task,
        max_new_episodes=args.max_new_episodes,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
