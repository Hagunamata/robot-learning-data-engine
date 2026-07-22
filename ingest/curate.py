"""Curation + eviction (M4): write passing episodes to curated, then evict raw.

This closes the process-and-evict loop (docs/01-conception.md §5): after the signal
gates decide which episodes pass, their frames are written to ``data/curated/<id>/``
and the raw acquired copy under ``data/raw/<id>/`` is deleted via the storage guard,
freeing budget for the next batch. Per-batch metrics (gate pass-rate, counts, storage)
are logged for the dashboard/catalog.

v3.0 note: episodes are aggregated per file, so curation *filters* the data parquet to
the passing ``episode_index`` set and updates ``meta/info.json`` counts. Videos are
copied as-is (not re-segmented to drop failed episodes' footage) — a documented M4
simplification; the curated ``meta`` records which episodes are valid. Production would
re-segment video with ffmpeg.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from acquisition.logging_utils import log_event
from acquisition.storage_guard import StorageGuard

from .canonicalize import ingest_source
from .config import QualityGates, load_quality_gates
from spark.jobs.signal_gates import Calibration, calibrate_local, run_signal_gates


@dataclass
class ValidationResult:
    source_id: str
    schema_action: str          # ready | quarantined | dropped | error
    total_episodes: int
    passed_episodes: int
    gate_pass_rate: float
    curated_frames: int
    raw_bytes_evicted: int


def curate_passing(raw_root: str | Path, curated_root: str | Path, passing: Iterable[int]) -> dict:
    """Filter the raw dataset to ``passing`` episodes and write the curated copy."""
    raw_root = Path(raw_root)
    curated_root = Path(curated_root)
    passing = sorted(set(int(p) for p in passing))

    if curated_root.exists():
        shutil.rmtree(curated_root)
    (curated_root / "meta").mkdir(parents=True)
    (curated_root / "data" / "chunk-000").mkdir(parents=True)

    # 1. Filter the aggregated data parquet to passing episodes.
    files = sorted((raw_root / "data").rglob("*.parquet"))
    table = pa.concat_tables([pq.read_table(f) for f in files])
    ep = np.asarray(table.column("episode_index").to_pylist())
    mask = np.isin(ep, passing)
    filtered = table.filter(pa.array(mask.tolist(), type=pa.bool_()))
    pq.write_table(filtered, curated_root / "data" / "chunk-000" / "file-000.parquet")

    # 2. Copy/patch meta.
    info = json.loads((raw_root / "meta" / "info.json").read_text(encoding="utf-8"))
    info["total_episodes"] = len(passing)
    info["total_frames"] = filtered.num_rows
    (curated_root / "meta" / "info.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    for name in ("tasks.parquet", "tasks.jsonl", "stats.json"):
        src = raw_root / "meta" / name
        if src.exists():
            shutil.copy2(src, curated_root / "meta" / name)
    ep_meta = raw_root / "meta" / "episodes"
    if ep_meta.exists():
        shutil.copytree(ep_meta, curated_root / "meta" / "episodes")

    # 3. Copy videos as-is (documented: not re-segmented).
    videos = raw_root / "videos"
    if videos.exists():
        shutil.copytree(videos, curated_root / "videos")

    return {"episodes": len(passing), "frames": int(filtered.num_rows)}


def get_or_build_calibration(
    calibrate_from: str,
    source_id: str,
    raw_root: Path,
    data_root: Path,
    anomaly_percentile: float,
) -> Calibration:
    """Load the persisted calibration artifact, or build it from the current raw data.

    The artifact is keyed by `calibrate_from` and reused across sources (e.g. calibrate
    on droid-100 once, reuse when gating droid-slice — droid-100 is evicted by then).
    """
    artifact = data_root / "calibration" / f"{calibrate_from}.json"
    if artifact.exists():
        log_event("calibration_loaded", calibrate_from=calibrate_from, artifact=str(artifact))
        return Calibration.from_file(artifact)
    if calibrate_from != source_id:
        log_event(
            "calibration_fallback",
            calibrate_from=calibrate_from,
            using=source_id,
            note="no persisted artifact; building from current source as a proxy",
        )
    calib = calibrate_local(raw_root, source=calibrate_from, anomaly_percentile=anomaly_percentile)
    calib.to_file(artifact)
    log_event(
        "calibration_built",
        calibrate_from=calibrate_from,
        n_frames=calib.n_frames,
        artifact=str(artifact),
    )
    return calib


def _write_episode_rejects(quarantine_root: Path, rejects: dict[int, list[str]]) -> None:
    quarantine_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "rejected_at": datetime.now(timezone.utc).isoformat(),
        "stage": "signal_gates",
        "episodes": {str(i): r for i, r in rejects.items()},
    }
    (quarantine_root / "episode_rejects.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_validation(
    source_id: str,
    guard: StorageGuard,
    data_root: str | Path = "./data",
    gates: QualityGates | None = None,
    engine: str = "spark",
) -> ValidationResult:
    """Full validation stage: schema (M3) -> signal gates -> curate -> evict raw (M4)."""
    gates = gates or load_quality_gates()
    root = Path(data_root)

    # M3 — schema gate. If it quarantines/drops, there is nothing to signal-gate.
    schema = ingest_source(source_id, data_root=root, gates=gates)
    if schema.action != "ready":
        log_event("validation_stopped", source=source_id, schema_action=schema.action)
        return ValidationResult(source_id, schema.action, 0, 0, 0.0, 0, 0)

    raw_root = root / "raw" / source_id

    # M4 — calibrate (once, reused) then signal-gate.
    calibrate_from = gates.signal.calibrate_from or source_id
    calibration = get_or_build_calibration(
        calibrate_from, source_id, raw_root, root, gates.signal.anomaly_percentile
    )
    report = run_signal_gates(raw_root, gates.signal, calibration, engine=engine)
    passing = [v.episode_index for v in report.verdicts if v.passed]
    failing = {v.episode_index: v.reasons for v in report.verdicts if not v.passed}
    log_event(
        "signal_gates_done",
        source=source_id,
        engine=engine,
        episodes_total=report.total,
        episodes_passed=report.passed,
        episodes_failed=report.failed,
        gate_pass_rate=report.pass_rate,
    )

    # Curate the passing episodes.
    curated_root = root / "curated" / source_id
    stats = curate_passing(raw_root, curated_root, passing)
    guard.log_usage("curated", source=source_id, curated_episodes=stats["episodes"], curated_frames=stats["frames"])

    if failing:
        _write_episode_rejects(root / "quarantine" / source_id, failing)

    # Evict the raw copy — process-and-evict.
    freed = guard.evict(raw_root)
    log_event("evicted_raw", source=source_id, freed_bytes=freed)
    guard.log_usage("final", source=source_id)

    # Per-batch metrics for the dashboard/catalog.
    log_event(
        "batch_metrics",
        source=source_id,
        gate_pass_rate=report.pass_rate,
        episodes_total=report.total,
        episodes_passed=report.passed,
        curated_frames=stats["frames"],
        raw_bytes_evicted=freed,
    )
    return ValidationResult(
        source_id=source_id,
        schema_action="ready",
        total_episodes=report.total,
        passed_episodes=report.passed,
        gate_pass_rate=report.pass_rate,
        curated_frames=stats["frames"],
        raw_bytes_evicted=freed,
    )
