"""Tests for the M6 scale runner — the measured process-and-evict invariant + resume.

Seeded synthetic multi-file source; network-free and deterministic.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scale import _calibration_fits, _source_signal_dims, run_synthetic_scale
from spark.jobs.signal_gates import Calibration, calibrate_local

PARAMS = dict(n_chunks=8, eps_per_chunk=2, frames=300)


def test_scale_invariant_holds(tmp_path: Path) -> None:
    r = run_synthetic_scale(tmp_path, seed=1, catalog_dsn=str(tmp_path / "catalog.db"), **PARAMS)
    # The headline: measured peak raw < budget, and total processed >> budget.
    assert r.peak_raw_bytes < r.budget_bytes
    assert r.total_processed_bytes > r.budget_bytes
    assert r.invariant_holds
    r.assert_invariant()  # must not raise
    # ~4x throughput vs budget (budget was set to total/4).
    assert r.total_processed_bytes / r.budget_bytes >= 3.0


def test_scale_processes_and_quarantines(tmp_path: Path) -> None:
    r = run_synthetic_scale(tmp_path, seed=1, catalog_dsn=str(tmp_path / "catalog.db"), **PARAMS)
    assert r.units_processed == PARAMS["n_chunks"]
    assert r.episodes_total == PARAMS["n_chunks"] * PARAMS["eps_per_chunk"]
    assert r.episodes_passed > 0
    assert r.episodes_quarantined > 0                      # bad episodes were flagged
    assert r.episodes_passed + r.episodes_quarantined == r.episodes_total
    # raw fully evicted; curated + catalog persisted.
    assert sum(1 for _ in (tmp_path / "raw" / "synthetic").rglob("*.parquet")) == 0
    assert (tmp_path / "curated" / "v0.0.0-synthetic-scale" / "meta" / "info.json").exists()
    con = sqlite3.connect(tmp_path / "catalog.db")
    n = con.execute("SELECT COUNT(*) FROM dataset_version").fetchone()[0]
    con.close()
    assert n == 1
    # quarantine keeps only reasons + tiny samples, well under the sub-cap.
    q = tmp_path / "quarantine" / "v0.0.0-synthetic-scale"
    assert (q / "episode_rejects.jsonl").exists()
    qsize = sum(f.stat().st_size for f in q.rglob("*") if f.is_file())
    assert qsize < r.budget_bytes * 0.05 + 4096


def test_scale_idempotent_resume(tmp_path: Path) -> None:
    first = run_synthetic_scale(tmp_path, seed=1, catalog_dsn=str(tmp_path / "catalog.db"), **PARAMS)
    # Re-run against the same data_root: every unit is already in the manifest.
    second = run_synthetic_scale(tmp_path, seed=1, catalog_dsn=str(tmp_path / "catalog.db"), **PARAMS)
    assert second.total_processed_bytes == first.total_processed_bytes   # no double counting
    assert second.units_processed == first.units_processed
    assert second.episodes_passed == first.episodes_passed


# --- schema-aware calibration (network-free) -------------------------------
# Guards the droid-100 (joint-space 7+7) vs droid-slice (cartesian 8+7) mismatch that
# broke Step 8: a calibration must be rejected/rebuilt when its signal width differs.

def _write_episode_parquet(dest: Path, *, state_dim: int, action_dim: int, episodes: int = 3) -> None:
    rng = np.random.default_rng(0)
    (dp := dest / "data" / "chunk-000").mkdir(parents=True, exist_ok=True)
    rows = []
    for ep in range(episodes):
        for t in range(60):
            rows.append({
                "episode_index": ep,
                "observation.state": list(rng.normal(size=state_dim)),
                "action": list(rng.normal(size=action_dim)),
                "timestamp": t / 15.0,
            })
    pq.write_table(pa.Table.from_pylist(rows), dp / f"episode_{0:06d}.parquet")


def test_source_signal_dims_reads_feature_shapes() -> None:
    info = {"features": {"observation.state": {"shape": [8]}, "action": {"shape": [7]}}}
    assert _source_signal_dims(info) == (15, 7)
    # a source with no state feature falls back to action-only dims
    assert _source_signal_dims({"features": {"action": {"shape": [7]}}}) == (7, 7)


def test_calibration_fits_detects_schema_mismatch(tmp_path: Path) -> None:
    # A joint-space dev calibration (7+7 -> 14) must NOT fit a cartesian slice (8+7 -> 15).
    _write_episode_parquet(tmp_path / "dev", state_dim=7, action_dim=7)
    dev = calibrate_local(tmp_path / "dev", source="droid-100")
    assert _calibration_fits(dev, 14, 7)
    assert not _calibration_fits(dev, 15, 7)


def test_calibrate_local_matches_slice_schema(tmp_path: Path) -> None:
    # Recalibrating from the slice's own 8+7 episodes yields a fitting 15/7 calibration.
    _write_episode_parquet(tmp_path / "slice", state_dim=8, action_dim=7)
    calib = calibrate_local(tmp_path / "slice", source="droid-slice")
    assert len(calib.jerk_center) == 15 and len(calib.action_center) == 7
    assert _calibration_fits(calib, 15, 7)
