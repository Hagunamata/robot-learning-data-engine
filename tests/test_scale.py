"""Tests for the M6 scale runner — the measured process-and-evict invariant + resume.

Seeded synthetic multi-file source; network-free and deterministic.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from scale import run_synthetic_scale

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
