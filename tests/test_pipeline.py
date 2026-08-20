"""End-to-end pipeline test: acquire(skip) -> validate -> catalog -> augment ->
catalog, via the local engine + sqlite catalog. No network, no JVM, no Postgres.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline import run_pipeline
from tests._helpers import build_dataset, clean_episode, make_calibration


def test_run_pipeline_end_to_end(tmp_path: Path) -> None:
    # Stage raw data so acquire is skipped; tasks are under-represented so augment fires.
    build_dataset(
        tmp_path / "raw" / "droid-100",
        [clean_episode(), clean_episode(), clean_episode()],
        task_indices=[0, 1, 1],
        task_names={0: "pick", 1: "place"},
    )
    # Deterministic gate: pre-place a clean unit calibration keyed by calibrate_from.
    make_calibration().to_file(tmp_path / "calibration" / "droid-100.json")

    db = tmp_path / "catalog.db"
    result = run_pipeline(
        "droid-100",
        data_root=tmp_path,
        engine="local",
        catalog_backend="sqlite",
        catalog_dsn=str(db),
        target_per_task=3,
        max_new_episodes=6,
    )

    # Two catalogued versions: real + augmented.
    assert result["versions"] == ["v0.1.0-droid-100", "v0.2.0-droid-100-aug"]
    con = sqlite3.connect(db)
    rows = con.execute("SELECT dataset_version, episode_count, notes FROM dataset_version ORDER BY dataset_version").fetchall()
    con.close()
    assert len(rows) == 2
    real, aug = rows
    assert real[1] == 3                    # real: 3 episodes curated
    assert aug[1] > real[1]                # augmented has more (synthetic added)
    assert "synthetic" in aug[2]

    # Curated outputs exist; raw was evicted (process-and-evict).
    assert (tmp_path / "curated" / "droid-100").exists()
    assert (tmp_path / "curated" / "droid-100-aug").exists()
    assert not (tmp_path / "raw" / "droid-100").exists()
