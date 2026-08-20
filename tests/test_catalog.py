"""Tests for the catalog record builder + sqlite writer (no Postgres needed)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from catalog import CatalogWriter, build_record, compute_task_distribution
from tests._helpers import build_dataset, clean_episode


def test_task_distribution_counts_episodes_per_task(tmp_path: Path) -> None:
    build_dataset(
        tmp_path / "ds",
        [clean_episode(), clean_episode(), clean_episode()],
        task_indices=[0, 0, 1],
        task_names={0: "pick cube", 1: "open drawer"},
    )
    dist = compute_task_distribution(tmp_path / "ds")
    assert dist == {"pick cube": 2, "open drawer": 1}


def test_build_record_fields(tmp_path: Path) -> None:
    build_dataset(tmp_path / "ds", [clean_episode(), clean_episode()], task_indices=[0, 1],
                  task_names={0: "a", 1: "b"})
    rec = build_record(
        "v0.1.0-droid-100", "droid-100", tmp_path / "ds",
        hf_repo="lerobot/droid_100", license="CC-BY-4.0", gate_pass_rate=0.94,
        git_commit="abc123", notes="real",
    )
    assert rec.episode_count == 2
    assert rec.frame_count == 120  # two 60-frame episodes
    assert rec.task_distribution == {"a": 1, "b": 1}
    assert rec.bytes_on_disk > 0
    assert rec.gate_pass_rate == 0.94 and rec.git_commit == "abc123"


def test_sqlite_writer_inserts_two_versions(tmp_path: Path) -> None:
    build_dataset(tmp_path / "ds", [clean_episode()], task_names={0: "a"})
    db = tmp_path / "catalog.db"
    writer = CatalogWriter("sqlite", dsn=str(db))
    writer.record_version(build_record("v0.1.0-droid-100", "droid-100", tmp_path / "ds", license="CC-BY-4.0"))
    writer.record_version(build_record("v0.2.0-droid-100-aug", "droid-100", tmp_path / "ds", license="CC-BY-4.0", notes="real+synthetic"))

    con = sqlite3.connect(db)
    rows = con.execute("SELECT dataset_version, task_distribution, notes FROM dataset_version ORDER BY dataset_version").fetchall()
    con.close()
    assert [r[0] for r in rows] == ["v0.1.0-droid-100", "v0.2.0-droid-100-aug"]
    assert json.loads(rows[0][1]) == {"a": 1}   # task_distribution round-trips as JSON
    assert rows[1][2] == "real+synthetic"
