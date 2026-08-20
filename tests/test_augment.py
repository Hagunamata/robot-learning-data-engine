"""Tests for the synthetic augmenter (no network, no JVM)."""

from __future__ import annotations

from pathlib import Path

from data_generator import augment_dataset, under_represented_tasks
from ingest.config import SignalGates
from spark.jobs.signal_gates import run_local
from tests._helpers import build_dataset, clean_episode, make_calibration


def test_under_represented_tasks() -> None:
    assert under_represented_tasks({"a": 1, "b": 3, "c": 2}, target=3) == {"a": 2, "c": 1}


def test_augment_generates_for_deficit_tasks(tmp_path: Path) -> None:
    # curated: task 'a' x1, task 'b' x2  -> at target 3, deficits a:2, b:1 = 3 episodes
    build_dataset(
        tmp_path / "curated",
        [clean_episode(), clean_episode(), clean_episode()],
        task_indices=[0, 1, 1],
        task_names={0: "a", 1: "b"},
    )
    info = augment_dataset(
        tmp_path / "curated", tmp_path / "synth", make_calibration(),
        target_per_task=3, max_new_episodes=10, seed=1,
    )
    assert info["generated"] == 3
    assert (tmp_path / "synth" / "data" / "chunk-000" / "file-000.parquet").exists()


def test_synthetic_episodes_pass_the_same_gates(tmp_path: Path) -> None:
    build_dataset(tmp_path / "curated", [clean_episode(), clean_episode()],
                  task_indices=[0, 1], task_names={0: "a", 1: "b"})
    augment_dataset(tmp_path / "curated", tmp_path / "synth", make_calibration(),
                    target_per_task=3, max_new_episodes=6, seed=2)
    # Synthetic episodes are smooth & in-distribution -> they clear the gates.
    report = run_local(tmp_path / "synth", SignalGates(), make_calibration())
    assert report.total > 0
    assert report.pass_rate == 1.0, [v.reasons for v in report.verdicts if not v.passed]
