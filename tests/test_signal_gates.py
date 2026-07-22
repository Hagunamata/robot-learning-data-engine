"""Tests for the M4 signal gates + curation/eviction (no JVM; local engine).

The pure metric core and the `local` engine are exercised directly. The `spark` engine
shares the identical core and is verified on Ubuntu (needs Java + pyspark).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingest.config import (
    AnnotationGates,
    QualityGates,
    SchemaGates,
    SignalGates,
)
from ingest.curate import curate_passing, run_validation
from spark.jobs.signal_gates import (
    Calibration,
    calibrate_local,
    compute_episode_metrics,
    evaluate,
    run_local,
)
from acquisition.storage_guard import StorageGuard

FPS = 10
IMG = "observation.images.exterior_image_1_left"


def make_calibration() -> Calibration:
    """Unit center/scale over 14 jerk dims (state 7 + action 7) and 7 action dims,
    with frame-score thresholds of 6 (jerk) / 5 (action). A clean sinusoid scores well
    under these; a single spike scores far above, so it flags anomalous frames."""
    return Calibration(
        source="test",
        n_frames=10_000,
        anomaly_percentile=99.9,
        jerk_center=[0.0] * 14,
        jerk_scale=[1.0] * 14,
        action_center=[0.0] * 7,
        action_scale=[1.0] * 7,
        jerk_score_threshold=6.0,
        action_score_threshold=5.0,
    )


def clean_episode(T: int = 60):
    t = np.arange(T)
    ts = t / FPS
    state = np.stack([np.sin(t / 5 + d) for d in range(7)], axis=1)
    action = np.stack([np.cos(t / 6 + d) for d in range(7)], axis=1)
    return state, action, ts


def make_thresholds() -> SignalGates:
    return SignalGates(anomaly_percentile=99.9, max_anomalous_frame_ratio=0.01, max_missing_frame_ratio=0.02)


# --- pure core (z-scored against global calibration) -----------------------
def test_clean_episode_passes() -> None:
    state, action, ts = clean_episode()
    m = compute_episode_metrics(0, state, action, ts, FPS, make_calibration())
    passed, reasons = evaluate(m, make_thresholds())
    assert passed, (reasons, m)
    assert m.missing_frame_ratio == 0.0


def test_jerk_spike_fails() -> None:
    state, action, ts = clean_episode()
    action = action.copy()
    action[len(action) // 2, 0] += 50.0  # discontinuity -> huge 3rd difference
    m = compute_episode_metrics(1, state, action, ts, FPS, make_calibration())
    passed, reasons = evaluate(m, make_thresholds())
    assert not passed
    assert any("jerk" in r for r in reasons)


def test_action_outlier_fails() -> None:
    state, action, ts = clean_episode()
    action = action.copy()
    action[10, 3] = 500.0  # gross outlier
    m = compute_episode_metrics(2, state, action, ts, FPS, make_calibration())
    passed, reasons = evaluate(m, make_thresholds())
    assert not passed
    assert any("action-anomalous" in r for r in reasons)


def test_missing_frames_fails() -> None:
    # 3 frames spanning 1.0s at 10 fps => expected ~11 frames => big missing ratio
    state = np.zeros((3, 7))
    action = np.zeros((3, 7))
    ts = np.array([0.0, 0.1, 1.0])
    m = compute_episode_metrics(3, state, action, ts, FPS, make_calibration())
    passed, reasons = evaluate(m, make_thresholds())
    assert not passed
    assert any("missing-frame" in r for r in reasons)


def test_short_episode_does_not_crash() -> None:
    state = np.zeros((2, 7))
    action = np.zeros((2, 7))
    ts = np.array([0.0, 0.1])
    m = compute_episode_metrics(4, state, action, ts, FPS, make_calibration())
    assert m.jerk_anomalous_ratio == 0.0  # cannot take 3rd difference of 2 frames


def test_calibrate_local_shapes(tmp_path: Path) -> None:
    build_dataset(tmp_path / "cal", [clean_episode(), clean_episode(), clean_episode()])
    calib = calibrate_local(tmp_path / "cal", source="cal", anomaly_percentile=99.9)
    assert len(calib.jerk_center) == 14 and len(calib.jerk_scale) == 14
    assert len(calib.action_center) == 7 and len(calib.action_scale) == 7
    assert calib.n_frames > 0
    assert np.isfinite(calib.jerk_score_threshold)
    assert np.isfinite(calib.action_score_threshold)


# --- fixtures for engine/curation tests ------------------------------------
def _list_col(arr: np.ndarray) -> pa.Array:
    return pa.array([row.tolist() for row in arr], type=pa.list_(pa.float32()))


def build_dataset(root: Path, episodes: list[tuple]) -> Path:
    """Write a v3.0-shaped LeRobot dataset aggregating the given episodes."""
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "data" / "chunk-000").mkdir(parents=True, exist_ok=True)

    states, actions, tss, eps, fidx, tidx = [], [], [], [], [], []
    for ep_i, (s, a, ts) in enumerate(episodes):
        states.append(s)
        actions.append(a)
        tss.append(ts)
        eps.extend([ep_i] * len(ts))
        fidx.extend(range(len(ts)))
        tidx.extend([0] * len(ts))
    table = pa.table(
        {
            "observation.state": _list_col(np.vstack(states)),
            "action": _list_col(np.vstack(actions)),
            "timestamp": pa.array(np.concatenate(tss), type=pa.float32()),
            "episode_index": pa.array(eps, type=pa.int64()),
            "frame_index": pa.array(fidx, type=pa.int64()),
            "task_index": pa.array(tidx, type=pa.int64()),
        }
    )
    pq.write_table(table, root / "data" / "chunk-000" / "file-000.parquet")

    info = {
        "codebase_version": "v3.0",
        "fps": FPS,
        "features": {
            "observation.state": {"dtype": "float32", "shape": [7]},
            "action": {"dtype": "float32", "shape": [7]},
            "task_index": {"dtype": "int64", "shape": [1]},
            "timestamp": {"dtype": "float32", "shape": [1]},
            IMG: {"dtype": "video", "shape": [180, 320, 3]},
        },
    }
    (root / "meta" / "info.json").write_text(json.dumps(info))
    pq.write_table(pa.table({"task_index": [0], "task": ["do it"]}), root / "meta" / "tasks.parquet")
    return root


def make_gates() -> QualityGates:
    return QualityGates(
        schema=SchemaGates(
            required_features=["observation.state", "action"],
            require_at_least_one_image=True,
            image_keys_any_of=[IMG],
        ),
        annotation=AnnotationGates(require_language_instruction=True),
        signal=make_thresholds(),
    )


# --- local engine ----------------------------------------------------------
def test_run_local_reports_pass_and_fail(tmp_path: Path) -> None:
    clean = clean_episode()
    s, a, ts = clean_episode()
    a = a.copy()
    a[30, 0] += 50.0  # jerky
    ds = build_dataset(tmp_path / "ds", [clean, (s, a, ts)])

    report = run_local(ds, make_thresholds(), make_calibration())
    assert report.total == 2
    assert report.passed == 1 and report.failed == 1
    assert report.pass_rate == 0.5
    verdict_by_ep = {v.episode_index: v for v in report.verdicts}
    assert verdict_by_ep[0].passed
    assert not verdict_by_ep[1].passed


# --- curation + full validation stage --------------------------------------
def test_curate_passing_filters_episodes(tmp_path: Path) -> None:
    build_dataset(tmp_path / "raw", [clean_episode(), clean_episode()])
    stats = curate_passing(tmp_path / "raw", tmp_path / "curated", passing=[0])
    assert stats["episodes"] == 1
    table = pq.read_table(tmp_path / "curated" / "data" / "chunk-000" / "file-000.parquet")
    assert set(table.column("episode_index").to_pylist()) == {0}
    info = json.loads((tmp_path / "curated" / "meta" / "info.json").read_text())
    assert info["total_episodes"] == 1


def test_run_validation_end_to_end_local(tmp_path: Path) -> None:
    s, a, ts = clean_episode()
    a = a.copy()
    a[30, 0] += 50.0  # episode 1 is jerky -> should fail and be excluded
    build_dataset(tmp_path / "raw" / "droid-100", [clean_episode(), (s, a, ts)])

    # Pre-place a clean (unit-std) calibration artifact so the gate is deterministic and
    # the jerky episode is not allowed to inflate its own baseline.
    make_calibration().to_file(tmp_path / "calibration" / "droid-100.json")

    guard = StorageGuard(tmp_path, budget_gb=10.0)
    res = run_validation(
        "droid-100", guard, data_root=tmp_path, gates=make_gates(), engine="local"
    )

    assert res.schema_action == "ready"
    assert res.total_episodes == 2 and res.passed_episodes == 1
    assert res.gate_pass_rate == 0.5
    # curated has only the passing episode; raw was evicted
    curated = tmp_path / "curated" / "droid-100" / "data" / "chunk-000" / "file-000.parquet"
    assert set(pq.read_table(curated).column("episode_index").to_pylist()) == {0}
    assert not (tmp_path / "raw" / "droid-100").exists()
    assert res.raw_bytes_evicted > 0
    rejects = json.loads((tmp_path / "quarantine" / "droid-100" / "episode_rejects.json").read_text())
    assert "1" in rejects["episodes"]
