"""Shared test fixtures: build tiny LeRobot v3.0-shaped datasets with PyArrow."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from spark.jobs.signal_gates import Calibration

FPS = 10
IMG = "observation.images.exterior_image_1_left"


def clean_episode(T: int = 60):
    t = np.arange(T)
    ts = (t / FPS).astype(np.float32)
    state = np.stack([np.sin(t / 5 + d) for d in range(7)], axis=1)
    action = np.stack([np.cos(t / 6 + d) for d in range(7)], axis=1)
    return state, action, ts


def make_calibration() -> Calibration:
    """Unit center/scale; frame-score thresholds 6 (jerk) / 5 (action)."""
    return Calibration(
        source="test", n_frames=10_000, anomaly_percentile=99.9,
        jerk_center=[0.0] * 14, jerk_scale=[1.0] * 14,
        action_center=[0.0] * 7, action_scale=[1.0] * 7,
        jerk_score_threshold=6.0, action_score_threshold=5.0,
    )


def _list_col(arr: np.ndarray) -> pa.Array:
    return pa.array([row.tolist() for row in arr], type=pa.list_(pa.float32()))


def build_dataset(
    root: Path,
    episodes: list[tuple],
    task_indices: list[int] | None = None,
    task_names: dict[int, str] | None = None,
) -> Path:
    """Write a v3.0-shaped LeRobot dataset aggregating `episodes`.

    `task_indices[i]` is the task for episode i (default all 0); `task_names` maps
    task_index -> string (default task_<idx>).
    """
    root = Path(root)
    task_indices = task_indices if task_indices is not None else [0] * len(episodes)
    used = sorted(set(task_indices))
    task_names = task_names or {i: f"task_{i}" for i in used}

    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "data" / "chunk-000").mkdir(parents=True, exist_ok=True)

    states, actions, tss, eps, fidx, tidx = [], [], [], [], [], []
    for ep_i, (s, a, ts) in enumerate(episodes):
        states.append(s)
        actions.append(a)
        tss.append(ts)
        eps.extend([ep_i] * len(ts))
        fidx.extend(range(len(ts)))
        tidx.extend([task_indices[ep_i]] * len(ts))
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
    pq.write_table(
        pa.table(
            {"task_index": list(task_names.keys()), "task": list(task_names.values())}
        ),
        root / "meta" / "tasks.parquet",
    )
    return root
