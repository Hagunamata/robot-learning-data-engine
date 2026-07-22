"""Synthetic episode augmenter (M5) — evolved from the prior repo's data_generator/.

Mints **LeRobot v3.0-format** episodes for under-represented tasks and writes them as a
normal raw dataset so they pass through the *same* schema + signal gates as real data
(no laxer path). Generation is procedural (smooth, in-distribution trajectories) — no
learned generative model in v1. Episodes are labelled synthetic in the catalog notes.

Smoothness + amplitudes are drawn within the calibration's action center/scale so the
episodes sit inside the real signal distribution and clear the gates; the point is to
*balance the task distribution*, not to imitate real robot behaviour.

See docs/01-conception.md §4.5.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from acquisition.logging_utils import log_event
from spark.jobs.signal_gates import Calibration


def under_represented_tasks(task_distribution: dict[str, int], target: int) -> dict[str, int]:
    """task -> deficit (how many episodes short of `target`)."""
    return {t: target - c for t, c in task_distribution.items() if c < target}


def _list_col(arr: np.ndarray) -> pa.Array:
    return pa.array([row.tolist() for row in arr], type=pa.list_(pa.float32()))


def _smooth(rng: np.random.Generator, n: int, center: np.ndarray, scale: np.ndarray) -> np.ndarray:
    """Smooth low-frequency trajectory: center + small-amplitude sinusoids per dim."""
    d = len(center)
    t = np.arange(n)
    freq = rng.uniform(0.01, 0.05, size=d)
    phase = rng.uniform(0, 2 * np.pi, size=d)
    amp = np.abs(scale) * rng.uniform(0.2, 0.7, size=d)
    return (center[None, :] + amp[None, :] * np.sin(freq[None, :] * t[:, None] + phase[None, :])).astype(np.float32)


def augment_dataset(
    base_curated: str | Path,
    out_root: str | Path,
    calibration: Calibration,
    *,
    target_per_task: int = 3,
    max_new_episodes: int = 20,
    episode_len_range: tuple[int, int] = (120, 260),
    seed: int = 0,
) -> dict:
    """Generate synthetic episodes for under-represented tasks in `base_curated`.

    Writes a v3.0 LeRobot dataset to `out_root` (data parquet + copied/patched meta),
    ready to be validated by the same gates. Returns a summary dict.
    """
    base = Path(base_curated)
    out_root = Path(out_root)
    info = json.loads((base / "meta" / "info.json").read_text(encoding="utf-8"))
    fps = info.get("fps") or 15
    features = info.get("features", {})
    ds = int(features.get("observation.state", {}).get("shape", [7])[0])
    da = int(features.get("action", {}).get("shape", [7])[0])

    # task <-> index maps from the base tasks table (robust to schema variants).
    from catalog.record import compute_task_distribution, read_task_map  # local import avoids a cycle

    task_to_idx = {name: idx for idx, name in read_task_map(base / "meta" / "tasks.parquet").items()}
    dist = compute_task_distribution(base)
    deficits = under_represented_tasks(dist, target_per_task)

    # Episode indices continue after the base dataset's max.
    base_table = pa.concat_tables([pq.read_table(f) for f in sorted((base / "data").rglob("*.parquet"))])
    next_ep = (max(base_table.column("episode_index").to_pylist()) + 1) if base_table.num_rows else 0

    action_center = np.asarray(calibration.action_center[:da], dtype=float)
    action_scale = np.asarray(calibration.action_scale[:da], dtype=float)
    if action_center.size < da:  # calibration shape mismatch — fall back to unit range
        action_center, action_scale = np.zeros(da), np.ones(da)

    rng = np.random.default_rng(seed)
    states, actions, tss, eps, fidx, tidx = [], [], [], [], [], []
    generated = 0
    per_task: dict[str, int] = {}
    for task, deficit in sorted(deficits.items()):
        if task not in task_to_idx:
            continue
        for _ in range(deficit):
            if generated >= max_new_episodes:
                break
            n = int(rng.integers(*episode_len_range))
            state = _smooth(rng, n, np.zeros(ds), np.ones(ds))
            action = _smooth(rng, n, action_center, action_scale)
            states.append(state)
            actions.append(action)
            tss.append((np.arange(n) / fps).astype(np.float32))
            eps.extend([next_ep] * n)
            fidx.extend(range(n))
            tidx.extend([task_to_idx[task]] * n)
            per_task[task] = per_task.get(task, 0) + 1
            next_ep += 1
            generated += 1

    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "meta").mkdir(parents=True, exist_ok=True)
    (out_root / "data" / "chunk-000").mkdir(parents=True, exist_ok=True)

    if generated:
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
        pq.write_table(table, out_root / "data" / "chunk-000" / "file-000.parquet")

    # Meta: patched info.json (only the features/fps matter for the gates) + tasks copy.
    info["total_episodes"] = generated
    info["total_frames"] = len(eps)
    (out_root / "meta" / "info.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    pq.write_table(pq.read_table(base / "meta" / "tasks.parquet"), out_root / "meta" / "tasks.parquet")

    log_event(
        "augment_generated",
        out_root=str(out_root),
        episodes=generated,
        per_task=per_task,
        under_represented=len(deficits),
        target_per_task=target_per_task,
    )
    return {"generated": generated, "per_task": per_task, "out_root": str(out_root)}
