"""Build a catalog record (one dataset version) from a curated dataset on disk."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class CatalogRecord:
    dataset_version: str
    source_id: str
    hf_repo: Optional[str]
    license: Optional[str]
    episode_count: int
    frame_count: int
    task_distribution: dict[str, int]
    gate_pass_rate: Optional[float]
    bytes_on_disk: int
    git_commit: Optional[str]
    created_at: str
    notes: Optional[str]

    def as_row(self) -> dict:
        return asdict(self)


def _bytes_on_disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


def _data_table(curated_root: Path) -> Optional[pa.Table]:
    files = sorted((curated_root / "data").rglob("*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files]) if files else None


def read_task_map(tasks_path: str | Path) -> dict[int, str]:
    """task_index -> task string. v3.0 stores it as index col ``__index_level_0__``; other exports use a ``task`` column."""
    tasks_path = Path(tasks_path)
    if not tasks_path.exists():
        return {}
    table = pq.read_table(tasks_path)
    cols = table.column_names
    if "task_index" not in cols:
        return {}
    data = table.to_pydict()
    str_col = next(
        (c for c in ("task", "__index_level_0__") if c in cols),
        next((c for c in cols if c != "task_index"), None),
    )
    names = data[str_col] if str_col else [f"task_{i}" for i in data["task_index"]]
    return {int(i): str(n) for i, n in zip(data["task_index"], names)}


def compute_task_distribution(curated_root: str | Path) -> dict[str, int]:
    """task string -> number of episodes, via each episode's task_index."""
    root = Path(curated_root)
    idx2task = read_task_map(root / "meta" / "tasks.parquet")

    table = _data_table(root)
    if table is None or "episode_index" not in table.column_names:
        return {}
    ep = table.column("episode_index").to_pylist()
    ti = table.column("task_index").to_pylist() if "task_index" in table.column_names else [0] * len(ep)
    first_task: dict[int, int] = {}
    for e, task in zip(ep, ti):
        first_task.setdefault(int(e), int(task))
    counts = Counter(idx2task.get(t, f"task_{t}") for t in first_task.values())
    return dict(counts)


def build_record(
    dataset_version: str,
    source_id: str,
    curated_root: str | Path,
    *,
    hf_repo: Optional[str] = None,
    license: Optional[str] = None,
    gate_pass_rate: Optional[float] = None,
    git_commit: Optional[str] = None,
    notes: Optional[str] = None,
    created_at: Optional[str] = None,
) -> CatalogRecord:
    root = Path(curated_root)
    table = _data_table(root)
    frame_count = int(table.num_rows) if table is not None else 0
    episode_count = (
        len(set(table.column("episode_index").to_pylist()))
        if table is not None and "episode_index" in table.column_names
        else 0
    )
    return CatalogRecord(
        dataset_version=dataset_version,
        source_id=source_id,
        hf_repo=hf_repo,
        license=license,
        episode_count=episode_count,
        frame_count=frame_count,
        task_distribution=compute_task_distribution(root),
        gate_pass_rate=gate_pass_rate,
        bytes_on_disk=_bytes_on_disk(root),
        git_commit=git_commit,
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
        notes=notes,
    )
