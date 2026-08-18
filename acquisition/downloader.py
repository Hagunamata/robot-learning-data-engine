"""Selective, file-granular acquisition from the Hugging Face Hub, gated by the guard.

DROID datasets in LeRobot v3.0 aggregate many episodes into a single file, so the atomic
acquisition unit is the file, not the episode. The version is detected from the source's
own meta/info.json at runtime, so a v2.0 (per-episode) source would also work here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from huggingface_hub import HfApi, hf_hub_download

from .config import Source
from .logging_utils import log_event
from .storage_guard import BYTES_PER_GB, StorageGuard

# File-class ordering: meta first (tiny, needed to detect the format), then the small
# low-dim data parquet, then the bulky videos last.
_META, _DATA, _VIDEO, _OTHER = "meta", "data", "video", "other"


@dataclass
class AcquireSummary:
    source_id: str
    hf_repo: str
    dry_run: bool
    codebase_version_detected: Optional[str]
    files_pulled: int
    bytes_pulled: int
    stopped_at: Optional[str]  # path of the first file that did not fit, if any

    @property
    def gb_pulled(self) -> float:
        return round(self.bytes_pulled / BYTES_PER_GB, 3)


def _classify(path: str) -> str:
    if path.startswith("meta/"):
        return _META
    if path.startswith("data/"):
        return _DATA
    if path.startswith("videos/"):
        return _VIDEO
    return _OTHER


def _size_of(repo_file) -> int:
    """Best-effort byte size of a RepoFile (LFS entries carry size under .lfs)."""
    size = getattr(repo_file, "size", None)
    if size:
        return int(size)
    lfs = getattr(repo_file, "lfs", None)
    if lfs is not None and getattr(lfs, "size", None):
        return int(lfs.size)
    return 0


def acquire(source: Source, guard: StorageGuard, *, dry_run: bool = False) -> AcquireSummary:
    """Stream ``source``'s files into ``data/raw/<id>/`` under the storage guard.

    Files are pulled meta -> data -> video; acquisition stops before the first file that
    would exceed the budget rather than overshooting. In ``dry_run`` no data is written.
    """
    api = HfApi()
    dest = guard.data_root / "raw" / source.id
    log_event(
        "acquire_start",
        source=source.id,
        hf_repo=source.hf_repo,
        revision=source.revision,
        dry_run=dry_run,
        budget_gb=guard.budget_gb,
        max_episodes=source.max_episodes,
    )

    # Downloaded to the HF cache (not data_root), so it never counts against budget.
    info_local = hf_hub_download(
        source.hf_repo, "meta/info.json", repo_type="dataset", revision=source.revision
    )
    info = json.loads(Path(info_local).read_text(encoding="utf-8"))
    detected = info.get("codebase_version")
    log_event(
        "info_detected",
        source=source.id,
        codebase_version=detected,
        total_episodes=info.get("total_episodes"),
        total_frames=info.get("total_frames"),
        total_tasks=info.get("total_tasks"),
    )
    if source.codebase_version and detected and detected != source.codebase_version:
        log_event(
            "codebase_version_mismatch",
            source=source.id,
            expected=source.codebase_version,
            detected=detected,
            note="proceeding with the detected version (detect_codebase_version)",
        )

    tree = api.list_repo_tree(
        source.hf_repo, repo_type="dataset", recursive=True, revision=source.revision
    )
    files = [f for f in tree if type(f).__name__ == "RepoFile"]
    order = {_META: 0, _DATA: 1, _VIDEO: 2, _OTHER: 3}
    files.sort(key=lambda f: (order[_classify(f.path)], f.path))

    # Gating uses a PROJECTED cumulative total (baseline on disk + accepted file sizes
    # from Hub metadata). This is deterministic and lets --dry-run faithfully predict the
    # stop point without downloading. Real-mode logging still reports true on-disk usage
    # via guard.log_usage() (which re-measures the disk).
    baseline = guard.used_bytes()
    pulled = 0
    bytes_pulled = 0
    stopped_at: Optional[str] = None
    for f in files:
        size = _size_of(f)
        if baseline + bytes_pulled + size > guard.budget_bytes:
            stopped_at = f.path
            guard.log_usage(
                "budget_reached",
                source=source.id,
                next_file=f.path,
                next_bytes=size,
                projected_bytes=baseline + bytes_pulled + size,
            )
            break
        if dry_run:
            log_event("would_pull", source=source.id, path=f.path, bytes=size)
        else:
            hf_hub_download(
                source.hf_repo,
                filename=f.path,
                repo_type="dataset",
                revision=source.revision,
                local_dir=str(dest),
            )
            guard.log_usage("pulled", source=source.id, path=f.path, bytes=size)
        pulled += 1
        bytes_pulled += size

    summary = AcquireSummary(
        source_id=source.id,
        hf_repo=source.hf_repo,
        dry_run=dry_run,
        codebase_version_detected=detected,
        files_pulled=pulled,
        bytes_pulled=bytes_pulled,
        stopped_at=stopped_at,
    )
    log_event(
        "acquire_done",
        source=source.id,
        dry_run=dry_run,
        files_pulled=summary.files_pulled,
        gb_pulled=summary.gb_pulled,
        stopped_at=summary.stopped_at,
        codebase_version_detected=summary.codebase_version_detected,
    )
    return summary
