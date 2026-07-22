"""Smoke test for the storage guard (no network, stdlib only).

Exercises the core M2 invariant: the guard measures real on-disk usage, refuses an
admission that would exceed the budget, and frees bytes on eviction. Run with:

    python -m pytest tests/test_storage_guard.py
"""

from __future__ import annotations

from pathlib import Path

import pytest

from acquisition.storage_guard import (
    BYTES_PER_GB,
    StorageBudgetExceeded,
    StorageGuard,
)


def _write(path: Path, nbytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"\0" * nbytes)


def test_empty_root_uses_zero(tmp_path: Path) -> None:
    guard = StorageGuard(tmp_path, budget_gb=1.0)
    assert guard.used_bytes() == 0
    assert guard.remaining_bytes() == guard.budget_bytes


def test_used_bytes_counts_files_recursively(tmp_path: Path) -> None:
    guard = StorageGuard(tmp_path, budget_gb=1.0)
    _write(tmp_path / "raw" / "a.bin", 1000)
    _write(tmp_path / "raw" / "sub" / "b.bin", 2000)
    assert guard.used_bytes() == 3000


def test_can_admit_respects_budget(tmp_path: Path) -> None:
    # tiny budget expressed in bytes-as-GiB to keep the test fast
    guard = StorageGuard(tmp_path, budget_gb=10_000 / BYTES_PER_GB)
    assert guard.budget_bytes == 10_000
    _write(tmp_path / "raw" / "a.bin", 6_000)
    assert guard.can_admit(4_000) is True      # 6000 + 4000 == 10000, fits exactly
    assert guard.can_admit(4_001) is False     # would exceed


def test_admit_raises_when_over_budget(tmp_path: Path) -> None:
    guard = StorageGuard(tmp_path, budget_gb=10_000 / BYTES_PER_GB)
    _write(tmp_path / "raw" / "a.bin", 9_000)
    guard.admit(1_000, label="fits")           # no raise
    with pytest.raises(StorageBudgetExceeded):
        guard.admit(2_000, label="too-big")


def test_evict_frees_bytes(tmp_path: Path) -> None:
    guard = StorageGuard(tmp_path, budget_gb=1.0)
    target = tmp_path / "raw" / "droid-100"
    _write(target / "videos" / "cam.mp4", 5_000)
    assert guard.used_bytes() == 5_000
    freed = guard.evict(target)
    assert freed == 5_000
    assert guard.used_bytes() == 0
    assert not target.exists()
