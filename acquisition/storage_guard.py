"""Storage guard — enforces the local disk budget and drives process-and-evict.

The budget is treated as binary GiB (1 GB = 1024**3 bytes) to match how disk capacity
is actually reported. Usage is measured from disk (ground truth), not a separate ledger,
so it stays correct even if a download is interrupted.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from .logging_utils import log_event

BYTES_PER_GB = 1024 ** 3


class StorageBudgetExceeded(RuntimeError):
    """Raised when an operation would exceed the configured storage budget."""


class StorageGuard:
    """Accounts resident bytes under ``data_root`` against a fixed budget."""

    def __init__(self, data_root: str | Path, budget_gb: float = 400.0) -> None:
        self.data_root = Path(data_root)
        self.budget_gb = float(budget_gb)
        self.budget_bytes = int(self.budget_gb * BYTES_PER_GB)
        self.data_root.mkdir(parents=True, exist_ok=True)

    # --- measurement -------------------------------------------------------
    def used_bytes(self) -> int:
        """Sum the size of every regular file under ``data_root`` (ground truth)."""
        total = 0
        for p in self.data_root.rglob("*"):
            try:
                if p.is_file() and not p.is_symlink():
                    total += p.stat().st_size
            except OSError:
                # a file vanished mid-walk (e.g. concurrent eviction) — ignore
                continue
        return total

    def remaining_bytes(self) -> int:
        return max(0, self.budget_bytes - self.used_bytes())

    def can_admit(self, nbytes: int) -> bool:
        """True if writing ``nbytes`` more would stay within budget."""
        return self.used_bytes() + max(0, int(nbytes)) <= self.budget_bytes

    # --- enforcement -------------------------------------------------------
    def admit(self, nbytes: int, *, label: str = "") -> None:
        """Assert that ``nbytes`` fits; raise :class:`StorageBudgetExceeded` if not."""
        if not self.can_admit(nbytes):
            self.log_usage("admit_refused", label=label, requested_bytes=int(nbytes))
            raise StorageBudgetExceeded(
                f"admitting {nbytes} bytes for {label!r} would exceed the "
                f"{self.budget_gb} GB budget (used {self.used_bytes()} bytes)"
            )

    def evict(self, path: str | Path) -> int:
        """Delete ``path`` (file or directory) and return the bytes freed."""
        p = Path(path)
        freed = 0
        if p.is_file():
            freed = p.stat().st_size
            p.unlink()
        elif p.is_dir():
            freed = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
            shutil.rmtree(p)
        self.log_usage("evict", path=str(p), freed_bytes=freed)
        return freed

    # --- observability -----------------------------------------------------
    def log_usage(self, stage: str, **extra: object) -> None:
        """Emit the structured disk-used-vs-budget line for ELK."""
        used = self.used_bytes()
        log_event(
            "storage_usage",
            stage=stage,
            used_bytes=used,
            used_gb=round(used / BYTES_PER_GB, 3),
            budget_gb=self.budget_gb,
            pct_used=round(100 * used / self.budget_bytes, 1) if self.budget_bytes else None,
            remaining_gb=round(self.remaining_bytes() / BYTES_PER_GB, 3),
            **extra,
        )
