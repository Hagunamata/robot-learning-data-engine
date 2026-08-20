"""Selective, file-granular acquisition from the Hugging Face Hub, gated by a
storage guard.

Public surface:
    - StorageGuard, StorageBudgetExceeded  (storage_guard)
    - acquire, AcquireSummary              (downloader)
    - load_sources, Source, SourcesConfig  (config)
"""

from .storage_guard import BYTES_PER_GB, StorageBudgetExceeded, StorageGuard

__all__ = ["StorageGuard", "StorageBudgetExceeded", "BYTES_PER_GB"]
