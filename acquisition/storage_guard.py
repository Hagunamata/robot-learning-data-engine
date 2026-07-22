"""Storage guard — enforces the local disk budget and drives process-and-evict.

The guard accounts bytes under ``data_root`` against ``storage_budget_gb`` (default
400 GB) and refuses any acquisition that would exceed the cap. After a batch is
curated, its raw copy is evicted so the budget frees up for the next batch. The
running disk-used-vs-budget figure is logged at every stage.

See docs/01-conception.md §5. Implemented in M2.
"""

from __future__ import annotations


class StorageGuard:
    """Accounts resident bytes against the configured budget.

    TODO(M2): measure ``data_root`` usage, expose ``can_admit(nbytes)``,
    ``evict(path)``, and a structured ``log_usage()`` line for ELK.
    """

    def __init__(self, data_root: str, budget_gb: float = 400.0) -> None:
        raise NotImplementedError("StorageGuard is implemented in M2")
