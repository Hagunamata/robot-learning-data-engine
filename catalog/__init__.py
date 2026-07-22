"""Data catalog — one row per dataset version in the Postgres `catalog` schema
(or a local sqlite file for dev/CI).

Public surface:
    - build_record, CatalogRecord, compute_task_distribution  (record)
    - CatalogWriter                                            (writer)

See docs/01-conception.md §4.4 and CLAUDE_CODE_BRIEF.md §6.3. Implemented in M5.
"""

from .record import CatalogRecord, build_record, compute_task_distribution
from .writer import CatalogWriter

__all__ = ["CatalogRecord", "build_record", "compute_task_distribution", "CatalogWriter"]
