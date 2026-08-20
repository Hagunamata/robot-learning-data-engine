"""Data catalog — one row per dataset version in the Postgres `catalog` schema
(or a local sqlite file for dev/CI).

Public surface:
    - build_record, CatalogRecord, compute_task_distribution  (record)
    - CatalogWriter                                            (writer)
"""

from .record import CatalogRecord, build_record, compute_task_distribution
from .writer import CatalogWriter

__all__ = ["CatalogRecord", "build_record", "compute_task_distribution", "CatalogWriter"]
