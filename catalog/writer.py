"""Catalog writer — records a dataset-version row after a pipeline run.

One row per version: source, license, episode/frame counts, task distribution,
gate pass-rate, bytes-on-disk, git commit (schema in postgres/init/).

See docs/01-conception.md §4.4. Implemented in M5.
"""

from __future__ import annotations


def record_version(**fields: object) -> None:
    """Insert one dataset-version row into the `catalog` schema.

    TODO(M5): connect via env creds, INSERT the fields defined in
    CLAUDE_CODE_BRIEF.md §6.3 (dataset_version, source_id, license, ...).
    """
    raise NotImplementedError("catalog writer is implemented in M5")
