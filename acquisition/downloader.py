"""Selective / streamed downloader over ``huggingface_hub`` + ``datasets``.

Pulls only the chosen episodes for an enabled source in ``config/sources.yaml``,
one bounded batch at a time, checking the StorageGuard before each pull. Never
materialises a full corpus.

See docs/01-conception.md §4.1. Implemented in M2.
"""

from __future__ import annotations


def stream_source(source_id: str) -> None:
    """Stream a bounded batch of episodes for ``source_id`` into ``data/raw/``.

    TODO(M2): resolve the (verified) hf_repo + max_episodes from sources.yaml,
    iterate episodes under the storage guard, and hand each batch to ingest.
    """
    raise NotImplementedError("downloader is implemented in M2")
