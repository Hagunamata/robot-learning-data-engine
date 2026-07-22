"""Convert a raw source episode into the canonical LeRobot on-disk format.

The exact required feature keys are pinned against the REAL LeRobot spec in M1 and
cited in comments here before this is written — not guessed (see working agreement).

See docs/01-conception.md §4.2. Implemented in M3.
"""

from __future__ import annotations


def canonicalize_episode(raw_path: str, out_dir: str) -> str:
    """Write a LeRobot-format episode from a raw source episode; return its path.

    TODO(M3): map source fields to the verified LeRobot feature keys
    (state / action / task / >=1 image), write Parquet + metadata.
    """
    raise NotImplementedError("canonicalize is implemented in M3")
