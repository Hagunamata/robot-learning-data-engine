"""Signal-quality gate job: jerk, action outliers, and missing-frame ratio.

Reads thresholds from config/quality_gates.yaml (signal section). Passing episodes
are written to data/curated/ and their raw copy is evicted; failing episodes go to
data/quarantine/ (or are dropped) per the on_fail policy. Emits per-batch metrics
and the disk-used-vs-budget figure.

See docs/01-conception.md §4.3 and §5. Implemented in M4.
"""

from __future__ import annotations


def run(curated_root: str) -> None:
    """Apply signal-quality gates across a batch of canonical episodes.

    TODO(M4): 3rd-order finite-difference jerk z-score, action outlier z-score,
    missing-frame ratio; route pass/fail; trigger eviction of the raw batch.
    """
    raise NotImplementedError("signal_gates is implemented in M4")
