"""Schema-gate job: check each episode conforms to the required LeRobot features.

Reads thresholds/keys from config/quality_gates.yaml (schema section). Fails an
episode to quarantine/drop on missing required features or no image stream.

NOTE: whether this needs Spark or a plain PyArrow scan is a per-step DECISION to
clear with the human (see docs/01-conception.md §4.3). Implemented in M3.
"""

from __future__ import annotations


def run(curated_root: str) -> None:
    """Validate schema conformance across a batch of canonical episodes.

    TODO(M3): implement as a local-mode Spark job (or PyArrow, pending DECISION).
    """
    raise NotImplementedError("schema_validation is implemented in M3")
