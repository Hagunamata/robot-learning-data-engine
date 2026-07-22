"""Canonical ingest (M3): confirm a source is in LeRobot format, then schema-gate it.

For the DROID sources the raw data is *already* LeRobot (v3.0 / v2.0), so canonical
ingest here is a verification + schema-validation step rather than a format
conversion. Non-LeRobot sources (future OXE; synthetic episodes in M5) will do real
conversion at this stage — that path raises NotImplementedError for now.

On schema failure the dataset is routed per the `on_fail` policy (quarantine keeps a
rejects log; drop deletes it). Writing passing episodes to data/curated/ and evicting
the raw copy happens in M4 — this stage only accepts or rejects.

See docs/01-conception.md §4.2 and docs/02-development.md (M3).
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from acquisition.logging_utils import log_event

from .config import QualityGates, load_quality_gates
from .schema_gate import SchemaResult, validate_schema


@dataclass
class IngestResult:
    source_id: str
    passed: bool
    codebase_version: Optional[str]
    reasons: list[str]
    action: str  # "ready" | "quarantined" | "dropped" | "error"


def _is_lerobot(dataset_root: Path) -> bool:
    return (dataset_root / "meta" / "info.json").exists()


def _quarantine(raw_root: Path, quarantine_root: Path, result: SchemaResult) -> None:
    """Move a rejected dataset to quarantine and write a rejects log."""
    quarantine_root.parent.mkdir(parents=True, exist_ok=True)
    if quarantine_root.exists():
        shutil.rmtree(quarantine_root)
    shutil.move(str(raw_root), str(quarantine_root))
    rejects = {
        "rejected_at": datetime.now(timezone.utc).isoformat(),
        "stage": "schema_gate",
        "reasons": result.reasons,
        "codebase_version": result.codebase_version,
    }
    (quarantine_root / "_rejects.json").write_text(
        json.dumps(rejects, indent=2), encoding="utf-8"
    )


def ingest_source(
    source_id: str,
    data_root: str | Path = "./data",
    gates: QualityGates | None = None,
) -> IngestResult:
    """Canonicalize + schema-gate the acquired raw dataset for ``source_id``."""
    gates = gates or load_quality_gates()
    root = Path(data_root)
    raw_root = root / "raw" / source_id
    log_event("ingest_start", source=source_id, raw_root=str(raw_root))

    if not raw_root.exists():
        log_event("ingest_error", source=source_id, error=f"no acquired data at {raw_root}")
        return IngestResult(source_id, False, None, ["raw data not found — run acquisition first"], "error")

    # Canonicalize: DROID is already LeRobot; non-LeRobot conversion is future work.
    if not _is_lerobot(raw_root):
        raise NotImplementedError(
            f"{source_id}: non-LeRobot source conversion is not implemented yet "
            "(DROID sources are already LeRobot; OXE/synthetic land in later milestones)"
        )

    result = validate_schema(raw_root, gates)
    log_event(
        "schema_result",
        source=source_id,
        passed=result.passed,
        codebase_version=result.codebase_version,
        n_features=len(result.present_features),
        reasons=result.reasons,
    )

    if result.passed:
        log_event("ingest_ready", source=source_id, note="passed schema gate; awaits signal gates (M4)")
        return IngestResult(source_id, True, result.codebase_version, [], "ready")

    # Schema failed → apply policy.
    if gates.on_fail == "drop":
        shutil.rmtree(raw_root)
        log_event("ingest_dropped", source=source_id, reasons=result.reasons)
        return IngestResult(source_id, False, result.codebase_version, result.reasons, "dropped")

    quarantine_root = root / "quarantine" / source_id
    _quarantine(raw_root, quarantine_root, result)
    log_event(
        "ingest_quarantined",
        source=source_id,
        quarantine_root=str(quarantine_root),
        reasons=result.reasons,
    )
    return IngestResult(source_id, False, result.codebase_version, result.reasons, "quarantined")
