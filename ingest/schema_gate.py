"""Schema quality gate (PyArrow) for a LeRobot dataset."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

from .config import QualityGates

# LeRobot info.json marks camera streams with one of these dtypes.
_IMAGE_DTYPES = {"video", "image"}


@dataclass
class SchemaResult:
    dataset_root: str
    passed: bool
    codebase_version: Optional[str]
    present_features: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)  # why it failed (empty if passed)


def _read_info(dataset_root: Path) -> dict:
    info_path = dataset_root / "meta" / "info.json"
    if not info_path.exists():
        raise FileNotFoundError(f"no meta/info.json under {dataset_root} — not a LeRobot dataset")
    return json.loads(info_path.read_text(encoding="utf-8"))


def _first_data_parquet(dataset_root: Path) -> Optional[Path]:
    files = sorted((dataset_root / "data").rglob("*.parquet"))
    return files[0] if files else None


def _language_ok(features: dict, dataset_root: Path) -> bool:
    """A resolvable language instruction is present (v3.0 or v2.0 shapes)."""
    if "task_index" in features:
        tasks_meta = (dataset_root / "meta" / "tasks.parquet").exists() or (
            dataset_root / "meta" / "tasks.jsonl"
        ).exists()
        if tasks_meta:
            return True
    # v2.0 / alternative shapes carry the string directly
    return "language_instruction" in features or "task" in features


def validate_schema(dataset_root: str | Path, gates: QualityGates) -> SchemaResult:
    """Validate a dataset's LeRobot schema against the configured gates."""
    root = Path(dataset_root)
    info = _read_info(root)
    features: dict = info.get("features", {}) or {}
    codebase_version = info.get("codebase_version")
    reasons: list[str] = []

    for rf in gates.schema.required_features:
        if rf not in features:
            reasons.append(f"missing required feature '{rf}'")

    if gates.schema.require_at_least_one_image:
        if not any(k in features for k in gates.schema.image_keys_any_of):
            reasons.append(
                "no image feature present (none of "
                f"{gates.schema.image_keys_any_of})"
            )

    if gates.annotation.require_language_instruction and not _language_ok(features, root):
        reasons.append("no resolvable language instruction (task_index+tasks table or language_instruction)")

    # Guard against an info.json that over-declares: non-image required features must
    # actually be columns in the data parquet. Image features are video files, not
    # columns, so they are excluded here.
    data_parquet = _first_data_parquet(root)
    if data_parquet is None:
        reasons.append("no data parquet found under data/")
    else:
        columns = set(pq.read_schema(data_parquet).names)
        for rf in gates.schema.required_features:
            is_image = features.get(rf, {}).get("dtype") in _IMAGE_DTYPES
            if not is_image and rf not in columns:
                reasons.append(f"required feature '{rf}' declared but not a data column")

    return SchemaResult(
        dataset_root=str(root),
        passed=not reasons,
        codebase_version=codebase_version,
        present_features=sorted(features.keys()),
        reasons=reasons,
    )
