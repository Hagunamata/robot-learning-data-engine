"""Schema quality gate (PyArrow) for a LeRobot dataset.

Validates that an acquired dataset conforms to the required LeRobot schema *before*
the signal gates (M4) and curation run. Schema is a dataset-level property (every
episode shares one `meta/info.json`), so this gate accepts or quarantines the dataset
as a whole; per-episode signal quality is the separate M4 concern.

Engine decision (M3): this is PyArrow, not Spark — it is a feature-existence/dtype
check, the "~50-line PyArrow job" the brief points at. Spark is reserved for the M4
signal gates. See docs/02-development.md (M3 decision record).

Verified against lerobot/droid_100 meta/info.json (v3.0):
  - low-dim features (observation.state, action, task_index, ...) are PARQUET COLUMNS
  - image features (observation.images.*) are declared in info.json but stored as
    separate .mp4 video files, NOT parquet columns
  - the language instruction is `task_index` (int) resolved via meta/tasks.parquet
"""

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

    # 1. Required low-dim features must be declared.
    for rf in gates.schema.required_features:
        if rf not in features:
            reasons.append(f"missing required feature '{rf}'")

    # 2. At least one image/camera stream.
    if gates.schema.require_at_least_one_image:
        if not any(k in features for k in gates.schema.image_keys_any_of):
            reasons.append(
                "no image feature present (none of "
                f"{gates.schema.image_keys_any_of})"
            )

    # 3. Language instruction present and resolvable.
    if gates.annotation.require_language_instruction and not _language_ok(features, root):
        reasons.append("no resolvable language instruction (task_index+tasks table or language_instruction)")

    # 4. Cross-check: non-image required features must actually be columns in the data
    #    parquet (guards against an info.json that over-declares). Image features are
    #    video files, not columns, so they are excluded from this check.
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
