"""Tests for the schema gate + canonical-ingest quarantine (no network).

Builds tiny LeRobot v3.0-shaped fixtures with PyArrow: `meta/info.json` (features),
a `data/chunk-000/file-000.parquet` of low-dim columns, `meta/tasks.parquet`, and a
stub video file for the image feature. Mirrors the real lerobot/droid_100 layout
verified on 2026-07-22.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingest.canonicalize import ingest_source
from ingest.config import AnnotationGates, QualityGates, SchemaGates
from ingest.schema_gate import validate_schema

IMG = "observation.images.exterior_image_1_left"

# A valid v3.0 feature declaration: low-dim features + one video (image) feature.
VALID_FEATURES = {
    "observation.state": {"dtype": "float32", "shape": [7]},
    "action": {"dtype": "float32", "shape": [7]},
    "task_index": {"dtype": "int64", "shape": [1]},
    "timestamp": {"dtype": "float32", "shape": [1]},
    IMG: {"dtype": "video", "shape": [180, 320, 3]},
}
# Image feature is a video file, so it is NOT a parquet column.
VALID_COLUMNS = ["observation.state", "action", "task_index", "timestamp"]


def make_gates(on_fail: str = "quarantine") -> QualityGates:
    return QualityGates(
        schema=SchemaGates(
            required_features=["observation.state", "action"],
            require_at_least_one_image=True,
            image_keys_any_of=[IMG, "observation.images.wrist_image_left"],
        ),
        annotation=AnnotationGates(require_language_instruction=True),
        on_fail=on_fail,
    )


def build_dataset(
    root: Path,
    *,
    features: dict | None = None,
    columns: list[str] | None = None,
    with_tasks: bool = True,
    with_data: bool = True,
) -> Path:
    features = VALID_FEATURES if features is None else features
    columns = VALID_COLUMNS if columns is None else columns
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "info.json").write_text(
        json.dumps({"codebase_version": "v3.0", "features": features})
    )
    if with_tasks:
        pq.write_table(
            pa.table({"task_index": [0], "task": ["pick up the cube"]}),
            root / "meta" / "tasks.parquet",
        )
    if with_data:
        ddir = root / "data" / "chunk-000"
        ddir.mkdir(parents=True, exist_ok=True)
        cols = {
            c: (pa.array([0, 1]) if c == "task_index" else pa.array([0.1, 0.2]))
            for c in columns
        }
        pq.write_table(pa.table(cols), ddir / "file-000.parquet")
    # stub video for the image feature
    vdir = root / "videos" / IMG / "chunk-000"
    vdir.mkdir(parents=True, exist_ok=True)
    (vdir / "file-000.mp4").write_bytes(b"\0" * 16)
    return root


# --- validate_schema -------------------------------------------------------
def test_valid_dataset_passes(tmp_path: Path) -> None:
    ds = build_dataset(tmp_path / "ds")
    res = validate_schema(ds, make_gates())
    assert res.passed, res.reasons
    assert res.codebase_version == "v3.0"
    assert "observation.state" in res.present_features


def test_missing_required_feature_fails(tmp_path: Path) -> None:
    feats = {k: v for k, v in VALID_FEATURES.items() if k != "action"}
    cols = [c for c in VALID_COLUMNS if c != "action"]
    ds = build_dataset(tmp_path / "ds", features=feats, columns=cols)
    res = validate_schema(ds, make_gates())
    assert not res.passed
    assert any("action" in r for r in res.reasons)


def test_no_image_feature_fails(tmp_path: Path) -> None:
    feats = {k: v for k, v in VALID_FEATURES.items() if k != IMG}
    ds = build_dataset(tmp_path / "ds", features=feats)
    res = validate_schema(ds, make_gates())
    assert not res.passed
    assert any("image" in r for r in res.reasons)


def test_missing_language_fails(tmp_path: Path) -> None:
    feats = {k: v for k, v in VALID_FEATURES.items() if k != "task_index"}
    cols = [c for c in VALID_COLUMNS if c != "task_index"]
    ds = build_dataset(tmp_path / "ds", features=feats, columns=cols, with_tasks=False)
    res = validate_schema(ds, make_gates())
    assert not res.passed
    assert any("language" in r for r in res.reasons)


def test_declared_but_missing_column_fails(tmp_path: Path) -> None:
    # 'action' declared in info.json but absent from the actual parquet columns
    ds = build_dataset(tmp_path / "ds", columns=[c for c in VALID_COLUMNS if c != "action"])
    res = validate_schema(ds, make_gates())
    assert not res.passed
    assert any("not a data column" in r for r in res.reasons)


def test_no_data_parquet_fails(tmp_path: Path) -> None:
    ds = build_dataset(tmp_path / "ds", with_data=False)
    res = validate_schema(ds, make_gates())
    assert not res.passed
    assert any("data parquet" in r for r in res.reasons)


# --- ingest_source (canonicalize + policy) --------------------------------
def test_ingest_ready_on_pass(tmp_path: Path) -> None:
    build_dataset(tmp_path / "raw" / "droid-100")
    res = ingest_source("droid-100", data_root=tmp_path, gates=make_gates())
    assert res.action == "ready" and res.passed
    assert (tmp_path / "raw" / "droid-100").exists()  # left in place for the signal-gate stage


def test_ingest_quarantines_on_fail(tmp_path: Path) -> None:
    feats = {k: v for k, v in VALID_FEATURES.items() if k != "action"}
    cols = [c for c in VALID_COLUMNS if c != "action"]
    build_dataset(tmp_path / "raw" / "droid-100", features=feats, columns=cols)
    res = ingest_source("droid-100", data_root=tmp_path, gates=make_gates())
    assert res.action == "quarantined" and not res.passed
    assert not (tmp_path / "raw" / "droid-100").exists()
    qroot = tmp_path / "quarantine" / "droid-100"
    assert qroot.exists()
    rejects = json.loads((qroot / "_rejects.json").read_text())
    assert rejects["reasons"] and rejects["stage"] == "schema_gate"


def test_ingest_drops_on_fail_when_policy_drop(tmp_path: Path) -> None:
    feats = {k: v for k, v in VALID_FEATURES.items() if k != IMG}
    build_dataset(tmp_path / "raw" / "droid-100", features=feats)
    res = ingest_source("droid-100", data_root=tmp_path, gates=make_gates(on_fail="drop"))
    assert res.action == "dropped"
    assert not (tmp_path / "raw" / "droid-100").exists()
    assert not (tmp_path / "quarantine" / "droid-100").exists()
