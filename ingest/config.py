"""Load and parse config/quality_gates.yaml into typed objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

DEFAULT_GATES_PATH = "config/quality_gates.yaml"


@dataclass
class SchemaGates:
    detect_codebase_version: bool = True
    required_features: list[str] = field(default_factory=list)
    require_at_least_one_image: bool = True
    image_keys_any_of: list[str] = field(default_factory=list)


@dataclass
class AnnotationGates:
    require_language_instruction: bool = True


@dataclass
class SignalGates:
    """Robust percentile + anomalous-frame-fraction gate: calibrate a per-signal frame score threshold at `anomaly_percentile` over `calibrate_from`, then fail an episode only if the fraction of frames exceeding it is above `max_anomalous_frame_ratio`."""

    calibrate_from: Optional[str] = None
    anomaly_percentile: float = 99.9
    max_anomalous_frame_ratio: float = 0.01
    max_missing_frame_ratio: float = 0.02


@dataclass
class QualityGates:
    schema: SchemaGates
    annotation: AnnotationGates
    on_fail: str = "quarantine"           # quarantine | drop
    signal: SignalGates = field(default_factory=SignalGates)


def load_quality_gates(path: str | Path = DEFAULT_GATES_PATH) -> QualityGates:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    schema_raw = raw.get("schema", {}) or {}
    annotation_raw = raw.get("annotation", {}) or {}
    policy_raw = raw.get("policy", {}) or {}
    signal_raw = raw.get("signal", {}) or {}
    return QualityGates(
        schema=SchemaGates(
            detect_codebase_version=schema_raw.get("detect_codebase_version", True),
            required_features=list(schema_raw.get("required_features", [])),
            require_at_least_one_image=schema_raw.get("require_at_least_one_image", True),
            image_keys_any_of=list(schema_raw.get("image_keys_any_of", [])),
        ),
        annotation=AnnotationGates(
            require_language_instruction=annotation_raw.get("require_language_instruction", True),
        ),
        on_fail=policy_raw.get("on_fail", "quarantine"),
        signal=SignalGates(
            calibrate_from=signal_raw.get("calibrate_from"),
            anomaly_percentile=float(signal_raw.get("anomaly_percentile", 99.9)),
            max_anomalous_frame_ratio=float(signal_raw.get("max_anomalous_frame_ratio", 0.01)),
            max_missing_frame_ratio=float(signal_raw.get("max_missing_frame_ratio", 0.02)),
        ),
    )
