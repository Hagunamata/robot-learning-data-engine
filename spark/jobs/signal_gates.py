"""Signal-quality gates: jerk, action outliers, missing frames — per episode.

The per-episode metric maths lives in a pure numpy core (``compute_episode_metrics`` /
``evaluate``). Two engines feed it the same per-episode arrays: ``run_spark`` (scale) and
``run_local`` (dev/CI), which give identical results.

Gate design — robust percentile + anomalous-frame fraction: calibrate per-dim robust
center/scale (median / MAD) over the ``calibrate_from`` corpus and a per-signal
frame-score threshold at a high percentile (a frame's score is the max over dims of its
robust |z|), then fail an episode only when the fraction of frames exceeding that
threshold is above ``max_anomalous_frame_ratio``. A single sharp frame no longer kills a
long episode; concentrated anomalies do.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ingest.config import SignalGates

STATE_COL = "observation.state"
ACTION_COL = "action"
TS_COL = "timestamp"
EP_COL = "episode_index"
_MAD_TO_STD = 1.4826  # MAD -> std-equivalent for normal data


# --- calibration -----------------------------------------------------------
@dataclass
class Calibration:
    """Robust per-dim center/scale + percentile frame-score thresholds."""

    source: str
    n_frames: int
    anomaly_percentile: float
    jerk_center: list[float]
    jerk_scale: list[float]
    action_center: list[float]
    action_scale: list[float]
    jerk_score_threshold: float
    action_score_threshold: float

    def to_file(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(asdict(self), indent=2), encoding="utf-8")

    @classmethod
    def from_file(cls, path: str | Path) -> "Calibration":
        return cls(**json.loads(Path(path).read_text(encoding="utf-8")))


@dataclass
class EpisodeMetrics:
    episode_index: int
    n_frames: int
    duration_s: float
    jerk_anomalous_ratio: float
    action_anomalous_ratio: float
    missing_frame_ratio: float


@dataclass
class EpisodeVerdict:
    episode_index: int
    passed: bool
    reasons: list[str]
    metrics: EpisodeMetrics


@dataclass
class GateReport:
    total: int
    passed: int
    failed: int
    verdicts: list[EpisodeVerdict] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return round(self.passed / self.total, 4) if self.total else 0.0


# --- pure core (no Spark, no I/O) ------------------------------------------
def _jerk(signal: np.ndarray) -> np.ndarray:
    """3rd-order finite difference along time; (T, D) -> (max(T-3,0), D)."""
    return np.diff(signal, n=3, axis=0) if signal.shape[0] >= 4 else np.empty((0, signal.shape[1]))


def _concat_signal(state: np.ndarray, action: np.ndarray) -> np.ndarray:
    action = np.atleast_2d(action).astype(float)
    state = np.atleast_2d(state).astype(float) if state is not None and state.size else np.empty((action.shape[0], 0))
    return np.hstack([state, action]) if state.size else action


def _robust_center_scale(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Per-column (median, MAD*1.4826). Constant columns get scale 0 (ignored later)."""
    med = np.median(values, axis=0)
    scale = np.median(np.abs(values - med), axis=0) * _MAD_TO_STD
    return med, scale


def _frame_scores(values: np.ndarray, center: Iterable[float], scale: Iterable[float]) -> np.ndarray:
    """Per-frame anomaly score = max over dims of robust |z|. (T, D) -> (T,)."""
    if values.size == 0:
        return np.zeros((0,))
    scale = np.asarray(scale, dtype=float).copy()
    scale[scale == 0] = np.nan  # constant dims contribute no deviation
    z = np.abs((values - np.asarray(center, dtype=float)) / scale)
    z = np.where(np.isfinite(z), z, 0.0)
    return z.max(axis=1) if z.shape[1] else np.zeros(values.shape[0])


def compute_episode_metrics(
    episode_index: int,
    state: np.ndarray,
    action: np.ndarray,
    timestamps: np.ndarray,
    fps: float | None,
    calibration: Calibration,
) -> EpisodeMetrics:
    """Signal-quality metrics for one episode, as anomalous-frame fractions."""
    action = np.atleast_2d(action).astype(float)
    signal = _concat_signal(state, action)
    n = action.shape[0]
    duration = float(timestamps[-1] - timestamps[0]) if n > 1 else 0.0

    jerk_scores = _frame_scores(_jerk(signal), calibration.jerk_center, calibration.jerk_scale)
    action_scores = _frame_scores(action, calibration.action_center, calibration.action_scale)
    jerk_ratio = float((jerk_scores > calibration.jerk_score_threshold).mean()) if jerk_scores.size else 0.0
    action_ratio = float((action_scores > calibration.action_score_threshold).mean()) if action_scores.size else 0.0

    missing_ratio = 0.0
    if fps and n > 1 and duration > 0:
        expected = int(round(duration * fps)) + 1
        if expected > 0:
            missing_ratio = max(0.0, (expected - n) / expected)
    if signal.size and n:
        missing_ratio = max(missing_ratio, int(np.isnan(signal).any(axis=1).sum()) / n)

    return EpisodeMetrics(
        episode_index=int(episode_index),
        n_frames=int(n),
        duration_s=round(duration, 4),
        jerk_anomalous_ratio=round(jerk_ratio, 6),
        action_anomalous_ratio=round(action_ratio, 6),
        missing_frame_ratio=round(missing_ratio, 6),
    )


def evaluate(metrics: EpisodeMetrics, thresholds: SignalGates) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if metrics.jerk_anomalous_ratio > thresholds.max_anomalous_frame_ratio:
        reasons.append(
            f"jerk-anomalous frames {metrics.jerk_anomalous_ratio} > {thresholds.max_anomalous_frame_ratio}"
        )
    if metrics.action_anomalous_ratio > thresholds.max_anomalous_frame_ratio:
        reasons.append(
            f"action-anomalous frames {metrics.action_anomalous_ratio} > {thresholds.max_anomalous_frame_ratio}"
        )
    if metrics.missing_frame_ratio > thresholds.max_missing_frame_ratio:
        reasons.append(
            f"missing-frame ratio {metrics.missing_frame_ratio} > {thresholds.max_missing_frame_ratio}"
        )
    return (not reasons, reasons)


def _summarize(verdicts: Iterable[EpisodeVerdict]) -> GateReport:
    verdicts = sorted(verdicts, key=lambda v: v.episode_index)
    passed = sum(1 for v in verdicts if v.passed)
    return GateReport(len(verdicts), passed, len(verdicts) - passed, verdicts)


# --- shared I/O helpers ----------------------------------------------------
def _read_fps(dataset_root: Path) -> float | None:
    return json.loads((dataset_root / "meta" / "info.json").read_text(encoding="utf-8")).get("fps")


def _stack(values) -> np.ndarray:
    values = values.to_pylist() if hasattr(values, "to_pylist") else list(values)
    return np.asarray(values, dtype=float) if values else np.empty((0, 0))


def _iter_episodes(dataset_root: Path) -> Iterator[tuple[int, np.ndarray, np.ndarray, np.ndarray]]:
    """Yield (episode_index, state, action, timestamps) for each episode."""
    files = sorted((dataset_root / "data").rglob("*.parquet"))
    if not files:
        return
    table = pa.concat_tables([pq.read_table(f) for f in files])
    ep = np.asarray(table.column(EP_COL).to_pylist())
    state = _stack(table.column(STATE_COL)) if STATE_COL in table.column_names else None
    action = _stack(table.column(ACTION_COL))
    ts = np.asarray(table.column(TS_COL).to_pylist(), dtype=float)
    for e in np.unique(ep):
        m = ep == e
        yield int(e), (state[m] if state is not None else np.empty((int(m.sum()), 0))), action[m], ts[m]


# --- calibration builder (always local; the calibrate_from source is small) ---
def calibrate_local(dataset_root: str | Path, source: str, anomaly_percentile: float = 99.9) -> Calibration:
    """Compute robust per-dim center/scale + percentile score thresholds over a dataset.

    Jerk is computed *within* each episode (3rd difference across boundaries is
    meaningless) then pooled; scores are pooled across all frames for the percentile.
    """
    root = Path(dataset_root)
    jerks: list[np.ndarray] = []
    actions: list[np.ndarray] = []
    for _e, state, action, _ts in _iter_episodes(root):
        j = _jerk(_concat_signal(state, action))
        if j.size:
            jerks.append(j)
        if action.size:
            actions.append(np.atleast_2d(action).astype(float))
    jerk_all = np.vstack(jerks) if jerks else np.zeros((1, 1))
    action_all = np.vstack(actions) if actions else np.zeros((1, 1))

    jc, js = _robust_center_scale(jerk_all)
    ac, asc = _robust_center_scale(action_all)
    jerk_scores = _frame_scores(jerk_all, jc, js)
    action_scores = _frame_scores(action_all, ac, asc)
    return Calibration(
        source=source,
        n_frames=int(action_all.shape[0]),
        anomaly_percentile=float(anomaly_percentile),
        jerk_center=jc.round(6).tolist(),
        jerk_scale=js.round(6).tolist(),
        action_center=ac.round(6).tolist(),
        action_scale=asc.round(6).tolist(),
        jerk_score_threshold=round(float(np.percentile(jerk_scores, anomaly_percentile)), 6) if jerk_scores.size else 0.0,
        action_score_threshold=round(float(np.percentile(action_scores, anomaly_percentile)), 6) if action_scores.size else 0.0,
    )


# --- local engine ----------------------------------------------------------
def run_local(dataset_root: str | Path, thresholds: SignalGates, calibration: Calibration) -> GateReport:
    root = Path(dataset_root)
    fps = _read_fps(root)
    verdicts: list[EpisodeVerdict] = []
    for e, state, action, ts in _iter_episodes(root):
        m = compute_episode_metrics(e, state, action, ts, fps, calibration)
        passed, reasons = evaluate(m, thresholds)
        verdicts.append(EpisodeVerdict(e, passed, reasons, m))
    return _summarize(verdicts)


# --- spark engine (production / scale) -------------------------------------
def run_spark(dataset_root: str | Path, thresholds: SignalGates, calibration: Calibration) -> GateReport:
    """Spark local-mode engine; groups the data parquet by episode_index and runs the same pure core per group via applyInPandas, so results match run_local."""
    if not os.getenv("JAVA_HOME") and not shutil.which("java"):
        raise RuntimeError(
            "the 'spark' engine needs a Java runtime (JAVA_HOME unset and `java` not on PATH). "
            "Install a JDK — e.g. `sudo apt install default-jdk` — or use the 'local' engine "
            "(ENGINE=local), which needs no JVM and returns identical verdicts."
        )
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    root = Path(dataset_root)
    fps = _read_fps(root)
    out_schema = StructType(
        [
            StructField("episode_index", LongType()),
            StructField("n_frames", LongType()),
            StructField("jerk_anomalous_ratio", DoubleType()),
            StructField("action_anomalous_ratio", DoubleType()),
            StructField("missing_frame_ratio", DoubleType()),
            StructField("passed", BooleanType()),
            StructField("reasons", StringType()),
        ]
    )

    def _gate_group(pdf):
        import pandas as pd

        e = int(pdf[EP_COL].iloc[0])
        state = np.asarray(pdf[STATE_COL].to_list(), dtype=float) if STATE_COL in pdf else np.empty((len(pdf), 0))
        action = np.asarray(pdf[ACTION_COL].to_list(), dtype=float)
        ts = pdf[TS_COL].to_numpy(dtype=float)
        m = compute_episode_metrics(e, state, action, ts, fps, calibration)
        passed, reasons = evaluate(m, thresholds)
        return pd.DataFrame(
            [{
                "episode_index": e,
                "n_frames": m.n_frames,
                "jerk_anomalous_ratio": m.jerk_anomalous_ratio,
                "action_anomalous_ratio": m.action_anomalous_ratio,
                "missing_frame_ratio": m.missing_frame_ratio,
                "passed": passed,
                "reasons": "; ".join(reasons),
            }]
        )

    spark = SparkSession.builder.appName("rlde-signal-gates").master("local[*]").getOrCreate()
    try:
        df = spark.read.parquet(str(root / "data"))
        rows = df.groupby(EP_COL).applyInPandas(_gate_group, schema=out_schema).collect()
    finally:
        spark.stop()

    verdicts = [
        EpisodeVerdict(
            episode_index=r["episode_index"],
            passed=r["passed"],
            reasons=[s for s in (r["reasons"] or "").split("; ") if s],
            metrics=EpisodeMetrics(
                r["episode_index"], r["n_frames"], 0.0,
                r["jerk_anomalous_ratio"], r["action_anomalous_ratio"], r["missing_frame_ratio"],
            ),
        )
        for r in rows
    ]
    return _summarize(verdicts)


def run_signal_gates(
    dataset_root: str | Path,
    thresholds: SignalGates,
    calibration: Calibration,
    engine: str = "spark",
) -> GateReport:
    """Dispatch to the chosen engine ('spark' for scale, 'local' for dev/CI)."""
    if engine == "local":
        return run_local(dataset_root, thresholds, calibration)
    if engine == "spark":
        return run_spark(dataset_root, thresholds, calibration)
    raise ValueError(f"unknown engine {engine!r} (expected 'spark' or 'local')")
