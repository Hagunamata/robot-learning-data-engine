"""Storage-aware scale runner (M6) — batched acquire -> process -> evict -> repeat.

Proves the project's headline invariant, measured (not narrated):

    peak concurrent on-disk RAW  <  storage_budget_gb        (bounded working set)
    total bytes processed        >> storage_budget_gb        (streamed through)

The loop pulls one *unit* (a chunk = data file + its videos) at a time, gates its
episodes, appends passing ones to `curated/`, commits the catalog row, and only THEN
evicts the raw copy — so raw never exceeds the budget while arbitrarily much data flows
through. Correctness rules (see the M6 spec in docs/02-development.md):

  1. Pre-admission guard: (raw on disk + estimated next unit) must fit budget*headroom;
     a unit larger than that is refused rather than overshooting.
  2. Evict only after durable commit: curated write + catalog upsert + manifest write
     all happen before the raw unit is deleted.
  3. Resumable + idempotent: a manifest tracks processed unit/episode ids so a re-run
     never re-pulls curated episodes or double-counts bytes.
  4. Quarantine can't leak the budget: failures keep a reason + a tiny frame sample only,
     under a small sub-cap; never the full raw unit.
  5. Peak is measured, not assumed: the raw directory size is sampled after every fetch
     and the observed maximum is reported.

Two sources implement the same protocol: `SyntheticScaleSource` (seeded, local,
network-free — the reproducible proof) and `HfScaleSource` (real DROID slice, run on
Ubuntu). Calibration comes from the small dev source (droid-100), never the slice.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterator, Optional, Protocol

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from acquisition.logging_utils import log_event
from acquisition.storage_guard import BYTES_PER_GB, StorageGuard
from catalog import CatalogWriter, build_record
from ingest.config import QualityGates, load_quality_gates
from spark.jobs.signal_gates import Calibration, EpisodeVerdict, run_signal_gates

_QUARANTINE_SAMPLE_FRAMES = 5


# --- source protocol -------------------------------------------------------
@dataclass
class Unit:
    """One acquirable chunk of a source."""

    id: str
    est_bytes: int
    payload: object  # source-specific handle used by fetch()


class ScaleSource(Protocol):
    def list_units(self) -> Iterator[Unit]:
        """Lazily yield units (no full-corpus materialization)."""

    def fetch(self, unit: Unit, dest: Path) -> int:
        """Materialize `unit` as a mini LeRobot dataset under `dest`; return bytes written."""


# --- helpers ---------------------------------------------------------------
def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def _read_manifest(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {
        "processed_units": [],
        "processed_episodes": [],
        "total_processed_bytes": 0,
        "peak_raw_bytes": 0,
        "episodes_passed": 0,
        "episodes_quarantined": 0,
    }


def _write_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _append_curated(curated_root: Path, batch_root: Path, unit_id: str, passing: set[int]) -> tuple[int, int]:
    """Write a unit's passing episodes as a new curated chunk file. Returns (episodes, frames)."""
    files = sorted((batch_root / "data").rglob("*.parquet"))
    table = pa.concat_tables([pq.read_table(f) for f in files])
    ep = np.asarray(table.column("episode_index").to_pylist())
    mask = np.isin(ep, list(passing))
    kept = table.filter(pa.array(mask.tolist(), type=pa.bool_()))

    out_dir = curated_root / "data" / f"chunk-{unit_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(kept, out_dir / "file-000.parquet")

    # Copy meta once (first unit).
    if not (curated_root / "meta" / "info.json").exists():
        (curated_root / "meta").mkdir(parents=True, exist_ok=True)
        for name in ("info.json", "tasks.parquet", "stats.json"):
            src = batch_root / "meta" / name
            if src.exists():
                shutil.copy2(src, curated_root / "meta" / name)
    return len(passing), int(kept.num_rows)


def _quarantine_failures(
    quarantine_root: Path, batch_root: Path, failures: dict[int, list[str]], subcap_bytes: int
) -> None:
    """Rule 4: keep a reason + a tiny frame sample per failed episode, under a sub-cap."""
    if not failures:
        return
    quarantine_root.mkdir(parents=True, exist_ok=True)
    reasons_path = quarantine_root / "episode_rejects.jsonl"
    with reasons_path.open("a", encoding="utf-8") as fh:
        for ep, reasons in sorted(failures.items()):
            fh.write(json.dumps({"episode_index": int(ep), "reasons": reasons}) + "\n")

    # Tiny sample only, and only while under the sub-cap.
    if _dir_size(quarantine_root) >= subcap_bytes:
        return
    files = sorted((batch_root / "data").rglob("*.parquet"))
    table = pa.concat_tables([pq.read_table(f) for f in files])
    ep = np.asarray(table.column("episode_index").to_pylist())
    for episode in sorted(failures):
        if _dir_size(quarantine_root) >= subcap_bytes:
            break
        idx = np.where(ep == episode)[0][:_QUARANTINE_SAMPLE_FRAMES]
        if idx.size:
            sample = table.take(pa.array(idx.tolist()))
            sdir = quarantine_root / "samples"
            sdir.mkdir(parents=True, exist_ok=True)
            pq.write_table(sample, sdir / f"episode-{episode}.parquet")


# --- report ----------------------------------------------------------------
@dataclass
class ScaleReport:
    budget_bytes: int
    peak_raw_bytes: int
    total_processed_bytes: int
    units_processed: int
    episodes_total: int
    episodes_passed: int
    episodes_quarantined: int

    @property
    def invariant_holds(self) -> bool:
        return self.peak_raw_bytes < self.budget_bytes and self.total_processed_bytes > self.budget_bytes

    def assert_invariant(self) -> None:
        if not self.invariant_holds:
            raise AssertionError(
                f"scale invariant FAILED: peak_raw={self.peak_raw_bytes} "
                f"budget={self.budget_bytes} total_processed={self.total_processed_bytes}"
            )

    def as_dict(self) -> dict:
        d = asdict(self)
        d["invariant_holds"] = self.invariant_holds
        d.update(
            budget_mb=round(self.budget_bytes / 1e6, 3),
            peak_raw_mb=round(self.peak_raw_bytes / 1e6, 3),
            total_processed_mb=round(self.total_processed_bytes / 1e6, 3),
            headroom_ratio=round(self.peak_raw_bytes / self.budget_bytes, 3) if self.budget_bytes else None,
            throughput_x_budget=round(self.total_processed_bytes / self.budget_bytes, 2) if self.budget_bytes else None,
        )
        return d


# --- the runner ------------------------------------------------------------
def run_scale(
    source: ScaleSource,
    *,
    scale_id: str,
    dataset_version: str,
    data_root: str | Path,
    budget_gb: float,
    gates: QualityGates,
    calibration: Calibration,
    engine: str = "local",
    catalog_backend: str = "sqlite",
    catalog_dsn: str | None = None,
    headroom: float = 0.9,
    quarantine_subcap_frac: float = 0.05,
    max_units: int | None = None,
    hf_repo: str | None = None,
    license: str | None = None,
) -> ScaleReport:
    root = Path(data_root)
    raw_root = root / "raw" / scale_id
    curated_root = root / "curated" / dataset_version
    quarantine_root = root / "quarantine" / dataset_version
    manifest_path = root / "manifest" / f"{dataset_version}.json"

    guard = StorageGuard(raw_root, budget_gb=budget_gb)  # RAW-only accounting
    budget_bytes = guard.budget_bytes
    subcap_bytes = int(budget_bytes * quarantine_subcap_frac)
    writer = CatalogWriter(catalog_backend, catalog_dsn)

    manifest = _read_manifest(manifest_path)
    processed_units = set(manifest["processed_units"])
    processed_eps = set(manifest["processed_episodes"])
    peak = int(manifest["peak_raw_bytes"])
    total_processed = int(manifest["total_processed_bytes"])
    ep_total = len(processed_eps)
    ep_passed = int(manifest["episodes_passed"])
    ep_quar = int(manifest["episodes_quarantined"])

    log_event(
        "scale_start", scale_id=scale_id, dataset_version=dataset_version,
        budget_gb=budget_gb, headroom=headroom, resuming_units=len(processed_units),
    )

    new_units = 0
    for unit in source.list_units():
        if unit.id in processed_units:
            log_event("scale_unit_skipped", unit=unit.id, reason="already in manifest (idempotent)")
            continue
        if max_units is not None and new_units >= max_units:
            log_event("scale_max_units_reached", max_units=max_units)
            break

        # Rule 1: pre-admission. With per-unit eviction, raw is ~empty here; a unit that
        # cannot fit budget*headroom is refused rather than overshooting.
        current_raw = guard.used_bytes()
        if current_raw + unit.est_bytes > budget_bytes * headroom:
            if unit.est_bytes > budget_bytes * headroom:
                log_event("scale_unit_too_big", unit=unit.id, est_bytes=unit.est_bytes,
                          budget_bytes=budget_bytes, note="unit exceeds budget*headroom; skipped")
                continue
            log_event("scale_backpressure", unit=unit.id, current_raw=current_raw, est=unit.est_bytes)

        batch_dir = raw_root / unit.id
        actual = source.fetch(unit, batch_dir)
        peak = max(peak, guard.used_bytes())  # Rule 5: measure right after fetch
        total_processed += actual

        # Process: gate this unit's episodes.
        report = run_signal_gates(batch_dir, gates.signal, calibration, engine=engine)
        passing = {v.episode_index for v in report.verdicts if v.passed}
        failing = {v.episode_index: v.reasons for v in report.verdicts if not v.passed}

        # Rule 2 (durable commit before evict): curate -> quarantine -> catalog -> manifest.
        _append_curated(curated_root, batch_dir, unit.id, passing)
        _quarantine_failures(quarantine_root, batch_dir, failing, subcap_bytes)

        processed_eps.update(v.episode_index for v in report.verdicts)
        ep_total = len(processed_eps)
        ep_passed += len(passing)
        ep_quar += len(failing)

        rec = build_record(
            dataset_version, scale_id, curated_root,
            hf_repo=hf_repo, license=license,
            gate_pass_rate=round(ep_passed / ep_total, 4) if ep_total else None,
            notes=f"scale run; {ep_passed}/{ep_total} episodes passed",
        )
        writer.record_version(rec)  # committed BEFORE eviction

        processed_units.add(unit.id)
        new_units += 1
        manifest.update(
            dataset_version=dataset_version,
            budget_bytes=budget_bytes,
            processed_units=sorted(processed_units),
            processed_episodes=sorted(processed_eps),
            total_processed_bytes=total_processed,
            peak_raw_bytes=peak,
            episodes_passed=ep_passed,
            episodes_quarantined=ep_quar,
        )
        _write_manifest(manifest_path, manifest)

        # Rule 2: evict raw only now.
        freed = guard.evict(batch_dir)
        peak = max(peak, guard.used_bytes())
        log_event(
            "scale_unit_done", unit=unit.id, batch_bytes=actual, freed_bytes=freed,
            passed=len(passing), failed=len(failing), raw_now=guard.used_bytes(),
        )

    result = ScaleReport(
        budget_bytes=budget_bytes,
        peak_raw_bytes=peak,
        total_processed_bytes=total_processed,
        units_processed=len(processed_units),
        episodes_total=ep_total,
        episodes_passed=ep_passed,
        episodes_quarantined=ep_quar,
    )
    log_event("scale_done", **result.as_dict())
    return result


# --- synthetic source (seeded, local, network-free) ------------------------
def _list_col(arr: np.ndarray) -> pa.Array:
    return pa.array([row.tolist() for row in arr], type=pa.list_(pa.float32()))


def make_synthetic_store(
    store: Path, *, n_chunks: int = 8, eps_per_chunk: int = 3, frames: int = 1200,
    bad_every: int = 7, seed: int = 0,
) -> Path:
    """Generate a multi-chunk v3.0-shaped dataset (one data file per chunk).

    Every `bad_every`-th global episode gets a jerk spike so it fails the gates
    (exercises quarantine). Deterministic given `seed`.
    """
    store = Path(store)
    if store.exists():
        shutil.rmtree(store)
    (store / "meta").mkdir(parents=True)
    rng = np.random.default_rng(seed)
    global_ep = 0
    for c in range(n_chunks):
        states, actions, tss, eps, fidx, tidx = [], [], [], [], [], []
        for _ in range(eps_per_chunk):
            t = np.arange(frames)
            state = np.stack([np.sin(t / 5 + d) + rng.normal(0, 0.01, frames) for d in range(7)], axis=1)
            action = np.stack([np.cos(t / 6 + d) + rng.normal(0, 0.01, frames) for d in range(7)], axis=1)
            if global_ep % bad_every == 0:
                # Sustained chatter over ~5% of frames -> anomalous-frame fraction
                # exceeds the 1% gate at any episode length (a lone spike would not).
                k = max(6, int(0.05 * frames))
                seg = slice(frames // 2, frames // 2 + k)
                action[seg, 0] += 50.0 * ((-1) ** np.arange(k))
            states.append(state)
            actions.append(action)
            tss.append((t / 10).astype(np.float32))
            eps.extend([global_ep] * frames)
            fidx.extend(range(frames))
            tidx.extend([global_ep % 4] * frames)
            global_ep += 1
        table = pa.table(
            {
                "observation.state": _list_col(np.vstack(states)),
                "action": _list_col(np.vstack(actions)),
                "timestamp": pa.array(np.concatenate(tss), type=pa.float32()),
                "episode_index": pa.array(eps, type=pa.int64()),
                "frame_index": pa.array(fidx, type=pa.int64()),
                "task_index": pa.array(tidx, type=pa.int64()),
            }
        )
        d = store / "data" / f"chunk-{c:03d}"
        d.mkdir(parents=True)
        pq.write_table(table, d / "file-000.parquet")

    info = {
        "codebase_version": "v3.0", "fps": 10,
        "features": {
            "observation.state": {"dtype": "float32", "shape": [7]},
            "action": {"dtype": "float32", "shape": [7]},
            "task_index": {"dtype": "int64", "shape": [1]},
            "observation.images.cam": {"dtype": "video", "shape": [8, 8, 3]},
        },
    }
    (store / "meta" / "info.json").write_text(json.dumps(info))
    pq.write_table(pa.table({"task_index": [0, 1, 2, 3], "task": ["a", "b", "c", "d"]}),
                   store / "meta" / "tasks.parquet")
    return store


class SyntheticScaleSource:
    """Serves each chunk of a local store as a fetchable unit."""

    def __init__(self, store: Path) -> None:
        self.store = Path(store)
        self._chunks = sorted((self.store / "data").glob("chunk-*"))

    def list_units(self) -> Iterator[Unit]:
        for ch in self._chunks:
            est = _dir_size(ch)
            yield Unit(id=ch.name.split("-", 1)[1], est_bytes=est, payload=ch)

    def fetch(self, unit: Unit, dest: Path) -> int:
        dest = Path(dest)
        (dest / "data" / f"chunk-{unit.id}").mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.store / "meta", dest / "meta", dirs_exist_ok=True)
        for f in Path(unit.payload).glob("*.parquet"):
            shutil.copy2(f, dest / "data" / f"chunk-{unit.id}" / f.name)
        return _dir_size(dest)


def _synthetic_calibration() -> Calibration:
    """Fixed unit calibration (center 0, scale 1); jerk>6 / action>5 flags a frame."""
    return Calibration(
        source="synthetic", n_frames=10_000, anomaly_percentile=99.9,
        jerk_center=[0.0] * 14, jerk_scale=[1.0] * 14,
        action_center=[0.0] * 7, action_scale=[1.0] * 7,
        jerk_score_threshold=6.0, action_score_threshold=5.0,
    )


def run_synthetic_scale(
    data_root: str | Path, *, seed: int = 0, catalog_dsn: str | None = None,
    n_chunks: int = 8, eps_per_chunk: int = 3, frames: int = 1200,
) -> ScaleReport:
    """Seeded synthetic proof. Budget is set to total/4 so total processed = 4x budget
    while each unit (= total/n_chunks) fits comfortably under budget."""
    root = Path(data_root)
    store = make_synthetic_store(root / "_synth_store", n_chunks=n_chunks,
                                 eps_per_chunk=eps_per_chunk, frames=frames, seed=seed)
    total = _dir_size(store / "data")
    budget_gb = (total / 4) / BYTES_PER_GB  # total processed will be ~4x the budget
    gates = load_quality_gates()  # real gate thresholds (config/quality_gates.yaml)
    return run_scale(
        SyntheticScaleSource(store),
        scale_id="synthetic", dataset_version="v0.0.0-synthetic-scale",
        data_root=root, budget_gb=budget_gb, gates=gates, calibration=_synthetic_calibration(),
        engine="local", catalog_backend="sqlite",
        catalog_dsn=catalog_dsn or str(root / "catalog.db"),
        license="synthetic (generated)",
    )


# --- real Hugging Face source (run on Ubuntu — see docs/02-development.md) --------
class HfScaleSource:
    """Serves each chunk of a Hub LeRobot dataset as a fetchable unit.

    Perf note (the "gotchas at scale" fix): enumeration is scoped to the ``data`` prefix
    and iterated lazily, so we never materialize the full repo tree (a full-DROID mirror
    has thousands of video files — listing all of them recursively is what hung in M2).
    Per-unit video sizes/paths are resolved from the ``video_path`` template + a targeted
    ``get_paths_info`` call, not a recursive listing. Path templates are read from the
    dataset's own ``info.json``, so this adapts to v2.0 (per-episode) or v3.0 (aggregated).
    """

    def __init__(self, hf_repo: str, revision: str | None = None) -> None:
        from huggingface_hub import HfApi, hf_hub_download

        self.api = HfApi()
        self.repo = hf_repo
        self.revision = revision
        info_local = hf_hub_download(hf_repo, "meta/info.json", repo_type="dataset", revision=revision)
        self.info = json.loads(Path(info_local).read_text(encoding="utf-8"))
        self.video_path_tmpl = self.info.get("video_path")
        self.video_keys = [
            k for k, v in self.info.get("features", {}).items() if v.get("dtype") in ("video", "image")
        ]

    def _indices(self, data_path: str) -> tuple[int, int]:
        import re

        m = re.search(r"chunk-(\d+)/(?:file|episode)-(\d+)", data_path)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    def list_units(self) -> Iterator[Unit]:
        tree = self.api.list_repo_tree(
            self.repo, path_in_repo="data", repo_type="dataset", recursive=True, revision=self.revision
        )
        for f in tree:
            if type(f).__name__ != "RepoFile" or not f.path.endswith(".parquet"):
                continue
            chunk, fi = self._indices(f.path)
            data_size = int(getattr(f, "size", 0) or (getattr(f, "lfs", None) and f.lfs.size) or 0)
            vpaths, vsize = [], 0
            if self.video_path_tmpl and self.video_keys:
                vpaths = [
                    self.video_path_tmpl.format(video_key=k, chunk_index=chunk, file_index=fi)
                    for k in self.video_keys
                ]
                for pi in self.api.get_paths_info(self.repo, vpaths, repo_type="dataset", revision=self.revision):
                    vsize += int(getattr(pi, "size", 0) or 0)
            yield Unit(id=f"{chunk:03d}-{fi:03d}", est_bytes=data_size + vsize,
                       payload={"data": f.path, "videos": vpaths})

    def fetch(self, unit: Unit, dest: Path) -> int:
        from huggingface_hub import hf_hub_download

        dest = Path(dest)
        for m in ("meta/info.json", "meta/tasks.parquet"):
            try:
                hf_hub_download(self.repo, m, repo_type="dataset", revision=self.revision, local_dir=str(dest))
            except Exception:
                pass
        for path in [unit.payload["data"], *unit.payload["videos"]]:
            try:
                hf_hub_download(self.repo, path, repo_type="dataset", revision=self.revision, local_dir=str(dest))
            except Exception:
                pass  # a camera stream may be absent for some units
        return _dir_size(dest)


def run_hf_scale(
    source_id: str, *, data_root: str | Path = "./data", engine: str = "spark",
    catalog_backend: str = "postgres", catalog_dsn: str | None = None, max_units: int | None = None,
) -> ScaleReport:
    """Real DROID-slice scale run (Ubuntu). Requires the calibrate_from artifact to exist
    (produced by an earlier droid-100 `make demo`)."""
    root = Path(data_root)
    cfg = load_sources("config/sources.yaml")
    src = cfg.get(source_id)
    gates = load_quality_gates()
    calib_path = root / "calibration" / f"{gates.signal.calibrate_from or source_id}.json"
    if not calib_path.exists():
        raise FileNotFoundError(
            f"calibration artifact {calib_path} not found — run `make demo SOURCE=droid-100` first "
            "so thresholds are calibrated from the dev source."
        )
    calibration = Calibration.from_file(calib_path)
    return run_scale(
        HfScaleSource(src.hf_repo, src.revision),
        scale_id=source_id, dataset_version=f"v0.1.0-{source_id}",
        data_root=root, budget_gb=cfg.storage_budget_gb, gates=gates, calibration=calibration,
        engine=engine, catalog_backend=catalog_backend, catalog_dsn=catalog_dsn,
        max_units=max_units, hf_repo=src.hf_repo, license=src.license,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="scale", description=__doc__)
    p.add_argument("--synthetic", action="store_true", help="run the seeded synthetic proof (local)")
    p.add_argument("--source", default=None, help="real source id from sources.yaml (e.g. droid-slice) [Ubuntu]")
    p.add_argument("--data-root", default="./data")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--engine", choices=["spark", "local"], default="local")
    p.add_argument("--catalog-backend", choices=["postgres", "sqlite"], default="sqlite")
    p.add_argument("--max-units", type=int, default=None, help="stop after N new units (quick guard-trip confirmation)")
    args = p.parse_args(argv)

    if args.source and not args.synthetic:
        report = run_hf_scale(args.source, data_root=args.data_root, engine=args.engine,
                              catalog_backend=args.catalog_backend, max_units=args.max_units)
    else:
        report = run_synthetic_scale(args.data_root, seed=args.seed)
    d = report.as_dict()
    print("\n=== SCALE INVARIANT (measured) ===")
    print(f"  budget            : {d['budget_mb']} MB")
    print(f"  peak concurrent raw: {d['peak_raw_mb']} MB   (measured max of raw dir)")
    print(f"  total processed   : {d['total_processed_mb']} MB   ({d['throughput_x_budget']}x budget)")
    print(f"  peak/budget ratio : {d['headroom_ratio']}")
    print(f"  episodes          : {report.episodes_passed} passed / {report.episodes_quarantined} quarantined / {report.episodes_total} total")
    print(f"  INVARIANT HOLDS   : {d['invariant_holds']}  (peak<budget AND total>>budget)")
    report.assert_invariant()
    return 0


if __name__ == "__main__":
    sys.exit(main())
