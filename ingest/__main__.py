"""CLI entry for the validation stage (canonical ingest + quality gates)."""

from __future__ import annotations

import argparse
import sys

from acquisition.config import load_sources
from acquisition.logging_utils import log_event
from acquisition.storage_guard import StorageGuard

from .canonicalize import ingest_source
from .config import DEFAULT_GATES_PATH, load_quality_gates
from .curate import run_validation


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__)
    parser.add_argument("--source", default="droid-100", help="source id from sources.yaml")
    parser.add_argument("--config", default="config/sources.yaml", help="path to sources.yaml")
    parser.add_argument("--gates", default=DEFAULT_GATES_PATH, help="path to quality_gates.yaml")
    parser.add_argument(
        "--engine",
        choices=["spark", "local"],
        default="spark",
        help="signal-gate engine: 'spark' (scale) or 'local' (dev/CI, no JVM)",
    )
    parser.add_argument(
        "--schema-only", action="store_true", help="stop after the M3 schema gate"
    )
    args = parser.parse_args(argv)

    cfg = load_sources(args.config)
    gates = load_quality_gates(args.gates)

    if args.schema_only:
        result = ingest_source(args.source, data_root=cfg.data_root, gates=gates)
        log_event("ingest_done", source=result.source_id, passed=result.passed, action=result.action)
        return 1 if result.action == "error" else 0

    guard = StorageGuard(cfg.data_root, budget_gb=cfg.storage_budget_gb)
    guard.log_usage("startup", source=args.source)
    result = run_validation(
        args.source, guard, data_root=cfg.data_root, gates=gates, engine=args.engine
    )
    log_event(
        "validate_done",
        source=result.source_id,
        schema_action=result.schema_action,
        gate_pass_rate=result.gate_pass_rate,
        curated_frames=result.curated_frames,
    )
    return 1 if result.schema_action == "error" else 0


if __name__ == "__main__":
    sys.exit(main())
