"""CLI entry for canonical ingest + schema validation.

    python -m ingest --source droid-100

Operates on data acquired by M2 (data/raw/<id>). Wired to `make validate` (schema
stage; the M4 signal gates extend it). See docs/02-development.md (M3).
"""

from __future__ import annotations

import argparse
import sys

from acquisition.config import load_sources
from acquisition.logging_utils import log_event

from .canonicalize import ingest_source
from .config import DEFAULT_GATES_PATH, load_quality_gates


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__)
    parser.add_argument("--source", default="droid-100", help="source id from sources.yaml")
    parser.add_argument("--config", default="config/sources.yaml", help="path to sources.yaml")
    parser.add_argument("--gates", default=DEFAULT_GATES_PATH, help="path to quality_gates.yaml")
    args = parser.parse_args(argv)

    cfg = load_sources(args.config)
    gates = load_quality_gates(args.gates)
    result = ingest_source(args.source, data_root=cfg.data_root, gates=gates)

    log_event("ingest_done", source=result.source_id, passed=result.passed, action=result.action)
    # Non-zero exit only on an operational error; a clean quarantine is a valid outcome.
    return 1 if result.action == "error" else 0


if __name__ == "__main__":
    sys.exit(main())
