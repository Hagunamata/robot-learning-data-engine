"""CLI entry for acquisition.  `python -m acquisition --source droid-100 [--dry-run]`."""

from __future__ import annotations

import argparse
import sys

from .config import DEFAULT_SOURCES_PATH, load_sources
from .downloader import acquire
from .logging_utils import log_event
from .storage_guard import StorageGuard


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="acquisition", description=__doc__)
    parser.add_argument("--source", default="droid-100", help="source id from sources.yaml")
    parser.add_argument("--config", default=DEFAULT_SOURCES_PATH, help="path to sources.yaml")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list files and projected footprint without downloading data",
    )
    parser.add_argument(
        "--budget-gb",
        type=float,
        default=None,
        help="override storage_budget_gb from config (useful for guard-trip demos)",
    )
    parser.add_argument(
        "--data-root",
        default=None,
        help="override data_root from config; point a demo at an isolated dir so it "
        "never touches the shared ./data (useful for guard-trip demos)",
    )
    args = parser.parse_args(argv)

    cfg = load_sources(args.config)
    try:
        source = cfg.get(args.source)
    except KeyError as exc:
        log_event("acquire_error", error=str(exc))
        return 2
    if not source.enabled:
        log_event("acquire_skipped", source=source.id, reason="disabled in sources.yaml")
        return 0

    budget_gb = args.budget_gb if args.budget_gb is not None else cfg.storage_budget_gb
    data_root = args.data_root if args.data_root is not None else cfg.data_root
    guard = StorageGuard(data_root, budget_gb=budget_gb)
    guard.log_usage("startup", source=source.id)
    acquire(source, guard, dry_run=args.dry_run)
    guard.log_usage("final", source=source.id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
