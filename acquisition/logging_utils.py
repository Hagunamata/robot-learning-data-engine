"""Structured stdout logging: one JSON object per line for ELK ingestion."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any


def log_event(event: str, **fields: Any) -> None:
    """Emit one structured JSON log line to stdout."""
    record: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
    }
    record.update(fields)
    sys.stdout.write(json.dumps(record, default=str) + "\n")
    sys.stdout.flush()
