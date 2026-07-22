"""Structured stdout logging for the pipeline.

Every stage emits one JSON object per line so the ELK stack can ingest it without a
grok pattern. The storage guard uses this to log the running disk-used-vs-budget
figure at every stage (see docs/01-conception.md §5, working agreement §9).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any


def log_event(event: str, **fields: Any) -> None:
    """Emit one structured JSON log line to stdout.

    Args:
        event: short machine-readable event name (e.g. "storage_usage", "pulled").
        **fields: arbitrary JSON-serializable context for the event.
    """
    record: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
    }
    record.update(fields)
    sys.stdout.write(json.dumps(record, default=str) + "\n")
    sys.stdout.flush()
