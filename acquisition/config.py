"""Load and parse config/sources.yaml.

Keeps the config contract (CLAUDE_CODE_BRIEF.md §6.1) in one typed place so the rest
of acquisition never touches raw YAML. Values are the human's (M1); repo IDs and
licenses were verified against the live sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

DEFAULT_SOURCES_PATH = "config/sources.yaml"


@dataclass
class Source:
    """One entry from `sources.yaml`."""

    id: str
    hf_repo: str
    role: str = ""
    codebase_version: Optional[str] = None  # expected LeRobot format; detected at runtime
    revision: Optional[str] = None           # git ref on the Hub; None => default branch
    max_episodes: Optional[int] = None       # a ceiling; the storage guard is the real limit
    license: Optional[str] = None            # cited basis (official DROID = CC-BY-4.0)
    license_mirror_stated: Optional[str] = None
    enabled: bool = False


@dataclass
class SourcesConfig:
    """Top-level `sources.yaml` contents."""

    storage_budget_gb: float
    data_root: str
    sources: list[Source] = field(default_factory=list)

    def get(self, source_id: str) -> Source:
        for s in self.sources:
            if s.id == source_id:
                return s
        raise KeyError(
            f"source '{source_id}' not found in config "
            f"(known: {', '.join(s.id for s in self.sources)})"
        )


def load_sources(path: str | Path = DEFAULT_SOURCES_PATH) -> SourcesConfig:
    """Parse `sources.yaml` into a typed :class:`SourcesConfig`."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    sources = [Source(**entry) for entry in raw.get("sources", [])]
    return SourcesConfig(
        storage_budget_gb=float(raw["storage_budget_gb"]),
        data_root=str(raw["data_root"]),
        sources=sources,
    )
