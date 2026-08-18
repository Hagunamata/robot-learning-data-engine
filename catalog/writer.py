"""Persist one dataset-version row to the catalog (postgres or sqlite)."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from acquisition.logging_utils import log_event

from .record import CatalogRecord

_ENV_LOADED = False


def _load_dotenv_once() -> None:
    """Load a project-root .env into os.environ without overriding existing vars."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True
    for candidate in (Path(".env"), Path(__file__).resolve().parent.parent / ".env"):
        if candidate.is_file():
            for line in candidate.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
            break

COLUMNS = [
    "dataset_version", "source_id", "hf_repo", "license", "episode_count",
    "frame_count", "task_distribution", "gate_pass_rate", "bytes_on_disk",
    "git_commit", "created_at", "notes",
]

_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS dataset_version (
    dataset_version    TEXT PRIMARY KEY,
    source_id          TEXT NOT NULL,
    hf_repo            TEXT,
    license            TEXT NOT NULL,
    episode_count      INTEGER,
    frame_count        INTEGER,
    task_distribution  TEXT,      -- JSON (sqlite has no JSONB)
    gate_pass_rate     REAL,
    bytes_on_disk      INTEGER,
    git_commit         TEXT,
    created_at         TEXT,
    notes              TEXT
)
"""


class CatalogWriter:
    def __init__(self, backend: str = "sqlite", dsn: str | None = None) -> None:
        self.backend = backend
        self.dsn = dsn

    def record_version(self, record: CatalogRecord) -> None:
        row = record.as_row()
        row["task_distribution"] = json.dumps(row["task_distribution"])
        if self.backend == "sqlite":
            self._sqlite(row)
        elif self.backend == "postgres":
            self._postgres(row)
        else:
            raise ValueError(f"unknown catalog backend {self.backend!r} (expected 'postgres' or 'sqlite')")
        log_event(
            "catalog_recorded",
            backend=self.backend,
            dataset_version=record.dataset_version,
            episodes=record.episode_count,
            frames=record.frame_count,
            gate_pass_rate=record.gate_pass_rate,
        )

    def _sqlite(self, row: dict) -> None:
        path = self.dsn or "data/catalog.db"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path)
        try:
            con.execute(_SQLITE_DDL)
            placeholders = ", ".join(["?"] * len(COLUMNS))
            con.execute(
                f"INSERT OR REPLACE INTO dataset_version ({', '.join(COLUMNS)}) VALUES ({placeholders})",
                [row[c] for c in COLUMNS],
            )
            con.commit()
        finally:
            con.close()

    def _postgres(self, row: dict) -> None:
        import psycopg

        _load_dotenv_once()
        dsn = self.dsn or (
            f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
            f"port={os.getenv('POSTGRES_PORT', '5432')} "
            f"dbname={os.getenv('POSTGRES_DB', 'robot_learning')} "
            f"user={os.getenv('POSTGRES_USER', 'rlde')} "
            f"password={os.getenv('POSTGRES_PASSWORD', '')}"
        )
        cols = ", ".join(COLUMNS)
        values = ", ".join("%s::jsonb" if c == "task_distribution" else "%s" for c in COLUMNS)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "dataset_version")
        sql = (
            f"INSERT INTO catalog.dataset_version ({cols}) VALUES ({values}) "
            f"ON CONFLICT (dataset_version) DO UPDATE SET {updates}"
        )
        with psycopg.connect(dsn) as con:
            with con.cursor() as cur:
                cur.execute(sql, [row[c] for c in COLUMNS])
            con.commit()
