-- Catalog schema — one row per published dataset version.
-- DRAFT (M0): columns follow CLAUDE_CODE_BRIEF.md §6.3. Applied on first `make up`
-- once Postgres is wired in (M5). Reused pattern from the prior repo's postgres/init.

CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.dataset_version (
    dataset_version    TEXT PRIMARY KEY,   -- e.g. v0.1.0-droid100
    source_id          TEXT        NOT NULL,
    hf_repo            TEXT,
    license            TEXT        NOT NULL,
    episode_count      INT,
    frame_count        BIGINT,
    task_distribution  JSONB,              -- task -> count
    gate_pass_rate     NUMERIC,            -- passed / (passed + failed)
    bytes_on_disk      BIGINT,
    git_commit         TEXT,
    created_at         TIMESTAMPTZ DEFAULT now(),
    notes              TEXT
);
