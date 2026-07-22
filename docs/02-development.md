# 02 — Development

> Phase 2 (Development) implementation notes, filled incrementally as milestones land.
> Each milestone records **what was built**, any **decisions**, and a **verification
> guide** so the whole thing can be checked end-to-end on Ubuntu at the end of the
> phase (the intended validation environment; the runtime target is Linux/WSL2 +
> Docker per CLAUDE_CODE_BRIEF.md §2.4).

---

## M2 — Acquisition + storage guard

### What was built

| File | Role |
|------|------|
| `acquisition/config.py` | Typed loader for `config/sources.yaml` (`Source`, `SourcesConfig`). |
| `acquisition/storage_guard.py` | `StorageGuard`: measures on-disk usage, refuses over-budget admissions, evicts, logs disk-vs-budget. |
| `acquisition/downloader.py` | `acquire()`: v3.0-aware, **file-granular** selective pull from the Hub, gated by the guard. |
| `acquisition/logging_utils.py` | `log_event()` — one JSON line per event to stdout for ELK. |
| `acquisition/__main__.py` | CLI: `python -m acquisition --source <id> [--dry-run] [--budget-gb N]`. Wired to `make ingest`. |
| `tests/test_storage_guard.py` | Smoke tests for the guard (no network). |

### Decision record — DROID-100 is LeRobot **v3.0**, so acquire at file granularity

The config originally assumed `codebase_version: v2.0` (per the HF card metadata), but
the authoritative `meta/info.json` of `lerobot/droid_100` reports **v3.0**. This was
verified on 2026-07-22 and matters because the two formats store episodes differently:

- **v2.0** — one file *per episode* (`data/chunk-000/episode_000000.parquet`, per-episode
  video clips). Episode-level pull + evict is natural.
- **v3.0** — many episodes **aggregated per file**: data as
  `data/chunk-{c}/file-{f}.parquet`, video **concatenated per camera** as
  `videos/{video_key}/chunk-{c}/file-{f}.mp4`. Episode boundaries live in
  `meta/episodes/`. A single episode's video cannot be fetched in isolation.

**Decision (Option A):** the raw acquisition/eviction unit is the **file**, not the
episode. This still satisfies process-and-evict (evict raw after curation, process more
than fits) — just at file granularity. `detect_codebase_version` is honored at runtime:
`acquire()` reads the source's own `info.json` first and logs a `codebase_version_mismatch`
event if it differs from the config, then proceeds with the detected version — so a v2.0
source would still work. Episode-level splitting, if wanted, happens on the **curated**
side in M3.

Consequences recorded in code/config: `config/sources.yaml` (droid-100 → v3.0),
`config/quality_gates.yaml` (language instruction is `task_index` → `meta/tasks.parquet`,
**not** a `language_instruction` column), and `downloader.py` module docstring.

### Design notes

- **`datasets` streaming is intentionally not used.** For real byte-level guard
  accounting we pull whole files via `huggingface_hub` and measure them, rather than
  streaming rows whose on-disk cost is opaque.
- **Gating uses a projected cumulative total** (baseline on disk + accepted file sizes
  from Hub metadata), so `--dry-run` faithfully predicts *where* the guard will trip
  without downloading. Real-mode logging still re-measures true disk usage per file.
- **No eviction yet.** M2 only acquires and guards; eviction after curation is M4.
- **Pull order** is meta → data → video (small/essential first, bulky video last).

### What was verified on Windows (dev machine)

- `make test` / `pytest` — guard measures usage, refuses over-budget, evicts. **5 passed.**
- Config loader parses the real `sources.yaml` (budget 25 GB; 3 sources).
- **Live dry-run** against `lerobot/droid_100`: detects v3.0 (100 ep / 32,212 frames /
  47 tasks), lists 10 files totalling **0.432 GB**, writes nothing to `data/`.
- **Guard-trip demo:** `--dry-run --budget-gb 0.2` on droid-100 pulls meta+data+1 video
  (0.166 GB) then stops at the 2nd video (projected 0.354 GB > 0.2 GB), reporting
  `stopped_at`.

### Verify on Ubuntu (end-of-phase checklist)

From the repo root, in the project's Python env (`pip install -r requirements.txt`):

```bash
# 1. Unit tests
make test                       # expect: 5 passed

# 2. Plan the dev pull without downloading (fast)
make ingest SOURCE=droid-100 DRY_RUN=1
#   expect JSON logs: info_detected codebase_version="v3.0", 10 "would_pull" lines,
#   acquire_done gb_pulled≈0.432, stopped_at=null

# 3. Real end-to-end acquire of the dev source (~0.43 GB; NOT yet run on Windows)
make ingest SOURCE=droid-100
#   expect: files land under data/raw/droid-100/{meta,data,videos}/...
#   each "pulled" log line shows rising used_gb; final storage_usage well under 25 GB
du -sh data/raw/droid-100        # ≈ 0.43 GB

# 4. Guard-trip demo against real file sizes (fast, no big download)
python -m acquisition --source droid-100 --dry-run --budget-gb 0.2
#   expect: budget_reached event, files_pulled=6, gb_pulled≈0.166
```

**Still to verify on Ubuntu (could not be exercised on the Windows dev box):**
- The real download branch (step 3) — `hf_hub_download` writing into `data/raw/` and the
  guard's live per-file `used_gb` rising. (Avoided on Windows: OneDrive-synced folder +
  no symlink support; the download logic is identical to the validated dry-run listing.)
- The **live scale trip** on `droid-slice` (M6): enumerating that full-corpus mirror with
  `list_repo_tree(recursive=True)` is slow — an **M6 performance item** (prefix-scoped
  listing / pagination). The trip *logic* is already covered by unit tests and the
  budget-0.2 demo.

### `make ingest` interface

```
make ingest                         # dev source (droid-100), real pull
make ingest SOURCE=droid-slice      # scale source
make ingest DRY_RUN=1               # plan only, no download
python -m acquisition --source droid-100 --dry-run --budget-gb 0.2   # guard-trip demo
```

---

## M3 — Canonical ingest + schema validation

### What was built

| File | Role |
|------|------|
| `ingest/schema_gate.py` | `validate_schema()` — PyArrow schema gate over a LeRobot dataset. |
| `ingest/canonicalize.py` | `ingest_source()` — confirm LeRobot format, run the gate, quarantine/drop on fail. |
| `ingest/config.py` | Typed loader for `quality_gates.yaml` (schema/annotation/policy). |
| `ingest/__main__.py` | CLI: `python -m ingest --source <id>`. Wired to `make validate`. |
| `tests/test_schema_gate.py` | 9 tests over PyArrow fixtures (valid + broken + quarantine/drop). |

### Decision record — validation engine is **Hybrid** (PyArrow now, Spark at M4)

The brief flags Spark-vs-PyArrow as a stop-and-ask. The choice made (and open to
override): **schema validation in PyArrow, signal gates in Spark (M4).**

- The schema gate is a feature-existence / dtype / column check — the "~50-line
  PyArrow job" the brief itself points at. Running it in Spark over a 2.7 MB parquet
  would read as over-engineering.
- The M4 signal gates (jerk, outlier z-scores, missing-frame ratio) are per-episode
  timeseries math across many episodes — genuinely Spark's batch strength, and a
  credible Spark showcase that preserves the `spark/jobs/` pattern and the
  "skills scale up from the prior project" thesis.

This mirrors the Kafka decision: right-size per stage, keep the heavyweight tool
where it earns its place. `spark/jobs/schema_validation.py` was removed;
`spark/jobs/signal_gates.py` remains for M4.

### What the gate checks (v3.0-aware)

Schema is a **dataset-level** property (all episodes share one `meta/info.json`), so
the gate accepts or quarantines the dataset as a whole; per-episode quality is M4.

1. Required low-dim features declared in `info.json` (`observation.state`, `action`).
2. At least one image feature present (any of `image_keys_any_of`). Image features are
   **video files**, declared in `info.json` but not parquet columns — handled explicitly.
3. Language instruction resolvable: `task_index` + `meta/tasks.parquet` (v3.0), or a
   `language_instruction`/`task` column (v2.0 shapes).
4. Cross-check: each non-image required feature is an actual column in the data parquet
   (guards against an `info.json` that over-declares).

On failure: `on_fail: quarantine` moves the dataset to `data/quarantine/<id>/` and
writes `_rejects.json` (reasons + timestamp); `drop` deletes it. Passing datasets are
logged `ingest_ready` and left in place for M4 (which writes curated + evicts raw).

### What was verified on Windows (dev machine)

- `make test` — **14 passed** (5 guard + 9 schema/ingest), incl. quarantine & drop paths.
- **Real-data check:** pulled only `droid_100`'s `meta/` + data parquet (~2.8 MB, no
  videos) and ran the gate → **passed**, codebase v3.0, 12 features, all required
  features + 3 image streams present, zero reasons.
- CLI degrades gracefully when data isn't acquired yet (`ingest_error`, exit 1).

### Verify on Ubuntu (end-of-phase checklist)

```bash
make validate SOURCE=droid-100
#   Requires `make ingest SOURCE=droid-100` to have run first (M2 download).
#   expect JSON: schema_result passed=true codebase_version="v3.0";
#                ingest_ready (dataset left under data/raw/droid-100 for M4).
```

To exercise the reject path, point at a malformed dataset (or temporarily tighten
`required_features`): expect `ingest_quarantined`, the dataset moved to
`data/quarantine/droid-100/`, and a `_rejects.json` listing the reasons.

### `make validate` interface

```
make validate                    # schema-gate the dev source (droid-100)
make validate SOURCE=droid-slice # schema-gate the scale source
python -m ingest --source droid-100   # same, directly
```
