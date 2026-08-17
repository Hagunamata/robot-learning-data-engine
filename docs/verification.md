# Verification runbook

A step-by-step checklist to bring the pipeline up on a fresh **Ubuntu** machine and
confirm, in the terminal, what works. Each step lists **what it does**, the **command**,
and the **expected result** to check against. Work top to bottom; later steps assume the
earlier ones ran.

> Everything except the real `droid-slice` scale run (Step 8) and the containerised stack
> (Step 9) was exercised on the developer's machine; the numbers below are what to expect.
> If something differs, note it in the results table at the bottom.

## Conventions

- Run all commands from the repository root.
- `make test`, the synthetic proof, and every `ENGINE=local` / `CATALOG=sqlite` step need
  **no Java, no Postgres, no network**.
- The real acquisition steps download from the Hugging Face Hub; the Spark engine needs
  Java; the Postgres catalog and Airflow DAG need the Docker stack.

## Prerequisites

```bash
git clone <this-repo> && cd robot-learning-data-engine
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Optional, only for the steps that use them:
sudo apt install default-jdk   # Step 4b / Step 8 with ENGINE=spark — Spark needs a JVM
pip install pyspark            # Step 4b / Step 8 with ENGINE=spark
pip install "psycopg[binary]"  # Step 9 with CATALOG=postgres
# Docker + Docker Compose v2   # Step 9 (the stack)
# sqlite3 CLI is NOT required — the steps below query the catalog with Python.
```

> **Everything works without Spark.** The `local` engine (pyarrow+numpy) returns
> identical verdicts and needs no Java. Only use `ENGINE=spark` if you want to exercise
> the Spark path; if `java` isn't installed the run stops with a clear message telling
> you to install a JDK or use `ENGINE=local`.

---

## Step 0 — Unit & integration tests (no network/JVM/DB)

Confirms the whole codebase's logic: storage guard, schema gate, signal-gate maths,
curation, catalog, augmenter, full pipeline, and the scale invariant.

```bash
make test
```

**Expect:** `33 passed`.

- [ ] working? ______

---

## Step 1 — Synthetic process-and-evict proof (the headline, no network)

Runs the batched acquire→gate→curate→evict loop on a seeded synthetic multi-file source
with a deliberately tiny budget, and **asserts** the storage invariant.

```bash
make scale        # == python -m scale --synthetic
```

**Expect** a block like:

```
=== SCALE INVARIANT (measured) ===
  budget             : 0.625 MB
  peak concurrent raw : 0.314 MB      (measured max of the raw dir)
  total processed    : 2.508 MB      (4.01x budget)
  peak/budget ratio  : 0.502
  episodes           : 20 passed / 4 quarantined / 24 total
  INVARIANT HOLDS    : True   (peak < budget  AND  total >> budget)
```

The exact MB depend on the machine, but the relationship must hold: **peak < budget <
total**, and it exits 0 (the run asserts the invariant).

- [ ] working? ______

---

## Step 2 — Acquisition plan (dry-run, network but no download)

Lists exactly what would be pulled for the dev source and the projected footprint,
without downloading data. Confirms Hub connectivity + version detection.

```bash
make ingest SOURCE=droid-100 DRY_RUN=1
```

**Expect** JSON log lines including:
- `info_detected ... "codebase_version": "v3.0", "total_episodes": 100, "total_frames": 32212, "total_tasks": 47`
- ten `would_pull` lines (meta → data → videos)
- `acquire_done ... "gb_pulled": 0.432, "stopped_at": null`
- `data/` still contains only `.gitkeep` files (nothing downloaded).

- [ ] working? ______

---

## Step 3 — Acquire the dev source for real (~0.43 GB download)

Pulls `lerobot/droid_100` into `data/raw/droid-100/` under the storage guard.

```bash
make ingest SOURCE=droid-100
du -sh data/raw/droid-100
```

**Expect:** `pulled` log lines with a rising `used_gb`, a final `storage_usage` well
under 25 GB, and `du` ≈ **0.43 GB**. Files land under
`data/raw/droid-100/{meta,data,videos}/…`.

- [ ] working? ______

---

## Step 4 — Validate → gate → curate → evict (the process-and-evict loop on real data)

### 4a. Local engine (no Java)

```bash
make validate SOURCE=droid-100 ENGINE=local
```

**Expect:**
- `schema_result ... "passed": true, "codebase_version": "v3.0"`
- `calibration_built ... "n_frames": 32212`
- `signal_gates_done ... "gate_pass_rate": 0.94` (≈ 94/100 episodes pass)
- `evicted_raw` then `batch_metrics`
- After: `data/curated/droid-100/` exists, **`data/raw/droid-100/` is gone**,
  `data/quarantine/droid-100/episode_rejects.json` lists ~6 episodes.

```bash
ls data/curated/droid-100 && test ! -e data/raw/droid-100 && echo "raw evicted OK"
```

- [ ] working? ______

### 4b. Spark engine (optional — needs a JDK + pyspark) — same result path

> Requires Java: `sudo apt install default-jdk` and `pip install pyspark`. If Java is
> not installed the run stops with a clear message — that is expected; **you can skip
> 4b**, since 4a already proved the gate logic and both engines share one pure core.

Step 4a evicted the raw copy, so re-acquire first, then gate with Spark:

```bash
make ingest SOURCE=droid-100
make validate SOURCE=droid-100 ENGINE=spark
```

**Expect:** the same verdicts as 4a; a Spark session starts and stops. This confirms the
`applyInPandas` job.

- [ ] working? ______  (or skipped — no JDK)

---

## Step 5 — Full pipeline (`make demo`): two catalogued versions

Acquire (skipped if raw present) → validate → catalog the real version → mint synthetic
episodes for under-represented tasks → gate + curate them → merge → catalog the augmented
version.

```bash
make demo SOURCE=droid-100 ENGINE=local CATALOG=sqlite

# Read the catalog with Python (no sqlite3 CLI needed):
python - <<'PY'
import sqlite3
for r in sqlite3.connect("data/catalog.db").execute(
    "SELECT dataset_version, episode_count, frame_count, gate_pass_rate, notes "
    "FROM dataset_version ORDER BY dataset_version"):
    print(r)
PY
# (Or, if you have the CLI: sudo apt install sqlite3; then sqlite3 data/catalog.db "SELECT ...".)
```

**Expect** two rows:
- `v0.1.0-droid-100 | 94 | 31052 | 0.94 | real`
- `v0.2.0-droid-100-aug | 114 | 34829 | 0.95 | real+synthetic (20 synthetic episodes)`

(Numbers may shift slightly if you re-tune the gate thresholds.)

- [ ] working? ______

---

## Step 6 — Data-quality dashboard (Streamlit, reads the catalog)

```bash
make report        # opens http://localhost:8501
```

**Expect** a page with: **Dataset versions & lineage** (the rows from Step 5), **Gate
pass-rate by version**, **Task distribution** (per selected version), and — once you have
run a scale step — **Storage: peak concurrent raw vs budget** with `Invariant HOLDS ✅`.

- [ ] working? ______

---

## Step 7 — Guard-trip on real file sizes (fast, no big download)

Shows the guard refusing to overshoot: with a 0.2 GB budget it pulls meta + data + the
first video, then stops.

> Run this against an **empty, isolated** data-root. The guard measures the data-root's
> footprint, so if you point it at the `data/` you've already filled in Steps 3–6 it will
> (correctly) report you're already over the 0.2 GB budget and pull nothing. `--data-root`
> gives it a clean budget to demonstrate against.

```bash
python -m acquisition --source droid-100 --dry-run --budget-gb 0.2 --data-root /tmp/rlde-guardtrip
```

**Expect:** `would_pull` for meta + data + the first video, then a `budget_reached` event
and `acquire_done ... "files_pulled": 6, "gb_pulled": 0.166` (stops before the second
video, projected 0.354 GB > 0.2 GB).

- [ ] working? ______

---

## Step 8 — Real scale run on `droid-slice` (the storage-aware proof on real DROID)

> This streams tens of GB through a 25 GB budget. Start bounded with `--max-units`; it is
> resumable (re-run to continue — already-pulled chunks are skipped).

```bash
# Calibration must exist first (Step 5 produced data/calibration/droid-100.json).
python -m scale --source droid-slice --max-units 60 --engine local --catalog-backend sqlite

# Inspect the measured figures and the exact episodes pulled:
cat data/manifest/v0.1.0-droid-slice.json
```

**Expect:** a `scale_done` log with `peak_raw_mb` < the 25 GB budget, `total_processed_mb`
≫ budget, `invariant_holds: true`. The manifest records `processed_units`,
`processed_episodes` (the real DROID episode indices pulled), `peak_raw_bytes`, and
`total_processed_bytes`. Re-running continues from where it stopped.

- [ ] working? ______  — record peak / budget / total: ______

---

## Step 9 — The containerised stack (Postgres catalog, optional Spark, Airflow)

```bash
cp .env.example .env         # review values
make up                      # starts Postgres; catalog schema applied from postgres/init/
make demo SOURCE=droid-100 ENGINE=spark CATALOG=postgres
psql "postgresql://rlde:change_me@localhost:5432/robot_learning" \
  -c "SELECT dataset_version, episode_count, task_distribution FROM catalog.dataset_version;"
make down
```

**Expect:** Postgres healthy; the catalog rows in the `catalog.dataset_version` table
(same content as the sqlite run). The Airflow DAG (`airflow/dags/robot_learning_dag.py`)
runs the same stage callables — trigger it from the Airflow UI once Airflow is added to
the compose stack.

- [ ] working? ______

---

## Results log

| Step | What | Result | Notes / issues |
|------|------|--------|----------------|
| 0 | `make test` (33 passed) | | |
| 1 | synthetic invariant | | |
| 2 | acquire dry-run | | |
| 3 | acquire droid-100 (~0.43 GB) | | |
| 4a | validate local (94/100, evict) | | |
| 4b | validate spark | | |
| 5 | `make demo` (2 versions) | | |
| 6 | dashboard | | |
| 7 | guard-trip | | |
| 8 | droid-slice scale (peak<budget<total) | | |
| 9 | stack (postgres/spark) | | |

**Known constraints carried from development** (context for anything that misbehaves):
- The `droid-slice` mirror is the full DROID corpus (~1.7 TB on the Hub); always bound the
  first run with `--max-units`.
- Enumerating that mirror is scoped to the `data/` prefix and iterated lazily — a naive
  full recursive listing hangs (see the development history).
- Videos are curated whole, not re-segmented per episode; `data/curated/` grows to the
  kept corpus (bounded by disk, separate from the 25 GB *raw* budget).
- Gate thresholds (`config/quality_gates.yaml`) are a starting point — tune
  `anomaly_percentile` / `max_anomalous_frame_ratio` and re-run to change the pass-rate.
