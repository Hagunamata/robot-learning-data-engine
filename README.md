# Robot-Learning Data Engine

> A storage-aware batch pipeline that acquires public multimodal robot-demonstration
> data, normalizes it to a canonical format (LeRobot), validates and quality-filters
> it, augments it with synthetic episodes, and publishes a versioned, catalogued
> dataset plus a data-quality view — all under a hard local-disk budget.
>
> Second of a two-part portfolio arc. Sibling project:
> [dark-factory-data-platform](https://github.com/Hagunamata/dark-factory-data-platform).

> **Status:** end-to-end pipeline implemented and tested (33 tests). Runs fully locally
> (`make demo ENGINE=local CATALOG=sqlite`, no server or JVM) or on the Docker stack.
> Read [docs/01-conception.md](docs/01-conception.md) for the design and reasoning,
> [docs/02-development.md](docs/02-development.md) for how it was built, and
> [docs/verification.md](docs/verification.md) to reproduce it step by step.

## At a Glance

```
   Public sources (Hugging Face Hub)
              │
   Storage guard ─► Selective acquisition   (stream only chosen episodes; disk-budget cap)
              │
              ▼
        Canonical ingest       (→ LeRobot format)
              │
   Synthetic ─► Quality gates   (schema: PyArrow · signal: Spark; jerk, outliers)
   generator      │
              ▼
        Curated dataset          (versioned; raw copy evicted)
              │
      ┌───────┴────────┐
      ▼                ▼
  Data catalog     Quality dashboard
  (Postgres/sqlite) (Streamlit)

   Orchestrated by Airflow · packaged with Docker Compose + Makefile
```

**Headline mechanism — process-and-evict:** the storage guard caps resident data at a
configurable disk budget (`storage_budget_gb` in `config/sources.yaml`; set to **25 GB**
for the scale demo, a ≤400 GB design ceiling); acquisition streams a bounded batch; once
a batch is curated, its raw copy is **evicted**. The pipeline thus processes far more
total data than fits on disk at any instant — the ~90 GB `droid-slice` through a 25 GB
budget trips the guard on purpose. The running disk-used-vs-budget figure is logged at
every stage.

## Tech Stack

| Layer                | Component                          | Why                                                                    |
|----------------------|------------------------------------|------------------------------------------------------------------------|
| Acquisition          | `huggingface_hub`                  | Selective, file-granular pulls — never a full-corpus download. *(new)* |
| Storage guard        | Byte-accounting wrapper            | Enforces the configurable disk budget; drives process-and-evict. *(new)* |
| Canonical format     | LeRobot (v3.0)                     | The de-facto standard for robot-learning datasets. *(new)*            |
| Schema gate          | PyArrow                            | Feature/dtype checks — right-sized, no Spark overhead. *(new)*        |
| Signal gates         | Apache Spark (local mode)          | Per-episode jerk/outlier/missing-frame maths at scale. *(reused)*     |
| Catalog / metadata   | PostgreSQL (+ sqlite fallback)     | Durable, queryable dataset-version records. *(reused)*                 |
| Synthetic data       | `data_generator/` (evolved)        | Mints LeRobot episodes for under-represented tasks. *(reused)*         |
| Orchestration        | Apache Airflow                     | Pipeline stages map cleanly to DAG tasks. *(reused)*                   |
| Observability (logs) | ELK stack                          | Centralized, searchable container logs. *(reused)*                     |
| Quality dashboard    | Streamlit                          | Pass-rate, task mix, peak-vs-budget — read from the catalog. *(new)*  |
| Runtime              | Docker Compose v2 + Makefile       | One-command up. *(reused)*                                             |

Kafka — used in the prior project — is **deliberately dropped**; the sources are
pull-based dataset files, not live streams. See
[Why not Kafka](docs/01-conception.md#why-not-kafka).

## Quick Start

> Requires Linux or WSL2. The fully-local path below needs only Python; the stack path
> (`make up`) needs Docker + Docker Compose v2.

```bash
git clone <this-repo>
cd robot-learning-data-engine
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make test                                   # 33 tests, no network/JVM/DB
make scale                                  # synthetic process-and-evict proof
make demo ENGINE=local CATALOG=sqlite       # full pipeline on DROID-100, no server
make report                                 # Streamlit dashboard (reads the catalog)
```

For the Docker stack (Postgres catalog, Spark gates): `cp .env.example .env && make up`,
then `make demo`. Other verbs: `make ingest` · `make validate`. A guided, step-by-step
walkthrough with expected output is in [docs/verification.md](docs/verification.md).

## Project Structure

```
robot-learning-data-engine/
├── README.md
├── docker-compose.yml
├── Makefile                      # up / demo / ingest / validate / report / scale / test
├── .env.example
├── .gitignore                    # excludes data/ and large artifacts
├── requirements.txt
├── config/
│   ├── sources.yaml              # source manifest + storage budget
│   └── quality_gates.yaml        # validation thresholds
├── acquisition/                  # HF downloader + storage guard
├── ingest/                       # schema gate + curation/eviction orchestrator
├── spark/jobs/                   # signal-quality gates (pure core + spark/local engines)
├── data_generator/               # synthetic episode augmenter
├── catalog/                      # dataset-version catalog (postgres / sqlite)
├── pipeline.py                   # `make demo` orchestrator
├── scale.py                      # batched process-and-evict scale runner
├── postgres/init/                # catalog schema init SQL
├── airflow/dags/                 # the end-to-end DAG
├── elk/logstash/                 # log pipeline
├── dashboard/                    # Streamlit data-quality app
├── data/                         # gitignored: raw/ (evicted), curated/, quarantine/, manifest/
├── sample_data/                  # committed sample episodes (schema reference)
└── docs/
    ├── 01-conception.md          # design & reasoning
    ├── 02-development.md          # how it was built (why/what/how)
    ├── verification.md           # step-by-step reproduction runbook
    ├── 03-finalization.md
    └── architecture.svg
```

## Data Lineage & Licenses

One row per source; DROID's official release is **CC-BY 4.0** and that is the basis
cited here even where a Hub mirror is relabelled. Non-commercial (CC-BY-NC) sources
are flagged and excluded from v1 unless approved.

| Source ID       | Role   | Hugging Face repo              | License (basis / mirror)   | In v1 | Notes                             |
|-----------------|--------|--------------------------------|----------------------------|-------|-----------------------------------|
| `droid-100`     | dev    | `lerobot/droid_100`            | CC-BY 4.0 / **MIT**        | yes   | 100-episode subset; build & debug |
| `droid-slice`   | scale  | `IPEC-COMMUNITY/droid_lerobot` | CC-BY 4.0 / **Apache-2.0** | yes   | Bounded slice; trips the guard    |
| `oxe-component` | future | *(verify a LeRobot OXE mirror)*| *(per source)*             | no    | Cross-embodiment; deferred        |

> Repo IDs, feature keys, and licenses were verified against the live dataset cards and
> each dataset's own `meta/info.json`. **License basis / mirror:** the cited basis is the
> official DROID release (CC-BY 4.0); each mirror's *stated* card license (MIT /
> Apache-2.0) is recorded alongside it in `config/sources.yaml` and the catalog. Both
> mirrors are permissive — no non-commercial (CC-BY-NC) source is used.

## License

Code in this repository is for educational / portfolio use. Dataset licenses are the
responsibility of each upstream source and are recorded in the table above and in the
catalog. See `sample_data/README.md` for the license of any committed samples.
