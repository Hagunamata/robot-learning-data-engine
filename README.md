# Robot-Learning Data Engine

> A storage-aware batch pipeline that acquires public multimodal robot-demonstration
> data, normalizes it to a canonical format (LeRobot), validates and quality-filters
> it, augments it with synthetic episodes, and publishes a versioned, catalogued
> dataset plus a data-quality view — all under a hard local-disk budget.
>
> Second of a two-part portfolio arc. Sibling project:
> [dark-factory-data-platform](https://github.com/Hagunamata/dark-factory-data-platform).

> **Status: M0 — scaffold.** Structure and design docs are in place; pipeline logic
> is implemented milestone-by-milestone (see [docs/01-conception.md](docs/01-conception.md) §11).

## At a Glance

```
   Public sources (Hugging Face Hub)
              │
   Storage guard ─► Selective acquisition   (stream only chosen episodes; disk-budget cap)
              │
              ▼
        Canonical ingest       (→ LeRobot format)
              │
   Synthetic ─► Batch validation (Spark; schema, jerk, outliers)
   generator      │
              ▼
        Curated dataset          (versioned; raw copy evicted)
              │
      ┌───────┴────────┐
      ▼                ▼
  Data catalog     Quality dashboard
  (Postgres)       (Kibana or Streamlit)

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
| Acquisition          | `huggingface_hub` + `datasets`     | Selective, streamed pulls — never a full-corpus download. *(new)*      |
| Storage guard        | Byte-accounting wrapper            | Enforces the configurable disk budget; drives process-and-evict. *(new)* |
| Canonical format     | LeRobot                            | The de-facto standard for robot-learning datasets. *(new)*            |
| Batch processing     | Apache Spark (local mode)          | Parallel schema + signal validation over Parquet episodes. *(reused)* |
| Catalog / metadata   | PostgreSQL (`catalog` schema)      | Durable, queryable dataset-version records. *(reused)*                 |
| Synthetic data       | `data_generator/` (evolved)        | Mints LeRobot episodes for under-represented tasks. *(reused)*         |
| Orchestration        | Apache Airflow                     | Pipeline stages map cleanly to DAG tasks. *(reused)*                   |
| Observability        | ELK stack                          | Centralized, searchable container logs. *(reused)*                     |
| Quality dashboard    | Kibana **or** Streamlit *(TBD)*    | Yield, task mix, storage used. Decision pending (see conception §4.8). |
| Runtime              | Docker Compose v2 + Makefile       | One-command up. *(reused)*                                             |

Kafka — used in the prior project — is **deliberately dropped**; the sources are
pull-based dataset files, not live streams. See [docs/01-conception.md](docs/01-conception.md) §6.

## Quick Start

> Requires Linux or WSL2 with Docker + Docker Compose v2.
> *(Targets are stubs at M0; each is wired up in its milestone — see conception §11.)*

```bash
git clone <this-repo>
cd robot-learning-data-engine
cp .env.example .env          # review values
make up                       # bring up Postgres, Airflow, ELK, dashboard
make demo                     # run the full pipeline on DROID-100
```

Other verbs: `make ingest` · `make validate` · `make report`.

## Project Structure

```
robot-learning-data-engine/
├── README.md
├── docker-compose.yml
├── Makefile                      # up / seed / demo / ingest / validate / report
├── .env.example
├── .gitignore                    # excludes data/ and large artifacts
├── requirements.txt
├── config/
│   ├── sources.yaml              # source manifest + storage budget
│   └── quality_gates.yaml        # validation thresholds
├── acquisition/                  # HF streaming downloader + storage guard
├── ingest/                       # convert-to-LeRobot canonicalizer
├── spark/jobs/                   # batch validation + quality-gate jobs
├── data_generator/               # synthetic episode augmenter
├── catalog/                      # Postgres catalog writer
├── postgres/init/                # schema init SQL
├── airflow/dags/                 # the end-to-end DAG
├── elk/logstash/                 # log pipeline
├── dashboard/                    # Kibana export OR Streamlit app
├── data/                         # gitignored: raw/ (evicted), curated/, quarantine/
├── sample_data/                  # a few committed sample episodes (schema reference)
└── docs/
    ├── 01-conception.md
    ├── 02-development.md
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

> Repo IDs, feature keys, and licenses verified against the live dataset cards
> (2026-07-22). **License basis / mirror:** per brief §2.3 the cited basis is the
> official DROID release (CC-BY 4.0); the mirror's *stated* card license (MIT /
> Apache-2.0) is recorded alongside it in `config/sources.yaml` and the catalog.
> Both mirrors are permissive — no non-commercial (CC-BY-NC) source in v1.

## License

Code in this repository is for educational / portfolio use. Dataset licenses are the
responsibility of each upstream source and are recorded in the table above and in the
catalog. See `sample_data/README.md` for the license of any committed samples.
