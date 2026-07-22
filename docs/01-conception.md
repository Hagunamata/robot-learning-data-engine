# 01 — Conception

> Phase 1 (Conception) design document for the **Robot-Learning Data Engine**.
> Companion to `docs/architecture.svg`. Mirrors the conception phase of the prior
> project ([dark-factory-data-platform](https://github.com/Hagunamata/dark-factory-data-platform))
> and reuses its "why X, not Y?" justification style.

---

## 1. Project context

This is the **second** portfolio project in a two-part arc. The first,
`dark-factory-data-platform`, built a batch data platform over simulated tabular
factory telemetry: Kafka → Postgres (`raw`) → Spark → Postgres (`analytics`),
orchestrated by Airflow, observed through the ELK stack, packaged with Docker
Compose and a Makefile.

The thesis of *this* project is that the **same batch-processing and orchestration
discipline scales from tabular telemetry to real, heterogeneous, multimodal robot
demonstration data — under a hard storage constraint.** Concretely, the engine:

1. **acquires** public robot-demonstration episodes from the Hugging Face Hub,
2. **normalizes** them to a single canonical on-disk format (LeRobot),
3. **validates and quality-filters** them in batch,
4. **augments** the corpus with synthetic episodes for under-represented tasks,
5. **publishes** a versioned, catalogued dataset plus a data-quality view.

The design philosophy is inherited verbatim from the prior repo: **architectural
clarity over production hardening.** Every component runs single-node in the
simplest reasonable configuration; what would change at scale is *documented*
rather than *built*. The course brief asks for this reasoning explicitly, and it is
the strongest defence against the criticism that a stack was assembled without
thought.

## 2. Data sources and volume strategy

### 2.1 Sources

Robot-learning data is large, multimodal (proprioception + one or more camera
streams + a language instruction), and published as pull-based dataset files — not
live event streams. The v1 corpus is built from the **DROID** family of manipulation
demonstrations:

| Role     | Source (v1)                            | Purpose                                          | License (basis / mirror)   |
|----------|----------------------------------------|--------------------------------------------------|----------------------------|
| `dev`    | `lerobot/droid_100` (v2.0)             | Tiny (100 episodes); build & debug the pipeline  | CC-BY 4.0 / MIT            |
| `scale`  | `IPEC-COMMUNITY/droid_lerobot` (v2.0)  | Prove the storage-aware path; trip the guard     | CC-BY 4.0 / Apache-2.0     |
| `future` | OXE component (off)                    | Cross-embodiment ingest; deferred past v1        | (per source)               |

> **Repo IDs, LeRobot codebase version (v2.0), feature keys, and licenses were
> verified against the live dataset cards in M1 (2026-07-22)** — not invented. Both
> DROID mirrors relabel the license on their card (MIT / Apache-2.0); per §2.3 the
> cited *basis* is the official DROID release (CC-BY 4.0), with each mirror's stated
> license recorded alongside it in `config/sources.yaml`. The v2.0 language field is
> `language_instruction` (not `task`) — pinned in `config/quality_gates.yaml`.

**Licensing.** DROID's official release is **CC-BY 4.0**; that is the basis cited in
the catalog and README even where a Hub mirror is relabelled Apache-2.0. Any
non-commercial (CC-BY-NC) source is flagged loudly and kept out of v1 unless
explicitly approved.

### 2.2 The volume problem

The working machine holds roughly **500 GB total**. A full robot-learning corpus is
far larger than that — the complete DROID release alone is measured in terabytes.
The naïve "download everything, then process" pattern is therefore impossible by
construction, which is exactly the constraint that makes this project interesting.

The volume strategy is **selective, streamed acquisition under a hard budget**: pull
only the chosen episodes, cap resident bytes at a configurable budget
(`storage_budget_gb`, a ≤400 GB design ceiling under the 500 GB physical limit), and —
the headline mechanism — **evict raw episodes as soon as they have been curated** (§5).
This lets the pipeline process far more total data than can ever sit on disk at one
instant. For the M6 scale demo the budget is deliberately set **low (25 GB)** so the
~90 GB `droid-slice` visibly trips the guard; a full run simply raises the number.

## 3. Architecture overview

A single vertical batch pipeline. Two stages are direct evolutions of the prior
project (marked *reused*).

```
        Public sources (Hugging Face Hub: DROID-100, DROID slice, OXE later)
                                  │
                                  ▼
   Storage guard  ───────►  Selective acquisition   (stream/pull only chosen episodes)
   (~400 GB cap)                  │
                                  ▼
                          Canonical ingest           (convert to LeRobot format)
                                  │
                                  ▼
   Synthetic data  ──────►  Batch validation          (*reused* Spark; schema, jerk, outliers)
   (*reused* generator)           │
                                  ▼
                          Curated dataset             (versioned; raw copy evicted)
                                  │
                     ┌────────────┴────────────┐
                     ▼                          ▼
              Data catalog                Quality dashboard
        (Postgres; source, license,   (Kibana or Streamlit — DECISION;
         stats, version)               yield, task mix, storage used)

           Orchestrated by Airflow · packaged with Docker Compose + Makefile
```

The stage boundaries map cleanly onto Airflow tasks, and each stage logs its
**running disk-used-vs-budget figure** to stdout for ELK to pick up.

## 4. Component justifications

Each component follows the prior repo's template: **Why**, **Why not the
alternatives**, **Implementation discipline** (the single-node scope we actually
ship).

### 4.1 Acquisition + storage guard  *(new)*

- **Why:** `huggingface_hub` + `datasets` streaming pulls only the selected episodes
  and never materialises a full corpus. A thin **storage guard** accounts bytes
  against the 400 GB cap before each pull and refuses any acquisition that would
  exceed it.
- **Why not** a plain bulk download or `git lfs clone`? Both fetch the entire repo,
  which does not fit on disk and defeats the entire premise of the project.
- **Implementation discipline:** single-process, synchronous streaming. Byte
  accounting is a simple on-disk `du`-style measurement plus a running ledger — not
  a distributed quota service.

### 4.2 Canonical ingest → LeRobot format  *(new)*

- **Why:** heterogeneous sources must land in one shape before validation is even
  meaningful. **LeRobot** is the format the field has converged on for robot-learning
  datasets, so canonicalising to it maximises downstream compatibility.
- **Why not** invent a bespoke schema? It would isolate the dataset from the
  ecosystem's tooling and training loops, and add a translation burden for no gain.
- **Implementation discipline:** the exact required feature keys
  (e.g. state / action / task / at-least-one image) are pinned against the *real*
  LeRobot spec in M1 and cited in code comments — not guessed here.

### 4.3 Batch validation + quality gates — Spark, local mode  *(reused)*

- **Why:** validation (schema conformance, signal-quality gates such as jerk and
  outlier z-scores, missing-frame ratio) is an embarrassingly parallel scan over
  Parquet episode data — precisely Spark's batch strength, and a direct reuse of the
  prior repo's `spark/jobs/` pattern.
- **Why not** always Spark? For a step a 50-line PyArrow/pandas job would handle,
  Spark is overkill. **DECISION:** the choice of Spark vs plain PyArrow for any
  individual step is escalated to the human before it is made.
- **Implementation discipline:** Spark runs in local mode, single node, default
  config. No cluster, no YARN/K8s.

### 4.4 Catalog / metadata — PostgreSQL  *(reused)*

- **Why:** a durable, queryable record of *what was produced* — one row per dataset
  version carrying source, license, episode/frame counts, task distribution, gate
  pass-rate, bytes-on-disk, and git commit. This is the `analytics`-schema spirit of
  the prior repo, renamed to a `catalog` schema.
- **Why not** a flat JSON/CSV manifest? It loses queryability, referential
  discipline, and the ability to diff versions — and Postgres is already in the stack.
- **Implementation discipline:** a single `catalog` schema, one table (see brief
  §6.3), initialised from `postgres/init/`.

### 4.5 Synthetic augmenter — `data_generator/` evolved  *(reused)*

- **Why:** real corpora are unbalanced; rare tasks are under-represented. The prior
  repo's `data_generator/` expanded a small CSV into a synthetic set; here it evolves
  to **mint LeRobot-format episodes** for under-represented tasks and routes them
  through the *same* validation gates as real data.
- **Why not** a separate, laxer path for synthetic data? Bypassing the gates would
  let synthetic artefacts pollute the curated set undetected. Same gates, same rigour.
- **Implementation discipline:** generation is procedural and clearly labelled in the
  catalog as synthetic; no learned generative model in v1.

### 4.6 Orchestration — Airflow  *(reused)*

- **Why:** the pipeline stages are a DAG with clear dependencies; Airflow makes the
  ordering, retries, and per-task logging explicit, reusing the prior `airflow/dags/`
  layout.
- **Why not** a shell script or Makefile-only flow? It hides dependencies and gives
  no per-task observability or retry semantics. (`make demo` still exists as the
  human-facing one-command entry point; it *triggers* the DAG.)
- **Implementation discipline:** a single DAG, `LocalExecutor`, single scheduler.

### 4.7 Observability — ELK  *(reused)*

- **Why:** centralised, searchable container logs — identical need and solution to
  the prior repo. Every stage logs structured lines (including disk-used-vs-budget)
  that Logstash ships to Elasticsearch.
- **Why not** scattered stdout only? It does not survive container restarts and is
  not searchable across stages.
- **Implementation discipline:** single-node ELK, default indices.

### 4.8 Quality dashboard — **DECISION (Kibana vs Streamlit)**

- The dashboard must show gate pass-rate, task distribution, and storage used.
  **Kibana** maximises continuity with the prior repo's ELK stack; a small
  **Streamlit** app would be lighter and read directly from the catalog. Per the
  brief this is a human decision — default to **Kibana** if unanswered. The scaffold
  ships only a `dashboard/README.md` placeholder until the decision lands (M1/M6).

## 5. The process-and-evict design (headline mechanism)

This is the defining behaviour of the project and the single feature most worth
demonstrating.

**The invariant:** *raw data is never fully resident.* At any instant, disk holds
only (a) the curated dataset, (b) a bounded working set of raw episodes currently
in flight, and (c) the quarantine of rejects. The full source corpus — terabytes —
flows *through* the 400 GB budget rather than sitting *inside* it.

**The loop:**

1. The **storage guard** checks current usage against the 400 GB cap.
2. Acquisition **streams a bounded batch** of raw episodes into `data/raw/` — only as
   many as fit under the cap.
3. Ingest canonicalises the batch to LeRobot format; validation applies the gates.
4. Passing episodes are written to `data/curated/`; failing ones go to
   `data/quarantine/` (or are dropped — a **DECISION** on `on_fail`).
5. **The raw copy of the batch is evicted**, freeing budget for the next batch.
6. The running **disk-used-vs-budget** figure is logged at every stage.

**Why this matters:** it converts a hard physical limit into a throughput
parameter. The scale test (M6) points the pipeline at the bounded DROID slice and
demonstrates the guard *tripping* — i.e. more total data processed than was ever
held at once, with peak disk usage staying below the 400 GB cap. That observable is
the project's headline result.

**Production extension:** at scale, eviction becomes an object-store lifecycle
policy (e.g. S3 tiering) and the guard becomes a quota service; the *logic* is
identical, only the substrate changes.

## 6. Why not Kafka (a deliberate right-sizing)

The prior project used Kafka as a durable ingestion buffer between continuous event
producers and Postgres. **This project deliberately drops Kafka**, and that omission
is a positive design signal, not a gap:

- **The sources are pull-based dataset files, not live streams.** There are no
  continuous producers to decouple from a slow consumer — the "buffer between a
  firehose and a database" problem Kafka solves does not exist here.
- **The natural back-pressure mechanism is the storage guard, not a broker.** What
  bounds intake is the 400 GB budget and the process-and-evict loop, which a message
  queue would neither help nor replace.
- **Adding Kafka would be resume-driven architecture.** It would introduce a broker,
  topics, and consumer-group semantics that carry operational weight and justify
  nothing. Kleppmann's framing applies: choose the buffer only when the shape of the
  data flow demands one.

Dropping a tool used in the prior project — and being able to say *precisely why* —
demonstrates judgement more convincingly than carrying it forward would. Right-sizing
is the point.

## 7. Cross-cutting concerns

Following the prior repo, each concern separates what is **Implemented** from
**Production extensions**.

### 7.1 Storage safety
- **Implemented:** hard 400 GB guard checked before every acquisition; eviction after
  curation; disk-used-vs-budget logged everywhere.
- **Production extensions:** object-store tiering, quota service, soft/hard watermarks.

### 7.2 Data quality
- **Implemented:** schema + signal gates (jerk, outlier, missing-frame) with a
  quarantine-vs-drop policy; per-batch pass-rate metrics.
- **Production extensions:** learned anomaly detection, human-in-the-loop review of
  quarantine, per-embodiment gate tuning.

### 7.3 Lineage & versioning
- **Implemented:** one catalog row per dataset version with source, license, stats,
  and git commit.
- **Production extensions:** content-addressed dataset storage, full provenance graph.

### 7.4 Observability
- **Implemented:** structured stdout → ELK; dashboard for yield / task mix / storage.
- **Production extensions:** metric SLOs, alerting on pass-rate regressions.

### 7.5 Licensing compliance
- **Implemented:** license recorded per source in catalog + README; NC sources flagged.
- **Production extensions:** automated license-gate in CI.

### 7.6 Reproducibility
- **Implemented:** Docker Compose + Makefile, pinned config, committed sample episodes.
- **Production extensions:** fully pinned data snapshots, CI that runs the demo.

## 8. Reproducibility

The one-command runtime mirrors the prior repo:

1. `git clone` the repository.
2. `cp .env.example .env` and review values.
3. `make up` — brings up the stack (Postgres, Airflow, ELK, dashboard).
4. `make demo` — runs the full pipeline end-to-end on **DROID-100** with no manual steps.

Additional verbs: `make ingest` (acquire the dev source), `make validate` (run the
quality gates), `make report` (wire/refresh the dashboard). No dataset files are
committed; only code, config, scripts, docs, and a handful of sample episodes.

## 9. Advantages and disadvantages

**Advantages**
- Demonstrates that batch/orchestration skills transfer from tabular to multimodal
  robot data.
- The process-and-evict loop is a concrete, demonstrable answer to a real hardware
  constraint.
- Honest right-sizing (dropping Kafka) shows architectural judgement.
- Maximal reuse of the prior stack lowers risk and highlights transferable skill.

**Disadvantages / caveats**
- **Synthetic-data caveat:** procedurally minted episodes are not a substitute for
  real demonstrations; they only balance task distribution and are labelled as such.
- Single-node Spark/Airflow/ELK are demonstrative, not production-grade.
- v1 covers one dataset family (DROID); cross-embodiment (OXE) is future work.

## 10. Risks and open questions

- **Format drift:** the LeRobot spec evolves; exact feature keys must be verified
  against the live spec before ingest is written (M1/M2).
- **Repo-ID accuracy:** Hub repo IDs are placeholders until verified against the real
  dataset cards.
- **Open DECISIONs (owned by the human, resolved in M1):**
  - Quality-gate thresholds and `on_fail` = quarantine vs drop (§6.2 of the brief).
  - Which sources and `max_episodes` go into v1 (§6.1).
  - Kibana vs Streamlit for the dashboard (§4).
  - Spark vs plain PyArrow for any individual step (§4).

## 11. Next phase

**M1 — Config contracts.** Fill `config/sources.yaml` and `config/quality_gates.yaml`
with the human's chosen values (resolving the open DECISIONs above); verify the
Hugging Face repo IDs and LeRobot feature keys against the real sources. **No data
movement yet.**

Thereafter: M2 acquisition + storage guard → M3 canonical ingest + schema validation
→ M4 signal gates + eviction → M5 synthetic augmenter + catalog + `make demo` →
M6 scale test (guard trips) + dashboard → M7 finalization docs.
