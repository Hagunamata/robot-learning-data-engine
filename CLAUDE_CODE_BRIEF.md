# Claude Code Brief — Robot-Learning Data Engine

> **How to use this file (for the human):** paste this whole document into Claude Code as the opening
> message of a new project, or drop it in the repo root as `CLAUDE_CODE_BRIEF.md` and point Claude Code
> at it. It is written as instructions *to the coding agent*. Sections marked **DECISION** are points
> where the agent must stop and ask you rather than choosing on its own — that keeps you in the
> conceptual / architect seat.

---

## 1. Context and goal

You are helping build the **second** portfolio project for a data engineer / robotics project manager
(GitHub handle `Hagunamata`). The first project is a batch-processing data platform for a simulated
"dark factory," here:

- **Prior repo:** https://github.com/Hagunamata/dark-factory-data-platform

Read its `README.md` and `docs/01-conception.md` conventions and **mirror them** wherever it makes sense.
That project used: Apache Kafka (ingestion), PostgreSQL `raw` + `analytics` schemas, Apache Spark
(batch aggregation), Apache Airflow (orchestration), the ELK stack + Kibana (observability), a
`data_generator/` module that expanded a small Kaggle CSV into a large synthetic set, and Docker Compose
+ a Makefile (`make up` / `make seed` / `make demo`) as the one-command runtime. It was delivered in
three phases — Conception, Development, Finalization — with matching docs.

**This new project** is a *robot-learning data engine*: a pipeline that acquires public multimodal robot
demonstration data, normalizes it to a canonical format, validates and quality-filters it in batch,
augments it with synthetic episodes, and publishes a versioned, catalogued dataset plus a data-quality
view. The point of the project is to show that the same batch-processing and orchestration skills scale
up from tabular factory telemetry to real, heterogeneous, multimodal robot data **under a real storage
constraint**.

Suggested repo name: **`robot-learning-data-engine`**.

---

## 2. Hard constraints (do not violate)

1. **Local disk budget ≈ 500 GB.** Assume the working machine can hold roughly 500 GB total; design so
   raw data is *never* fully resident. The default storage cap is **400 GB**, leaving headroom.
2. **Never commit datasets to git.** Commit only: code, config, download/ingest scripts, a tiny sample
   (a handful of episodes), and docs. Add data paths to `.gitignore`.
3. **Respect dataset licenses and record them.** For every source, store its license in the catalog and
   the README. Notably: DROID's *official* release is CC-BY 4.0 — prefer that as the cited basis even if
   a Hugging Face mirror is relabeled Apache-2.0. Watch out for non-commercial (CC-BY-NC) variants of
   otherwise similar datasets; flag any NC source loudly.
4. **Runtime target mirrors the prior project:** Linux or WSL2 with Docker + Docker Compose v2. Provide a
   Makefile with the same verbs where possible (`make up`, `make seed`, `make demo`, plus new ones below).
5. **Architectural clarity over production hardening**, same philosophy as the prior repo. Single-node,
   simplest reasonable config for each component. Document what would change at scale rather than building
   it.
6. **Verify external facts, don't trust this brief for exact IDs.** Hugging Face repo IDs, exact feature
   keys, and the current LeRobot dataset spec must be checked against the real sources before coding.

---

## 3. Target architecture

Vertical batch pipeline. Two of the stages are direct evolutions of the prior project (marked *reused*).

```
        Public sources (Hugging Face Hub: DROID-100, DROID slice, OXE optional)
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
                          Curated dataset             (versioned, lineage tracked)
                                  │
                     ┌────────────┴────────────┐
                     ▼                          ▼
              Data catalog                Quality dashboard
        (Postgres; source, license,   (ELK/Kibana or lightweight;
         stats, version)               yield, task mix, storage used)

           Orchestrated by Airflow · packaged with Docker Compose + Makefile
```

**Key mechanism — process-and-evict:** the storage guard enforces the GB cap; acquisition streams a
bounded number of episodes; once a batch passes validation and is written to the curated set, its raw
copy is deleted. This lets the pipeline process far more total data than fits on disk at any instant.
This behavior is the headline of the project — make it real and demonstrable, and log the running
disk-usage-vs-budget number at every stage.

---

## 4. Tech stack decisions (reuse vs. new)

| Concern | Choice | Note |
|---|---|---|
| Orchestration | **Airflow** (*reuse*) | Pipeline stages map cleanly to DAG tasks. Keep the `airflow/dags/` layout. |
| Batch processing | **Spark, local mode** (*reuse*) | Validation/aggregation over Parquet episode data. Reuses `spark/jobs/`. If Spark is overkill for a given step, a plain PyArrow/pandas job is acceptable — **DECISION**: ask before adding Spark to a step that a 50-line PyArrow script would handle. |
| Catalog / metadata | **PostgreSQL** (*reuse*) | A `catalog` schema, same spirit as the prior `analytics` schema. Reuse `postgres/init/`. |
| Observability (logs) | **ELK** (*reuse*) | Centralized container logs, same as before. |
| Quality dashboard | **DECISION** | Either reuse **Kibana** (max continuity) or a small **Streamlit** app. Ask the human which. Default to Kibana if no answer. |
| Synthetic data | **`data_generator/` evolved** (*reuse*) | Now emits LeRobot-format episodes to balance rare tasks, instead of CSV rows. |
| Acquisition | **`huggingface_hub` + `datasets` streaming** (*new*) | Selective / streamed pulls. No full-corpus downloads. |
| Canonical format | **LeRobot format** (*new*) | The de-facto standard the field has converged on. Validate against the real spec. |
| Ingestion buffer | **Kafka: intentionally dropped** | Sources are pull-based dataset files, not live streams — Kafka earns no place here. Document this decision in `docs/01-conception.md` as a deliberate right-sizing (this is a portfolio positive, not a gap). |
| IaC / runtime | **Docker Compose + Makefile** (*reuse*) | One command up. Same verbs where possible. |

---

## 5. Repository layout

Mirror the prior repo's shape; add the robotics-specific modules.

```
robot-learning-data-engine/
├── README.md                     # incl. data lineage + license table (see §2.3)
├── docker-compose.yml
├── Makefile                      # make up / seed / demo / ingest / validate / report
├── .env.example
├── .gitignore                    # excludes data/ and any large artifacts
├── config/
│   ├── sources.yaml              # source manifest + storage budget (see §6.1)
│   └── quality_gates.yaml        # validation thresholds (see §6.2)
├── acquisition/                  # HF streaming/selective downloader + storage guard
├── ingest/                       # convert-to-LeRobot canonicalizer
├── spark/jobs/                   # batch validation + quality-gate jobs (reused pattern)
├── data_generator/               # synthetic episode augmenter (evolved from prior repo)
├── catalog/                      # Postgres schema + writer
├── postgres/init/                # schema init SQL (reused pattern)
├── airflow/dags/                 # the end-to-end DAG (reused pattern)
├── elk/logstash/                 # log pipeline (reused)
├── dashboard/                    # Kibana export OR Streamlit app (per §4 DECISION)
├── data/                         # gitignored: raw/ (evicted), curated/, quarantine/
├── sample_data/                  # a few committed sample episodes (schema reference)
└── docs/
    ├── 01-conception.md          # design reasoning, incl. why-not-Kafka
    ├── 02-development.md          # implementation notes
    ├── 03-finalization.md        # reflection + how it extends (OXE, fleet, digital twin)
    └── architecture.svg          # architecture diagram
```

---

## 6. Config contracts (the human owns these values)

### 6.1 `config/sources.yaml`

```yaml
storage_budget_gb: 400            # storage guard cap; acquisition stops before exceeding
data_root: ./data
sources:
  - id: droid-100
    role: dev                     # tiny; used to build & debug the whole pipeline
    hf_repo: <verify exact id>    # the preprocessed 100-episode DROID subset
    max_episodes: 100
    license: CC-BY-4.0
    enabled: true
  - id: droid-slice
    role: scale                   # bounded slice to prove the storage-aware path
    hf_repo: <verify official DROID LeRobot repo>
    max_episodes: 2000            # a ceiling; the GB guard is the real limit
    license: CC-BY-4.0
    enabled: true
  - id: oxe-fractal
    role: future                  # cross-embodiment; leave off for v1
    hf_repo: <verify OXE component>
    enabled: false
```

### 6.2 `config/quality_gates.yaml`

```yaml
schema:
  required_features:              # verify exact LeRobot keys against the real spec
    - observation.state
    - action
    - task                        # language instruction
  require_at_least_one_image: true
signal:
  jerk_zscore_max: 6.0            # 3rd-order finite difference on action & state signals
  action_outlier_zscore_max: 5.0
  max_missing_frame_ratio: 0.02
annotation:
  require_language_instruction: true
policy:
  on_fail: quarantine             # quarantine | drop  (quarantine keeps a rejects log)
```

### 6.3 Catalog record (Postgres `catalog` schema, one row per dataset version)

```
dataset_version   TEXT     -- e.g. v0.1.0-droid100
source_id         TEXT
hf_repo           TEXT
license           TEXT
episode_count     INT
frame_count       BIGINT
task_distribution JSONB    -- task -> count
gate_pass_rate    NUMERIC  -- passed / (passed + failed)
bytes_on_disk     BIGINT
git_commit        TEXT
created_at        TIMESTAMPTZ
notes             TEXT
```

---

## 7. Ownership split — where to STOP and ask

Implement freely: the HF download/stream wrapper, the storage-guard accounting, the LeRobot
canonicalizer, the Spark validation runners, the catalog writer, the Airflow DAG wiring, the dashboard
scaffolding, the Dockerfiles and Makefile, and all glue/tests.

**DECISION points — stop and ask the human before deciding:**
- The exact quality-gate thresholds and whether `on_fail` is quarantine or drop (§6.2).
- Which sources and `max_episodes` go into v1 (§6.1).
- Kibana vs Streamlit for the dashboard (§4).
- Spark vs plain PyArrow for any individual step (§4).
- Anything that would commit more than a few MB of data, or add a heavyweight dependency.

When you hit a DECISION point, summarize the options in 3–5 lines and wait.

---

## 8. Milestone plan (feed these one at a time)

Aligned to the prior project's three phases.

**Phase 1 — Conception**
- **M0.** Scaffold repo per §5. Write `docs/01-conception.md`: architecture, component justification,
  the process-and-evict design, and the deliberate "why not Kafka" note. Produce `docs/architecture.svg`.
- **M1.** Define the config contracts (§6) with the human's chosen values. No data movement yet.

**Phase 2 — Development**
- **M2.** Acquisition + storage guard. Stream **DROID-100** to `data/raw/`; guard tracks bytes vs budget
  and refuses to exceed it. `make ingest` works end-to-end on the dev source.
- **M3.** Canonical ingest to LeRobot format + schema validation (Spark). Reject/quarantine on schema fail.
- **M4.** Signal quality gates (jerk / outlier / missing-frame) + write passing episodes to
  `data/curated/`, then **evict** the raw copy. Emit per-batch metrics. `make validate` works.
- **M5.** Synthetic augmenter (evolved `data_generator/`): mint LeRobot-format episodes for
  under-represented tasks; route them through the *same* validation gates. Catalog writer records the
  version. `make demo` runs the whole DAG on DROID-100.
- **M6.** Scale test: point at the bounded **DROID slice** and demonstrate the guard tripping — i.e.
  more data processed than held at once. Wire the dashboard (`make report`).

**Phase 3 — Finalization**
- **M7.** Write `docs/02-development.md` (implementation notes) and `docs/03-finalization.md` (reflection
  + roadmap: OXE cross-embodiment ingest, and the fleet-telemetry / digital-twin / safety-traceability
  follow-on projects as explicit future work). Finalize the README data-lineage + license table.

---

## 9. Working agreement

- Keep changes small and reviewable; one milestone per working session.
- Python first (match the prior repo's ~93% Python). Type hints + docstrings.
- Every stage logs to stdout in a structured way that ELK can pick up; always log the running
  disk-used-vs-budget figure.
- Add a minimal test per module (a smoke test on a single sample episode is enough).
- Cite the real LeRobot spec and dataset cards in code comments where a format assumption is made.
- Do **not** invent Hugging Face repo IDs or feature keys — verify them, and if unsure, ask.

---

## 10. Definition of done (v1)

- `make up && make demo` brings up the stack and runs the full pipeline on DROID-100 with no manual steps.
- The storage guard is demonstrably enforced (a scale run on the DROID slice shows raw eviction and a
  peak disk usage below the 400 GB cap).
- The catalog has at least two dataset versions (a real one and a synthetically-augmented one) with
  license and lineage populated.
- The dashboard shows gate pass-rate, task distribution, and storage used.
- All three `docs/` phase documents exist; the README has a data-lineage + license table; no dataset
  files are committed.
```
