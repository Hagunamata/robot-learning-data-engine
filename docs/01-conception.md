# Conception & design

This document explains **what** the Robot-Learning Data Engine is, **why** it is built
the way it is, and the trade-offs I made along the way. If you are reviewing or
reproducing this project, start here for the reasoning, then follow
[`verification.md`](verification.md) to run it yourself.

It is the second of a two-part portfolio arc. The first,
[dark-factory-data-platform](https://github.com/Hagunamata/dark-factory-data-platform),
was a batch data platform over simulated tabular factory telemetry (Kafka → Postgres →
Spark → Postgres, orchestrated by Airflow, observed with ELK). This project takes the
same batch-processing and orchestration discipline and points it at something much
harder: **real, heterogeneous, multimodal robot-demonstration data, under a hard storage
constraint.** My aim was to show that those skills transfer from tidy tabular rows to
terabyte-scale robot episodes — and to design honestly for a machine that cannot hold the
data it processes.

## What it does

The engine is a single vertical batch pipeline that:

1. **acquires** public robot-demonstration episodes from the Hugging Face Hub,
2. treats them in the canonical **LeRobot** on-disk format,
3. **validates and quality-filters** them (schema + signal-quality gates),
4. **augments** the corpus with synthetic episodes for under-represented tasks,
5. **publishes** a versioned, catalogued dataset plus a data-quality dashboard,

all while **never letting raw data exceed a fixed disk budget** — the idea I most wanted
to demonstrate (see [Process-and-evict](#process-and-evict-the-headline)).

My guiding principle throughout is **architectural clarity over production hardening**:
every component runs single-node in the simplest configuration that makes the point, and
where something would be built differently at scale, I *document* that rather than build
it. I would rather ship a system whose every part I can justify than a pile of tools
assembled by reflex.

## Why this data is hard

Robot-learning data is large, multimodal (joint/end-effector state, one or more camera
streams, and a language instruction), and shipped as pull-based dataset files rather than
live event streams. The full [DROID](https://droid-dataset.github.io/) release is measured
in **terabytes**, while a realistic working machine here has roughly **500 GB** of disk.

"Download everything, then process" is therefore impossible by construction — and that
constraint is exactly what makes the project interesting. The whole design is organised
around processing far more data than can ever sit on disk at one instant.

## Data sources

| Role | Source | Purpose | License (basis / mirror) |
|------|--------|---------|--------------------------|
| dev | [`lerobot/droid_100`](https://huggingface.co/datasets/lerobot/droid_100) | 100 episodes — build and debug the whole pipeline fast | CC-BY 4.0 / MIT |
| scale | [`IPEC-COMMUNITY/droid_lerobot`](https://huggingface.co/datasets/IPEC-COMMUNITY/droid_lerobot) | The full DROID corpus — prove the storage-aware path by tripping the guard | CC-BY 4.0 / Apache-2.0 |
| future | an Open X-Embodiment component | Cross-embodiment ingest — deliberately deferred | (per source) |

I verified every repo ID, the LeRobot format version, the feature keys, and the licenses
against the live dataset cards and each dataset's own `meta/info.json` rather than
trusting any single label. Two things worth calling out:

- **Licensing.** DROID's official release is **CC-BY 4.0**. Both Hub mirrors relabel their
  card (MIT / Apache-2.0), so I record the **official basis (CC-BY 4.0)** as the cited
  license and keep the mirror's stated license alongside it in
  [`config/sources.yaml`](../config/sources.yaml) and the catalog. No non-commercial
  (CC-BY-NC) source is used. Being explicit about provenance matters more to me than
  picking whichever label is most convenient.
- **Format.** `lerobot/droid_100` is LeRobot **codebase v3.0** (the card metadata says
  v2.0, but `info.json` — the authoritative source — says v3.0). v3.0 aggregates many
  episodes into each data/video file, which shaped the acquisition granularity; the
  [development history](02-development.md) tells that story.

## Architecture

```
        Public sources (Hugging Face Hub: DROID-100 dev, DROID slice at scale, OXE later)
                                  │
   Storage guard  ───────►  Selective acquisition   (stream/pull only what fits the budget)
   (configurable cap)             │
                                  ▼
                          Canonical LeRobot data     (schema-gated)
                                  │
   Synthetic  ───────────►  Signal-quality gates      (jerk / outliers / missing frames)
   augmenter                     │
                                  ▼
                          Curated dataset             (versioned; raw copy evicted)
                                  │
                     ┌────────────┴────────────┐
                     ▼                          ▼
              Data catalog                Quality dashboard
        (Postgres / sqlite:           (Streamlit, reads the catalog:
         source, license, stats,       pass-rate, task mix, peak-vs-budget)
         version, lineage)

           Orchestrated by Airflow · logs to ELK · packaged with Docker Compose + Makefile
```

Every stage emits structured JSON to stdout — including the running
disk-used-vs-budget figure — so ELK can pick it up, and the stage boundaries map cleanly
onto Airflow DAG tasks.

## Component choices

For each component I give the reasoning, the alternative I rejected, and the deliberately
modest scope I actually ship.

**Acquisition + storage guard.** `huggingface_hub` pulls only the files that fit the
budget and never materialises a full corpus; a thin storage guard measures on-disk bytes
and refuses any pull that would exceed the cap. I avoided a bulk `git lfs` clone (it
fetches the whole repo — the exact thing that cannot fit) and `datasets` row-streaming (a
streamed row's on-disk cost is opaque; I want real byte accounting). Scope: a single
process, byte accounting by direct measurement — not a distributed quota service.

**Canonical format — LeRobot.** LeRobot is the format the field has converged on, so
building around it keeps the output compatible with the ecosystem's tooling and training
loops. Inventing a bespoke schema would isolate the dataset for no gain. I pin the exact
feature keys against each dataset's real `info.json` rather than guessing them.

**Validation — a hybrid of PyArrow and Spark.** Schema validation is a feature-existence
/ dtype check — a small PyArrow job; running it on Spark would be theatre. The signal
gates (jerk, outliers, missing frames) are per-episode timeseries maths across many
episodes — genuinely Spark's batch strength, and the natural continuation of the Spark
work in my first project. So I use PyArrow for schema and **Spark (local mode) for the
signal gates**, with a pure-Python engine alongside for the small dev source and CI. This
is the same right-sizing judgement as the Kafka decision below.

**Catalog — PostgreSQL (with a sqlite fallback).** A durable, queryable record of *what
was produced* — one row per dataset version with source, license, counts, task
distribution, gate pass-rate, bytes-on-disk, and git commit. A flat JSON manifest would
lose queryability and version diffing. Postgres is the stack's catalog; a stdlib sqlite
backend lets the whole thing run with no server for local work and CI.

**Synthetic augmenter.** Real corpora are unbalanced. The augmenter mints LeRobot-format
episodes for under-represented tasks and routes them through the **same** validation gates
as real data — no laxer path, or synthetic artefacts could quietly pollute the curated
set. Generation is procedural and clearly labelled synthetic in the catalog; there is no
learned generative model here, and I do not claim these episodes substitute for real
demonstrations — they only balance the task mix.

**Orchestration — Airflow; observability — ELK; dashboard — Streamlit.** Airflow makes the
stage dependencies, retries, and per-task logging explicit; ELK centralises the structured
logs. For the *metrics* dashboard I chose **Streamlit reading the catalog directly** over
Kibana: the numbers a reviewer cares about (pass-rate, task mix, peak-vs-budget) already
live in the catalog and manifests, so serving them from there is lighter and more honest
than standing up Elasticsearch to visualise logs. Kibana stays for pipeline **logs**.

## Process-and-evict (the headline)

This is the defining behaviour, and the one result I most want to be *measured* rather
than asserted.

**The invariant:** raw data is never fully resident. At any instant the disk holds only
the curated output, a bounded working set of raw files in flight, and a small quarantine
of rejects. The full source corpus flows *through* the budget rather than sitting *inside*
it.

**The loop:** the guard checks usage against the budget → acquisition pulls a bounded
batch that fits → the batch is gated and its passing episodes are written to the curated
set → **the raw copy is evicted**, freeing the budget for the next batch → the
disk-used-vs-budget figure is logged throughout.

**Why it matters:** it turns a hard physical limit into a throughput parameter. The scale
run points the pipeline at the full DROID slice with the budget set deliberately low
(25 GB) and demonstrates, with three measured figures, that **peak concurrent raw stays
below the budget while total bytes processed greatly exceeds it**. At production scale the
eviction would become an object-store lifecycle policy and the guard a quota service — the
logic is identical, only the substrate changes.

## Why not Kafka

My first project used Kafka as a durable buffer between continuous producers and Postgres.
I **deliberately dropped it here**, and I think the omission is a design signal, not a gap:

- The sources are pull-based dataset files, not live streams — there is no firehose to
  decouple from a slow consumer, which is the problem Kafka solves.
- The natural back-pressure mechanism is the storage guard and the process-and-evict loop,
  not a message broker.
- Adding Kafka would be resume-driven architecture: a broker, topics, and consumer-group
  semantics that carry real operational weight and justify nothing here.

Dropping a tool I used before, and being able to say *precisely why*, demonstrates more
judgement than carrying it forward would. Right-sizing is the point.

## In scope vs. at scale

I keep an honest line between what this repo implements and what a production system would
add:

| Concern | Implemented here | At production scale |
|---------|------------------|---------------------|
| Storage safety | Hard budget checked before every pull; eviction after curation; disk-vs-budget logged | Object-store tiering, a quota service, soft/hard watermarks |
| Data quality | Schema + signal gates with quarantine, calibrated thresholds, per-batch metrics | Learned anomaly detection, human review of quarantine, per-embodiment tuning |
| Lineage | One catalog row per version + a reproducible run manifest | Content-addressed storage, a full provenance graph |
| Observability | Structured stdout → ELK; Streamlit metrics dashboard | Metric SLOs, alerting on pass-rate regressions |
| Compute | Spark local mode; single-node everything | A real cluster; distributed acquisition |

## Trade-offs & limitations

- **Videos are not re-segmented.** In v3.0 a video file spans many episodes; when an
  episode is quarantined I still keep the whole video file in the curated set and record
  which episodes are valid, rather than re-encoding with ffmpeg. Curated storage therefore
  grows to the kept corpus — bounded by disk, and separate from the raw budget.
- **Gate thresholds are a starting point,** calibrated from the dev source and open to
  tuning; they are configuration, not physics.
- **Single-node Spark/Airflow/ELK are demonstrative,** not production-grade.
- **v1 covers one dataset family (DROID);** cross-embodiment (OXE) is future work.

## Roadmap

- **OXE cross-embodiment ingest** — the acquisition + canonicalisation path generalises;
  the next dataset family is the obvious extension.
- **Video re-segmentation** on curation, to drop quarantined footage.
- **Fleet-telemetry / digital-twin / safety-traceability** follow-on projects that consume
  the curated, catalogued output.

## Reproducing this

The runtime is Docker Compose v2 + a Makefile, targeting Linux/WSL2. The one-command path
is `make up && make demo`; a fully local path (no server, no JVM) is
`make demo ENGINE=local CATALOG=sqlite`. For a guided, step-by-step confirmation with
expected output at each stage, follow [`verification.md`](verification.md). No dataset
files are committed — only code, config, docs, and a tiny sample. The story of building
it, including the decisions that changed under contact with real data, is in
[`02-development.md`](02-development.md).
