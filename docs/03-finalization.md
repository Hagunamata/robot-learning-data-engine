# 03 — Finalization

The closing phase: what the engine actually delivers against what it set out to do, what
was proven and where, the limitations I am keeping honest about, and the work that follows
from here. The design rationale is in [`01-conception.md`](01-conception.md); the build
story, including the decisions that changed under contact with real data, is in
[`02-development.md`](02-development.md); the reproducible, step-by-step confirmation is in
[`verification.md`](verification.md).

## What shipped

The [five things the engine set out to do](01-conception.md#what-it-does) are all in place,
and each is backed by something you can run and check rather than a claim:

| Goal | Delivered | Evidence |
|------|-----------|----------|
| Acquire under a disk budget | Storage guard measures the real on-disk footprint and refuses any pull that would exceed the cap | `python -m acquisition --dry-run` predicts the exact guard-trip; the guard-trip step stops mid-pull |
| Treat data in canonical LeRobot format | Version-aware downloader + schema gate; adapts to v2.0 and v3.0 from each dataset's own `info.json` | Real DROID acquisition; v2.0/v3.0 path-templating covered by tests |
| Validate & quality-filter | PyArrow schema gate + calibrated signal gates with quarantine | Real `droid-100`: **94/100 episodes pass**, thresholds calibrated over 32,212 frames |
| Augment under-represented tasks | Synthetic augmenter that passes the *same* gates as real data | Augmented version adds 20 synthetic episodes |
| Publish versioned, catalogued output + dashboard | Catalog (Postgres/sqlite) + Streamlit data-quality view | Two versions: `v0.1.0-droid-100` (94 eps) and `v0.2.0-droid-100-aug` (114 eps) |

And the one idea I most wanted to demonstrate — [process-and-evict](01-conception.md#process-and-evict-the-headline)
— holds as a *measured* invariant, not a narrated one:

> **peak concurrent on-disk raw  <  budget  ≪  total bytes processed**

On the real `droid-slice` scale run it measured **peak ≈ 14 MB against a 100 MB budget
while ≈ 252 MB streamed through (2.35× the budget)**, with 56 episodes passing and 4
quarantined across 60 units — the working set stayed bounded while arbitrarily more data
flowed past it. The seeded synthetic proof shows the same shape on any machine with no
network (peak < budget < total, ≈ 4× throughput) and **asserts** the invariant in code.

## Verified, and where

The project was built to be verifiable in isolation and then confirmed end-to-end on Linux,
because a Windows dev box cannot download the corpus or run the JVM. That split is the whole
reason [`verification.md`](verification.md) exists, and the first real Ubuntu pass is what
turned several latent assumptions into fixed bugs:

- The **synthetic proof, the schema/signal-gate maths, curation, catalog, and the augmenter**
  run with no network, no Java, and no database — the current suite is **36 passing tests**.
- The **real acquisition, quality gates, and two-version catalog** were confirmed against
  live `lerobot/droid_100`.
- The **storage-aware scale run** was confirmed against the real `IPEC-COMMUNITY/droid_lerobot`
  mirror, which forced the honest discoveries in [`02-development.md`](02-development.md):
  a different codebase version (v2.0 vs v3.0) and, more subtly, a different *embodiment
  schema* (cartesian 8+7 vs joint-space 7+7) that made cross-source calibration invalid and
  drove a source-specific recalibration path.
- The **containerised catalog** (Postgres, applied schema, `make demo CATALOG=postgres`) and
  the **Streamlit dashboard** were confirmed against the running stack.

## What I want a reader to take away

Three things, looking back at the whole build:

1. **The constraint was the design.** Bounding raw disk is what forced the acquire → gate →
   curate → commit → evict loop, and that loop — not any single tool — is the thing worth
   showing. A system organised around its hardest constraint tends to be the one you can
   explain end to end.
2. **The interesting engineering lives at the boundary with reality.** The format version,
   the `tasks.parquet` shape, the signal gate that was wrong twice before it was right, the
   cross-embodiment schema mismatch on the scale mirror — none of these were visible from
   the plan. The value was in running the thing on real data, reading the distribution, and
   adjusting.
3. **Right-sizing over reflex.** Spark where per-episode timeseries maths at scale earns it;
   PyArrow for a few-megabyte schema check; a stdlib sqlite fallback so the whole pipeline
   runs with no server; no Kafka for a batch problem. Where something would be built
   differently at scale, I documented that rather than building it.

## Limitations I am keeping honest

Carried forward from the [conception's trade-offs](01-conception.md#trade-offs--limitations),
because a finalization that hides them isn't one:

- **Curated video is not re-segmented.** A v3.0 video file spans many episodes; a quarantined
  episode still leaves its video in the curated set (with `meta` recording which episodes are
  valid) rather than triggering an ffmpeg re-encode. Curated storage grows to the kept corpus
  — bounded by disk, and deliberately separate from the raw budget the guard enforces.
- **Gate thresholds are a calibrated starting point,** not physics; they are configuration,
  meant to be tuned per source and per embodiment.
- **Single-node Spark / Airflow / ELK are demonstrative,** not production-grade.
- **v1 covers one dataset family (DROID).** The scale mirror already exposed how much a
  second embodiment representation can differ — which is exactly the point of the next step.

## What follows from here

- **OXE cross-embodiment ingest.** The acquisition and canonicalisation path generalises;
  a second dataset family is the obvious extension, and the droid-slice schema mismatch is a
  preview of the per-embodiment calibration work it will need.
- **Video re-segmentation on curation,** to physically drop quarantined footage and let
  curated storage track only the kept frames.
- **Downstream consumers of the curated, catalogued output** — the fleet-telemetry,
  digital-twin, and safety-traceability follow-on projects. This engine produces the clean,
  versioned, lineage-tracked datasets those systems assume as their input; it is the first
  link, not the whole chain.

## In one line

A storage-aware, quality-gated, lineage-tracked robot-data pipeline whose headline property
— bounded raw disk while unbounded data flows through — is measured and asserted, not
asserted and hoped for.
