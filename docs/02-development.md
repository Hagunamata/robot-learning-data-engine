# Development history

The story of how the engine was built and, more importantly, the decisions that shaped
it — especially the ones that changed once the plan met real data. This is a *why / what
/ how* narrative, not a command reference; for step-by-step instructions with expected
output, see [`verification.md`](verification.md).

I built it one stage at a time — acquisition, then ingest, then the quality gates, then
cataloguing and synthesis, then the scale runner and dashboard — keeping the test suite
green at every step (it ends at 33 tests). Wherever a real machine couldn't exercise
something (a multi-terabyte download, a JVM I didn't have on the dev box), I built the
logic to be verifiable in isolation and left a documented path to confirm it end-to-end on
Linux. That discipline is why the [runbook](verification.md) reads the way it does.

## Acquisition, the storage guard, and a format surprise

The first stage pulls episodes from the Hub under a byte budget. The guard
([`acquisition/storage_guard.py`](../acquisition/storage_guard.py)) measures the actual
on-disk footprint and refuses any pull that would exceed the cap — I chose measurement
over a bookkeeping ledger so it stays correct even if a download is interrupted.

The surprise came immediately. The plan assumed `lerobot/droid_100` was LeRobot **v2.0**
(the Hub card's metadata says so), where each episode is its own file. But the dataset's
own `meta/info.json` — the authoritative source — reports **v3.0**, which *aggregates many
episodes into each data file* and concatenates video per camera. You cannot pull a single
episode's video in isolation.

Rather than fight the format, I let it set the granularity: the raw acquisition and
eviction **unit is the file, not the episode**. Process-and-evict still holds, just at file
granularity. The downloader reads each source's `info.json` first and logs a
`codebase_version_mismatch` if it disagrees with the config, then proceeds with the
*detected* version — so a genuinely v2.0 source would still work. Two smaller decisions
fell out of this: I pull whole files via `huggingface_hub` (not `datasets` row-streaming)
so byte accounting is real, and I made `--dry-run` gate on a *projected* cumulative total
so it can predict exactly where the guard will trip without downloading anything.

## Splitting validation: PyArrow and Spark

The plan had a single Spark validation stage. In practice that bundles two very different
jobs. Schema validation is a feature-existence / dtype / column check over a few megabytes
— a ~50-line PyArrow job; doing it on Spark would look like over-engineering to anyone
reading the code. The signal gates, on the other hand, are per-episode timeseries maths
across many episodes — exactly what Spark's `groupBy` + `applyInPandas` is for, and the
natural continuation of the Spark work in my first project.

So I split them: **PyArrow for schema** ([`ingest/schema_gate.py`](../ingest/schema_gate.py)),
**Spark for the signal gates**. To keep the signal maths testable without a JVM I put it
in a pure-numpy core and gave it two engines — `spark` for scale and a `local`
(pyarrow+numpy) engine that returns identical results for the small dev source and CI.
This is the same right-sizing instinct as dropping Kafka: keep the heavyweight tool where
it earns its place, and only there.

The schema gate is deliberately dataset-level — every episode shares one `info.json`, so
the dataset is accepted or quarantined as a whole; per-episode quality is the signal
gate's job. It also handles the v3.0 reality that image features are *video files*, not
parquet columns, and that the language instruction is a `task_index` resolved through
`meta/tasks.parquet` rather than a text column.

## The signal gate I had to design three times

This was the most instructive part of the whole project, so I'm keeping the dead ends in
the record.

**Attempt 1 — z-score each episode against itself.** It failed *every one* of the 100 real
episodes, with jerk z-scores in the millions. The reason is physical: robot episodes
contain long still or slow segments, so the per-episode spread collapses toward zero and
any real motion looks infinitely anomalous. Normalising a signal against its own quiet
moments is the wrong idea.

**Attempt 2 — z-score against global mean + 6σ.** Better founded (calibrate from the
corpus, not the episode), but still failed 99/100. Robot jerk is sharply peaked at zero
with heavy tails, so its global standard deviation is tiny and ordinary motion reads as
~14σ. Mean/σ is the wrong scale for a spiky, heavy-tailed signal.

**Attempt 3 (shipped) — robust percentile + anomalous-frame fraction.** I calibrate a
per-signal frame-score threshold at a high percentile (99.9) over the corpus, then fail an
episode only when the *fraction* of frames exceeding that threshold is above a small ratio
(1%). A single sharp frame no longer condemns a 500-frame episode; a *concentration* of
anomalies does. On real `droid_100` this yields a believable **94/100 pass rate** (the
rejects are episodes with sustained action anomalies) instead of 0% or 100%. The
thresholds live in [`config/quality_gates.yaml`](../config/quality_gates.yaml) and are
meant to be tuned — they are configuration, not physics.

The lesson I want a reader to take from this: a plausible-sounding metric can be
completely wrong on real data, and the only way to find out is to run it on real data and
look at the distribution.

## Closing the loop: curation and eviction

With verdicts in hand, [`ingest/curate.py`](../ingest/curate.py) writes the passing
episodes to `data/curated/`, records the failures (reason + a tiny sample) in quarantine,
and then **evicts the raw copy** through the guard — the moment that makes
process-and-evict real. On the dev source this ran end to end: schema pass → calibrate over
32,212 frames → gate 94/100 → curate 94 episodes → raw directory gone → metrics emitted.

One honest simplification: because a v3.0 video file spans many episodes, curation copies
video files whole rather than re-encoding to drop quarantined footage. The curated `meta`
records which episodes are valid, and curated storage grows to the kept corpus — bounded by
disk, and deliberately separate from the *raw* budget the guard enforces.

## Cataloguing and synthesis, and two data-shape gotchas

The catalog ([`catalog/`](../catalog)) writes one row per dataset version — source,
license, counts, task distribution, pass-rate, bytes-on-disk, git commit. I gave it two
backends: Postgres for the stack and a stdlib **sqlite** fallback, so the full pipeline
runs with no server for local work. The synthetic augmenter
([`data_generator/augment.py`](../data_generator/augment.py)) mints smooth, in-distribution
episodes for under-represented tasks and — crucially — sends them through the *same* gates
as real data before they can enter the curated set.

Two real-data shapes bit me and are worth recording:

- **`tasks.parquet` isn't shaped the way fixtures are.** Real v3.0 stores the task string
  as the pandas *index* (`__index_level_0__`) with a separate `task_index` column, while my
  test fixtures used a plain `task` column. The catalog's `read_task_map` now handles both.
- **Schemas don't line up on merge.** The real DROID parquet carries `next.reward`,
  `next.done`, and `index` columns that synthetic episodes lack, so concatenating them into
  the augmented version failed until I unioned the schemas with null-fill.

The end result is what the definition of done asked for: two catalogued versions — a real
one (`v0.1.0-droid-100`, 94 episodes) and a synthetically-augmented one
(`v0.2.0-droid-100-aug`, 114 episodes) — each with real license and lineage. `make demo`
runs the whole chain; the Airflow DAG wraps the identical stage callables so the scheduled
run and the one-command demo execute the same logic.

## Scaling it: the batched loop and an enumeration trap

The headline deliverable is a *measured* invariant, not a narrated one:

    peak concurrent on-disk raw  <  budget  <<  total bytes processed

[`scale.py`](../scale.py) implements the batched **acquire → gate → curate → commit →
evict** loop with five correctness rules I held myself to: check headroom *before*
fetching (never overshoot between download and eviction); evict a batch only *after* its
curated write and catalog row are durably committed; track processed ids in a resumable
manifest so an interrupted run never re-pulls or double-counts; keep quarantine to a
reason + a tiny sample under its own sub-cap; and **measure** the peak by sampling the raw
directory rather than trusting the guard's own accounting.

I proved it on a seeded, network-free synthetic source with a deliberately tiny budget —
reproducible on any machine, and asserted in the tests: peak raw ≈ 0.31 MB against a
0.625 MB budget while 2.5 MB (≈ 4× the budget) flowed through, with the invariant checked
in code.

The trap here was enumeration. A naive `list_repo_tree(recursive=True)` over the full
DROID mirror hangs — it has thousands of video files and materialises the entire tree. The
fix is to scope enumeration to the `data/` prefix, iterate lazily, and resolve each unit's
video paths and sizes from the `video_path` template plus a targeted `get_paths_info`,
never a recursive video listing. Because the path templates come from the dataset's own
`info.json`, the same code adapts to v2.0 or v3.0. The full real-DROID run streams tens of
GB through the 25 GB budget on Linux; the [runbook](verification.md) has the bounded
command and what to expect.

## The dashboard

[`dashboard/app.py`](../dashboard/app.py) is a small Streamlit app that reads the catalog
and the scale manifests directly and shows dataset versions and lineage, gate pass-rate by
version, task distribution, and the peak-vs-budget invariant per scale run. It reads from
where the numbers actually live rather than from logs, and `pandas` ships with Streamlit so
it adds no dependency of its own. Kibana remains for logs; it is not in the metrics path.

## Reflections

Two things stand out looking back. First, most of the interesting engineering happened at
the boundary with reality — the format version, the `tasks.parquet` shape, the gate that
was wrong three times — none of which a plan can fully anticipate; the value was in noticing
and adjusting. The first Ubuntu pass through [`verification.md`](verification.md) added to
that list: it caught a missing `load_sources` import on the real `droid-slice` path (which
the Windows dev box couldn't exercise, since it can't download the corpus), surfaced that
the storage guard measures the whole data-root — so the tiny-budget guard-trip demo needs
an isolated directory — and reminded me that Spark's JVM and the `sqlite3` CLI are
environment prerequisites worth stating plainly. That is exactly what a verification
runbook is for. Second, the constraint *was* the design: bounding raw disk forced the
process-and-evict loop, and that loop is the thing worth showing. What I would build next
is written up in the [conception's roadmap](01-conception.md#roadmap).
