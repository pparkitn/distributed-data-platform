# Engineering Decisions

This document records the *why* behind the platform's architecture — the
context, the choice, the alternatives considered, and the trade-offs
accepted. The goal is to show not just *what* the system does, but *why these
technologies belong together*.

Every decision below reflects something actually built in this repository,
measured where possible, and revisable when the evidence changes.

---

## 1. Why Parquet?

**Context.** The platform stores ~36.7k files / ~24 GB / 1.04B records and
must read them repeatedly with three different engines (boto3, DuckDB
httpfs, Spark s3a).

**Decision.** All datasets — raw, summary, strategy, and ledger — are
Parquet.

**Why.**

- **Column pruning** — queries only read the columns they reference; a
  correlation job that needs `symbol/date/close` never reads `vwap`.
- **Compression** — the ~24 GB bucket is mostly compressed columnar data,
  not raw text.
- **Predicate pushdown** — Parquet row-group statistics let Spark and DuckDB
  skip data that cannot match a filter.
- **Engine-neutral** — Parquet is the interchange format that pandas, DuckDB,
  and Spark all read natively, so the same files serve every stage.

**Alternatives.** CSV (10× larger, no schema), JSON (same), Avro (good but
row-oriented — wrong access pattern for analytics), raw binary (no ecosystem).

**Trade-off.** Parquet is write-heavy and not ideal for single-record updates;
the platform treats it as immutable dataset storage, never a database.

---

## 2. Why S3 (object storage)?

**Context.** Compute (Kaggle sessions, local machines) is ephemeral; data must
persist permanently and be shared across engines.

**Decision.** S3-compatible object storage is the **single source of truth**;
every stage reads inputs from S3 and writes outputs back. Nothing lives only
on a machine that might vanish.

**Why.**

- **Decouples storage from compute** — Kaggle bursts, local runs, and a
  future Spark cluster can all read the same objects.
- **Cheap, durable, infinite** — ~24 GB is a few dollars/month.
- **Uniform state** — S3 keys double as the data catalog: "which tickers are
  already downloaded" is derivable from object keys alone (no extra database.

**Alternatives.** A traditional database (adds schema rigidity + cost), a
filesystem NFS (binds storage to a machine).

**Trade-off.** Object stores are eventually consistent under some access
patterns, and LIST operations cost money; the platform mitigates this by
deriving incremental logic with key prefixes rather than listing everything.

---

## 3. Why Spark (distributed)?

**Context.** The raw minute dataset is 1.02B rows; the daily-OHLCV aggregation
and correlation analysis are full-table scans with shuffles.

**Decision.** Spark implements the heavy stages (02-01, 03-01, 04-01),
reading `s3a://` via hadoop-aws.

**Why.**

- **Scale beyond single-node memory** — 1B rows exceed what a local engine
  holds comfortably.
- **Parallelism** — filtering, joins, and aggregations split across tasks;
  the correlation self-join is a genuinely distributed problem.
- **Cluster-readiness** — the same jobs run on a Kaggle CPU session today
  and a multi-node cluster tomorrow without code changes.

**Alternatives.** Pure DuckDB for everything (see #4), raw Python loops over
14k files (serial, slow), Presto/Trino (SQL-only, less control over
shuffles).

**Trade-off.** Overhead: per-task scheduling, serialization, JVM memory. Below
~100M rows DuckDB often wins — which is why the platform has both (see #4
and the benchmarks).

---

## 4. Why DuckDB (local vectorized)?

**Context.** Several stages (04-02, 05-01, 05-02) aggregate millions of
rows, and benchmarks should answer *"when is single-node enough?"*

**Decision.** DuckDB implements the same OHLCV workload and the analysis
stages, reading `s3://` directly via the httpfs extension.

**Why.**

- **No cluster required** — vectorized execution on one machine beats Spark's
  overhead at moderate scale.
- **SQL over Parquet, in-process** — `read_parquet('s3://...')` with
  `union_by_name` handles heterogeneous run outputs without a schema
  migration.
- **Comparable benchmarks** — the identical OHLCV definition on both engines
  (docs/performance.md) turns "Spark vs DuckDB" from an opinion into a
  measurement.

**Alternatives.** SQLite (single-threaded), Polars (great, but no SQL/PQ
glue for this pipeline), doing everything in Spark (overhead at small scale).

**Trade-off.** Single-node memory and CPU bound the engine; when the working
set outgrows it, the Spark path is the escape hatch.

---

## 5. Why Kaggle (cloud execution)?

**Context.** The platform needs bursts of heavy compute (Spark on 1B rows,
10-hour correlation loops) without owning a cluster.

**Decision.** Notebooks run on Kaggle CPU sessions (12h, ~30 GB RAM,
internet enabled, PySpark preinstalled) and are pushed with
`kaggle kernels push`.

**Why.**

- **Cheap/free burst compute** — no server to babysit, sessions billed by
  quota not by the hour.
- **Preinstalled Spark** — the distributed stages run with zero cluster
  setup.
- **Parallel search** — batch_runner.py keeps N instances of a stage running
  in a loop (the 03-01 strategy search), which a fixed cluster wouldn't
  grant this cheaply.

**Alternatives.** A permanent EC2/EMR cluster (costs money while idle), local
processing (bounded by one machine), Colab (weaker CPU sessions, no
preinstalled Spark).

**Trade-off.** Sessions are ephemeral (12h), quota-limited, and reconnects
must be resumable — which drove decision #6.

---

## 6. Why incremental ingestion?

**Context.** Downloading 14k tickers of minute bars at a rate-limited ~5
calls/min spans *many* 12h sessions; re-downloading everything would be
wasteful and slow.

**Decision.** Ingestion stages derive "what already exists" from S3 object
keys (one Parquet per ticker) and fetch only the set difference; the merge
stage then reconciles staging into the final tree.

**Why.**

- **Resumability** — a session that dies at hour 11 restarts at the exact
  missing tickers, not from zero.
- **Cost** — no re-download, no re-upload of already-processed data.
- **Idempotence** — re-running a stage is safe (dedup on merge).

**Alternatives.** Full re-download per run (wastes API quota and wall-clock),
a checkpoint database (extra state to keep consistent).

**Trade-off.** Object-key enumeration costs LIST requests; fine at 14k keys,
worth revisiting at millions.

---

## 7. Why separate raw and derived datasets?

**Context.** The pipeline produces many layers: raw minute bars, daily
summaries, correlation pairs, backtest trades, metrics.

**Decision.** Layers are physically separate S3 prefixes
(`minute_data_final/`, `summary/`, `strategies/`, `backtest/`,
`analysis/`) and are never overwritten in place; run outputs live under
`<run_timestamp>/` folders.

**Why.**

- **Lineage** — every derived dataset points back to the exact raw inputs
  and run that produced it.
- **Reproducibility** — re-running a stage never mutates an existing
  dataset, so old results remain valid and comparable.
- **Recovery** — a bad run is deleted, not "fixed"; re-derivation is a
  pipeline re-run, not a hand edit.

**Alternatives.** In-place updates (destroys lineage), one giant blob (no
layer boundaries, no pruning).

**Trade-off.** Storage grows with runs; mitigated by the Kaggle dataset
mirror (02-02) for the frequently-read layers and the small size of strategy
outputs.

---

## 8. Why one Parquet per symbol for raw minute data?

**Context.** 14,015 symbols × 390 minutes/day; the correlation and backtest
stages are symbol-centric.

**Decision.** `minute_data_final/<SYMBOL>.parquet` — the incremental
ingestion unit (see #6) is also the query unit.

**Why.**

- Per-symbol scans and merges are a single object read.
- Date partitioning at minute granularity would explode into millions of
  small files (the classic small-file problem).
- Symbol-level files keep the object count at ~14k — manageable for
  listing, uploads, and DuckDB/Spark globs.

**Alternatives.** `year=..//month=../day=..` Hive partitioning (tested in
docs/partitioning.md — better for date-range scans, worse for per-symbol
workloads and file counts).

**Trade-off.** Date-range scans must touch every symbol file; the summary
layer (derived, date-oriented) exists precisely to serve those queries.

---

## 9. Why explicit Spark schemas and epoch-ms timestamps?

**Context.** Per-ticker files drifted over time (INT64 vs DOUBLE volume,
`"60s"` string timestamps in one corrupt batch), and downstream code needs a
stable contract.

**Decision.**

- Timestamps are stored as **epoch-ms int64** (`date`), the Massive API's
  native format — no timezone parsing at ingest.
- Spark reads use an **explicit schema** (`MINUTE_SCHEMA`: volume as DOUBLE
  to absorb INT64/DOUBLE drift), and analysis joins use
  `union_by_name=true` for heterogeneous run schemas.
- `volume` is DOUBLE even though it is conceptually integral — the cost is
  trivial, the drift resilience is real.

**Why.** Schema-on-read beats schema-on-write when the writer is an external
API with version drift; explicit schemas turn corrupt batches into clean
failures instead of silent garbage.

**Trade-off.** Loses strictness at the write boundary; compensated by the
validation layer and quality filters that gate downstream analysis.

---

## 10. Why self-contained notebooks as the pipeline interface?

**Context.** The pipeline must run unattended on Kaggle, where there is no
build step and no shared package install.

**Decision.** Each notebook installs its own dependencies and carries its
own helper code (config, storage, quality filters), so any stage runs
standalone.

**Why.** A Kaggle kernel is a self-contained artifact: copying one notebook
and running it must work, period. This also makes stages independently
re-runnable and shareable.

**Alternatives.** A shared `src/ package installed into every kernel
(more elegant code organization, but couples all stages to a build/release
cycle — which failed in practice on Kaggle's execution model).

**Trade-off.** Duplicated helper code across notebooks — deliberately
accepted and documented; the contracts live once in docs/services.md.

---

## 11. Why explicit input contracts (validation layer)

**Context.** Multiple stages depend on the same minute-bar contract —
`symbol, date(epoch ms), open/high/low/close, volume, vwap, trades` — and
validation must gate correctness before any aggregation.

**Decision.** Validation is a dedicated, engine-agnostic layer: schema checks,
nulls, invalid timestamps, duplicates, negative volume, and OHLC-relationship
rules run before transformations; invalid records are dropped or reported.

**Why.** "Fast pipelines are useless if they produce incorrect data" — the
checklist that gatekeeps aggregation is the same checklist that keeps
backtests honest.

**Trade-off.** Validation cost per record is spent once at ingest; the summary
ETL is what pays it forward.

---

## 12. Why paper trading first (trading integration)

**Context.** The optional trading component (scanner/executor/sell-manager +
Interactive Brokers) exists as a downstream consumer.

**Decision.** Defaults to the **paper trading account**; live requires
explicit multi-file manual configuration changes; risk gates fail closed.

**Why.** The value of the platform is the datasets and signals; execution is
a demonstration of the hand-off, and correctness at that boundary is tested
against virtual money first.

**Trade-off.** Paper fills are optimistic (no market impact) — documented as
a known limitation in docs/services.md.

---

## Summary of trade-off themes

| Theme | Where | Resolution |
|---|---|---|
| Local vs distributed | engines | Both, measured (Spark + DuckDB, same workload) |
| Schema flexibility vs strictness | ingest/analysis | Explicit read schemas + validation gates |
| Reproducibility vs storage growth | derived data | Immutable per-run outputs + Kaggle mirror |
| Self-containment vs DRY | notebooks | Self-contained kernels, contracts in docs |
| Compute cost vs speed | execution platform | Kaggle bursts + incremental ingestion |