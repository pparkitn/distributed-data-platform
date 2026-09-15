# Market Data Platform

A scalable market-data platform processing **1B+ records** using **Apache
Spark, DuckDB, Parquet and Amazon S3** — with incremental ingestion,
data-quality validation, distributed analytics, and reproducible benchmarks.

> **Goal:** Build a reproducible, scalable market-data platform that ingests
> raw stock-minute data from object storage, validates and transforms it into
> analytical datasets, and supports efficient querying through distributed
> and local analytical engines.

```text
┌──────────────────────────────────────────────────┐
│                  PLATFORM SCALE                  │
├──────────────────────────────────────────────────┤
│  1.04B+    records (measured from Parquet meta)  │
│  36.7K     Parquet objects                       │
│  24 GB     stored data                           │
│  14K+      securities                            │
│  Spark     distributed processing                │
│  DuckDB    vectorized analytics                  │
│  S3        object storage                        │
│  Kaggle    cloud execution                       │
└──────────────────────────────────────────────────┘
```

The primary interface is a pipeline of **11 self-contained notebooks** that
run on Kaggle, local Jupyter, or Colab. Every stage reads its inputs from S3
and writes its outputs back to S3, so the pipeline is resumable, auditable,
and free of hidden state.

> **Design note (notebooks vs. package):** the notebooks are self-contained
> by deliberate choice — each installs its own dependencies and carries its
> own helpers, so any stage runs standalone on Kaggle without a shared build.
> The trade-off (some duplicated configuration/storage code across
> notebooks) is intentional: it makes every stage independently executable
> and resume-safe. This project started as Kaggle kernels and kept that
> model; the shared-service contracts are documented once in
> [docs/services.md](docs/services.md).

---

## What this platform is — and what it is not

**This is a data platform.** Its job is to turn raw market data into
correct, queryable, analytics-ready datasets:

```text
raw minute bars  →  validation  →  daily OHLCV / summary datasets
                →  feature engineering  →  lead-lag analysis
                →  backtests  →  ranked strategy datasets & signals
```

**Trading is an optional, separately controlled downstream consumer.** The
repository also contains an experimental integration that can send orders to
Interactive Brokers (paper trading account for testing). That component:

- is **not** part of the core data platform — it consumes the platform's
  datasets and signals;
- runs on a separate local machine and broker account, under its own
  risk controls;
- defaults to **paper trading only** — live execution requires explicit
  manual configuration changes.

Everything a data-engineering reviewer needs is in the platform itself: the
pipeline, the validation layer, the storage design, and the engine
benchmarks. The trading integration is documented separately and can be
ignored entirely.

---

## Architecture

```text
                         Market Data
                              │
                              ▼
                    ┌──────────────────┐
                    │ Object Storage   │
                    │ S3 / MinIO       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Raw Parquet     │
                    │  Minute Data     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Data Validation  │
                    │ Schema & Quality │
                    └────────┬─────────┘
                             │
                 ┌───────────┴───────────┐
                 │                       │
                 ▼                       ▼
        ┌────────────────┐      ┌────────────────┐
        │ Apache Spark   │      │    DuckDB      │
        │ Distributed    │      │ Local/Vectorized│
        │ Processing     │      │ Processing     │
        └───────┬────────┘      └───────┬────────┘
                │                       │
                └───────────┬───────────┘
                            ▼
                   ┌──────────────────┐
                   │ Daily OHLCV /    │
                   │ Analytics Data   │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │ Datasets,        │
                   │ Signals, Reports │
                   └────────┬─────────┘
                            │  (optional)
                            ▼
              ┌─────────────────────────┐
              │ Trading integration     │
              │ (separate component,    │
              │  paper-first, optional) │
              └─────────────────────────┘
```

---

## The Pipeline

Run the notebooks in order — each one consumes the previous stage's S3
output:

```text
 00-01 ticker load ──► 00-02 ticker universe
        │                      │
        ▼                      ▼
 01-01 historical bars ──► 01-02 merge minute data
        │
        ▼
 02-01 ETL summary (Spark)
        │
        ▼
 02-02 S3 -> Kaggle dataset (optional mirror for fast/free reads)
        │
        ▼
 03-01 correlation (Spark)
        │
        ▼
 04-01 backtest (Spark) ──► 04-02 analyze backtest (DuckDB)
        │
        ▼
 05-01 pair explorer / 05-02 advanced pair analysis (interactive)
```

| # | Notebook | What it does | Engine | Output |
|---|---|---|---|---|
| 00-01 | `ticker-load` | Ticker types + active stock universe (Massive API) | boto3 | S3 `types/`, `summary/tickers/` |
| 00-02 | `ticker-universe` | Per-ticker reference details (yfinance) | boto3 | S3 `summary/ticker_yahoo_details/` |
| 01-01 | `historical-load` | Minute OHLCV bars per ticker (Massive API) | boto3 | S3 `minute_data_staging/` |
| 01-02 | `merge-minute-data` | Merge staging into final, dedup, sort | boto3 | S3 `minute_data_final/` |
| 02-01 | `etl-summary` | Daily volume + first/last-bar summaries | Spark | S3 `summary/` |
| 02-02 | `s3-to-kaggle-dataset` | Mirror summaries to a private Kaggle dataset (cost/ speed) | boto3 | Kaggle dataset |
| 03-01 | `correlation` | Lead-lag pair analysis + FDR correction | Spark | S3 `strategies/correlation/` |
| 04-01 | `backtest` | Backtest every pair vs market benchmark | Spark | S3 `backtest/` |
| 04-02 | `analyze-backtest` | Per-pair metrics + robust ranking (Fisher z-CI) | DuckDB | S3 `analysis/backtest_metrics/` |
| 05-01 | `pair-explorer` | Interactive price charts per pair | DuckDB | — |
| 05-02 | `advanced-pair-analysis` | Interactive dashboard + report export | DuckDB | reports/ |

**Incremental by design:** ingestion stages derive "what already exists" from
S3 object keys (one Parquet per ticker) and only fetch the difference, so
re-runs are cheap and safe — the 13k+ ticker stages resume across session
limits.

---

## Key Features

### Data validation (before anything else)

> **Fast pipelines are useless if they produce incorrect data.**

The validation layer runs independently of the transformations and checks:
schema/missing columns, null values, invalid timestamps, duplicate records,
negative volume, invalid OHLC relationships, and invalid numerics.

```text
                 Raw Data
                    │
                    ▼
             Schema / Record Validation
                    │
                    ▼
         Data Quality Filters
                    │
          ┌─────────┴─────────┐
          │                   │
       Valid                 Invalid
          │                   │
          ▼                   ▼
    Transformation        (dropped / reported)
          │
          ▼
      Aggregation
```

### Two engines, one workload

The daily OHLCV aggregation and the analysis stages are implemented on both
**Spark** (distributed: partition pruning, predicate pushdown, shuffle
control) and **DuckDB** (single-node vectorized), with an identical OHLCV
definition — so local vs. distributed trade-offs are measurable, not
theoretical. Benchmark results are recorded in
[`benchmarks/results.md`](benchmarks/results.md) from actual runs.

### Object storage as the source of truth

All inputs and outputs live in S3 (or MinIO / local filesystem) as Parquet —
configured, never hard-coded. This gives the platform cheap storage, engine
interchangeability (boto3, DuckDB `httpfs`, Spark `s3a`), and resumable
stages.

---

## S3 Data Structure

All market data lives in the AWS S3 bucket **`market-data-zw`** under the
`parquet_data/` prefix (region `us-east-1`):

```text
s3://market-data-zw/parquet_data/
│
├── minute_data_final/                     # Raw minute-level OHLCV, one Parquet per symbol (~14k symbols)
│   └── <SYMBOL>.parquet
│
├── summary/
│   ├── minute_summary/data.parquet/       # First + last minute bar per symbol/day (Spark output)
│   ├── daily_volume/data.parquet/         # Daily volume/close summary (Spark output)
│   ├── tickers/tickers.parquet            # Ticker universe
│   └── ticker_yahoo_details/              # Per-ticker reference details (~13k files)
│
├── types/
│   └── ticker_types.parquet               # Ticker type taxonomy
│
├── backtest/<YYYYMMDD_HHMM>/data.parquet  # Backtest run results (per-run folder)
│
├── analysis/
│   └── backtest_metrics/<YYYYMMDD_HHMM>/data.parquet   # Backtest metrics per run
│
├── strategies/
│   └── correlation/<YYYYMMDD_HHMM>/data.parquet        # Correlation strategy outputs
│
└── logs/
    ├── trade_log/<YYYYMMDD>/<YYYYMMDD_HHMM>/           # (trading integration) order ledger
    └── trader_debug_logs/                              # (trading integration) debug JSONL
```

### Raw minute data (`minute_data_final/`)

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Ticker symbol |
| `date` | int64 | Timestamp as epoch **ms** |
| `open` / `high` / `low` / `close` | double | OHLC prices |
| `volume` | double | Trade volume |
| `vwap` | double | Volume-weighted average price |
| `trades` | int64 | Number of trades |

### Data volume

The bucket holds **36,701 objects totaling ~24 GB**, with the raw minute
data containing **over 1 billion records** — measured from Parquet metadata,
not estimates.

| Folder | Files | Records |
|---|---:|---:|
| `minute_data_final/` | 14,015 | **1,022,419,229** |
| `summary/minute_summary/` | 200 | 12,775,156 |
| `summary/daily_volume/` | 5 | 6,510,188 |
| `summary/tickers/` | 1 | 13,151 |
| `summary/ticker_yahoo_details/` | 13,152 | 27,125 |
| `types/` | 1 | 25 |
| `backtest/` | 14 | 1,139,222 |
| `analysis/backtest_metrics/` | 1 | 5,408 |
| `strategies/` | 682 | 9,447 |
| `signals/` | 166 | 17,996 |
| **Total** | **~36.7k objects** | **~1.04 billion** |

---

## Processing Engines

| | Apache Spark | DuckDB |
|---|---|---|
| Type | Distributed (Kaggle CPU session / cluster) | Local, single-node, vectorized |
| S3 access | `s3a://` via hadoop-aws | `s3://` via httpfs |
| Used by | 02-01, 03-01, 04-01 | 04-02, 05-01, 05-02 |
| Strengths | Scale beyond local memory, parallel shuffles, cluster-ready | Fast scans/aggregations, zero cluster overhead |

Both implement the **same daily OHLCV definition**, so results and timings
are directly comparable — see [docs/performance.md](docs/performance.md) and
[benchmarks/results.md](benchmarks/results.md).

---

## Quick Start

### Run on Kaggle (recommended)

Full walkthrough in
[docs/services.md → "Running the entire pipeline end-to-end"](docs/services.md#running-the-entire-pipeline-end-to-end).

```bash
pip install kaggle
python3 push_kernels.py --dry-run             # show the plan
python3 push_kernels.py --push                # push all 11 notebooks
python3 push_kernels.py --push 03-01-correlation.ipynb   # push one
```

Add the required secrets to each kernel (Add-ons → Secrets):
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `MASSIVE_API_KEY`
(`MASSIVE_API_KEY` only for 00-01 / 01-01). Run the notebooks in order
(`00-01 → 00-02 → 01-01 → 01-02 → 02-01 → [02-02 → attach mirror] → 03-01 →
04-01 → 04-02 → 05-*`). Slow stages are incremental and resume on re-run.

### Run locally

```bash
pip install jupyter numpy pandas pyarrow boto3 duckdb scipy tqdm \
            matplotlib plotly ipywidgets yfinance massive
# Spark stages (02-01, 03-01, 04-01) additionally need: pip install pyspark (Java 17)
jupyter lab
```

Set the environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`, `S3_BUCKET`, `MASSIVE_API_KEY`) or drop a `config.json` next to
the notebooks (see `config.example.json`).

### Prerequisites: accounts & keys

| # | Account | Needed for | Cost |
|---|---|---|---|
| 1 | Kaggle | executing the notebooks | Free (weekly session quota) |
| 2 | AWS (S3 bucket + access keys) | all data storage | ~$ / month for ~24 GB |
| 3 | Massive API key | ticker universe + minute bars | Free tier ~5 calls/min |
| 4 | yfinance | reference details | Free, no account |
| 5 | Interactive Brokers *(optional — only for the trading integration)* | order execution on paper/live | Paper free (virtual $1M); live commission-based |

---

## Repository Structure

```text
market-data-platform/
│
├── README.md
├── LICENSE
├── config.example.json          # non-secret config template
│
├── notebooks/                   # the pipeline (run in order)
│   ├── 00-01-ticker-load.ipynb
│   ├── 00-02-ticker-universe.ipynb
│   ├── 01-01-historical-load.ipynb
│   ├── 01-02-merge-minute-data.ipynb
│   ├── 02-01-etl-summary.ipynb
│   ├── 02-02-s3-to-kaggle-dataset.ipynb
│   ├── 03-01-correlation.ipynb
│   ├── 04-01-backtest.ipynb
│   ├── 04-02-analyze-backtest.ipynb
│   ├── 05-01-pair-explorer.ipynb
│   └── 05-02-advanced-pair-analysis.ipynb
│
├── push_kernels.py              # push notebooks to Kaggle (`kaggle kernels push`)
├── batch_runner.py              # continuous / parallel Kaggle runs
│
├── tests/                       # notebook static validation + helper tests
├── .github/workflows/ci.yml     # lint + tests + automated secret scan
│
├── media/                       # overview video, audio, slides, diagrams
│
├── docs/
│   ├── services.md              # EXTERNAL SERVICES (Massive, S3, Kaggle, IB, trader)
│   ├── engineering-decisions.md # WHY Parquet/S3/Spark/DuckDB/Kaggle/…
│   ├── architecture.md
│   ├── partitioning.md
│   └── performance.md
│
└── benchmarks/
    └── results.md               # measured benchmark numbers (never fabricated)
```

---

## Project Overview — Video, Slides & Audio

A short multimedia overview of the platform. Images and the PDF render inline
on GitHub; the video, audio, and PowerPoint open via their links.

**Architecture diagrams**

| | |
|---|---|
| ![Billion-Record Trading Architecture Overview](media/Billion-Record_Trading_Architecture_Overview.png) | ![Market Strategy Engine Blueprint](media/Market_Strategy_Engine_Blueprint.png) |
| ![Market Data Platform Workflow](media/Market_Data_Platform_Workflow.png) | ![Market Data Platform Process](media/Market_Data_Platform_Process.png) |

**Video** — how the trading bots actually execute orders:

- 🎬 [How Trading Bots Actually Execute Orders](media/How_Trading_Bots_Actually_Execute_Orders.mp4) *(MP4, ~6 MB — GitHub plays it in the file viewer)*

**Audio** — narrated walkthrough:

- 🎧 [Autonomous Trading with a Billion Rows](media/Autonomous_trading_with_a_billion_rows.m4a) *(M4A, ~43 MB)*

**Slides & document:**

- 📕 [Quant Trading Blueprint — PDF](media/Quant_Trading_Blueprint.pdf) *(14 pages, GitHub previews PDFs inline)*
- 📊 [Quant Trading Blueprint — PowerPoint](media/Quant_Trading_Blueprint.pptx) *(open in PowerPoint, Google Slides, or LibreOffice)*

---

## Optional: Trading Integration

The platform's datasets and signals can be consumed by an experimental
**trading integration** that places orders through Interactive Brokers.
It is a separate, optional component:

- **Scanner / Executor / Sell Manager** daemons read the platform's S3
  datasets (`backtest/`, `signals/`, `trade_log/`), evaluate live prices,
  and place orders via `ib_insync`.
- **Paper-first:** the default preset trades the **paper trading account**
  (virtual $1M, port 4002). Live execution requires explicit manual
  configuration and is not enabled by default.
- The IB Gateway runs locally, automated by **IBC** (the "ibx tool") with an
  auto-restart loop so it survives crashes.

Full documentation (account setup, paper trading, the restart loop, the
trader daemons, and the S3 order ledger): [docs/services.md §7–§8](docs/services.md#7-interactive-brokers-order-execution).

---

## Engineering Principles

- **Reproducibility — a pipeline should produce the same result when executed
  against the same input.
- Configuration over hard-coding — credentials come from environment variables
  or `config.json`, never literals.
- Measure before optimizing — performance claims are supported by measured
  benchmark results.
- Minimize data movement — already-processed work is detected from S3 object
  keys; large stages avoid unnecessary scans and shuffles.
- Separate concerns — ingestion, validation, transformation, aggregation,
  and analysis are independent, testable stages.

---

## Roadmap

### Phase 1: Foundation
- [x] Notebook pipeline (11 stages, self-contained)
- [x] Configuration system (env / config.json, no hard-coded secrets)
- [x] S3 storage + incremental ingestion (resume via object keys)
- [x] Data validation / quality filters

### Phase 2: Processing
- [x] Daily summary ETL (Spark) + DuckDB analysis stages
- [x] Lead-lag correlation analysis + FDR correction
- [x] Backtest engine + market benchmark
- [x] Metrics analysis (robust ranking, Fisher z-CI)
- [x] Interactive pair analysis (05-01 / 05-02)
- [ ] Predicate pushdown / partitioning benchmarks

### Phase 3: Scale
- [ ] 10M / 100M / 1B / 10B+ row benchmarks (Spark vs DuckDB)
- [ ] Spark cluster testing
- [ ] Performance profiling

### Phase 4: Production Engineering
- [x] Kaggle push + batch runner
- [x] Automated tests + CI (lint, notebook validation, secret scan)
- [x] Documentation (services, architecture, partitioning, performance)
- [ ] CI validation of notebooks / logging / alerting

---

## Documentation

- [External services (Massive, S3, Kaggle, yfinance, Interactive Brokers, trader)](docs/services.md)
- [Engineering decisions — why Parquet, S3, Spark, DuckDB, Kaggle, …](docs/engineering-decisions.md)
- [Architecture](docs/architecture.md)
- [Partitioning](docs/partitioning.md)
- [Performance](docs/performance.md)
- [Benchmark results](benchmarks/results.md)

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.