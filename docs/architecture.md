# Architecture

## Overview

The repo is a **full trading stack** in two halves:

1. **Discovery** — a pipeline of **11 fully self-contained Jupyter
   notebooks**. Each notebook performs one stage: it installs its own
   dependencies, reads its inputs (from AWS S3 or an upstream notebook's
   output), and writes its outputs back to S3. Running the notebooks in
   order takes the raw market-data feed from the Massive API and produces
   correlation strategies, backtest results, and interactive pair analysis.
2. **Execution** — a local trading daemon (scanner → executor → sell
   manager, see `services.md` §8) that reads the discovered pairs from S3,
   monitors live prices, and places orders through Interactive Brokers
   (paper trading account for testing).

The rest of this document covers the notebook half; the execution half is
documented in [services.md §7–§8](services.md#7-interactive-brokers-order-execution).

Every notebook is standalone — there is no shared package, no build step, and
no installation required beyond what the setup cell does. The same notebook
runs on **Kaggle**, local Jupyter, or Google Colab.

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

`02-02` is optional: it mirrors the small-but-heavily-read S3 data into a
private Kaggle dataset. The consumer notebooks (03-01, 04-01, 04-02, 05-01,
05-02) automatically prefer the mounted mirror and fall back to S3, so the
pipeline runs identically with or without it.

## Stages

| Stage | Notebook | Input | Output (S3) | Engine |
|---|---|---|---|---|
| Ticker load | `00-01` | Massive API | `types/ticker_types.parquet`, `summary/tickers/tickers.parquet` | boto3 |
| Ticker universe | `00-02` | S3 tickers + yfinance | `summary/ticker_yahoo_details/<TICKER>.parquet` | boto3 + httpfs |
| Historical bars | `01-01` | S3 tickers + Massive API | `minute_data_staging/<TICKER>.parquet` | boto3 |
| Merge minute data | `01-02` | staging + final | `minute_data_final/<TICKER>.parquet` | boto3 |
| ETL summary | `02-01` | minute final | `summary/minute_summary`, `summary/daily_volume` | **Spark** |
| S3 → Kaggle mirror | `02-02` | mirrored S3 prefixes | private Kaggle dataset `dsptlp/market-data-s3-dataset` | boto3 |
| Correlation | `03-01` | summaries + tickers | `strategies/correlation/<run>/data.parquet` | **Spark** |
| Backtest | `04-01` | summary + correlation pairs | `backtest/<run>/data.parquet` | **Spark** |
| Analyze backtest | `04-02` | backtest trades | `analysis/backtest_metrics/<run>/data.parquet` | DuckDB httpfs |
| Pair explorer | `05-01` | metrics + prices | — (interactive) | DuckDB httpfs |
| Pair analysis | `05-02` | metrics + trades | reports/ (local) | DuckDB httpfs |

## Notebook anatomy

Every notebook follows the same layout:

1. **Markdown title** — what the stage does and what it produces.
2. **Setup cell** — `!pip install` missing packages, imports, and an inline
   `load_config()` that resolves secrets from environment variables or
   `config.json` (never hard-coded). Creates `cfg`, the boto3 `s3` client,
   and (where needed) a DuckDB `con` or Spark `spark`.
3. **Library cell(s)** — the self-contained helper functions for that stage
   (storage helpers, quality filters, feature engineering, correlation,
   backtest, or analysis code). These are plain Python cells — no package
   imports.
4. **Pipeline cells** — step-by-step execution mirroring the original Kaggle
   notebooks: load → transform → compute → upload to S3.
5. **(05-01/05-02) widget cells** — interactive ipywidgets / Plotly dashboards.

Because each notebook carries its own copy of the helpers it needs, stages
can be re-run, reordered, or skipped (e.g. re-run only `03-01` with different
parameters) without affecting other notebooks.

## Data flow and state

All durable state lives in S3; notebooks are stateless compute:

```text
S3 parquet_data/
├── minute_data_staging/     ← 01-01 writes, 01-02 consumes + clears
├── minute_data_final/       ← 01-02 writes, 02-01 consumes
├── summary/                 ← 02-01 writes; 02-02 mirrors; 03-01 / 04-01 / 05-01 consume
├── strategies/correlation/  ← 03-01 writes; 02-02 mirrors; 04-01 consumes
├── backtest/                ← 04-01 writes; 02-02 mirrors; 04-02 / 05-02 consume
└── analysis/backtest_metrics/ ← 04-02 writes; 02-02 mirrors; 05-01 / 05-02 consume

Kaggle dataset (optional mirror)
└── market-data-s3-dataset/  ← 02-02 writes; consumers prefer this local copy
```

Incremental behavior: ingestion stages derive "what is already done" from S3
object keys (one Parquet per ticker) and only fetch the set difference —
re-running them is cheap and safe.

## Configuration & credentials

See [`docs/services.md`](services.md) for the full external-service
documentation. In short:

| Service | Credential | Purpose |
|---|---|---|
| AWS S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | all storage |
| Massive API | `MASSIVE_API_KEY` | ticker + minute data |
| Kaggle | `KAGGLE_USERNAME`, `KAGGLE_API_TOKEN` | push/execute notebooks |

Resolution order in every setup cell: **environment variables → config.json
→ defaults**. On Kaggle, secrets are set via notebook settings
(Add-ons → Secrets) and appear as environment variables.

## Design principles

- **Notebooks as the interface** — the pipeline is readable end-to-end; each
  notebook is the unit of execution, sharing, and documentation.
- **Configuration over hard-coding** — credentials and S3 paths come from
  env/config; bucket, region, and prefixes are configurable.
- **Reproducibility** — deterministic filters are separated from randomized
  sampling; identical inputs produce identical outputs.
- **Measure before optimizing** — benchmark numbers are measured, never
  fabricated (`benchmarks/results.md`).
- **Minimize data movement** — derived state (who has been downloaded, what
  pairs exist) is reconstructed from object keys and Parquet metadata instead
  of re-scanning.
- **Two engines, one workload** — the daily OHLCV aggregation and the
  summary ETL exist for Spark (distributed) and the analysis stages use
  DuckDB (local vectorized), so the trade-offs between them are visible in
  the same pipeline.