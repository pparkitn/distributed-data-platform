# Performance

## Method

Benchmarks measure the **same analytical workload** on the **same dataset**
with two engines:

- **DuckDB** — a single machine, vectorized, local analytical engine
  (the analysis stages 04-02 / 05-01 / 05-02).
- **Apache Spark** — a distributed engine (the ETL/correlation/backtest
  stages 02-01 / 03-01 / 04-01), running on Kaggle's 30 GB CPU session.

Both implement the same daily OHLCV aggregation — Spark in the
`02-01-etl-summary` notebook, DuckDB via the `read_parquet` SQL used in the
analysis notebooks — so timings are directly comparable.

## What is measured

- Execution time (wall clock) of the pipeline cells
- Input size (bytes / rows scanned)
- Output size (bytes / rows written)
- Number of partitions
- CPU utilization
- Memory utilization
- Shuffle volume (Spark UI)

## How to run a benchmark

1. Run `02-01-etl-summary` on Kaggle and time the summary + daily-OHLCV
   cells; record the Spark UI metrics.
2. Run the same aggregation with DuckDB (a local notebook / script with
   `read_parquet('s3://.../minute_data_final/*.parquet')`) and time it.
3. Record both in `benchmarks/results.md`.

For local experiments, a synthetic dataset can be generated with a short
pandas script (symbols x days of minute bars) and written to `minute_data_final/`.

## Scaling notes

- DuckDB is highly optimized for local scans and aggregations and typically
  dominates at 10M–100M rows on a single machine, where it avoids Spark's
  per-task and serialization overhead.
- Spark becomes advantageous when the working set exceeds local memory, when
  the cluster is parallelized across many cores/nodes, or when the workload
  requires shuffles that can be spread across machines.
- The raw dataset (`minute_data_final/`) contains **1.02 billion rows**; the
  benchmark roadmap scales from 10M to 10B+ rows.

## Results

Actual, measured numbers live in [`benchmarks/results.md`](../benchmarks/results.md).
Per the project's principle, no fabricated numbers are published.