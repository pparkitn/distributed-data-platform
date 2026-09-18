# Partitioning

The platform evaluates how different Parquet partitioning strategies affect
query performance, file counts, partition pruning, data skew, small-file
problems, and Spark shuffle behavior.

## Strategies

### Strategy 1: Date

```text
data/
├── trade_date=2026-01-01/
├── trade_date=2026-01-02/
└── trade_date=2026-01-03/
```

Good for time-range queries (predicate pushdown on `trade_date` prunes whole
directories). Bad for single-symbol queries — every date directory must be
scanned. Small files if few symbols trade per day.

### Strategy 2: Symbol + Date

```text
data/
├── symbol=AAPL/
│   ├── trade_date=2026-01-01/
│   ├── trade_date=2026-01-02/
│   └── ...
├── symbol=MSFT/
│   └── ...
```

Best for per-symbol workloads (correlation pair lookups, backtests), at the
cost of file-count explosion: `symbols x dates` directories.

### Strategy 3: Year / Month / Day

```text
data/
├── year=2026/
│   ├── month=01/
│   │   ├── day=01/
│   │   └── day=02/
```

Balances granularity and directory depth; coarser ranges can prune at the
`year` / `month` level.

## How partitioning is applied

In the notebook pipeline:

- **Ingestion** writes one Parquet per symbol (`minute_data_final/<SYMBOL>.parquet`)
  — the canonical raw layout.
- **`02-01-etl-summary`** (Spark) writes `summary/minute_summary` and
  `summary/daily_volume` as single datasets; the optional daily-OHLCV cell in
  the same notebook can be adjusted to write with `partitionBy("trade_date")`
  to produce Strategy 1:

  ```python
  df_daily_ohlcv.write.mode("overwrite").partitionBy("trade_date").parquet(...)
  ```

- DuckDB stages (`04-02`, `05-01`, `05-02`) read whatever layout exists on
  disk, so layout comparisons isolate the layout from the engine.

## What is measured

For each strategy, benchmark runs record:

- Query performance (full scan vs. partition-pruned scan)
- File count and average file size (small-file problem)
- Partition pruning effectiveness (rows scanned vs. rows returned)
- Data skew (largest vs. median partition size)
- Spark shuffle volume when repartitioning between stages

## Current recommendation

For the time-series workload in this platform:

- **Raw minute bars**: one Parquet per symbol (`minute_data_final/<SYMBOL>.parquet`)
  — avoids the small-file explosion of minute-level date partitioning while
  keeping per-symbol scans efficient.
- **Daily analytics output**: partition by `trade_date` — downstream
  correlation/backtest stages query by ticker first and then by date range,
  so date-partitioned output plus predicate pushdown on `trade_date`
  minimizes I/O for range queries.

The partitioning experiments are tracked in `benchmarks/results.md` as
measurements are collected.