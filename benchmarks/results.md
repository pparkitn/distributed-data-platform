# Benchmark Results

All numbers are measured from actual executions against the same workload and
dataset. No numbers are fabricated.

## Workload

Daily OHLCV aggregation (open / high / low / close / volume / trades / vwap)
over the same dataset, implemented with both engines:

- Spark: the daily-OHLCV cells in `notebooks/02-01-etl-summary.ipynb`
- DuckDB: the equivalent `read_parquet` / `date_trunc` SQL used by the
  analysis notebooks (`04-02`, `05-01`, `05-02`)

## Roadmap

| Dataset | Rows | DuckDB | Spark | Notes |
|---|---:|---:|---:|---|
| Small | 10M | TBD | TBD | |
| Medium | 100M | TBD | TBD | |
| Large | 1B | TBD | TBD | |
| Very Large | 10B+ | TBD | TBD | |

## Local runs (development machine)

_To be filled in as benchmarks are executed. Run instructions in
`docs/performance.md`._

| Dataset | Rows | Engine | Time (s) | Input (GB) | Output (GB) | Partitions | CPU | Mem |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| TBD | TBD | DuckDB | TBD | TBD | TBD | TBD | TBD | TBD |
| TBD | TBD | Spark | TBD | TBD | TBD | TBD | TBD | TBD |

## Partitioning experiments

Results for the strategies documented in `docs/partitioning.md` will be
recorded here (file counts, scan rows vs. returned rows, skew, shuffle
volume).
