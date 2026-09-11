# Market Data Platform

A scalable data-processing platform for transforming **billions of financial market-data records** into analytics-ready datasets using **Apache Spark, DuckDB, Parquet, and S3-compatible object storage**.

The project explores practical approaches to processing large-scale time-series data, including data validation, partitioning, predicate pushdown, distributed transformations, and performance benchmarking.

> **Goal:** Build and benchmark a production-oriented data platform capable of processing large volumes of market data efficiently and reproducibly.

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
                   │ Daily OHLCV      │
                   │ Analytics Data   │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │ Analytics / ML   │
                   │ / Backtesting    │
                   └──────────────────┘
```

---

## Why This Project?

Financial market data is a useful workload for exploring large-scale data engineering because it combines:

- High-volume time-series data
- Small records with relatively simple transformations
- Frequent filtering by symbol and time
- Large sequential scans
- Aggregations across billions of records
- Strong requirements for data correctness
- Different performance characteristics between local and distributed processing

The project uses this workload to investigate how storage layout, partitioning, query engines, and distributed processing affect real-world performance.

---

## Key Features

### Large-Scale Data Processing

Process minute-level market data stored as Parquet files and aggregate it into daily OHLCV datasets.

Example:

```text
Minute Data
    │
    ├── AAPL
    ├── MSFT
    ├── NVDA
    └── ...
         │
         ▼
    Daily OHLCV
         │
         ├── Open
         ├── High
         ├── Low
         ├── Close
         ├── Volume
         └── VWAP
```

The pipeline is designed to scale from development datasets to workloads containing **billions of records**.

---

### Object Storage

Input and output data can be stored in:

- Amazon S3
- MinIO
- Local filesystem

Storage locations are configured rather than hard-coded.

Example:

```yaml
input:
  uri: s3://market-data/raw/

output:
  uri: s3://market-data/processed/
```

This allows the same pipeline to run locally or against cloud object storage.

---

### Data Validation

The platform validates incoming datasets before processing.

Expected fields include:

```text
symbol
timestamp
open
high
low
close
volume
vwap
trades
```

Validation includes:

- Schema validation
- Missing columns
- Null values
- Invalid timestamps
- Duplicate records
- Negative volume
- Invalid OHLC relationships
- Invalid numerical values

Example OHLC constraint:

```text
high >= max(open, close, low)

low <= min(open, close, high)
```

Invalid records can be rejected or reported depending on pipeline configuration.

---

## Processing Engines

The same analytical workload is implemented using two different engines.

### Apache Spark

Spark provides the distributed processing implementation.

The pipeline focuses on:

- Partition pruning
- Predicate pushdown
- Repartitioning
- Shuffle reduction
- Parallel aggregation
- Avoiding unnecessary caching
- Distributed execution

Example:

```text
Parquet
   │
   ▼
Spark DataFrame
   │
   ▼
Filter / Partition Pruning
   │
   ▼
Group By Symbol + Date
   │
   ▼
OHLCV Aggregation
   │
   ▼
Parquet
```

---

### DuckDB

DuckDB provides a local analytical implementation of the same workload.

The DuckDB implementation is useful for investigating how far a highly optimized local analytical engine can scale before distributed processing becomes advantageous.

Example:

```text
Parquet
   │
   ▼
DuckDB
   │
   ▼
SQL Aggregation
   │
   ▼
Daily OHLCV
```

---

## Benchmarking

One of the primary goals of the project is to measure actual performance rather than relying on theoretical assumptions.

The same workload is executed using Spark and DuckDB across progressively larger datasets.

Example benchmark format:

| Dataset | Rows | DuckDB | Spark |
|---|---:|---:|---:|
| Small | 10M | TBD | TBD |
| Medium | 100M | TBD | TBD |
| Large | 1B | TBD | TBD |
| Very Large | 10B+ | TBD | TBD |

Measurements will include:

- Execution time
- Input size
- Output size
- Number of records
- Partition count
- CPU utilization
- Memory utilization
- Shuffle volume where available

**Benchmark numbers will be measured from actual executions and will not be fabricated.**

Detailed results are maintained in:

```text
benchmarks/results.md
```

---

## Partitioning Experiments

The project evaluates different Parquet partitioning strategies.

### Strategy 1: Date

```text
data/
├── date=2026-01-01/
├── date=2026-01-02/
└── date=2026-01-03/
```

### Strategy 2: Symbol + Date

```text
data/
├── symbol=AAPL/date=2026-01-01/
├── symbol=AAPL/date=2026-01-02/
├── symbol=MSFT/date=2026-01-01/
└── ...
```

### Strategy 3: Year / Month / Day

```text
data/
├── year=2026/
│   ├── month=01/
│   │   ├── day=01/
│   │   └── day=02/
```

The project measures how these layouts affect:

- Query performance
- File counts
- Partition pruning
- Data skew
- Small-file problems
- Storage organization
- Spark shuffle behavior

The goal is not simply to choose a partitioning scheme, but to document **why** a particular layout is appropriate for the workload.

---

## Repository Structure

```text
market-data-platform/
│
├── README.md
├── LICENSE
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
│
├── src/
│   ├── ingestion/
│   ├── validation/
│   ├── transformations/
│   ├── aggregation/
│   └── utils/
│
├── jobs/
│   ├── spark_daily_ohlcv.py
│   └── duckdb_daily_ohlcv.py
│
├── tests/
│   ├── test_schema.py
│   ├── test_validation.py
│   ├── test_ohlcv.py
│   └── test_partitioning.py
│
├── benchmarks/
│   └── results.md
│
├── docs/
│   ├── architecture.md
│   ├── partitioning.md
│   └── performance.md
│
└── examples/
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | Application and pipeline development |
| **Apache Spark** | Distributed data processing |
| **PySpark** | Spark Python API |
| **DuckDB** | Local analytical processing |
| **Parquet** | Columnar data storage |
| **S3 / MinIO** | Object storage |
| **PyArrow** | Parquet and Arrow integration |
| **pytest** | Testing |
| **Ruff** | Linting and code quality |
| **Docker** | Reproducible environments |
| **GitHub Actions** | Continuous integration |

---

## Quick Start

### Requirements

- Python 3.11+
- Docker
- Java runtime compatible with the selected Spark version

For local development:

```bash
git clone https://github.com/pparkitn/market-data-platform.git

cd market-data-platform

python -m venv .venv

source .venv/bin/activate
```

Install dependencies:

```bash
pip install -e ".[dev]"
```

Run the test suite:

```bash
pytest
```

---

## Run with Docker

Build the image:

```bash
docker build -t market-data-platform .
```

Start the development environment:

```bash
docker compose up
```

---

## Example

Run the DuckDB implementation:

```bash
python jobs/duckdb_daily_ohlcv.py \
    --input s3://market-data/raw/ \
    --output s3://market-data/processed/
```

Run the Spark implementation:

```bash
spark-submit \
    jobs/spark_daily_ohlcv.py \
    --input s3://market-data/raw/ \
    --output s3://market-data/processed/
```

The exact configuration and command-line options may evolve as the platform develops.

---

## Data Quality

A key design principle is:

> **Fast pipelines are useless if they produce incorrect data.**

The validation layer therefore runs independently from the transformation logic.

Example validation flow:

```text
                 Raw Data
                    │
                    ▼
             Schema Validation
                    │
                    ▼
            Record Validation
                    │
          ┌─────────┴─────────┐
          │                   │
       Valid                 Invalid
          │                   │
          ▼                   ▼
    Transformation        Error Report
          │
          ▼
      Aggregation
```

This separation allows validation rules to be tested independently from Spark and DuckDB implementations.

---

## Testing

The test suite covers both individual components and end-to-end transformations.

Examples:

```text
✓ Schema validation
✓ Missing columns
✓ Null values
✓ Invalid timestamps
✓ Duplicate records
✓ Negative volume
✓ Invalid OHLC relationships
✓ Daily OHLCV aggregation
✓ Partition handling
```

Run:

```bash
pytest
```

For coverage:

```bash
pytest --cov=src
```

---

## Engineering Principles

The project is built around several principles.

### Reproducibility

A pipeline should produce the same result when executed against the same input.

### Configuration over hard-coding

Storage locations, partitioning strategies, and processing options should be configurable.

### Measure before optimizing

Performance decisions should be supported by benchmark results.

### Minimize data movement

Large-scale processing should avoid unnecessary:

- Shuffles
- Network transfers
- Materialization
- Serialization
- Data scans

### Separate concerns

The project separates:

```text
Ingestion
    ↓
Validation
    ↓
Transformation
    ↓
Aggregation
    ↓
Storage
```

This makes individual components easier to test and replace.

---

## Design Questions

This project is intentionally more than an implementation exercise.

It investigates questions such as:

- When is DuckDB sufficient?
- When does Spark become advantageous?
- How does Parquet partitioning affect performance?
- How much does predicate pushdown reduce I/O?
- What happens when the dataset reaches billions of records?
- How should data be partitioned for time-series workloads?
- When does repartitioning improve performance versus creating additional shuffle overhead?
- When does caching help, and when does it waste memory?
- How does file size affect distributed processing?
- How does data skew affect Spark jobs?
- What are the trade-offs between local and distributed analytical engines?

The answers are documented in the benchmark and architecture documentation.

---

## Roadmap

### Phase 1: Foundation

- [x] Project structure
- [ ] Configuration system
- [ ] Local Parquet ingestion
- [ ] S3-compatible storage
- [ ] Schema validation
- [ ] Unit tests

### Phase 2: Processing

- [ ] DuckDB daily OHLCV pipeline
- [ ] Spark daily OHLCV pipeline
- [ ] Partitioning strategies
- [ ] Predicate pushdown
- [ ] Repartitioning experiments
- [ ] Data-quality reporting

### Phase 3: Scale

- [ ] 10M-row benchmark
- [ ] 100M-row benchmark
- [ ] 1B-row benchmark
- [ ] 10B+ row benchmark
- [ ] Spark cluster testing
- [ ] Performance profiling

### Phase 4: Production Engineering

- [ ] Docker environment
- [ ] CI pipeline
- [ ] Automated tests
- [ ] Configuration management
- [ ] Logging
- [ ] Metrics
- [ ] Error handling
- [ ] Documentation

### Phase 5: Analytics Layer

- [ ] Daily analytics datasets
- [ ] Query examples
- [ ] Market-data feature generation
- [ ] Backtesting integration
- [ ] Example analytical workloads

---

## Performance Results

Benchmark results will be published as the system develops.

See:

```text
benchmarks/results.md
```

For detailed analysis:

```text
docs/performance.md
```

---

## Documentation

Additional technical documentation:

- [Architecture](docs/architecture.md)
- [Partitioning](docs/partitioning.md)
- [Performance](docs/performance.md)

---

## What This Project Demonstrates

This project is designed to demonstrate practical experience with:

- Large-scale data processing
- Distributed computing
- Data engineering
- Columnar storage
- Object storage
- Data validation
- Query optimization
- Performance engineering
- Python development
- Testing
- Containerization
- CI/CD

Rather than treating Spark, DuckDB, Parquet, and S3 as isolated technologies, the project focuses on how they work together as components of a scalable data platform.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
