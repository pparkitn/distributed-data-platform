# External Services

The pipeline is notebook-only: every notebook is fully self-contained and
orchestrates a set of external services. This document explains each service,
what it does, how the API works, and how the notebooks use it.

## Service overview

```text
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│  Massive API  │     │   yfinance    │     │     Kaggle    │
│ ticker+bar    │     │ reference     │     │ executes the  │
│ market data   │     │ details       │     │ notebooks     │
└───────┬───────┘     └───────┬───────┘     └───────┬───────┘
        │                     │                     │
        ▼                     ▼                     │
┌───────────────────────────────────────┐           │
│            S3 (market-data-zw)        │◄──────────┘
│  all Parquet data lives here          │
└───────┬───────────────────────┬───────┘
        │                       │
        ▼                       ▼
   DuckDB httpfs            Spark s3a
   (04-02, 05-01, 05-02)   (02-01, 03-01, 04-01)
```

| Service | Purpose in the pipeline | Used by |
|---|---|---|
| **Massive API** | Ticker types, stock universe, minute OHLCV bars | `00-01`, `01-01` |
| **yfinance** | Per-ticker reference details (sector, market cap, ...) | `00-02` |
| **AWS S3** | All Parquet storage (raw, summary, strategy results) | every notebook |
| **Kaggle** | Execution platform (runs the notebooks in the cloud) | all |
| **DuckDB httpfs** | Local engine + direct Parquet reads from S3 | `02-01*`, `04-02`, `05-01`, `05-02` |
| **Apache Spark (s3a)** | Distributed engine + S3 reads/writes | `02-01`, `03-01`, `04-01` |
| **Interactive Brokers + IBC** | Placing orders (local trading machine; consumes the signals) | trader code, not the notebooks |
| **AutoTrade Trader** | Local execution code (scanner / executor / sell manager): reads S3 backtest + signals, keeps the S3 order ledger | trader code, not the notebooks |

\* `02-01` runs Spark (the DuckDB HTTP client is only used elsewhere).

---

## 1. Massive API (market data provider)

### What it is

The **Massive API** is a financial market-data REST API (Polygon-style) that
supplies:

- the **ticker universe**: every active stock ticker with metadata
  (name, exchange, type, currency, FIGI, ...),
- the **ticker-type taxonomy** (CSM, ETF, ETN, ...),
- **minute-level OHLCV bars** (open/high/low/close/volume/vwap/trades) for
  any ticker and date range.

This is the only source of raw market data in the platform. It is accessed
through the official Python client `massive` (PyPI), which wraps the REST
endpoints.

### Authentication

Every request carries an **API key** (`MASSIVE_API_KEY`). The key is provided
when constructing the client:

```python
from massive import RESTClient
client = RESTClient(api_key=cfg.massive_api_key, retries=10)
```

> **Important:** the key is never hard-coded in the notebooks. It is read
> from an environment variable or `config.json` by the setup cell.

### The client: `RESTClient`

`RESTClient` provides typed methods that auto-paginate and auto-retry:

| Method | Endpoint (conceptually) | Returns | Used in |
|---|---|---|---|
| `get_ticker_types()` | `GET /ticker-types` | list of `{code, name, asset_class, description}` | `00-01` |
| `list_tickers(market="stocks", active=True, sort="ticker", limit=1000)` | `GET /tickers` (cursor-paginated) | iterator of ticker objects | `00-01` |
| `list_aggs(ticker, multiplier=1, timespan="minute", from_, to, limit=50000)` | `GET /aggs/ticker/{t}/range/1/minute/{from}/{to}` | iterator of bar objects | `01-01` |

**Pagination** — `list_tickers` and `list_aggs` are lazy iterators: each
iteration fetches the next page using the response cursor (up to `limit`
records per call, 50k for bars). This means a single `for` loop downloads the
entire universe / full bar history without manual paging code.

**Rate limiting & retries** — the free tier allows roughly **5 calls per
minute**. The client is constructed with `retries=10` so it transparently
retries on HTTP 429 (Too Many Requests). On top of that, `01-01` sleeps
`12` seconds between tickers (≈5 calls/min) and `00-02` sleeps 1 second
between yfinance calls.

### Minute bar fields (used by `01-01`)

| API field | Notebook column | Type | Meaning |
|---|---|---|---|
| `a.timestamp` | `date` | int64 | epoch **ms** (minute open time) |
| `a.open` / `a.high` / `a.low` / `a.close` | `open/high/low/close` | double | OHLC prices |
| `a.volume` | `volume` | double | traded volume |
| `a.vwap` | `vwap` | double | volume-weighted average price |
| `a.transactions` | `trades` | int64 | number of trades |

### Limits & costs

- Free tier: ~5 calls/minute (hence the sleeps in the notebooks).
- One call returns up to 50,000 minute bars (≈ 130 trading days per call).
- Paid tiers raise the rate limit; the notebooks do not depend on a tier.

---

## 2. AWS S3 (object storage)

### What it is

**S3** is Amazon's object storage service: files ("objects") are stored under
keys in a bucket, organized as `prefix/path/file.parquet`. There are no
folders per se — a "folder" is just a key prefix. This is the single source
of truth for all data in the platform.

### The bucket

```
Bucket: market-data-zw        (region: us-east-1)
Root:   parquet_data/
```

Everything the notebooks read or write lives under `parquet_data/` (see the
README's "S3 Data Structure" section for the full layout). Config keys:

| Setting | Env var | Default |
|---|---|---|
| Access key | `AWS_ACCESS_KEY_ID` | — |
| Secret key | `AWS_SECRET_ACCESS_KEY` | — |
| Region | `AWS_REGION` | `us-east-1` |
| Bucket | `S3_BUCKET` | `market-data-zw` |

### How the notebooks access S3

There are three independent access paths; each notebook picks the one that
fits its engine:

**1. boto3 (pandas)** — upload/download single Parquet objects:

```python
s3 = s3_client(cfg)                       # boto3 client with adaptive retries
s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
obj = s3.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj["Body"].read()))
```

`list_s3_keys()` uses the S3 **list-objects paginator** (1000 keys per page)
to enumerate existing objects — the notebooks derive "which tickers already
exist" purely from object-key filenames, avoiding expensive glob scans.

**2. DuckDB httpfs** — SQL over Parquet directly from S3:

```python
con = duckdb_s3_connect(cfg)              # INSTALL httpfs; LOAD httpfs; SET s3_* credentials
df = con.execute("SELECT * FROM read_parquet('s3://market-data-zw/parquet_data/...')").df()
```

**3. Spark s3a** — distributed reads/writes through Hadoop:

```python
spark = spark_session(cfg)                # hadoop-aws 3.4.1 jar + fs.s3a.* settings
df = spark.read.schema(MINUTE_SCHEMA).parquet("s3a://market-data-zw/parquet_data/...")
df.write.mode("overwrite").parquet("s3a://market-data-zw/parquet_data/...")
```

### Why object storage

- **Separation of compute and storage** — Kaggle provides ephemeral compute;
  S3 keeps the ~24 GB of data permanently and cheaply.
- **Parquet-native** — columnar files, predicate pushdown, and partition
  pruning work across all three access paths.
- **Cheap reads** — the same data is consumed by Spark, DuckDB, and pandas
  without copying.

### Costs (ballpark)

- Storage: ~24 GB is a few dollars/month.
- Requests: every `list_objects_v2` page costs one GET request; the
  ingestion notebooks' per-ticker `put_object` calls are the main driver.
  LIST requests are the ones to watch at scale (billing is per 1k requests).

---

## 3. Kaggle (execution platform)

### What it is

**Kaggle** is Google's data-science platform. **Kaggle Notebooks** ("kernels")
are cloud-hosted Jupyter notebooks that run in a managed VM. The entire
pipeline in this repo is designed to run as Kaggle notebooks, pushed from
this repository.

### How execution works

- Each notebook runs in an isolated Linux VM.
- **CPU session**: up to **12 hours** per run (4 GPU / 12h CPU on the free
  tier), with ~**30 GB RAM** and a 5 GB disk working directory
  (`/kaggle/working`).
- **Internet access is enabled** (`enable_internet: true`), which is required
  for `pip install`, S3 access, and the Massive/yfinance APIs.
- **PySpark is pre-installed** — Spark jobs (02-01, 03-01, 04-01) work
  out of the box.
- Datasets can be mounted read-only under `/kaggle/input/...`. This repo's
  notebooks read everything from S3 by default, and **optionally** use a
  Kaggle dataset mirror (`02-02`) for fast, free reads — see below.
- Notebooks can be interactive (widgets/Plotly in 05-01 / 05-02) or run
  headless via `nbconvert`.

### The Kaggle dataset mirror (`02-02-s3-to-kaggle-dataset`)

Reading every stage's input from S3 works but costs money (S3 GET requests)
and network time on each run. The **`02-02` notebook** solves both by
mirroring the small-but-heavily-read S3 data into a **private Kaggle
dataset**, which Kaggle then mounts on the local disk of every notebook that
declares it — reads become local and free.

**What it mirrors** (skipping the 24 GB minute bars, which exceed Kaggle's
dataset size limit):

```text
parquet_data/summary/    # minute_summary, daily_volume, tickers  (big win)
parquet_data/types/
parquet_data/strategies/ # correlation pair outputs
parquet_data/backtest/   # trade results
parquet_data/analysis/   # backtest metrics
```

**How it works** — `02-02` downloads those S3 prefixes with boto3 to
`/tmp/upload/s3_data`, writes a `dataset-metadata.json`, and runs
`kaggle datasets version -p /tmp/upload --dir-mode zip -m "Updated"` to
create/update the private dataset `dsptlp/market-data-s3-dataset`.

**How consumers use it** — 03-01, 04-01, 04-02, 05-01 and 05-02 contain a
`resolve()` helper that prefers the mounted mirror and falls back to S3
transparently:

```python
MINUTE_SUMMARY = resolve(cfg.minute_summary_prefix, "data.parquet")
# -> "/kaggle/input/market-data-s3-dataset/s3_data/parquet_data/summary/minute_summary/data.parquet"
#    when mounted, else "s3a://market-data-zw/parquet_data/summary/minute_summary/data.parquet"
```

To activate the mirror on a kernel, attach the dataset in the kernel editor
(**Add-ons → Datasets → `dsptlp/market-data-s3-dataset`**). Without it, the
same notebook still runs against S3 — the mirror is purely an optimization.

**Refresh cadence** — re-run `02-02` (one session, minutes) after any of
02-01 / 03-01 / 04-01 / 04-02 produce new output, then re-run the consumer
stages. Note: a private dataset update takes several minutes to process on
Kaggle's side before the new version is mountable.

**Trade-offs**

| | S3 direct | Kaggle mirror |
|---|---|---|
| Read speed | network-bound | local disk (fast) |
| S3 cost | GET/list requests per run | none for reads |
| Freshness | always current | one `02-02` run behind |
| Mount needed | no | attach dataset to kernel |

### Credentials on Kaggle

Secrets are injected as **environment variables**: in the notebook editor,
open **Add-ons → Secrets** (or Settings → Secrets) and create
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`,
`MASSIVE_API_KEY`, etc. The setup cell's `load_config()` reads them with
env-var priority, falling back to `config.json` if you upload one.

### Pushing notebooks from this repo

```bash
pip install kaggle
export KAGGLE_USERNAME=dsptlp
export KAGGLE_API_TOKEN=...        # or run `kaggle configure` / ~/.kaggle/kaggle.json

python3 push_kernels.py --dry-run            # show the plan
python3 push_kernels.py --push               # push all 11 notebooks
python3 push_kernels.py --push 03-01-correlation.ipynb   # push one
```

`push_kernels.py` builds a `kernel-metadata.json` for each notebook and calls
`kaggle kernels push`:

```json
{
  "id": "dsptlp/autotrade-03-01-correlation",
  "title": "Market Data 03-01 - Correlation",
  "code_file": "03-01-correlation.ipynb",
  "kernel_type": "notebook",
  "enable_internet": true,
  "dataset_sources": []
}
```

### Continuous runs: `batch_runner.py`

For workloads like the 10-hour correlation loop, several instances of a
notebook can run in parallel:

```bash
python3 batch_runner.py --once --count 4 --kernels 03-01          # one burst of 4
python3 batch_runner.py --daemon --count 4 --kernels 03-01 --interval 60  # refill forever
```

Notes learned in production:

- **5 concurrent batch-CPU sessions** is the account cap; pushes beyond it
  fail with "Maximum batch CPU session count of 5 reached" and the runner
  backs off (30 s doubling to 300 s).
- Kaggle's API only exposes the **latest session per kernel**, so refills are
  batch-based: when the newest run finishes, the whole batch is re-pushed.
- Run with `python3 -u` when redirecting to a log file (stdout is
  block-buffered otherwise).

### Session cost & quotas

- Each notebook run consumes a session; free-tier users have a weekly quota.
  Prefer `--count` bursts over daemon mode when you only need a fixed number
  of runs.
- 03-01 randomizes its parameters each run, so re-running produces fresh
  (and varied) pair universes for the backtest stage.

### Strategy search: keep N instances running in a loop

The correlation notebook (`03-01`) **randomizes its search parameters on
every run** — `lag_days` (1-20), `lookback_days` (1-30), `persistence_window`
(5-30) — and samples a fresh random set of 1,000 tickers. No single run can
cover the whole parameter space, so a common pattern is to keep **several
instances running continuously in a loop**, accumulating pairs until a
specific lead-lag pattern shows up:

```bash
# Keep 3 instances of 03-01 running forever:
python3 -u batch_runner.py --daemon --count 3 --kernels 03-01 --interval 60
```

What happens:

1. **Phase 1 (warm-up)** — pushes 3 instances of `03-01` in parallel.
2. **Phase 2 (refill loop)** — every 60 s it polls the kernel's latest
   session status:
   - `COMPLETE` → pushes a fresh batch of **3** again;
   - `ERROR` → pushes **1** (so a broken notebook can't burn quota);
   - still `RUNNING`/`QUEUED` → waits.
3. Each run writes its own output folder
   `strategies/correlation/<run_timestamp>/data.parquet` with its randomized
   parameters recorded as columns (`lag_days`, `lookback_days`,
   `persistence_window`), so the accumulated S3 data becomes a searchable
   log of every parameter combination tried.

To keep it running in the background and log the activity:

```bash
nohup python3 -u batch_runner.py --daemon --count 3 --kernels 03-01 --interval 60 \
    > batch_runner.log 2>&1 &
tail -f batch_runner.log
```

To stop: `Ctrl-C` (or `kill` the process); the already-running Kaggle
sessions finish on their own.

Adjust the search density:

```bash
python3 batch_runner.py --daemon --count 3 --kernels 03-01 --interval 30   # tighter loop
python3 batch_runner.py --daemon --count 5 --kernels 03-01 --interval 60   # max parallelism (5-session cap)
python3 batch_runner.py --once   --count 3 --kernels 03-01                 # just one burst, then exit
```

When you find an interesting pattern (a run whose pair list looks promising
in `05-01`/`05-02`), stop the daemon, then run the backtest stage — `04-01`
reads **all** accumulated correlation runs, so the search results feed
straight into the backtest.

> **Quota note:** daemon mode runs until stopped and each refill consumes
> sessions, so watch your weekly session quota; `--once --count N` is the
> cheaper option when a fixed number of runs is enough.

### Running the entire pipeline end-to-end

The pipeline is a linear chain: each notebook consumes S3 objects written by
the previous one. Here is the complete runbook.

#### Step 0 — One-time setup

1. **Kaggle CLI credentials** (local machine):

   ```bash
   pip install kaggle
   export KAGGLE_USERNAME=dsptlp
   export KAGGLE_API_TOKEN=...     # or `kaggle configure` -> ~/.kaggle/kaggle.json
   ```

2. **Push all 11 notebooks** (creates one private kernel per notebook):

   ```bash
   python3 push_kernels.py --push
   ```

3. **Add secrets to each kernel.** Kaggle secrets are per-kernel: open each
   kernel in the editor and add them under **Add-ons → Secrets**:

   - every notebook: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
   - `00-01-ticker-load`, `01-01-historical-load`: also `MASSIVE_API_KEY`

   (Alternative: upload a `config.json` containing the same keys — the setup
   cells read it as a fallback. Env vars are simpler.)

#### Step 1 — Ingest the universe (00-01, 00-02)

| Notebook | What it does | Expected runtime |
|---|---|---|
| `00-01-ticker-load` | Fetches ticker types + ~14k tickers from Massive | minutes |
| `00-02-ticker-universe` | yfinance details for every ticker (~1 s each) | **hours** (13k+ tickers) |

Run `00-01` once. Then run `00-02`: it is **incremental** — it lists the
object keys already under `summary/ticker_yahoo_details/` and only fetches
the difference. If the 12 h session ends before it finishes, just re-run it
(ideally re-push with `batch_runner.py --once --count 1 --kernels 00-02`); it
resumes where it left off.

#### Step 2 — Download minute bars (01-01, 01-02)

| Notebook | What it does | Expected runtime |
|---|---|---|
| `01-01-historical-load` | Minute bars per ticker (~12 s sleep per ticker) | **many sessions** (~14k tickers) |
| `01-02-merge-minute-data` | Merge staging into `minute_data_final`, clear staging | minutes |

`01-01` is the longest stage: at ~12 s per ticker (free-tier rate limit) the
full universe spans multiple 12 h sessions. It is fully **resumable** — it
derives the already-downloaded set from the `minute_data_staging/` object
keys and continues with the remainder. Re-run (or re-push) it until the
"Missing" count reaches 0, then run `01-02` once to merge everything.

> Note: do **not** run multiple instances of `01-01` in parallel — instances
> race on the same missing-ticker set and waste API quota. Sequential
> re-runs are the right pattern here (unlike 03-01, where parallel runs are
> intentional).

#### Step 3 — Build the daily summaries (02-01)

Run `02-01-etl-summary` (Spark). It reads the ~1 B rows of
`minute_data_final/`, writes `summary/minute_summary` and
`summary/daily_volume`, and (optionally) the daily-OHLCV table. This is the
first stage that needs the full minute dataset, so do not start it until
`01-02` reports "Merged 14,0xx tickers". Typical runtime: tens of minutes to
~1 h on a Kaggle CPU session.

Verify with:

```bash
aws s3 ls s3://market-data-zw/parquet_data/summary/minute_summary/
aws s3 ls s3://market-data-zw/parquet_data/summary/daily_volume/
```

#### Step 3b — Mirror to Kaggle (02-02, optional but recommended)

Once the summaries exist, run `02-02-s3-to-kaggle-dataset` to copy the
summary/strategy/backtest data into the private Kaggle dataset. It needs the
`KAGGLE_USERNAME` + `KAGGLE_API_TOKEN` env vars (in addition to AWS). Then
attach `dsptlp/market-data-s3-dataset` to the 03-01 / 04-01 / 04-02 /
05-01 / 05-02 kernels — they will read from local disk instead of S3.
Re-run 02-02 after each new batch of 03-01 / 04-01 / 04-02 output to refresh
it. See [The Kaggle dataset mirror](#the-kaggle-dataset-mirror-02-02-s3-to-kaggle-dataset).

#### Step 4 — Correlation strategy (03-01)

Run `03-01-correlation` (Spark). Each run randomizes its parameters
(`lag_days`, `lookback_days`, `persistence_window`) and samples 1,000
tickers, so consecutive runs produce different pair universes. Each run
writes one folder under `strategies/correlation/<run>/`.

To accumulate a large pair universe across a full 12 h session, push several
instances in parallel — this is the intended use of the batch runner:

```bash
python3 batch_runner.py --once --count 4 --kernels 03-01   # 4 overlapping runs
python3 batch_runner.py --daemon --count 4 --kernels 03-01 --interval 60  # keep refilling
```

To keep 3 instances running in a loop indefinitely while **searching for a
specific correlation pattern** (every run randomizes its parameters, so each
run explores a different part of the search space), see
[Strategy search: keep N instances running in a loop](#strategy-search-keep-n-instances-running-in-a-loop).

#### Step 5 — Backtest (04-01) and analyze (04-02)

| Notebook | What it does | Expected runtime |
|---|---|---|
| `04-01-backtest` | Backtests every deduplicated correlation pair (Spark) | **hours** (hundreds of pairs × thousands of trades) |
| `04-02-analyze-backtest` | Per-pair metrics + robust ranking (DuckDB) | minutes |

Run `04-01` once all correlation runs you care about are in S3 (it reads the
`strategies/correlation/*/*` glob, so more runs = more pairs). Then run
`04-02`, which produces `analysis/backtest_metrics/<run>/data.parquet` — the
input for the interactive notebooks.

#### Step 6 — Explore interactively (05-01, 05-02)

`05-01-pair-explorer` and `05-02-advanced-pair-analysis` are interactive
widget notebooks. On Kaggle they run in an interactive session (open the
kernel and run cells manually; widgets work in the browser). You can also
download the two notebooks and run them locally against S3 — they need only
AWS credentials.

#### The complete runbook at a glance

```text
push all 11 notebooks + add secrets (once)
  │
  ▼
00-01 ──► 00-02 (re-run until done) ──► 01-01 (re-run until done)
  ──► 01-02 ──► 02-01 ──► 02-02 (mirror -> Kaggle dataset, attach to consumers)
  ──► 03-01 x N (batch_runner) ──► 04-01 ──► 04-02 ──► 05-01 / 05-02 (interactive)
```

Refresh `02-02` after each batch of 03-01 / 04-01 / 04-02 output so the
consumer stages keep reading the fast local mirror.

#### Monitoring & verification

- **Kernel status**: `kaggle kernels status dsptlp/autotrade-03-01-correlation`
  or the Kaggle web UI (Kernels → your kernels).
- **Output**: every stage prints an "Uploaded -> s3://..." line when done;
  check it in the kernel log before moving to the next stage.
- **S3 verification**:

  ```bash
  aws s3 ls s3://market-data-zw/parquet_data/strategies/correlation/   # new run folders
  aws s3 ls s3://market-data-zw/parquet_data/backtest/                 # new trade files
  ```

#### Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `MASSIVE_API_KEY not set` / empty config | Secrets not added to that kernel — re-check Add-ons → Secrets |
| 12 h session ends mid-stage | Normal for 00-02 / 01-01 — both are resumable; re-push and re-run |
| `Maximum batch CPU session count of 5 reached` | Account cap — `batch_runner.py` backs off automatically |
| Weekly quota exhausted | Wait for reset, or reduce `--count` / avoid daemon mode |
| Spark out-of-memory | Reduce `SPARK_EXECUTOR_MEMORY` / `SPARK_DRIVER_MEMORY` env vars (default 24 g fits the 30 GB session) |
| pip install fails | Enable internet in kernel metadata (`enable_internet: true` is already set by `push_kernels.py`) |

---

## 4. yfinance (reference data enrichment)

### What it is

**yfinance** is an open-source Python library that scrapes **Yahoo Finance**
data. The platform uses it only in `00-02-ticker-universe` to enrich each
ticker with reference details that Massive does not provide: sector,
industry, market cap, employees, 52-week ranges, etc.

### How it works

```python
import yfinance as yf
t = yf.Ticker("AAPL")
fi = t.fast_info     # fast, stable subset (price, market cap, ...)
info = t.info        # large, brittle full profile
```

The notebook flattens both into one wide dict per ticker, prefixed
`fast_*` / `info_*`, and writes one Parquet per ticker to
`summary/ticker_yahoo_details/<TICKER>.parquet`.

### Limits & robustness

- **Rate limited** — the notebook sleeps 1 second per ticker; Yahoo can
  throttle harder, and the fetch is wrapped in try/except so a single
  missing field never fails the ticker.
- **Delisted tickers 404** — benign; the notebook records an empty payload
  and moves on.
- It is a **free, unofficial** source: fine for reference data, not for
  production market data (which comes from Massive).

---

## 5. DuckDB httpfs (local engine + S3 bridge)

### What it is

**DuckDB** is an embedded analytical database (single-node, vectorized).
Its **httpfs extension** gives it native S3 support, so `read_parquet` /
`write_parquet` work directly on `s3://` URIs with credentials set per
connection:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_access_key_id='...';
SET s3_secret_access_key='...';
SET s3_region='us-east-1';
SELECT * FROM read_parquet('s3://market-data-zw/parquet_data/backtest/*/*', union_by_name = true);
```

The notebooks wrap this in `duckdb_s3_connect(cfg)`. `union_by_name=true`
handles the heterogeneous schemas produced by different runs (e.g. varying
`lag_days` column names in correlation outputs).

Used by: `04-02-analyze-backtest`, `05-01-pair-explorer`,
`05-02-advanced-pair-analysis`.

---

## 6. Apache Spark (s3a bridge)

### What it is

**Apache Spark** is the distributed processing engine. PySpark ships
pre-installed on Kaggle. To talk to S3 it needs the `hadoop-aws` JAR and
`fs.s3a.*` Hadoop config; the notebooks' `spark_session(cfg)` helper sets:

```
spark.jars.packages = org.apache.hadoop:hadoop-aws:3.4.1
fs.s3a.access.key / fs.s3a.secret.key / fs.s3a.endpoint
spark.local.dir, spark.sql.warehouse.dir   -> /tmp/spark (Kaggle scratch)
```

Spark uses **`s3a://`** URIs (Hadoop's S3 filesystem), distinct from DuckDB's
`s3://`. Write behaviour is configured with the directory committer to avoid
temporary `_temporary/` object litter.

Used by: `02-01-etl-summary`, `03-01-correlation`, `04-01-backtest`.

---

## 7. Interactive Brokers (order execution)

The notebooks stop at *signals*: they find correlation pairs and backtest
them. Actually placing orders is done by a separate, local system — the
AutoTrader trader code — which connects to **Interactive Brokers** through
the **IB Gateway** running with **IBC** (the "ibx tool"). This section
explains how that works.

> The IB side runs **locally on the trading machine** (Docker/desktop), not
> on Kaggle. It consumes the same S3 signals the notebooks produce.

### What it is

| Item | Value |
|---|---|
| Broker | Interactive Brokers (IBKR) |
| Gateway | IB Gateway 10.50 (standalone Linux build) |
| Automation tool | **IBC** (Interactive Brokers Controller) 3.24.2 — the "ibx tool" |
| Location | `/home/pparkitn/github/docker/AutoTrader/ibc/` |
| Trading mode | **paper** (paper-trading account; API port **4002**) |
| API client library | `ib_insync` (Python) |
| Order types | Market / Limit |

### Why IBC is needed

IB Gateway has **no command-line login** — its launcher opens a GUI and waits
for a human to type credentials. **IBC** is a small Java program that
automates that GUI: it watches the login window, fills in the username and
password from `config.ini`, clicks **Log In**, and dismisses startup dialogs
(warnings, pending tasks, compliance notices). This lets the gateway run
unattended so the Python trader can connect to its API.

### How it works (the launch chain)

```text
start.sh                 your launcher (with auto-restart loop)
  └─ gatewaystart.sh     IBC's official launcher (env vars, version 1050)
       └─ scripts/displaybannerandlaunch.sh
            └─ scripts/ibcstart.sh            builds classpath from ~/ibgateway/jars + IBC.jar
                 └─ java ibcalpha.ibc.IbcGateway config.ini paper
                      └─ IB Gateway 10.50 starts, IBC auto-logs-in
                           └─ API listening on 127.0.0.1:4002  (paper)
```

Key files:

| Path | Purpose |
|---|---|
| `ibc/IBC.jar` | The IBC program itself |
| `ibc/config.ini` | IBC config — **contains your IBKR credentials** (mode `600`) |
| `ibc/gatewaystart.sh` | IBC's launcher; sets `TWS_MAJOR_VRSN=1050`, `TRADING_MODE=paper`, `LOG_PATH` |
| `ibc/start.sh` | **Your launcher** — starts the gateway and auto-restarts it on crash |
| `ibc/stop.sh` | **Your stopper** — stops the gateway and the restart loop |
| `ibc/logs/` | IBC diagnostics (rotates daily) + `startup.log` for daemon mode |
| `~/Jts` | Gateway settings (`jts.ini`: `tradingMode=p`, `ApiOnly=true`, port 4002) |
| `~/ibgateway` | The Gateway 10.50 installation (bundled JVM at `~/ibgateway/jre`) |

### Setting up an Interactive Brokers account (from scratch)

Step-by-step from a brand-new account to a working paper-trading gateway:

**1. Create the IBKR account**

1. Go to [interactivebrokers.com](https://www.interactivebrokers.com) and
   click **Open Account** (an "Individual" or "Joint" account is enough).
2. Complete the application (personal details, financial profile, risk
   questionnaire). Approval typically takes 1–3 business days.
3. Fund the account (some paper-trading features are gated until the
   account is active/funded).

**2. Enable the paper trading account**

1. Log in to the **Client Portal** (login.interactivebrokers.com) with your
   username/password.
2. Go to **Settings → Account Settings → Trading Configuration → Paper
   Trading**.
3. Click **Enable** and pick a starting balance: **$100,000 / $1,000,000 /
   $10,000,000** virtual dollars.
4. A **separate paper account** is created — its account number ends in
   **`D`** (e.g. `DU1234567`). Keep it enabled; you will log into it in
   "paper" mode.

> Without this step there is nothing to trade against: the paper mode in
> TWS/Gateway logs into this virtual account, not your real one.

**3. Install the standalone IB Gateway (Linux)**

IBC can only automate the **standalone (non-self-updating)** build:

```bash
# From IBKR's download page (Linux → "IB Gateway", not TWS), e.g. 10.50:
# https://www.interactivebrokers.com/en/trading/ibgateway-stable.php
wget <ibgateway-<version>-standalone-linux-x64.sh>
chmod +x ibgateway-<version>-standalone-linux-x64.sh
./ibgateway-<version>-standalone-linux-x64.sh   # installs to ~/ibgateway
```

Verify: `~/ibgateway/ibgateway` exists and `~/ibgateway/jre/bin/java -version`
works (bundled JVM, no system Java needed).

**4. Install IBC (the ibx tool)**

```bash
# Release zip from https://github.com/IbcAlpha/IBC/releases (e.g. 3.24.2)
unzip IBC-linux-3.24.2.zip -d ~/ibc        # -> ~/ibc/IBC.jar, ~/ibc/scripts, ...
chmod +x ~/ibc/scripts/*.sh                # required by gatewaystart.sh
# Download the matching gatewaystart.sh / twsstart.sh into ~/ibc
```

**5. Configure `config.ini`**

Create `~/ibc/config.ini` (keep the directory `700` / file `600` — it holds
your password):

```ini
[IBGateway]
FIX=no
IbLoginId=<your IBKR username>
IbPassword=<your IBKR password>
TradingMode=paper
ApiOnly=yes
AcceptIncomingConnectionAction=accept
TrustedTwsApiClientIPs=127.0.0.1
LoginDialogDisplayTimeout=60
SuppressInfoMessages=yes
```

**6. Launch and verify**

```bash
~/ibc/start.sh -daemon          # auto-login + auto-restart on crash
pgrep -af ibcalpha.ibc.IbcGateway   # process up?
ss -tlnp | grep 4002                # paper API listening?
tail -f ~/ibc/logs/ibc-3.24.2_GATEWAY-1050_$(date +%A).txt   # "Login has completed"
```

**7. First Python connectivity test**

```bash
pip install ib_insync nest-asyncio
python - <<'EOF'
from ib_insync import IB
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)
print(ib.accountSummary()[0].__dict__ if ib.accountSummary() else 'no summary')
ib.disconnect()
EOF
```

If it prints account values (e.g. `NetLiquidation`), the paper account is
ready for the trader (section 8).

### The paper trading account (testing)

**What it is** — a virtual account (ends in `D`) funded with fake money
(default $1,000,000) that trades against **real market prices**:

- Orders execute like live orders (fills at real prices; no market impact).
- Commissions, margin and risk checks are simulated.
- It is the **default target of this stack**: the trader's `paper` preset
  connects to port 4002 and places orders there — the exact same code path
  as live, minus the money.

**How to use it for testing (recommended flow)**

1. Start the gateway (`~/ibc/start.sh -daemon`) — it logs into paper mode.
2. Validate the stack without touching the market:
   `python -m autotrade_lib.executor --once --dry-run` (simulates orders,
   writes nothing).
3. Run the real loop against paper:
   `python -m autotrade_lib.scanner --continuous` +
   `python -m autotrade_lib.executor --continuous` +
   `python -m autotrade_lib.sell_manager --daily`.
4. Watch results in S3 (`logs/trade_log/` ledger) and the paper account in
   Client Portal / TWS.

**Managing the paper account**

- **Reset** — Client Portal → **Settings → Account Settings → Paper
  Trading → Reset**: restores the chosen starting balance and closes all
  virtual positions. Useful to wipe a bad test.
- **Balance choices** — $100k / $1M / $10M virtual starting equity.
- IBKR may reset paper accounts from time to time; treat paper P&L as a
  harness, not a track record.

**Known limitations**

- **Delayed data without subscriptions**: real-time quotes need a market
  data subscription (or the paper account inherits delayed data), which
  affects the scanner's live price checks.
- **No market impact**: fills ignore your order size, so paper results are
  optimistic on illiquid names.
- **Maintenance window**: TWS/Gateway restarts daily (roughly midnight–2 am
  ET); IBC's auto-restart handles it.
- **Paper ≠ live**: order routing, short-locate rules and some market
  mechanics differ; passing paper does not guarantee live performance.
- **Switching to live** later means: `TradingMode=live` in `config.ini`,
  `TRADING_MODE=live` in `gatewaystart.sh`, `tradingMode=l` in `~/Jts/jts.ini`,
  port **4001** — and real funds. Do not flip this casually.

### Placing orders (Python side)

The trader connects to the gateway with `ib_insync` — a different API client
ID per component so scanner / executor / sell-manager can run concurrently:

```python
from ib_insync import IB, Stock, MarketOrder

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=200)   # e.g. executor client base
contract = Stock("MSFT", "SMART", "USD")
ib.qualifyContracts(contract)
trade = ib.placeOrder(contract, MarketOrder("BUY", 100))
ib.sleep(1)
print(trade.orderStatus.status)
```

- host `127.0.0.1`, port **4002** (paper; live gateway would be 4001)
- client-id bases: scanner `100`, executor `200`, sell-manager `300`
- orders: `MarketOrder` / `LimitOrder`, placed after `qualifyContracts`,
  tracked via `trade.orderStatus` and fill events

### The auto-restart code (`start.sh` / `stop.sh`)

The gateway must never be down during market hours, so `start.sh` wraps the
gateway in a **restart loop**: as long as a `.running` flag file exists, any
exit (crash, network failure, login failure, nightly re-login) is followed by
a 5-second wait and a fresh start.

```bash
#!/bin/bash
# start.sh -- launcher with auto-restart loop
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATEWAY_SCRIPT="$SCRIPT_DIR/gatewaystart.sh"
RUN_FILE="$SCRIPT_DIR/.running"

cleanup() {
    rm -f "$RUN_FILE"
}
trap cleanup EXIT INT TERM

if [[ "$1" == "-daemon" ]]; then
    nohup "$0" -inline >> "$SCRIPT_DIR/logs/startup.log" 2>&1 &
    echo "IBC/Gateway launching detached (pid $!)."
    echo "Logs: $SCRIPT_DIR/logs/"
    exit 0
fi

touch "$RUN_FILE"
echo "Starting IBC/Gateway with auto-restart (stop with ./stop.sh)"

while [[ -f "$RUN_FILE" ]]; do
    "$GATEWAY_SCRIPT" -inline
    EXIT_CODE=$?
    if [[ -f "$RUN_FILE" ]]; then
        echo "IBC/Gateway exited with code $EXIT_CODE. Restarting in 5s..."
        sleep 5
    fi
done

echo "Stop requested. Exiting."
```

How it works:

1. `touch .running` creates the flag that arms the loop.
2. `while [[ -f "$RUN_FILE" ]]` re-launches `gatewaystart.sh -inline`
   whenever it exits.
3. **`trap cleanup EXIT INT TERM`** removes the flag on Ctrl-C / SIGTERM /
   normal exit, so the loop knows to stop.
4. `-daemon` re-executes itself under `nohup`, so a backgrounded gateway
   also auto-restarts.

`stop.sh` performs the shutdown dance:

```bash
#!/bin/bash
# stop.sh -- stop the gateway and the restart loop
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_FILE="$SCRIPT_DIR/.running"
PATTERN="ibcalpha.ibc.IbcGateway"

rm -f "$RUN_FILE"                 # 1. disarm the restart loop FIRST

if [[ "$1" == "--force" ]]; then  # hard kill path (SIGKILL)
    pkill -9 -f "$PATTERN" || true
    exit 0
fi

pkill -TERM -f "$PATTERN" || true # 2. graceful SIGTERM to IBC/Gateway
for i in $(seq 15); do            # 3. wait up to 15s, then escalate
    if ! pgrep -f "$PATTERN" >/dev/null; then
        echo "Stopped gracefully."
        exit 0
    fi
    sleep 1
done
pkill -9 -f "$PATTERN" || true    # 4. SIGKILL fallback
```

The critical ordering: **the flag is removed before the process is killed**,
otherwise the restart loop would bring the gateway straight back up.

Behavior summary:

| Scenario | Result |
|---|---|
| Gateway crashes | restart loop re-launches it in ~5s |
| Network / login failure | restart loop re-launches it in ~5s |
| Nightly / weekly re-authentication | IBC re-logs-in; loop restarts if the process exits |
| `./stop.sh` (or `--force`) | flag removed → process killed → loop exits |
| Ctrl+C in foreground | `trap` removes flag → loop exits |

### Logs & troubleshooting

| Log | Path |
|---|---|
| IBC diagnostics (rotates daily) | `ibc/logs/ibc-3.24.2_GATEWAY-1050_<Day>.txt` |
| daemon startup | `ibc/logs/startup.log` |
| Gateway's own launcher | `~/Jts/launcher.log` |

```bash
# Status
pgrep -af ibcalpha.ibc.IbcGateway
ss -tlnp | grep 4002

# Follow logs
tail -f ibc/logs/ibc-3.24.2_GATEWAY-1050_$(date +%A).txt
tail -f ibc/logs/startup.log
```

A healthy IBC log ends with the auto-login sequence:
`Setting Trading mode = paper → Setting user name → Setting password →
Login has completed → Click button: I understand and accept`.

### Caveats

1. **Credentials in plain text** — `config.ini` holds the IBKR password
   (directory mode `700`, file mode `600`). Never commit it.
2. **Requires an X display** — IBC is GUI automation; it needs a desktop
   session (`DISPLAY=:0`) or a virtual framebuffer (Xvfb). No headless mode.
3. **IBC is being retired (September 2026)** — it keeps working, but
   consider an alternative (e.g. `IBAutoLogin`) for long-term use.
4. **2FA** — if IBKR Mobile / security-card 2FA is enabled, a manual
   acknowledgement is still needed at login.
5. **Paper vs live** — paper uses port 4002; live would require
   `TradingMode=live` in `config.ini`, `TRADING_MODE=live` in
   `gatewaystart.sh`, `tradingMode=l` in `~/Jts/jts.ini`, and port **4001**.
   Switching to live trades real funds.

---

## 8. AutoTrade Trader (the execution code)

The **Trader** directory contains the Python code that turns the
notebook-pipeline results into real (paper) orders. It is a separate local
system — **not a notebook pipeline** — built as three independent daemons
(`scanner`, `executor`, `sell_manager`) plus a manual TUI, sharing one
library package.

```text
/home/pparkitn/github/docker/AutoTrader/
├── Trader/
│   └── autotrade_lib/
│       └── autotrade_lib/        # the shared library (the .py code)
│           ├── scanner.py        # 1. find candidate pairs, fire signals
│           ├── executor.py       # 2. re-check + place BUY orders
│           ├── sell_manager.py   # 3. sell when holding period is up
│           ├── tui.py / tui_shared.py   # manual buy/sell/portfolio UI
│           ├── ib_connection.py  # IB connection manager (client IDs, retries)
│           ├── orders.py         # order placement, fill confirmation
│           ├── account.py        # account snapshot, positions, exposure
│           ├── risk.py           # risk limits + business-day math
│           ├── market.py         # market regime gate (SPY, QQQ, ...)
│           ├── models.py         # SignalRecord / OrderRecord / FillRecord
│           ├── s3_utils.py       # S3 parquet read/write + JSONL logging
│           ├── credentials.py    # AWS creds from config.json
│           └── config.py         # presets (paper / live, thresholds)
├── config.json                   # AWS credentials (read by the library)
└── run.sh / run-sell.sh          # legacy papermill loop wrappers
```

### The three components

```text
S3 backtest/ (from notebook 04-01)
        │
        ▼
┌─────────────┐  signals/{tactic}/{run_ts}_{uuid}/   ┌─────────────┐
│  SCANNER    │ ────────────────►──────────────────── │  EXECUTOR   │
│ (every 5m)  │  run_meta.parquet + signals.parquet   │ (every 60s) │
└─────────────┘                                      └──────┬──────┘
        │                                                  │ BUY orders
        ▼                                                  ▼
                          trade_log/{YYYYMMDD}/{HHMMSS}/
                        run_meta.parquet | orders.parquet | fills.parquet
        ▲                                                  │
        └───────────────── SELL rows ◄─────────────────────┘
            ┌─────────────┐
            │ SELL MANAGER│  reads the ledger, sells when
            │ (9:30 ET)   │  days_held >= holding_days
            └─────────────┘
```

**1. Scanner (`scanner.py`)** — decides *what to buy*:

1. Loads candidate pairs from S3 backtest results (see below).
2. Excludes leaders already traded today (from today's `orders.parquet`).
3. Checks the **market gate** (`market.py`): if SPY fell more than
   `market_block_threshold`, all pairs are logged as `market_block` and no
   signals fire (fails closed).
4. Checks **risk limits** (`risk.py`): max positions, cash per trade, total
   exposure.
5. For each pair, pulls the leader's 2-day daily bars from IB
   (`evaluate_leader`) and computes its day-over-day % change.
6. If `pct_change >= threshold_pct` → **signal fired**, with a per-signal
   **TTL** (time-to-live, 5–60 min) computed by `calculate_ttl_minutes`
   (correlation, holding days, volatility, tactic).
7. Writes everything to S3:
   `signals/{tactic}/{run_ts}_{uuid}/run_meta.parquet` + `signals.parquet`
   (every pair, fired or skipped, with `skip_reason`).

**2. Executor (`executor.py`)** — decides *whether to actually buy*:

1. Reads the latest fired signals from S3 (`load_fired_signals`), keeping
   only `signal_fired=True` and younger than `max_signal_age_min` (15 min).
2. Re-evaluates each one live: not TTL-expired, leader still above the
   threshold, correlation not drifted below
   `correlation × correlation_validation_threshold`.
3. Guards: account snapshot, market gate again, risk limits
   (`max_new` trades), and skips followers already held in the account
   (no doubling up).
4. Places **Market** (or bracket TP/SL) orders via `ib_insync`
   (`place_order_with_tracking`), waits for fill, cancels unfilled orders
   (logged as `BUY_OPEN`/`BUY_PARTIAL` so the ledger is exact), and
   confirms fills.
5. Writes the ledger to S3: `trade_log/{run_date}/{run_ts}/`
   (`run_meta.parquet`, `orders.parquet`, `fills.parquet`).

**3. Sell Manager (`sell_manager.py`)** — decides *when to sell*:

1. Reconstructs **open positions from the S3 ledger**: every BUY row minus
   every SELL row, matched by `buy_run_ts` (legacy unlinked sells reduce the
   earliest open buy FIFO-style). A position closes only when its remaining
   quantity reaches 0 — partial fills shrink, never close.
2. `days_held` is measured in **business days** (US-market calendar in
   `risk.py`); `holding_days` comes from the backtest's per-pair value.
3. Sells whatever is due (`sell_due = days_held >= holding_days`) and writes
   `SELL_EXECUTED`/`SELL_PARTIAL`/`SELL_OPEN` rows to the same ledger.

**TUI (`tui.py`)** — manual companion: browse signals/portfolio/trade
history, place manual buys/sells (logged as `MANUAL_*`), cancel orders.

### How it uses S3 to decide what to buy

The "what to buy" universe comes entirely from S3 — the notebook pipeline's
`backtest/` output — via DuckDB `httpfs`:

```python
# scanner.py — load_candidate_pairs()
SELECT leader, follower,
       MAX(holding_days) AS holding_days,
       MAX(correlation)  AS correlation,
       MAX(profit_pct)   AS best_profit_pct,
       MIN(profit_pct)   AS min_profit_pct
FROM read_parquet('s3://market-data-zw/parquet_data/backtest/*/*')
WHERE profit_pct  >= min_profit_pct          -- strategy threshold
  AND correlation >= min_correlation
  AND leader_gain > (market_dod_pct * 100)   -- beat the market
  AND follower NOT IN (SELECT ... WHERE profit_pct < -10)  -- no losers
GROUP BY leader, follower
ORDER BY correlation DESC
```

(prefers a pre-filtered `backtest/filtered_backtest.parquet` when present,
then dedupes by follower so each follower has one best leader).

Supporting S3 reads:

- `load_already_traded_leaders` — today's `orders.parquet` (avoid re-buying
  the same leader).
- `load_fired_signals` — latest `signals/{tactic}/{run_id}/signals.parquet`
  (executor input).
- `load_holding_days_map` — backtest `holding_days` per pair (sell timing).

### The ledger (S3 `trade_log`)

Every order and fill ever placed is an immutable row in S3:

```text
parquet_data/logs/trade_log/
└── {YYYYMMDD}/                       # trading day
    └── {YYYYMMDD_HHMMSS}/            # one folder per run (scanner/executor/sell)
        ├── run_meta.parquet          # run type, risk snapshot, preset
        ├── orders.parquet            # every order row (BUY/SELL/partial/open/manual)
        └── fills.parquet             # IB fill events (price, qty, commission)
```

Key `orders.parquet` fields: `run_ts`, `run_date`, `action`
(`BUY_EXECUTED`, `BUY_PARTIAL`, `BUY_OPEN`, `SELL_EXECUTED`, `SELL_PARTIAL`,
`SELL_OPEN`, `MANUAL_*`), `leader`, `follower`, `shares_ordered`,
`filled_qty`, `avg_fill_price`, `order_id`, `buy_run_ts` (links a sell back
to its buy), `correlation`, `holding_days`, `days_held`.

The sell manager treats this as a double-entry book: **open position =
bought shares − sold shares** (matched by `buy_run_ts`), which is why partial
fills are logged precisely and unfilled orders are cancelled rather than
recorded as fills.

Debug trail: every component also appends JSONL entries to
`parquet_data/logs/trader_debug_logs/{component}/{date}/{ts}_{uuid}.json`
(via `write_json_log`), which avoids read-modify-write races.

### How to run it

Prerequisites (see section 7): IB Gateway running on `127.0.0.1:4002`
(started via `ibc/start.sh`), and AWS credentials in `config.json` at the
AutoTrader root (the library searches `../config.json`, `Trader/config.json`,
then CWD).

```bash
# 1. Install the library (once)
cd /home/pparkitn/github/docker/AutoTrader/Trader/autotrade_lib
pip install -e . --break-system-packages
pip install rich questionary prompt-toolkit nest-asyncio --break-system-packages

# 2. Run the three daemons (three terminals, or nohup each)
cd /home/pparkitn/github/docker/AutoTrader
python -m autotrade_lib.scanner       --once        # single scan cycle
python -m autotrade_lib.scanner       --continuous # loop every 5 min
python -m autotrade_lib.executor      --once        # single execution cycle
python -m autotrade_lib.executor      --continuous # loop every 60 s
python -m autotrade_lib.sell_manager  --once        # check positions once
python -m autotrade_lib.sell_manager  --daily       # run daily at 9:30 ET
python -m autotrade_lib.sell_manager  --continuous  # loop every hour
```

Useful flags:

```bash
--preset paper|paper_aggressive|live_conservative|live   # strategy profile
--tactic correlation|momentum|mean_reversion             # scanner only
--dry-run                                                # simulate, no orders/S3 writes
```

Presets (`config.py`) control risk per profile:

| Preset | Scanner/Executor/Sell client IDs | Threshold | Shares | Notes |
|---|---|---|---|---|
| `paper` | 100 / 200 / 300 | 2.0% | 10 | bracket orders, TP 3% / SL 1.5% |
| `paper_aggressive` | 400 / 500 / 600 | 1.5% | 50 | lower bars, bigger size |
| `live_conservative` | 700 / 800 / 900 | 7.0% | 50 | tight risk caps (5%/trade, 60% exposure) |
| `live` | 1000 / 1100 / 1200 | 5.0% | 100 | no bracket orders |

Each component connects to IB with its own **client-ID base** (so they can
run concurrently on one gateway), auto-retrying with backoff and skipping
in-use IDs (`ib_connection.py`). For background use wrap each in
`nohup ... > logs/<component>.log 2>&1 &`; the sell manager's `--daily` mode
is the production schedule.

### Safety design (why it is safe to leave running)

- **Fails closed**: market-gate and position-lookup failures block new buys
  rather than allowing them.
- **No doubling up**: held followers and already-traded leaders are excluded.
- **TTL expiry**: a signal older than its TTL can't be executed.
- **Exact ledger**: unfilled/partial orders are cancelled and logged as
  `BUY_OPEN`/`BUY_PARTIAL`; the sell engine only sells what the ledger says
  is still held.
- **Per-slot isolation**: a failing order never aborts the cycle or discards
  already-recorded fills.

---

## Configuration reference

All secrets resolve in priority order: **environment variable → config.json
→ built-in default**. Nothing is hard-coded.

| Variable | Used by | Purpose |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | all | S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | all | S3 credentials |
| `AWS_REGION` | all | S3 region (default `us-east-1`) |
| `S3_BUCKET` | all | bucket (default `market-data-zw`) |
| `MASSIVE_API_KEY` | `00-01`, `01-01` | market-data API key |
| `KAGGLE_USERNAME` | push scripts | Kaggle account (default `dsptlp`) |
| `KAGGLE_API_TOKEN` | push scripts | Kaggle API token (or `~/.kaggle/kaggle.json`) |
| `IB_HOST` / `IB_PORT` | trader code (local) | IB Gateway host / API port (default `127.0.0.1:4002`, paper) |

`config.example.json` documents the non-secret options (Spark memory, etc.).
The IB credentials live in the IBC `config.ini` on the trading machine — they
are never part of the notebooks or this repo.

---

## Where each notebook touches each service

| Notebook | Massive | yfinance | Kaggle dataset | S3 write | S3 read | Engine | Interactive |
|---|---:|---:|---:|---:|---|---|---|
| `00-01-ticker-load` | ✅ | — | — | types, tickers | — | boto3 | |
| `00-02-ticker-universe` | — | ✅ | — | ticker details | tickers | boto3 + httpfs | |
| `01-01-historical-load` | ✅ | — | — | minute staging | tickers | boto3 + httpfs | |
| `01-02-merge-minute-data` | — | — | — | minute final | staging + final | boto3 | |
| `02-01-etl-summary` | — | — | — | summaries | minute final | **Spark** | |
| `02-02-s3-to-kaggle-dataset` | — | — | **writes** | (reads all mirrored) | all mirrored | boto3 | |
| `03-01-correlation` | — | — | reads* | correlation pairs | summaries | **Spark** | |
| `04-01-backtest` | — | — | reads* | backtest trades | summaries + pairs | **Spark** | |
| `04-02-analyze-backtest` | — | — | reads* | metrics | backtest trades | DuckDB httpfs | |
| `05-01-pair-explorer` | — | — | reads* | — | metrics + prices | DuckDB httpfs | ✅ |
| `05-02-advanced-pair-analysis` | — | — | reads* | (reports local) | metrics + trades | DuckDB httpfs | ✅ |

\* = optional — `resolve()` prefers the mounted Kaggle dataset mirror and
falls back to S3 automatically.

The notebooks form a linear pipeline; run them in order
(`00-01 → 00-02 → 01-01 → 01-02 → 02-01 → 02-02 → 03-01 → 04-01 → 04-02 →
05-*`). Each one is idempotent/incremental where possible
(already-downloaded tickers are detected via S3 keys and skipped).