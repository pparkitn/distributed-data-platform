# Pushing to Kaggle — how to make and push Kaggle notebooks

This repo's pipeline runs as **Kaggle Notebooks** (cloud Jupyter kernels).
Everything is pushed from this repository with `push_kernels.py`, which wraps
the official `kaggle kernels push` CLI.

---

## 1. Prerequisites

| # | What | How |
|---|---|---|
| 1 | Kaggle account | sign up at kaggle.com |
| 2 | Kaggle CLI | `pip install kaggle` |
| 3 | API credentials | `kaggle configure` (writes `~/.kaggle/kaggle.json`), or set env vars: `KAGGLE_USERNAME` + `KAGGLE_API_TOKEN` |

Credential resolution (same for push scripts and notebooks):

```text
environment variable  →  config.json  →  built-in default
```

Relevant variables:

| Variable | Purpose | Default |
|---|---|---|
| `KAGGLE_USERNAME` | Kaggle account name (also the kernel id prefix) | `dsptlp` |
| `KAGGLE_API_TOKEN` | API token (or `~/.kaggle/kaggle.json`) | — |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` / `S3_BUCKET` | added as **kernel secrets** (not needed locally) | `us-east-1` / `market-data-zw` |
| `MASSIVE_API_KEY` | kernel secret for `00-01`, `01-01` only | — |

---

## 2. The push script (`push_kernels.py`)

```bash
python3 push_kernels.py --dry-run                          # show the plan (default)
python3 push_kernels.py --push                             # push all 11 notebooks
python3 push_kernels.py --push 03-01-correlation.ipynb     # push just one
python3 push_kernels.py --push --timeout 36000 03-01       # set a run-time limit (s)
```

What it does for each notebook:

1. copies the notebook into a temp folder,
2. generates a `kernel-metadata.json` (Kaggle requires one per folder),
3. runs `kaggle kernels push -p <folder>`.

The generated metadata looks like this:

```json
{
  "id": "dsptlp/autotrade-03-01-correlation",
  "title": "Market Data 03-01 - Correlation",
  "code_file": "03-01-correlation.ipynb",
  "language": "python",
  "kernel_type": "notebook",
  "is_private": true,
  "enable_gpu": false,
  "enable_tpu": false,
  "enable_internet": true,
  "dataset_sources": [],
  "competition_sources": [],
  "kernel_sources": [],
  "model_sources": []
}
```

Key points:

- **`enable_internet: true`** — required for `pip install`, S3 access, and
  the Massive / yfinance APIs.
- **`dataset_sources: []`** — the notebooks are self-contained and read
  everything from S3, so no dataset mounts are required. The optional Kaggle
  dataset mirror (see section 5) is attached manually from the kernel editor,
  not via push metadata.
- Pushing the same slug again **updates** the existing kernel (version bump)
  instead of creating a duplicate.

---

## 3. First push + secrets (one-time setup)

```bash
pip install kaggle
export KAGGLE_USERNAME=dsptlp
export KAGGLE_API_TOKEN=...        # or `kaggle configure`

python3 push_kernels.py --push     # creates 11 private kernels
```

Then, **for every kernel**, add the S3 secrets in the web editor:

1. open the kernel (kaggle.com → your profile → Code),
2. **Add-ons → Secrets** (or Settings → Secrets),
3. create: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
   `S3_BUCKET`,
4. only for `00-01-ticker-load` and `01-01-historical-load`: also
   `MASSIVE_API_KEY`.

> Alternative: upload a `config.json` next to the notebook with the same
> keys — the setup cell's `load_config()` falls back to it. Env-var secrets
> are simpler.

---

## 4. Running the notebooks

- **Headless run**: use the Kaggle web UI's **Run All / Save Version** button,
  or the CLI: `kaggle kernels pull` → edit → `kaggle kernels push` (a push
  triggers a run in Kaggle).
- **Interactive run** (`05-01`, `05-02`): open the kernel and run cells
  manually — widgets render in the browser.
- **Session limits**: CPU sessions run up to 12 h (~30 GB RAM); long stages
  (`00-02`, `01-01`) are **resumable** — re-run until their S3 object-key
  diff reports 0 missing.

Check status:

```bash
kaggle kernels status dsptlp/autotrade-03-01-correlation
```

---

## 5. Making a new Kaggle notebook (from scratch)

Two options:

**Option A — add it to this repo.** Copy a `notebooks/*.ipynb` skeleton,
keep the self-contained style (own pip installs, own `load_config()`/storage
helpers — do not import shared packages), then add it to:

- `push_kernels.py` → the `KERNELS` dict (slug + title),
- `tests/test_notebooks.py` → `EXPECTED_NOTEBOOKS`.

Then push with `python3 push_kernels.py --push <name>.ipynb`.

**Option B — use the Kaggle editor directly.** Kaggle → Code → **New
Notebook**. When done, export/download the `.ipynb` into `notebooks/` and
follow Option A so it becomes reproducible.

---

## 6. The optional Kaggle dataset mirror (`02-02`)

Reading every stage's input from S3 works but costs S3 GET requests per run.
`02-02-s3-to-kaggle-dataset` mirrors the small, hot prefixes (summary,
types, strategies, backtest, analysis — **not** the 24 GB minute bars) into a
private Kaggle dataset `dsptlp/market-data-s3-dataset`.

```bash
# needs KAGGLE_USERNAME + KAGGLE_API_TOKEN in addition to AWS creds
python3 push_kernels.py --push 02-02-s3-to-kaggle-dataset.ipynb
```

Consumers (`03-01`, `04-01`, `04-02`, `05-01`, `05-02`) attach it via
**Add-ons → Datasets → `dsptlp/market-data-s3-dataset`** and their
`resolve()` helper prefers the mounted mirror, falling back to S3.

Refresh cadence: re-run `02-02` after each batch of 03-01 / 04-01 / 04-02
output (a dataset update takes a few minutes to become mountable).

---

## 7. Parallel / continuous runs (`batch_runner.py`)

For workloads like the randomized correlation search (each `03-01` run
explores different `lag_days`/`lookback_days`/`persistence_window`), keep
several instances running:

```bash
python3 batch_runner.py --once   --count 4 --kernels 03-01                       # one burst of 4
python3 batch_runner.py --daemon --count 3 --kernels 03-01 --interval 60         # refill forever
nohup python3 -u batch_runner.py --daemon --count 3 --kernels 03-01 --interval 60 \
    > batch_runner.log 2>&1 &                                                     # background + log
```

Behavior:

- warm-up pushes `--count` instances in parallel,
- polls the kernels' latest session every `--interval` seconds:
  - `COMPLETE` → push a fresh batch of `--count`,
  - `ERROR` → push **1** (a broken notebook can't burn quota),
  - running/queued → wait.

Production notes:

- **5 concurrent batch-CPU sessions** is the account cap; `batch_runner.py`
  backs off automatically (30 s doubling to 300 s).
- Kaggle's API only exposes the latest session per kernel, hence batch-based
  refills.
- Use `python3 -u` when redirecting to a log file (stdout is block-buffered
  otherwise).
- Stop with Ctrl-C / kill — already-running sessions finish on their own.
- Watch your **weekly session quota**; prefer `--once --count N` over daemon
  mode when a fixed number of runs is enough.

---

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `MASSIVE_API_KEY not set` / empty config | secrets missing on that kernel — re-check Add-ons → Secrets |
| `Maximum batch CPU session count of 5 reached` | account cap; the runner backs off automatically |
| Weekly quota exhausted | wait for reset, reduce `--count`, avoid daemon mode |
| `kaggle: command not found` | `pip install kaggle`, check PATH |
| Push fails with auth error | re-run `kaggle configure` or fix `~/.kaggle/kaggle.json` |
| pip install fails inside a kernel | kernel has `enable_internet: false` — `push_kernels.py` sets it true; if edited by hand, re-push |
| 12 h session ends mid-stage | normal for `00-02` / `01-01` — both resumable; re-push and re-run |
| Dataset mirror not picked up | private dataset update still processing; wait a few minutes and re-run |