#!/usr/bin/env python3
"""Push the pipeline notebooks (notebooks/) to Kaggle.

Each notebook is pushed from a temporary folder containing the notebook plus a
generated kernel-metadata.json (Kaggle's `kernels push` requires one metadata
file per folder).

The notebooks are fully self-contained: they install their own packages and
read/write S3 directly, so no Kaggle dataset sources are required.

Usage:
    python3 push_kernels.py --dry-run                      # show what would be pushed (default)
    python3 push_kernels.py --push                         # actually run `kaggle kernels push`
    python3 push_kernels.py --push 03-01-correlation.ipynb # push just one
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEBOOK_DIR = os.path.join(HERE, "notebooks")
KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME", "dsptlp")

KERNELS = {
    "00-01-ticker-load.ipynb":           {"slug": "autotrade-00-01-ticker-load",          "title": "Market Data 00-01 - Ticker Load"},
    "00-02-ticker-universe.ipynb":       {"slug": "autotrade-00-02-ticker-universe",      "title": "Market Data 00-02 - Ticker Universe"},
    "01-01-historical-load.ipynb":       {"slug": "autotrade-01-01-historical-load",      "title": "Market Data 01-01 - Historical Load"},
    "01-02-merge-minute-data.ipynb":     {"slug": "autotrade-01-02-merge-minute-data",    "title": "Market Data 01-02 - Merge Minute Data"},
    "02-02-s3-to-kaggle-dataset.ipynb":  {"slug": "autotrade-02-02-s3-to-kaggle-dataset", "title": "Market Data 02-02 - S3 to Kaggle Dataset"},
    "02-01-etl-summary.ipynb":           {"slug": "autotrade-02-01-etl-summary",          "title": "Market Data 02-01 - ETL Summary"},
    "03-01-correlation.ipynb":           {"slug": "autotrade-03-01-correlation",          "title": "Market Data 03-01 - Correlation"},
    "04-01-backtest.ipynb":              {"slug": "autotrade-04-01-backtest",             "title": "Market Data 04-01 - Backtest"},
    "04-02-analyze-backtest.ipynb":      {"slug": "autotrade-04-02-analyze-backtest",     "title": "Market Data 04-02 - Analyze Backtest"},
    "05-01-pair-explorer.ipynb":         {"slug": "autotrade-05-01-pair-explorer",        "title": "Market Data 05-01 - Pair Explorer"},
    "05-02-advanced-pair-analysis.ipynb": {"slug": "autotrade-05-02-advanced-pair-analysis", "title": "Market Data 05-02 - Advanced Pair Analysis"},
}


def build_metadata(entry, notebook):
    return {
        "id": f"{KAGGLE_USERNAME}/{entry['slug']}",
        "title": entry["title"],
        "code_file": notebook,
        "language": "python",
        "kernel_type": "notebook",
        "is_private": True,
        "enable_gpu": False,
        "enable_tpu": False,
        "enable_internet": True,  # pip installs + S3 + massive/yfinance
        "machine_shape": "",
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "model_sources": [],
    }


def push_one(notebook, push, timeout=None):
    entry = KERNELS[notebook]
    src = os.path.join(NOTEBOOK_DIR, notebook)
    if not os.path.exists(src):
        print(f"  !! missing notebook {notebook}")
        return

    tmp = tempfile.mkdtemp(prefix="kaggle_push_")
    shutil.copy(src, os.path.join(tmp, notebook))
    with open(os.path.join(tmp, "kernel-metadata.json"), "w") as f:
        json.dump(build_metadata(entry, notebook), f, indent=2)

    print(f"== {notebook}  ->  {KAGGLE_USERNAME}/{entry['slug']}  (self-contained, no datasets)")
    if not push:
        shutil.rmtree(tmp)
        return

    cmd = ["kaggle", "kernels", "push", "-p", tmp]
    if timeout:
        cmd += ["-t", str(timeout)]
    print(f"   $ {' '.join(cmd)}")
    r = subprocess.run(cmd)
    shutil.rmtree(tmp)
    if r.returncode != 0:
        print(f"   !! push failed (exit {r.returncode})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true", help="actually push (default is dry-run)")
    ap.add_argument("--dry-run", action="store_true", help="explicit dry-run (default)")
    ap.add_argument("--timeout", type=int, default=None,
                    help="run-time limit in seconds (passed to `kaggle kernels push -t`)")
    ap.add_argument("notebooks", nargs="*", help="optional list of notebooks to push")
    args = ap.parse_args()

    targets = args.notebooks or list(KERNELS)
    for nb in targets:
        if nb not in KERNELS:
            print(f"unknown notebook: {nb}")
            sys.exit(1)

    mode = "PUSH" if args.push else "DRY-RUN"
    print(f"{mode} - {len(targets)} notebook(s)\n")
    for nb in targets:
        push_one(nb, args.push, args.timeout)
    print("\nDone.")


if __name__ == "__main__":
    main()