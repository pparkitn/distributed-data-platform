#!/usr/bin/env python3
"""Batch runner for Kaggle kernels - pushes kernels with concurrency handling.

Usage:
    python batch_runner.py --once --count 2 --kernels 03-01        # push 2x 03-01
    python batch_runner.py --daemon --count 4 --kernels 03-01 --interval 60
        # push 4, then re-push 4 whenever the latest batch finishes (forever)
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

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
        "enable_internet": True,
        "machine_shape": "",
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "model_sources": [],
    }


def push_and_run(notebook_name, timeout=None):
    """Push a notebook and run it. Returns (success, error_msg)."""
    entry = KERNELS[notebook_name]
    src = os.path.join(NOTEBOOK_DIR, notebook_name)

    tmp = tempfile.mkdtemp(prefix="kaggle_batch_")
    try:
        shutil.copy(src, os.path.join(tmp, notebook_name))
        with open(os.path.join(tmp, "kernel-metadata.json"), "w") as f:
            json.dump(build_metadata(entry, notebook_name), f, indent=2)

        cmd = ["kaggle", "kernels", "push", "-p", tmp]
        if timeout:
            cmd += ["-t", str(timeout)]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout or 600)

        # Kaggle returns exit code 0 even for concurrency errors (in stdout)
        output = (result.stdout + result.stderr).strip()
        if result.returncode == 0 and "Maximum batch CPU session count" not in output:
            return True, "Success", notebook_name
        else:
            return False, output, notebook_name
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def is_concurrency_error(error_msg):
    """Check if error indicates Kaggle concurrency limit reached."""
    error_lower = error_msg.lower()
    return any(kw in error_lower for kw in [
        "concurrent", "limit", "quota", "too many", "maximum", "capacity",
        "running", "busy", "throttle", "rate limit",
        "maximum batch cpu session count"
    ])


ACTIVE_STATUSES = {"QUEUED", "RUNNING", "NEW_SCRIPT"}

_kernels_client = None


def _get_kernels_client():
    """Lazily build an authenticated Kaggle SDK kernels client."""
    global _kernels_client
    if _kernels_client is None:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        _kernels_client = api.build_kaggle_client().kernels.kernels_api_client
    return _kernels_client


def get_session_status(entry):
    """Status of the kernel's latest session (str) or None if unknown.

    Kaggle's API only exposes the latest session per kernel (per-version
    lookups return 404), so this is the best visibility available.
    """
    try:
        from kagglesdk.kernels.services.kernels_api_service import (
            ApiGetKernelSessionStatusRequest,
        )
        req = ApiGetKernelSessionStatusRequest()
        req.kernel_slug = entry["slug"]
        req.user_name = KAGGLE_USERNAME
        resp = _get_kernels_client().get_kernel_session_status(req)
        status = getattr(resp, "status", None)
        return getattr(status, "name", None)
    except Exception:
        return None


def run_batch(target_kernels, counts, max_concurrent=4, interval=60, once=False):
    """Simple batch runner: just push until we hit concurrency limit or desired count."""
    backoff = 30
    max_backoff = 300
    attempt = 0

    print(f"[{time.strftime('%H:%M:%S')}] Starting batch runner")
    print(f"  Target kernels: {target_kernels}")
    print(f"  Desired instances per kernel: {counts}")
    print(f"  Max concurrent: {max_concurrent}")
    print(f"  Mode: {'once' if once else 'daemon'}")

    pushed_counts = {k: 0 for k in target_kernels}

    while True:
        pushed_any = False
        for nb in target_kernels:
            if pushed_counts[nb] >= counts[nb]:
                continue
            attempt += 1
            print(f"\n[{time.strftime('%H:%M:%S')}] Attempt #{attempt} - pushing {nb} "
                  f"({pushed_counts[nb]}/{counts[nb]})...")
            success, msg, _ = push_and_run(nb)

            if success:
                print(f"  ok Pushed {nb} successfully")
                pushed_counts[nb] += 1
                backoff = 30
                pushed_any = True
            else:
                print(f"  X Failed: {msg}")
                if is_concurrency_error(msg):
                    print(f"  > Concurrency limit reached, waiting {backoff}s...")
                else:
                    print(f"  > Error occurred, waiting {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                break

        if not pushed_any:
            all_done = all(pushed_counts[nb] >= counts[nb] for nb in target_kernels)
            if all_done:
                print("\n  ok All desired pushes complete")
                break
            else:
                print(f"\n  > Waiting {interval}s before retry...")
                time.sleep(interval)

        if once and sum(pushed_counts.values()) >= sum(counts.values()):
            print("\n  ok All pushes complete")
            break

    # Phase 2: daemon refill -- poll the latest session status and re-push
    # when the latest run finishes.
    if not once:
        print(f"\n  Refill mode: checking sessions every {interval}s", flush=True)
        while True:
            for nb in target_kernels:
                status = get_session_status(KERNELS[nb])
                print(f"[{time.strftime('%H:%M:%S')}] {nb}: latest session = {status}",
                      flush=True)
                if status is None:
                    print("  ? status unknown, skipping this round", flush=True)
                    continue
                if status in ACTIVE_STATUSES:
                    continue
                refill = counts[nb] if status == "COMPLETE" else 1
                if status == "ERROR":
                    print("  ! latest run errored - pushing 1 instance this round",
                          flush=True)
                print(f"  + latest run finished ({status}) - pushing {refill} instance(s)",
                      flush=True)
                for i in range(refill):
                    attempt += 1
                    print(f"    Attempt #{attempt} - refill {i + 1}/{refill} of {nb}...",
                          flush=True)
                    success, msg, _ = push_and_run(nb)
                    if success:
                        print(f"    ok Pushed {nb}", flush=True)
                        pushed_counts[nb] += 1
                        backoff = 30
                    else:
                        print(f"    X Failed: {msg}", flush=True)
                        backoff = min(backoff * 2, max_backoff)
                        break
            time.sleep(interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Batch runner for Kaggle kernels")
    ap.add_argument("--max-concurrent", type=int, default=4, help="Max global concurrent kernels")
    ap.add_argument("--interval", type=int, default=60, help="Seconds between checks")
    ap.add_argument("--once", action="store_true", help="Single round then exit")
    ap.add_argument("--daemon", action="store_true", help="Run continuously")
    ap.add_argument("--count", type=int, default=1, help="Instances per kernel (default 1)")
    ap.add_argument("--kernels", nargs="+", default=["03-01"],
                    help="Kernels to run (e.g. 03-01 03-02 04-01)")
    args = ap.parse_args()

    # Convert short names to full notebook filenames
    target_kernels = []
    for k in args.kernels:
        if k.endswith(".ipynb"):
            target_kernels.append(k)
        else:
            matches = [nk for nk in KERNELS if k in nk]
            if matches:
                target_kernels.append(matches[0])
            else:
                print(f"Unknown kernel: {k}")
                sys.exit(1)

    for k in target_kernels:
        if k not in KERNELS:
            print(f"Unknown notebook: {k}")
            sys.exit(1)

    counts = {k: args.count for k in target_kernels}

    if not args.once and not args.daemon:
        args.daemon = True

    run_batch(target_kernels, counts, args.max_concurrent, args.interval, args.once)