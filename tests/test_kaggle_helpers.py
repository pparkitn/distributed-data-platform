"""Tests for the Kaggle push/batch helper scripts."""

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import batch_runner  # noqa: E402
import push_kernels  # noqa: E402

NOTES = set(p.name for p in (REPO / "notebooks").glob("*.ipynb"))


def test_every_notebook_is_registered():
    assert set(push_kernels.KERNELS) == NOTES
    assert set(batch_runner.KERNELS) == NOTES


def test_metadata_structure():
    entry = push_kernels.KERNELS["03-01-correlation.ipynb"]
    meta = push_kernels.build_metadata(entry, "03-01-correlation.ipynb")
    assert meta["kernel_type"] == "notebook"
    assert meta["enable_internet"] is True
    assert meta["enable_gpu"] is False
    assert meta["dataset_sources"] == []  # self-contained notebooks
    assert meta["code_file"] == "03-01-correlation.ipynb"
    assert meta["id"].endswith(entry["slug"])


def test_kaggle_username_env():
    assert push_kernels.KAGGLE_USERNAME == "dsptlp"  # env default


def test_metadata_json_is_valid():
    entry = push_kernels.KERNELS["00-01-ticker-load.ipynb"]
    meta = push_kernels.build_metadata(entry, "00-01-ticker-load.ipynb")
    json.dumps(meta)  # must be JSON-serializable


def test_batch_runner_matches_push_kernels():
    assert batch_runner.KERNELS == push_kernels.KERNELS


def test_short_name_resolution():
    # batch_runner maps short names like "03-01" to a notebook file
    targets = []
    for k in ["03-01"]:
        if k.endswith(".ipynb"):
            targets.append(k)
        else:
            matches = [nk for nk in batch_runner.KERNELS if k in nk]
            assert matches
            targets.append(matches[0])
    assert targets == ["03-01-correlation.ipynb"]
