"""Static validation of the notebook pipeline (the repo's primary "code").

These tests treat the notebooks as source code: every code cell must parse,
must not import old shared packages (`market_data`/`autotrade`), and must
only reference names that were defined in an earlier cell (or builtins).
"""

import ast
import builtins
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOTEBOOKS = sorted((REPO / "notebooks").glob("*.ipynb"))

BUILTINS = set(dir(builtins))
WHITELIST = {"display", "tqdm", "HTML", "clear_output"}
STAR_IMPORT_TYPES = {"DoubleType", "LongType", "StringType", "StructField", "StructType"}
MAGIC_LINE = re.compile(r"^\s*(!|%)")

EXPECTED_NOTEBOOKS = [
    "00-01-ticker-load.ipynb",
    "00-02-ticker-universe.ipynb",
    "01-01-historical-load.ipynb",
    "01-02-merge-minute-data.ipynb",
    "02-01-etl-summary.ipynb",
    "02-02-s3-to-kaggle-dataset.ipynb",
    "03-01-correlation.ipynb",
    "04-01-backtest.ipynb",
    "04-02-analyze-backtest.ipynb",
    "05-01-pair-explorer.ipynb",
    "05-02-advanced-pair-analysis.ipynb",
]


def _clean_source(src: str) -> str:
    return "\n".join(line for line in src.splitlines() if not MAGIC_LINE.match(line))


def _code_cells(nb: dict) -> list[str]:
    return [
        "".join(c["source"])
        for c in nb["cells"]
        if c["cell_type"] == "code"
    ]


def _collect_names(tree: ast.AST) -> tuple[set[str], set[str]]:
    """Return (defined_names, used_names) for one parsed cell."""
    defined: set[str] = set()
    used: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defined.add(node.name)
            for a in list(node.args.posonlyargs) + list(node.args.args) + list(node.args.kwonlyargs):
                defined.add(a.arg)
            if node.args.vararg:
                defined.add(node.args.vararg.arg)
            if node.args.kwarg:
                defined.add(node.args.kwarg.arg)
        elif isinstance(node, ast.Lambda):
            for a in list(node.args.args) + list(node.args.kwonlyargs):
                defined.add(a.arg)
        elif isinstance(node, ast.ClassDef):
            defined.add(node.name)
        elif isinstance(node, ast.Import):
            for a in node.names:
                defined.add((a.asname or a.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                defined.add(a.asname or a.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                for n in ast.walk(t):
                    if isinstance(n, ast.Name):
                        defined.add(n.id)
        elif isinstance(node, (ast.AugAssign, ast.For, ast.comprehension)):
            for n in ast.walk(node.target):
                if isinstance(n, ast.Name):
                    defined.add(n.id)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            defined.add(node.name)
        elif isinstance(node, ast.With):
            for item in node.items:
                if item.optional_vars:
                    for n in ast.walk(item.optional_vars):
                        if isinstance(n, ast.Name):
                            defined.add(n.id)

        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(node.id)
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and isinstance(node.value.ctx, ast.Load)
        ):
            used.add(node.value.id)

    return defined, used


@pytest.fixture(scope="module", params=[p.name for p in NOTEBOOKS])
def notebook_file(request) -> Path:
    return REPO / "notebooks" / request.param


def test_expected_notebook_set():
    actual = sorted(p.name for p in NOTEBOOKS)
    assert actual == EXPECTED_NOTEBOOKS, f"notebook set drifted: {actual}"


def test_all_cells_parse(notebook_file):
    nb = json.loads(notebook_file.read_text())
    for _i, src in enumerate(_code_cells(nb)):
        ast.parse(_clean_source(src))  # raises SyntaxError on failure


def test_no_package_imports(notebook_file):
    nb = json.loads(notebook_file.read_text())
    cells = _code_cells(nb)
    assert not any(
        "market_data" in c or "autotrade" in c for c in cells
    ), f"{notebook_file.name} imports a shared package (must be self-contained)"


def test_no_cell_outputs(notebook_file):
    nb = json.loads(notebook_file.read_text())
    for i, c in enumerate(nb["cells"]):
        assert not c.get("outputs"), f"{notebook_file.name} cell {i} has saved outputs"


def test_no_undefined_names(notebook_file):
    nb = json.loads(notebook_file.read_text())
    defined: set[str] = set()
    issues = []
    for i, c in enumerate(nb["cells"]):
        if c["cell_type"] != "code":
            continue
        src = _clean_source("".join(c["source"]))
        tree = ast.parse(src)
        d, u = _collect_names(tree)
        defined |= d
        missing = sorted(u - defined - BUILTINS - WHITELIST - STAR_IMPORT_TYPES)
        if missing:
            issues.append(f"cell {i}: {missing}")
    assert not issues, f"{notebook_file.name}: {issues}"


def test_every_notebook_has_setup_cell(notebook_file):
    nb = json.loads(notebook_file.read_text())
    assert any("load_config" in c for c in _code_cells(nb)), (
        f"{notebook_file.name} missing setup cell"
    )
