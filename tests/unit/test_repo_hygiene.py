from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "app"


def _collect_py_files(base: Path) -> list[Path]:
    files: list[Path] = []
    for path in base.rglob("*.py"):
        spath = str(path)
        if any(skip in spath for skip in ("__pycache__", "venv", ".venv")):
            continue
        files.append(path)
    return sorted(files)


PY_FILES = _collect_py_files(SRC)


@pytest.mark.parametrize("path", PY_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_inplace_true_in_app_sources(path: Path) -> None:
    text = path.read_text(encoding="utf-8", errors="ignore")
    assert not re.search(r"\binplace\s*=\s*True\b", text), f"`inplace=True` found in: {path}"
