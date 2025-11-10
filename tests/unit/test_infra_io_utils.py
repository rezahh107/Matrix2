from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.infra.io_utils import write_xlsx_atomic


@pytest.mark.skipif(importlib.util.find_spec('openpyxl') is None, reason="openpyxl لازم است")
def test_write_xlsx_atomic_sanitizes_and_deduplicates(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    out = tmp_path / "out.xlsx"

    write_xlsx_atomic({"Sheet/1": df, "Sheet:1": df, " ": df}, out)

    from openpyxl import load_workbook

    wb = load_workbook(out)
    names = wb.sheetnames

    assert names[0] == "Sheet 1"
    assert names[1] == "Sheet 1 (2)"
    assert names[2] == "Sheet"
