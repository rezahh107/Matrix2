from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.infra.io_utils import write_xlsx_atomic


@pytest.mark.parametrize("engine", ["openpyxl", "xlsxwriter"])
def test_write_xlsx_atomic_handles_duplicate_columns(tmp_path: Path, monkeypatch, engine: str) -> None:
    try:
        __import__(engine)
    except Exception:
        pytest.skip(f"engine {engine} not installed")

    monkeypatch.setenv("EXCEL_ENGINE", engine)
    df = pd.DataFrame(
        [[101, {"level": 1}, ["tag"]], [102, {"level": 2}, {"note": "x"}]],
        columns=["alias", "details", "details"],
    )
    data = {"allocations": df}
    output = tmp_path / f"safe-{engine}.xlsx"

    write_xlsx_atomic(data, output)

    assert output.exists()
