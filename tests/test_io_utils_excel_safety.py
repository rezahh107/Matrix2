from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.core.common.columns import canonicalize_headers
from app.infra import io_utils
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


def test_write_xlsx_atomic_falls_back_to_csv(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(io_utils, "_pick_engine", lambda: None)
    df = pd.DataFrame({"mentor_id": [101, 102], "remaining_capacity": [2, 1]})
    output = tmp_path / "fallback.xlsx"

    with pytest.warns(RuntimeWarning, match="No Excel engine available"):
        write_xlsx_atomic({"allocations": df}, output)

    assert not output.exists()
    csv_output = tmp_path / "fallback-allocations.csv"
    assert csv_output.exists()
    saved = pd.read_csv(csv_output)
    saved_internal = canonicalize_headers(saved, header_mode="en")
    expected_internal = canonicalize_headers(df, header_mode="en")
    saved_internal = saved_internal.rename(
        columns=lambda col: str(col).split("|")[0].strip()
    )
    expected_internal = expected_internal.rename(
        columns=lambda col: str(col).split("|")[0].strip()
    )
    pd.testing.assert_frame_equal(saved_internal, expected_internal)
