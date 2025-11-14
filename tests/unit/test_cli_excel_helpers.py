"""تست‌های واحد برای توابع پاک‌سازی خروجی CLI."""

from __future__ import annotations

import math

import pandas as pd

from app.infra import cli


def _nan() -> float:
    return float("nan")


def test_coalesce_duplicate_columns_handles_nan_headers() -> None:
    df = pd.DataFrame(
        [[1, None, 3], [4, 5, 6]],
        columns=[_nan(), _nan(), "B"],
    )

    result = cli._coalesce_duplicate_columns(df)

    assert list(result.columns)[1] == "B"
    assert math.isnan(result.columns[0])
    pd.testing.assert_series_equal(
        result.iloc[:, 0], pd.Series([1, 4], name=result.columns[0]), check_dtype=False
    )


def test_make_excel_safe_handles_nan_header_duplicates() -> None:
    df = pd.DataFrame(
        [[{"x": 1}, {"y": 2}]],
        columns=[_nan(), _nan()],
    )

    safe = cli._make_excel_safe(df)

    assert safe.shape == (1, 1)
    assert math.isnan(safe.columns[0])
    assert safe.iloc[0, 0].startswith("{")
