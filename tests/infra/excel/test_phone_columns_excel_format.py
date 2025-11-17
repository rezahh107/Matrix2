"""تست‌های دیباگ برای حفظ صفر پیشتاز ستون‌های موبایل در خروجی اکسل."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import openpyxl
import pandas as pd
import pytest

from app.infra.io_utils import _prepare_dataframe_for_excel, write_xlsx_atomic


def debug_column_profile(df: pd.DataFrame, column: str) -> Dict[str, Any]:
    """نمایهٔ سریع از dtype و طول رشته‌ها برای ستون موردنظر (صرفاً جهت دیباگ).

    مثال::
        >>> import pandas as pd
        >>> df = pd.DataFrame({"student_mobile": pd.Series([917], dtype="float64")})
        >>> profile = debug_column_profile(df, "student_mobile")
        >>> sorted(profile.keys())
        ['dtype', 'non_null', 'sample', 'string_lengths']
    """

    series = df[column]
    as_strings = series.astype("string")
    non_null = as_strings.dropna()
    lengths = non_null.map(len)
    sample = non_null.head(3).tolist()
    return {
        "dtype": str(series.dtype),
        "non_null": int(non_null.count()),
        "string_lengths": lengths.tolist(),
        "sample": sample,
    }


def test_float_mobile_values_lose_leading_zero_in_excel(tmp_path: Path) -> None:
    """بازتولید حذف صفر پیشتاز زمانی که ستون موبایل به‌صورت float خوانده شده است."""

    raw = pd.DataFrame({
        "student_mobile": pd.Series([9171075740.0, 9351234567.0], dtype="float64"),
        "score": [1, 2],
    })

    profile_before = debug_column_profile(raw, "student_mobile")

    prepared = _prepare_dataframe_for_excel(raw)
    profile_after = debug_column_profile(prepared, "student_mobile")

    output_path = tmp_path / "mobiles.xlsx"
    write_xlsx_atomic({"Sheet1": raw}, output_path)
    workbook = openpyxl.load_workbook(output_path)
    sheet = workbook["Sheet1"]
    excel_values = [sheet["A2"].value, sheet["A3"].value]
    excel_types = [sheet["A2"].data_type, sheet["A3"].data_type]

    expected = ["09171075740", "09351234567"]

    assert str(prepared["student_mobile"].dtype).startswith("string")
    assert prepared["student_mobile"].tolist() == expected, profile_after
    assert excel_values == expected, {
        "profile_before": profile_before,
        "profile_after": profile_after,
        "excel_types": excel_types,
        "excel_values": excel_values,
    }


def test_string_mobile_values_are_normalized_and_preserved(tmp_path: Path) -> None:
    """ورودی‌های متنی (با نویز یا بدون صفر) به رشتهٔ ۱۱ رقمی تبدیل می‌شوند."""

    raw = pd.DataFrame(
        {
            "student_mobile": ["9171075740", " 0917-107-5740 ", ""],
            "contact1_mobile": ["0935 123 4567", "0912۳۴۵۶۷۸۰", None],
        }
    )

    prepared = _prepare_dataframe_for_excel(raw)

    output_path = tmp_path / "mobiles_strings.xlsx"
    write_xlsx_atomic({"Sheet1": raw}, output_path)
    workbook = openpyxl.load_workbook(output_path)
    sheet = workbook["Sheet1"]
    excel_values_mobile = [sheet["A2"].value, sheet["A3"].value, sheet["A4"].value]
    excel_values_contact = [sheet["B2"].value, sheet["B3"].value, sheet["B4"].value]

    expected_mobile = ["09171075740", "09171075740", ""]
    expected_contact = ["09351234567", "09123456780", ""]

    assert prepared["student_mobile"].fillna("").tolist() == expected_mobile
    assert prepared["contact1_mobile"].fillna("").tolist() == expected_contact
    assert excel_values_mobile == ["09171075740", "09171075740", None]
    assert excel_values_contact == ["09351234567", "09123456780", None]
