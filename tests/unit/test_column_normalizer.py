import pandas as pd

from app.core.common.column_normalizer import ColumnNormalizationReport, normalize_input_columns
from app.core.common.columns import ensure_series


def test_normalize_input_columns_adds_aliases_and_cleans_values() -> None:
    df = pd.DataFrame(
        {
            "کد مدرسه ": ["4001.0", " ۴۰۰۲"],
            "نام مدرسه 1": ["مدرسه‌ی نمونه", None],
            "کد مدرسه 1": ["00123", "۱۲۳۴"],
            "کد آموزش و پرورش": [pd.NA, " 987 "],
            "ستون اضافی": ["x", "y"],
            "نام مدرسه جدید": ["نمونه ۱", "نمونه ۲"],
        }
    )

    normalized, report = normalize_input_columns(df, kind="SchoolReport")

    assert list(normalized["کد مدرسه"]) == [4001, 4002]
    assert list(normalized["school_code"]) == ["4001", "4002"]
    assert list(normalized["کد مدرسه 1"].astype("Int64")) == [123, 1234]
    assert list(normalized["school_code_1"]) == ["123", "1234"]
    assert list(normalized["نام مدرسه 1"]) == ["مدرسه ی نمونه", ""]
    assert list(normalized["school_name_1"]) == ["مدرسه ی نمونه", ""]
    edu_numeric = normalized["کد آموزش و پرورش"].astype("Int64")
    assert pd.isna(edu_numeric.iloc[0])
    assert edu_numeric.iloc[1] == 987
    assert list(normalized["edu_code"]) == ["", "987"]
    assert "نام مدرسه جدید" in report.unmatched


def test_normalize_input_columns_handles_english_headers() -> None:
    df = pd.DataFrame({"school_code": ["4001", "4002"]})

    normalized, report = normalize_input_columns(df, kind="InspactorReport")

    assert "کد مدرسه" in normalized.columns
    assert list(normalized["کد مدرسه"]) == [4001, 4002]
    assert not report.unmatched


def test_normalize_input_columns_handles_duplicate_numeric_columns() -> None:
    df = pd.DataFrame([["4001", "4002"]], columns=["کد مدرسه", "کد مدرسه"])

    normalized, _ = normalize_input_columns(
        df, kind="SchoolReport", include_alias=False, report=False
    )

    values = ensure_series(normalized["کد مدرسه"]).astype("Int64")
    assert list(values) == [4001]


def test_normalize_input_columns_collector_runs_even_when_report_disabled() -> None:
    df = pd.DataFrame({"کد مدرسه": ["4001"], "نام مدرسه": ["نمونه"]})
    captured: list[ColumnNormalizationReport] = []

    normalize_input_columns(
        df,
        kind="SchoolReport",
        report=False,
        collector=captured.append,
    )

    assert len(captured) == 1
    assert captured[0].aliases_added  # "school_code" + "school_name"
