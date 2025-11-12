"""تست‌های واحد برای موتور شمارندهٔ دانش‌آموز."""

from __future__ import annotations

import pandas as pd
import pytest
from pandas import testing as pd_testing

from app.core.counter import (
    assert_unique_student_ids,
    assign_counters,
    infer_year_strict,
)


def _frame(columns: list[str], rows: list[tuple[object, ...]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns)


def test_assign_counters_reuse_and_gender_sequences() -> None:
    students = _frame(
        ["national_id", "gender"],
        [
            ("0012345678", 1),
            ("0099999999", 1),
            ("0088888888", 0),
        ],
    )
    prior = _frame(
        ["national_id", "student_id"],
        [("0012345678", "533570042")],
    )
    current = _frame(
        ["student_id"],
        [("543570009"), ("543730041")],
    )

    result = assign_counters(
        students,
        prior_roster_df=prior,
        current_roster_df=current,
        academic_year=1404,
    )

    assert result.tolist() == ["533570042", "543570010", "543730042"]
    assert all(len(value) == 9 for value in result)

    summary = result.attrs.get("counter_summary", {})
    assert summary.get("reused_count") == 1
    assert summary.get("new_male_count") == 1
    assert summary.get("new_female_count") == 1


def test_assign_counters_zero_padding_from_scratch() -> None:
    students = pd.DataFrame({"national_id": ["1"], "gender": [0]})

    result = assign_counters(
        students,
        prior_roster_df=None,
        current_roster_df=None,
        academic_year=1404,
    )

    assert result.iloc[0] == "543730001"


def test_assign_counters_deterministic_sorting() -> None:
    students = pd.DataFrame(
        {
            "national_id": ["0000000001", "0000000003", "0000000002"],
            "gender": [1, 0, 1],
        },
        index=[5, 6, 7],
    )

    first = assign_counters(
        students,
        prior_roster_df=None,
        current_roster_df=None,
        academic_year=1404,
    )
    second = assign_counters(
        students.iloc[::-1],
        prior_roster_df=None,
        current_roster_df=None,
        academic_year=1404,
    )

    pd_testing.assert_series_equal(first.sort_index(), second.sort_index())


def test_assign_counters_duplicate_rows_raise_on_assert() -> None:
    students = pd.DataFrame({"national_id": ["1", "1"], "gender": [1, 1]})

    counters = assign_counters(
        students,
        prior_roster_df=None,
        current_roster_df=None,
        academic_year=1404,
    )

    assert counters.iloc[0] == counters.iloc[1]
    summary = counters.attrs.get("counter_summary", {})
    assert summary.get("reused_count") == 1
    with pytest.raises(ValueError):
        assert_unique_student_ids(counters)


def test_assign_counters_overflow_guard() -> None:
    students = pd.DataFrame({"national_id": ["123"], "gender": [1]})
    current = pd.DataFrame({"student_id": ["543579999"]})

    with pytest.raises(ValueError):
        assign_counters(
            students,
            prior_roster_df=None,
            current_roster_df=current,
            academic_year=1404,
        )


def test_infer_year_strict_handles_ambiguity() -> None:
    frame = pd.DataFrame({"student_id": ["543570001", "543730010"]})
    assert infer_year_strict(frame) == 1404

    ambiguous = pd.DataFrame({"student_id": ["533570001", "543730010"]})
    assert infer_year_strict(ambiguous) is None
