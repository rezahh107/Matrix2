from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from app.core.allocation.dedupe import (
    HISTORY_SNAPSHOT_COLUMNS,
    HistoryStatus,
    dedupe_by_national_id,
)


def _build_students_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"student_id": 1, "name": "allocated", "national_code": "0012345678"},
            {"student_id": 2, "name": "fresh", "national_code": "9876543210"},
            {"student_id": 3, "name": "invalid", "national_code": "123"},
        ]
    )


def _build_history_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "student_id": 100,
                "national_code": "0012345678",
                "mentor_id": 900,
                "مرکز گلستان صدرا": 10,
            },
        ]
    )


def test_dedupe_history_status_labels() -> None:
    students = _build_students_df()
    history = _build_history_df()

    already_allocated_df, new_candidates_df = dedupe_by_national_id(students, history)

    expected_columns = {"history_status", "dedupe_reason", *HISTORY_SNAPSHOT_COLUMNS}
    assert expected_columns.issubset(already_allocated_df.columns)
    assert expected_columns.issubset(new_candidates_df.columns)

    allocated_row = already_allocated_df.set_index("student_id").loc[1]
    assert allocated_row["history_status"] == HistoryStatus.ALREADY_ALLOCATED.value
    assert allocated_row["dedupe_reason"] == "history_match"

    new_rows = new_candidates_df.set_index("student_id")
    assert new_rows.loc[2, "history_status"] == HistoryStatus.NO_HISTORY_MATCH.value
    assert new_rows.loc[2, "dedupe_reason"] == "no_history_match"
    assert (
        new_rows.loc[3, "history_status"]
        == HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value
    )
    assert new_rows.loc[3, "dedupe_reason"] == "missing_or_invalid_national_code"



def test_dedupe_idempotent_with_history_status() -> None:
    students = _build_students_df()
    history = _build_history_df()

    first_allocated, first_new = dedupe_by_national_id(students, history)
    second_allocated, second_new = dedupe_by_national_id(students, history)

    assert_frame_equal(first_allocated, second_allocated)
    assert_frame_equal(first_new, second_new)


def test_dedupe_history_snapshot_mentor_id() -> None:
    students = pd.DataFrame(
        [
            {"student_id": 1, "name": "allocated", "national_code": "0012345678"},
            {"student_id": 2, "name": "fresh", "national_code": "9876543210"},
            {"student_id": 3, "name": "invalid", "national_code": None},
        ]
    )
    history = pd.DataFrame(
        [
            {
                "row": 1,
                "national_code": "0012345678",
                "mentor_id": 111,
                "مرکز گلستان صدرا": 20,
            },
            {
                "row": 2,
                "national_code": "0012345678",
                "mentor_id": 222,
                "مرکز گلستان صدرا": 30,
            },
        ]
    )

    already_allocated_df, new_candidates_df = dedupe_by_national_id(students, history)

    allocated = already_allocated_df.set_index("student_id")
    assert allocated.loc[1, "history_mentor_id"] == 222
    assert allocated.loc[1, "history_center_code"] == 30

    new_candidates = new_candidates_df.set_index("student_id")
    assert pd.isna(new_candidates.loc[2, "history_mentor_id"])
    assert pd.isna(new_candidates.loc[2, "history_center_code"])
    assert pd.isna(new_candidates.loc[3, "history_mentor_id"])
    assert pd.isna(new_candidates.loc[3, "history_center_code"])
