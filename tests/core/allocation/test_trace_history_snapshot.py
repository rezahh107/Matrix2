from __future__ import annotations

import pandas as pd

from app.core.allocation.trace import attach_history_snapshot


def test_attach_history_snapshot_enriches_summary() -> None:
    summary_df = pd.DataFrame(
        [
            {"student_id": 1, "final_status": "ALLOCATED"},
            {"student_id": 2, "final_status": "UNALLOCATED"},
            {"student_id": 3, "final_status": "ALLOCATED"},
        ]
    )
    history_info_df = pd.DataFrame(
        [
            {
                "student_id": 1,
                "history_mentor_id": 101,
                "history_center_code": 11,
            },
            {
                "student_id": 3,
                "history_mentor_id": 303,
                "history_center_code": 33,
            },
        ]
    )

    enriched = attach_history_snapshot(summary_df, history_info_df, key_column="student_id")

    assert list(enriched.columns) == [
        "student_id",
        "final_status",
        "history_mentor_id",
        "history_center_code",
    ]
    assert enriched.loc[0, "history_mentor_id"] == 101
    assert enriched.loc[0, "history_center_code"] == 11
    assert pd.isna(enriched.loc[1, "history_mentor_id"])
    assert pd.isna(enriched.loc[1, "history_center_code"])
    assert enriched.loc[2, "history_mentor_id"] == 303
    assert enriched.loc[2, "history_center_code"] == 33


def test_attach_history_snapshot_empty_summary_returns_copy() -> None:
    summary_df = pd.DataFrame(columns=["student_id", "final_status"])
    history_info_df = pd.DataFrame(
        [
            {
                "student_id": 1,
                "history_mentor_id": 101,
                "history_center_code": 11,
            }
        ]
    )

    enriched = attach_history_snapshot(summary_df, history_info_df)

    assert enriched.empty
    assert list(enriched.columns) == ["student_id", "final_status"]
