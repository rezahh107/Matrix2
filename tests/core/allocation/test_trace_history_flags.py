from __future__ import annotations

import pandas as pd

from app.core.allocation.trace import attach_history_flags


def test_attach_history_flags_enriches_summary() -> None:
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
                "history_status": "already_allocated",
                "dedupe_reason": "history_match",
            },
            {
                "student_id": 3,
                "history_status": "no_history_match",
                "dedupe_reason": "no_history_match",
            },
        ]
    )

    enriched = attach_history_flags(summary_df, history_info_df, key_column="student_id")

    assert list(enriched.columns) == [
        "student_id",
        "final_status",
        "history_status",
        "dedupe_reason",
    ]
    assert enriched.loc[0, "history_status"] == "already_allocated"
    assert enriched.loc[0, "dedupe_reason"] == "history_match"
    assert enriched.loc[1, "history_status"] == ""
    assert enriched.loc[1, "dedupe_reason"] == ""
    assert enriched.loc[2, "history_status"] == "no_history_match"
    assert enriched.loc[2, "dedupe_reason"] == "no_history_match"


def test_attach_history_flags_prefers_latest_history_record() -> None:
    summary_df = pd.DataFrame([{"student_id": 1}])
    history_info_df = pd.DataFrame(
        [
            {"student_id": 1, "history_status": "no_history_match", "dedupe_reason": "old"},
            {"student_id": 1, "history_status": "already_allocated", "dedupe_reason": "new"},
        ]
    )

    enriched = attach_history_flags(summary_df, history_info_df)

    assert enriched.loc[0, "history_status"] == "already_allocated"
    assert enriched.loc[0, "dedupe_reason"] == "new"


def test_attach_history_flags_empty_summary_returns_copy() -> None:
    summary_df = pd.DataFrame(columns=["student_id", "final_status"])
    history_info_df = pd.DataFrame(
        [{"student_id": 1, "history_status": "already_allocated", "dedupe_reason": "history_match"}]
    )

    enriched = attach_history_flags(summary_df, history_info_df)

    assert enriched.empty
    assert list(enriched.columns) == ["student_id", "final_status"]
