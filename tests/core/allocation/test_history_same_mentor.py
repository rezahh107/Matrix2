from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pandas.testing import assert_frame_equal

from app.core.allocation.engine import enrich_summary_with_history
from app.core.allocation.trace import attach_same_history_mentor
from app.core.policy.config import AllocationChannelConfig


@dataclass(frozen=True)
class _Columns:
    school_code: str


class _FakePolicy:
    """پالیسی حداقلی برای تست الحاق کانال و پرچم پشتیبان تاریخچه."""

    def __init__(self) -> None:
        self.columns = _Columns(school_code="school_code")
        self._stage_columns = {"center": "center_code"}
        self.allocation_channels = AllocationChannelConfig(
            school_codes=(10,),
            center_channels={"GOLESTAN": (1,), "SADRA": (2,)},
            registration_center_column="registration_center",
            educational_status_column="student_educational_status",
            active_status_values=(0,),
        )

    def stage_column(self, stage: str) -> str:
        return self._stage_columns[stage]


def test_attach_same_history_mentor_direct_helper() -> None:
    summary_df = pd.DataFrame(
        [
            {"student_id": 1, "mentor_id": 101, "final_status": "ALLOCATED"},
            {"student_id": 2, "mentor_id": 202, "final_status": "ALLOCATED"},
            {"student_id": 3, "mentor_id": 303, "final_status": "UNALLOCATED"},
        ]
    )
    history_info_df = pd.DataFrame(
        [
            {"student_id": 1, "history_mentor_id": 101},
            {"student_id": 2, "history_mentor_id": 999},
        ]
    )

    enriched = attach_same_history_mentor(summary_df, history_info_df)
    assert list(enriched["student_id"]) == [1, 2, 3]
    assert "same_history_mentor" in enriched.columns
    assert list(enriched["same_history_mentor"]) == [True, False, False]
    assert enriched["same_history_mentor"].dtype == bool

    enriched_again = attach_same_history_mentor(summary_df, history_info_df)
    assert_frame_equal(enriched, enriched_again)


def test_attach_same_history_mentor_with_empty_summary() -> None:
    summary_df = pd.DataFrame(columns=["student_id", "mentor_id"])
    history_info_df = pd.DataFrame(columns=["student_id", "history_mentor_id"])

    enriched = attach_same_history_mentor(summary_df, history_info_df)

    assert enriched.empty
    assert "same_history_mentor" in enriched.columns
    assert enriched["same_history_mentor"].dtype == bool


def test_enrich_summary_with_history_integration() -> None:
    policy = _FakePolicy()
    summary_df = pd.DataFrame(
        [
            {"student_id": 1, "mentor_id": 500, "final_status": "ALLOCATED"},
            {"student_id": 2, "mentor_id": 600, "final_status": "ALLOCATED"},
            {"student_id": 3, "mentor_id": 700, "final_status": "UNALLOCATED"},
        ]
    )
    students_df = pd.DataFrame(
        [
            {
                "student_id": 1,
                "school_code": 10,
                "center_code": 0,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 2,
                "school_code": 0,
                "center_code": 1,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 3,
                "school_code": 0,
                "center_code": 2,
                "registration_center": 0,
                "student_educational_status": 0,
            },
        ]
    )
    history_info_df = pd.DataFrame(
        [
            {
                "student_id": 1,
                "history_status": "already_allocated",
                "dedupe_reason": "history_match",
                "history_mentor_id": 500,
                "history_center_code": 11,
            },
            {
                "student_id": 2,
                "history_status": "no_history_match",
                "dedupe_reason": "no_history_match",
                "history_mentor_id": 999,
                "history_center_code": 22,
            },
            {
                "student_id": 3,
                "history_status": "no_history_match",
                "dedupe_reason": "no_history_match",
                "history_mentor_id": None,
                "history_center_code": None,
            },
        ]
    )

    enriched = enrich_summary_with_history(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )

    assert "same_history_mentor" in enriched.columns
    assert "allocation_channel" in enriched.columns
    assert "history_status" in enriched.columns
    assert "dedupe_reason" in enriched.columns
    assert "history_mentor_id" in enriched.columns
    assert "history_center_code" in enriched.columns
    assert list(enriched["student_id"]) == [1, 2, 3]
    assert enriched["same_history_mentor"].dtype == bool

    student_flags = enriched.set_index("student_id")["same_history_mentor"].to_dict()
    assert student_flags[1] is True
    assert student_flags[2] is False
    assert student_flags[3] is False

    history_values = enriched.set_index("student_id")["history_mentor_id"].to_dict()
    assert history_values[1] == 500
    assert history_values[2] == 999
    assert pd.isna(history_values[3])

    center_values = enriched.set_index("student_id")["history_center_code"].to_dict()
    assert center_values[1] == 11
    assert center_values[2] == 22
    assert pd.isna(center_values[3])

    enriched_again = enrich_summary_with_history(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )
    assert_frame_equal(enriched.sort_index(axis=1), enriched_again.sort_index(axis=1))
