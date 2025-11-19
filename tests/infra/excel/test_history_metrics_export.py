"""تست شیت HistoryMetrics در خروجی دیباگ تخصیص."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pandas.testing import assert_frame_equal

from app.core.allocation.dedupe import HistoryStatus
from app.core.allocation.engine import enrich_summary_with_history
from app.core.allocation.history_metrics import METRIC_COLUMNS, compute_history_metrics
from app.core.policy.config import AllocationChannelConfig
from app.infra.excel.export_allocations import collect_trace_debug_sheets


@dataclass(frozen=True)
class _Columns:
    school_code: str


class _FakePolicy:
    """پالیسی حداقلی برای تست تاریخچهٔ دیباگ."""

    def __init__(self) -> None:
        self.columns = _Columns(school_code="school_code")
        self.allocation_channels = AllocationChannelConfig(
            school_codes=(10,),
            center_channels={"GOLESTAN": (1,), "SADRA": (2,)},
            registration_center_column="registration_center",
            educational_status_column="student_educational_status",
            active_status_values=(0,),
        )
        self._stage_columns = {"center": "center_code"}

    def stage_column(self, stage: str) -> str:
        if stage not in self._stage_columns:
            raise KeyError(stage)
        return self._stage_columns[stage]


def _build_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"student_id": 1, "mentor_id": 11, "final_status": "ALLOCATED"},
            {"student_id": 2, "mentor_id": 22, "final_status": "ALLOCATED"},
            {"student_id": 3, "mentor_id": 33, "final_status": "UNALLOCATED"},
        ]
    )


def _build_students_df() -> pd.DataFrame:
    return pd.DataFrame(
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
                "registration_center": 1,
                "student_educational_status": 0,
            },
            {
                "student_id": 3,
                "school_code": 0,
                "center_code": 2,
                "registration_center": 2,
                "student_educational_status": 0,
            },
        ]
    )


def _build_history_info_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "student_id": 1,
                "history_status": HistoryStatus.ALREADY_ALLOCATED.value,
                "dedupe_reason": "history_match",
                "history_mentor_id": 11,
                "history_center_code": 0,
            },
            {
                "student_id": 2,
                "history_status": HistoryStatus.NO_HISTORY_MATCH.value,
                "dedupe_reason": "no_history_match",
                "history_mentor_id": 999,
                "history_center_code": 1,
            },
            {
                "student_id": 3,
                "history_status": HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value,
                "dedupe_reason": "missing_or_invalid_national_code",
                "history_mentor_id": None,
                "history_center_code": None,
            },
        ]
    )


def _build_trace(summary_df: pd.DataFrame) -> pd.DataFrame:
    trace_df = pd.DataFrame({"student_id": summary_df["student_id"]})
    trace_df.attrs["summary_df"] = summary_df
    trace_df.attrs["final_status_counts"] = summary_df["final_status"].value_counts()
    trace_df.attrs["unallocated_summary"] = pd.DataFrame()
    trace_df.attrs["policy_violations"] = pd.DataFrame()
    return trace_df


def test_collect_trace_debug_sheets_emits_history_metrics() -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()
    history_info_df = _build_history_info_df()
    trace_df = _build_trace(summary_df)

    sheets = collect_trace_debug_sheets(
        trace_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )

    assert "HistoryMetrics" in sheets
    enriched = enrich_summary_with_history(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )
    expected_metrics = compute_history_metrics(enriched)
    assert_frame_equal(sheets["HistoryMetrics"], expected_metrics)


def test_collect_trace_debug_sheets_without_history_info_returns_empty_sheet() -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()
    trace_df = _build_trace(summary_df)

    sheets = collect_trace_debug_sheets(
        trace_df,
        students_df=students_df,
        history_info_df=None,
        policy=policy,
    )

    metrics = sheets["HistoryMetrics"]
    assert metrics.empty
    assert list(metrics.columns) == METRIC_COLUMNS


def test_collect_trace_debug_sheets_history_metrics_idempotent() -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()
    history_info_df = _build_history_info_df()
    trace_df = _build_trace(summary_df)

    first = collect_trace_debug_sheets(
        trace_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )["HistoryMetrics"]
    second = collect_trace_debug_sheets(
        trace_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )["HistoryMetrics"]

    assert_frame_equal(first, second)
