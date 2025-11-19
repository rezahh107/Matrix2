"""تست لاگ‌گیری خلاصهٔ تاریخچه در CLI تخصیص."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pandas.testing import assert_frame_equal

from app.core.allocation.dedupe import HistoryStatus
from app.core.allocation.engine import enrich_summary_with_history
from app.core.allocation.history_metrics import compute_history_metrics
from app.core.policy.config import AllocationChannelConfig
from app.infra.cli import _log_history_metrics


@dataclass(frozen=True)
class _Columns:
    school_code: str


class _FakePolicy:
    """پالیسی حداقلی برای تست لاگ تاریخچه."""

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


def test_log_history_metrics_with_history(caplog) -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()
    history_info_df = _build_history_info_df()

    caplog.set_level("INFO")

    _log_history_metrics(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )

    enriched = enrich_summary_with_history(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )
    expected_metrics = compute_history_metrics(enriched)

    logged_lines = [rec.message for rec in caplog.records if rec.message.startswith("HistoryMetrics[")]
    assert len(logged_lines) == len(expected_metrics)

    for logged, (_, row) in zip(logged_lines, expected_metrics.iterrows()):
        expected_line = (
            "HistoryMetrics[channel=%s] total=%d already=%d no_match=%d missing=%d same_mentor=%d ratio=%.3f"
            % (
                row["allocation_channel"],
                row["students_total"],
                row["history_already_allocated"],
                row["history_no_history_match"],
                row["history_missing_or_invalid"],
                row["same_history_mentor_true"],
                row["same_history_mentor_ratio"],
            )
        )
        assert logged == expected_line


def test_log_history_metrics_without_history(caplog) -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()

    caplog.set_level("INFO")
    _log_history_metrics(
        summary_df,
        students_df=students_df,
        history_info_df=None,
        policy=policy,
    )

    messages = [rec.message for rec in caplog.records]
    assert "History metrics unavailable (no history info)." in messages


def test_log_history_metrics_idempotent(caplog) -> None:
    policy = _FakePolicy()
    summary_df = _build_summary_df()
    students_df = _build_students_df()
    history_info_df = _build_history_info_df()

    caplog.set_level("INFO")

    _log_history_metrics(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )
    first_messages = [rec.message for rec in caplog.records]

    caplog.clear()
    caplog.set_level("INFO")
    _log_history_metrics(
        summary_df,
        students_df=students_df,
        history_info_df=history_info_df,
        policy=policy,
    )
    second_messages = [rec.message for rec in caplog.records]

    assert_frame_equal(
        compute_history_metrics(
            enrich_summary_with_history(
                summary_df,
                students_df=students_df,
                history_info_df=history_info_df,
                policy=policy,
            )
        ),
        compute_history_metrics(
            enrich_summary_with_history(
                summary_df,
                students_df=students_df,
                history_info_df=history_info_df,
                policy=policy,
            )
        ),
    )
    assert first_messages == second_messages
