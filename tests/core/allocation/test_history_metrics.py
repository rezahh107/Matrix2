"""تست‌های ماژول history_metrics."""

from __future__ import annotations

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from app.core.allocation.dedupe import HistoryStatus
from app.core.allocation.history_metrics import compute_history_metrics


def _build_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": [1, 2, 3, 4, 5],
            "allocation_channel": [
                "SCHOOL",
                "SCHOOL",
                "SCHOOL",
                "GOLESTAN",
                "GOLESTAN",
            ],
            "history_status": [
                HistoryStatus.ALREADY_ALLOCATED.value,
                HistoryStatus.NO_HISTORY_MATCH.value,
                HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value,
                HistoryStatus.ALREADY_ALLOCATED.value,
                HistoryStatus.NO_HISTORY_MATCH.value,
            ],
            "same_history_mentor": [True, False, False, False, True],
        }
    )


def test_compute_history_metrics_basic() -> None:
    summary_df = _build_sample_df()

    metrics = compute_history_metrics(summary_df)

    expected_columns = [
        "allocation_channel",
        "students_total",
        "history_already_allocated",
        "history_no_history_match",
        "history_missing_or_invalid",
        "same_history_mentor_true",
        "same_history_mentor_ratio",
    ]
    assert list(metrics.columns) == expected_columns
    assert len(metrics) == 2
    assert list(metrics["allocation_channel"]) == ["GOLESTAN", "SCHOOL"]

    golestan = metrics.loc[metrics["allocation_channel"] == "GOLESTAN"].iloc[0]
    school = metrics.loc[metrics["allocation_channel"] == "SCHOOL"].iloc[0]

    assert golestan["students_total"] == 2
    assert golestan["history_already_allocated"] == 1
    assert golestan["history_no_history_match"] == 1
    assert golestan["history_missing_or_invalid"] == 0
    assert golestan["same_history_mentor_true"] == 1
    assert golestan["same_history_mentor_ratio"] == 0.5

    assert school["students_total"] == 3
    assert school["history_already_allocated"] == 1
    assert school["history_no_history_match"] == 1
    assert school["history_missing_or_invalid"] == 1
    assert school["same_history_mentor_true"] == 1
    assert school["same_history_mentor_ratio"] == pytest.approx(1 / 3)


def test_compute_history_metrics_empty_input() -> None:
    summary_df = pd.DataFrame(
        columns=["student_id", "allocation_channel", "history_status", "same_history_mentor"]
    )

    metrics = compute_history_metrics(summary_df)

    expected_columns = [
        "allocation_channel",
        "students_total",
        "history_already_allocated",
        "history_no_history_match",
        "history_missing_or_invalid",
        "same_history_mentor_true",
        "same_history_mentor_ratio",
    ]

    assert metrics.empty
    assert list(metrics.columns) == expected_columns
    assert len(metrics.index) == 0


def test_compute_history_metrics_idempotent() -> None:
    summary_df = _build_sample_df()

    first = compute_history_metrics(summary_df)
    second = compute_history_metrics(summary_df)

    assert_frame_equal(first, second)
