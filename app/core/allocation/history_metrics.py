"""ابزارهای تجمیع تاریخچه تخصیص برای داشبوردها."""

from __future__ import annotations

import pandas as pd

from app.core.allocation.dedupe import HistoryStatus

__all__ = ["compute_history_metrics"]


METRIC_COLUMNS = [
    "allocation_channel",
    "students_total",
    "history_already_allocated",
    "history_no_history_match",
    "history_missing_or_invalid",
    "same_history_mentor_true",
    "same_history_mentor_ratio",
]


def _validate_columns(summary_df: pd.DataFrame) -> None:
    required_columns = {
        "student_id",
        "allocation_channel",
        "history_status",
        "same_history_mentor",
    }
    missing = required_columns.difference(summary_df.columns)
    if missing:
        columns = ", ".join(sorted(missing))
        raise KeyError(f"summary_df is missing required columns: {columns}")


def compute_history_metrics(summary_df: pd.DataFrame) -> pd.DataFrame:
    """محاسبهٔ KPIهای تاریخچه برای هر کانال تخصیص.

    Parameters
    ----------
    summary_df:
        دیتافریم خروجی `enrich_summary_with_history` که حداقل ستون‌های
        `student_id`, `allocation_channel`, `history_status`, و
        `same_history_mentor` را دارد.

    Returns
    -------
    pd.DataFrame
        دیتافریمی مرتب‌شده بر اساس `allocation_channel` که شمارش وضعیت‌های
        تاریخچه و نسبت تطابق منتور قبلی را برای هر کانال نگه می‌دارد.
    """

    _validate_columns(summary_df)

    if summary_df.empty:
        return pd.DataFrame(columns=METRIC_COLUMNS)

    working_df = summary_df.assign(
        same_history_mentor=summary_df["same_history_mentor"].astype(bool)
    )

    grouped = working_df.groupby("allocation_channel", sort=True)

    students_total = grouped.size()
    history_already_allocated = (
        (working_df["history_status"] == HistoryStatus.ALREADY_ALLOCATED.value)
        .groupby(working_df["allocation_channel"])
        .sum()
    )
    history_no_history_match = (
        (working_df["history_status"] == HistoryStatus.NO_HISTORY_MATCH.value)
        .groupby(working_df["allocation_channel"])
        .sum()
    )
    history_missing_or_invalid = (
        (
            working_df["history_status"]
            == HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value
        )
        .groupby(working_df["allocation_channel"])
        .sum()
    )
    same_history_mentor_true = working_df.groupby("allocation_channel")[
        "same_history_mentor"
    ].sum()

    same_history_mentor_ratio = (
        same_history_mentor_true.divide(students_total).fillna(0.0)
    )

    metrics_df = pd.DataFrame(
        {
            "allocation_channel": students_total.index,
            "students_total": students_total.values,
            "history_already_allocated": history_already_allocated.reindex(
                students_total.index, fill_value=0
            ).values,
            "history_no_history_match": history_no_history_match.reindex(
                students_total.index, fill_value=0
            ).values,
            "history_missing_or_invalid": history_missing_or_invalid.reindex(
                students_total.index, fill_value=0
            ).values,
            "same_history_mentor_true": same_history_mentor_true.reindex(
                students_total.index, fill_value=0
            ).values,
            "same_history_mentor_ratio": same_history_mentor_ratio.reindex(
                students_total.index, fill_value=0.0
            ).values,
        }
    )

    return metrics_df.reset_index(drop=True)
