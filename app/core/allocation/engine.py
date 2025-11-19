"""ابزارهای کمکی سطح Engine برای برچسب‌گذاری کانال تخصیص."""

from __future__ import annotations

import pandas as pd

from app.core.policy_loader import PolicyConfig

from .channels import AllocationChannel, derive_channels_for_students


def annotate_students_with_channel(
    students_df: pd.DataFrame, policy: PolicyConfig
) -> pd.DataFrame:
    """نسخهٔ کپی‌شده از DataFrame دانش‌آموزان با ستون کانال تخصیص."""

    channels = derive_channels_for_students(students_df, policy)
    channel_strings = channels.map(lambda item: item.value)
    result = students_df.copy()
    result["allocation_channel"] = channel_strings
    return result


def derive_channel_map(students_df: pd.DataFrame, policy: PolicyConfig) -> pd.Series:
    """نگاشت شناسهٔ دانش‌آموز به نام کانال (رشته) را برمی‌گرداند."""

    if "student_id" not in students_df.columns:
        raise KeyError("students_df must contain 'student_id' column for channel mapping")
    channel_strings = derive_channels_for_students(students_df, policy).map(
        lambda item: item.value
    )
    return pd.Series(channel_strings.values, index=students_df["student_id"].values)


__all__ = [
    "AllocationChannel",
    "annotate_students_with_channel",
    "derive_channel_map",
]
