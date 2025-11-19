from __future__ import annotations

import enum
from typing import Iterable

import pandas as pd

from app.core.policy.config import AllocationChannelConfig
from app.core.policy_loader import PolicyConfig


class AllocationChannel(str, enum.Enum):
    """کانال‌های استاندارد تخصیص دانش‌آموز."""

    SCHOOL = "SCHOOL"
    GOLESTAN = "GOLESTAN"
    SADRA = "SADRA"
    GENERIC = "GENERIC"


def _to_int_safe(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _column_as_int(df: pd.DataFrame, column: str | None) -> pd.Series | None:
    if column is None or column not in df.columns:
        return None
    numeric = pd.to_numeric(df[column], errors="coerce")
    return numeric.astype("Int64")


def _active_student_mask(students_df: pd.DataFrame, policy: PolicyConfig) -> pd.Series:
    rules = policy.allocation_channels
    if not rules.active_status_values:
        return pd.Series(True, index=students_df.index, dtype=bool)
    column = rules.educational_status_column
    if not column or column not in students_df.columns:
        return pd.Series(True, index=students_df.index, dtype=bool)
    status_series = _column_as_int(students_df, column)
    if status_series is None:
        return pd.Series(True, index=students_df.index, dtype=bool)
    mask = status_series.isin(rules.active_status_values) | status_series.isna()
    return mask.fillna(True)


def _apply_center_channel(
    result: pd.Series,
    center_values: pd.Series | None,
    *,
    rules: AllocationChannelConfig,
    channel: AllocationChannel,
) -> None:
    if center_values is None:
        return
    center_ids: Iterable[int] = rules.center_channels.get(channel.value, tuple())
    if not center_ids:
        return
    mask = center_values.isin(center_ids)
    if mask.empty:
        return
    unresolved = result == AllocationChannel.GENERIC
    result.loc[mask & unresolved] = channel


def _channel_for_center(center_value: object, policy: PolicyConfig) -> AllocationChannel | None:
    center_id = _to_int_safe(center_value)
    if center_id is None:
        return None
    rules = policy.allocation_channels
    for channel in (AllocationChannel.GOLESTAN, AllocationChannel.SADRA):
        ids: Iterable[int] = rules.center_channels.get(channel.value, tuple())
        if center_id in ids:
            return channel
    return None


def _is_active_student(student: pd.Series, policy: PolicyConfig) -> bool:
    rules = policy.allocation_channels
    if not rules.active_status_values:
        return True
    column = rules.educational_status_column
    if not column or column not in student:
        return True
    status_value = _to_int_safe(student.get(column))
    return status_value is None or status_value in rules.active_status_values


def derive_allocation_channel(student: pd.Series, policy: PolicyConfig) -> AllocationChannel:
    """کانال تخصیص دانش‌آموز را طبق Policy برمی‌گرداند.

    ابتدا دانش‌آموزان با کد مدرسهٔ موجود در ``allocation_channels.school_codes`` و
    وضعیت تحصیلی فعال در کانال SCHOOL قرار می‌گیرند؛ سپس ستون مرحلهٔ مرکز و ستون
    ثبت‌نام (در صورت تعریف) برای تشخیص GOLESTAN/SADRA بررسی می‌شوند و در صورت عدم
    تطابق، مقدار GENERIC بازگردانده می‌شود.

    مثال:
        >>> import pandas as pd  # doctest: +SKIP
        >>> policy = ...  # پیکربندی PolicyConfig با قوانین کانال  # doctest: +SKIP
        >>> row = pd.Series({"کد مدرسه": 10, "student_educational_status": 0})  # doctest: +SKIP
        >>> derive_allocation_channel(row, policy)  # doctest: +SKIP
        <AllocationChannel.SCHOOL: 'SCHOOL'>
    """

    rules = policy.allocation_channels
    school_code = _to_int_safe(student.get(policy.columns.school_code))
    if school_code is not None and school_code in rules.school_codes:
        if _is_active_student(student, policy):
            return AllocationChannel.SCHOOL

    try:
        center_column = policy.stage_column("center")
    except KeyError:
        center_column = None
    if center_column and center_column in student:
        center_channel = _channel_for_center(student.get(center_column), policy)
        if center_channel:
            return center_channel

    registration_column = rules.registration_center_column
    if registration_column and registration_column in student:
        registration_channel = _channel_for_center(student.get(registration_column), policy)
        if registration_channel:
            return registration_channel

    return AllocationChannel.GENERIC


def derive_channels_for_students(
    students_df: pd.DataFrame, policy: PolicyConfig
) -> pd.Series:
    """برچسب‌گذاری کانال تخصیص به‌صورت برداری و دترمینیسیک."""

    if students_df.empty:
        return pd.Series(dtype=object, index=students_df.index)

    result = pd.Series(
        [AllocationChannel.GENERIC] * len(students_df), index=students_df.index, dtype=object
    )
    rules = policy.allocation_channels

    school_column = policy.columns.school_code
    if rules.school_codes and school_column in students_df.columns:
        school_series = _column_as_int(students_df, school_column)
        if school_series is not None:
            active_mask = _active_student_mask(students_df, policy)
            school_mask = school_series.isin(rules.school_codes) & active_mask
            result.loc[school_mask] = AllocationChannel.SCHOOL

    try:
        center_column = policy.stage_column("center")
    except KeyError:
        center_column = None

    for column in (center_column, rules.registration_center_column):
        series = _column_as_int(students_df, column)
        _apply_center_channel(
            result, series, rules=rules, channel=AllocationChannel.GOLESTAN
        )
        _apply_center_channel(
            result, series, rules=rules, channel=AllocationChannel.SADRA
        )

    return result


__all__ = [
    "AllocationChannel",
    "derive_allocation_channel",
    "derive_channels_for_students",
]
