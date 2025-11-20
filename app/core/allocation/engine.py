"""ابزارهای کمکی سطح Engine برای برچسب‌گذاری کانال تخصیص."""

from __future__ import annotations

import pandas as pd

from typing import Mapping

from app.core.policy_loader import PolicyConfig

from .channels import AllocationChannel, derive_channels_for_students
from .mentor_pool import filter_active_mentors


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
    channel_series = pd.Series(
        channel_strings.values, index=students_df["student_id"].values
    )
    if not channel_series.index.is_unique:
        channel_series = channel_series[~channel_series.index.duplicated(keep="first")]
    return channel_series


__all__ = [
    "AllocationChannel",
    "annotate_students_with_channel",
    "derive_channel_map",
    "build_mentor_pool",
    "enrich_summary_with_history",
]


def build_mentor_pool(
    mentors_df: pd.DataFrame,
    *,
    policy: PolicyConfig,
    overrides: Mapping[int | str | float, bool] | None = None,
    attach_status: bool = False,
    status_column: str = "mentor_status",
) -> pd.DataFrame:
    """اعمال حاکمیت استخر پشتیبان‌ها و بازگرداندن دیتافریم فعال.

    این تابع هیچ تغییری در ۶ کلید join یا سیاست رتبه‌بندی ایجاد نمی‌کند و تنها
    براساس تنظیمات Policy و overrideهای نوبتی، منتورهای غیرفعال را حذف می‌کند.
    خروجی برای ورودی برابر، دترمینیستیک است و در صورت نیاز ستون وضعیت نیز
    به خروجی افزوده می‌شود.
    """

    return filter_active_mentors(
        mentors_df,
        policy.mentor_pool_governance,
        overrides=overrides,
        attach_status=attach_status,
        status_column=status_column,
    )


def enrich_summary_with_history(
    summary_df: pd.DataFrame,
    *,
    students_df: pd.DataFrame,
    history_info_df: pd.DataFrame | None,
    policy: PolicyConfig,
) -> pd.DataFrame:
    """تکمیل خلاصهٔ تریس با کانال تخصیص و داده‌های تاریخچه.

    این تابع به ترتیب زیر ستون‌های تشریحی را اضافه می‌کند:
    1. ``allocation_channel`` بر اساس PolicyConfig.
    2. ``history_status`` و ``dedupe_reason`` در صورت وجود اطلاعات تاریخچه.
    3. ستون‌های اسنپ‌شات تاریخچه (مثلاً ``history_mentor_id``) برای استفادهٔ Trace.
    4. ستون بولی ``same_history_mentor`` که تنها بعد از دریافت اسنپ‌شات محاسبه می‌شود.

    ترتیب ردیف‌ها و کلیدهای اصلی تغییری نمی‌کند و منطق رتبه‌بندی همچنان دست‌نخورده
    باقی می‌ماند؛ ستون‌های جدید صرفاً برای تشخیص و گزارش‌گیری هستند.
    """

    if summary_df is None:
        raise ValueError("summary_df نباید None باشد")

    from .trace import (  # واردات تنبل برای جلوگیری از وابستگی حلقوی
        attach_allocation_channel,
        attach_history_flags,
        attach_history_snapshot,
        attach_same_history_mentor,
    )

    result = attach_allocation_channel(summary_df, students_df, policy=policy)
    if history_info_df is None:
        return result
    result = attach_history_flags(result, history_info_df, key_column="student_id")
    result = attach_history_snapshot(result, history_info_df, key_column="student_id")
    result = attach_same_history_mentor(
        result,
        history_info_df,
        key_column="student_id",
        mentor_column="mentor_id",
        history_mentor_column="history_mentor_id",
    )
    return result
