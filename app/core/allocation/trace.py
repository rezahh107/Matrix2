"""ابزارهای الحاق کانال تخصیص و داده‌های تاریخچه به خروجی تریس/خلاصه."""

from __future__ import annotations

import pandas as pd

from app.core.policy_loader import PolicyConfig

from .dedupe import HISTORY_SNAPSHOT_COLUMNS
from .engine import derive_channel_map


def attach_allocation_channel(
    summary_df: pd.DataFrame, students_df: pd.DataFrame, *, policy: PolicyConfig
) -> pd.DataFrame:
    """کپی summary با ستون «allocation_channel» مبتنی بر Policy."""

    if summary_df.empty or "student_id" not in summary_df.columns:
        return summary_df.copy()
    channel_map = derive_channel_map(students_df, policy)
    result = summary_df.copy()
    result["allocation_channel"] = result["student_id"].map(channel_map).fillna("")
    return result


def attach_history_flags(
    summary_df: pd.DataFrame,
    history_info_df: pd.DataFrame,
    *,
    key_column: str = "student_id",
) -> pd.DataFrame:
    """الحاق ستون‌های ``history_status`` و ``dedupe_reason`` به خلاصهٔ تخصیص.

    این تابع هیچ تغییری درجا انجام نمی‌دهد و نسخهٔ جدیدی از ``summary_df`` را برمی‌گرداند.
    اگر برای یک شناسه داده‌ای در ``history_info_df`` یافت نشود، مقدار خنثی (رشتهٔ خالی)
    در هر دو ستون قرار می‌گیرد تا هم‌خوانی با خروجی نهایی حفظ شود.
    """

    if summary_df.empty:
        return summary_df.copy()
    if key_column not in summary_df.columns:
        raise KeyError(f"{key_column!r} در summary_df وجود ندارد")
    if key_column not in history_info_df.columns:
        raise KeyError(f"{key_column!r} در history_info_df وجود ندارد")
    if "history_status" not in history_info_df.columns or "dedupe_reason" not in history_info_df.columns:
        raise KeyError("history_info_df باید ستون‌های 'history_status' و 'dedupe_reason' را داشته باشد")

    subset = history_info_df[[key_column, "history_status", "dedupe_reason"]].copy()
    subset = subset.drop_duplicates(subset=[key_column], keep="first")
    subset = subset.set_index(key_column)

    history_status_map = subset["history_status"]
    dedupe_reason_map = subset["dedupe_reason"]

    result = summary_df.copy()
    result["history_status"] = result[key_column].map(history_status_map).fillna("")
    result["dedupe_reason"] = result[key_column].map(dedupe_reason_map).fillna("")
    return result


def attach_history_snapshot(
    summary_df: pd.DataFrame,
    history_info_df: pd.DataFrame,
    *,
    key_column: str = "student_id",
    snapshot_columns: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """الحاق ستون‌های اسنپ‌شات تاریخی (مانند «history_mentor_id») به خلاصهٔ تخصیص.

    دیتافریم ورودی درجا تغییر نمی‌کند. در صورت نبود رکورد برای یک شناسه، مقدار خنثی
    (``pd.NA``) در ستون‌های اسنپ‌شات قرار داده می‌شود. تضاد شناسه‌ها نیز با حفظ آخرین
    رکورد موجود در ``history_info_df`` حل می‌شود تا رفتار کاملاً تعیین‌پذیر باشد.
    """

    if summary_df.empty:
        return summary_df.copy()
    if key_column not in summary_df.columns:
        raise KeyError(f"{key_column!r} در summary_df وجود ندارد")
    if key_column not in history_info_df.columns:
        raise KeyError(f"{key_column!r} در history_info_df وجود ندارد")

    if snapshot_columns is None:
        preferred = [col for col in HISTORY_SNAPSHOT_COLUMNS if col in history_info_df.columns]
        dynamic = [
            column
            for column in history_info_df.columns
            if column.startswith("history_")
            and column not in {"history_status", "history_snapshot", "history_flags"}
        ]
        snapshot_columns = tuple(dict.fromkeys(preferred + dynamic))

    result = summary_df.copy()
    if not snapshot_columns:
        return result

    subset = history_info_df[[key_column, *snapshot_columns]].copy()
    subset = subset.drop_duplicates(subset=[key_column], keep="last")
    subset = subset.set_index(key_column)

    for column in snapshot_columns:
        column_map = subset[column] if column in subset.columns else pd.Series(dtype="object")
        if column_map.empty:
            mapped = pd.Series(pd.NA, index=result.index, dtype="object")
        else:
            mapped = result[key_column].map(column_map)
            mapped = mapped.where(pd.notna(mapped), pd.NA)
        result[column] = mapped
    return result


def attach_same_history_mentor(
    summary_df: pd.DataFrame,
    history_info_df: pd.DataFrame,
    *,
    key_column: str = "student_id",
    mentor_column: str = "mentor_id",
    history_mentor_column: str = "history_mentor_id",
) -> pd.DataFrame:
    """الحاق ستون بولی «same_history_mentor» بر اساس مقایسهٔ پشتیبان فعلی و تاریخی.

    این تابع نسخه‌ای کپی‌شده از ``summary_df`` باز می‌گرداند و هرگز دادهٔ ورودی را درجا
    تغییر نمی‌دهد. سیاست تعیین مقدار ستون به صورت صریح است: تنها زمانی ``True`` تولید می‌شود
    که هر دو مقدار ``mentor_id`` و ``history_mentor_id`` موجود (غیر تهی) و برابر باشند؛ در تمام
    حالت‌های دیگر از جمله نبود یکی از مقادیر، نتیجهٔ ستون ``False`` خواهد بود تا رفتار برای
    گزارش‌گیری دترمینیستیک و ساده بماند. حتی در صورت خالی بودن ``summary_df``، ستون جدید با
    dtype بولی ایجاد می‌شود تا اسکیمای خروجی ثابت بماند.
    """

    for column_name, label in (
        (key_column, "summary_df"),
        (mentor_column, "summary_df"),
    ):
        if column_name not in summary_df.columns:
            raise KeyError(f"{column_name!r} در {label} وجود ندارد")
    if key_column not in history_info_df.columns:
        raise KeyError(f"{key_column!r} در history_info_df وجود ندارد")
    if history_mentor_column not in history_info_df.columns:
        raise KeyError(
            f"{history_mentor_column!r} در history_info_df وجود ندارد"
        )

    result = summary_df.copy()
    if result.empty:
        result["same_history_mentor"] = pd.Series(False, index=result.index, dtype=bool)
        return result

    subset = history_info_df[[key_column, history_mentor_column]].copy()
    subset = subset.drop_duplicates(subset=[key_column], keep="last")
    subset = subset.set_index(key_column)[history_mentor_column]

    history_series = result[key_column].map(subset)
    current_mentor = result[mentor_column]
    same = (
        current_mentor.eq(history_series)
        & current_mentor.notna()
        & history_series.notna()
    )
    result["same_history_mentor"] = same.astype(bool)
    return result


__all__ = [
    "attach_allocation_channel",
    "attach_history_flags",
    "attach_history_snapshot",
    "attach_same_history_mentor",
]
