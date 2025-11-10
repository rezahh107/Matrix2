"""توابع شناسهٔ پشتیبان‌ها و نگاشت کد کارمندی (Core-only).

این ماژول هیچ I/O انجام نمی‌دهد و برای پیش‌پردازش ستون‌های مربوط به
شناسهٔ پشتیبان استفاده می‌شود. تمرکز اصلی:

* ساخت premap «نام پشتیبان → کد کارمندی» با نرمال‌سازی پایدار
* تزریق مجدد کدهای کارمندی در استخرهای کاندید در صورت فقدان داده
* آماده‌سازی ستون‌های رتبه‌بندی بدون تغییر دیتافریم اولیه

مثال ساده::

    >>> import pandas as pd
    >>> from app.core.common.ids import build_mentor_id_map, inject_mentor_id
    >>> matrix = pd.DataFrame({
    ...     "پشتیبان": [" زهرا ", "علی"],
    ...     "کد کارمندی پشتیبان": ["EMP-001", "EMP-010"],
    ... })
    >>> id_map = build_mentor_id_map(matrix)
    >>> id_map["زهرا"]
    'EMP-001'
    >>> pool = pd.DataFrame({"پشتیبان": ["زهرا", "علی"], "occupancy_ratio": [0.1, 0.2]})
    >>> inject_mentor_id(pool, id_map)["کد کارمندی پشتیبان"].tolist()
    ['EMP-001', '']
"""

from __future__ import annotations

from typing import Any, Dict, Mapping
import re

import pandas as pd

from .utils import normalize_fa, to_numlike_str

__all__ = [
    "natural_key",
    "build_mentor_id_map",
    "inject_mentor_id",
    "ensure_ranking_columns",
]

_NUMERIC_RE = re.compile(r"(\d+)")


def natural_key(text: Any) -> tuple[object, ...]:
    """تبدیل شناسه به کلید طبیعی برای sort پایدار.

    ورودی‌های None/خالی، رشتهٔ تهی برمی‌گردانند تا در انتهای sort قرار گیرند.

    مثال::

        >>> natural_key("EMP-010") < natural_key("EMP-2")
        False
        >>> natural_key("EMP-2") < natural_key("EMP-010")
        True
    """

    if text is None:
        return ("",)
    s = str(text).strip()
    if not s:
        return ("",)
    parts: list[object] = []
    for token in _NUMERIC_RE.split(s):
        if token.isdigit():
            parts.append(int(token))
        elif token:
            parts.append(token.lower())
    return tuple(parts)


def _normalize_name(value: Any) -> str:
    """نام پشتیبان را به فرم پایدار فارسی تبدیل می‌کند."""

    return normalize_fa(value)


def _normalize_code_raw(value: Any) -> str:
    """کد کارمندی را برای ذخیره‌سازی بدون حذف صفرهای پیشرو نرمال می‌کند."""

    return normalize_fa(value)


def build_mentor_id_map(matrix_df: pd.DataFrame) -> Dict[str, str]:
    """ساخت نگاشت نام پشتیبان به کد کارمندی.

    Args:
        matrix_df: دیتافریم ماتریس واجد شرایط با ستون‌های «پشتیبان» و
            «کد کارمندی پشتیبان».

    Returns:
        dict[str, str]: نگاشت نرمال‌شدهٔ نام پشتیبان به کد کارمندی.

    Raises:
        KeyError: در صورت فقدان ستون‌های مورد نیاز.
    """

    required = {"پشتیبان", "کد کارمندی پشتیبان"}
    missing = required - set(matrix_df.columns)
    if missing:
        raise KeyError(f"Missing columns for mentor id map: {sorted(missing)}")

    mentor_series = matrix_df["پشتیبان"].map(_normalize_name)
    code_series = matrix_df["کد کارمندی پشتیبان"].map(_normalize_code_raw)

    mapping: Dict[str, str] = {}
    for name, code in zip(mentor_series, code_series, strict=False):
        if not name or not code:
            continue
        if name in mapping:
            continue  # اولین رخداد حفظ می‌شود (stable)
        mapping[name] = code
    return mapping


def inject_mentor_id(pool: pd.DataFrame, id_map: Mapping[str, str]) -> pd.DataFrame:
    """تزریق کد کارمندی در استخر کاندید بدون تغییر ورودی اصلی.

    Args:
        pool: دیتافریم ورودی با ستون‌های «پشتیبان» و در صورت وجود
            «کد کارمندی پشتیبان».
        id_map: نگاشت پیش‌محاسبه‌شده از :func:`build_mentor_id_map`.

    Returns:
        pd.DataFrame: کپی از ورودی با پر شدن مقادیر خالی کد کارمندی.

    Raises:
        KeyError: اگر ستون «پشتیبان» موجود نباشد.
    """

    if "پشتیبان" not in pool.columns:
        raise KeyError("Column 'پشتیبان' is required for mentor id injection")

    result = pool.copy()
    if "کد کارمندی پشتیبان" not in result.columns:
        result["کد کارمندی پشتیبان"] = ""

    current_codes = result["کد کارمندی پشتیبان"].map(_normalize_code_raw)
    missing_mask = current_codes.eq("")
    if not missing_mask.any():
        return result

    normalized_names = result.loc[missing_mask, "پشتیبان"].map(_normalize_name)
    filled_codes = normalized_names.map(lambda name: id_map.get(name, ""))
    result.loc[missing_mask, "کد کارمندی پشتیبان"] = filled_codes.fillna("")
    return result


def ensure_ranking_columns(pool: pd.DataFrame) -> pd.DataFrame:
    """تضمین وجود ستون‌های رتبه‌بندی و افزودن ستون‌های مشتق طبیعی.

    این تابع دیتافریم جدیدی برمی‌گرداند تا ورودی تغییری نکند. علاوه بر
    «mentor_id_str» که نسخهٔ رشته‌ای نرمال‌شده است، ستون «mentor_sort_key» نیز
    محاسبه می‌شود تا کلید طبیعی (tuple) برای sort پایدار آماده باشد.

    Args:
        pool: دیتافریم کاندید با ستون‌های مورد نیاز.

    Returns:
        pd.DataFrame: کپی با ستون‌های اضافه برای مرتب‌سازی طبیعی.

    Raises:
        KeyError: اگر ستون‌های ضروری وجود نداشته باشند.
    """

    required = {"occupancy_ratio", "allocations_new", "کد کارمندی پشتیبان"}
    missing = required - set(pool.columns)
    if missing:
        raise KeyError(f"Missing columns for ranking: {sorted(missing)}")

    result = pool.copy()
    result["mentor_id_str"] = result["کد کارمندی پشتیبان"].map(to_numlike_str)
    result["mentor_sort_key"] = result["کد کارمندی پشتیبان"].map(natural_key)
    return result
