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

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence
import re

import pandas as pd

from .columns import CANON_EN_TO_FA, ensure_series
from .utils import normalize_fa, to_numlike_str

__all__ = [
    "MentorAliasStats",
    "natural_key",
    "build_mentor_id_map",
    "build_mentor_alias_map",
    "extract_alias_code_series",
    "inject_mentor_id",
    "ensure_ranking_columns",
]

_NUMERIC_RE = re.compile(r"(\d+)")
_MENTOR_NAME_CANON = CANON_EN_TO_FA["mentor_name"]


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


def _resolve_mentor_name_column(df: pd.DataFrame) -> str:
    """انتخاب ستون معتبر نام پشتیبان با پشتیبانی از سینونیم‌های متداول."""

    candidates = (
        "پشتیبان",
        _MENTOR_NAME_CANON,
        f"{_MENTOR_NAME_CANON} | mentor_name",
        "mentor_name",
        "mentor name",
    )
    for column in candidates:
        if column in df.columns:
            return column
    raise KeyError(
        f"Missing mentor name column; tried {candidates} — seen: {list(df.columns)}"
    )


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

    mentor_column = _resolve_mentor_name_column(matrix_df)
    required = {mentor_column, "کد کارمندی پشتیبان"}
    missing = required - set(matrix_df.columns)
    if missing:
        raise KeyError(
            f"Missing columns for mentor id map: {sorted(missing)} | seen: {list(matrix_df.columns)}"
        )

    mentor_series = ensure_series(matrix_df[mentor_column]).map(_normalize_name)
    code_series = ensure_series(matrix_df["کد کارمندی پشتیبان"]).map(_normalize_code_raw)

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

    mentor_column = _resolve_mentor_name_column(pool)

    result = pool.copy()
    if "پشتیبان" not in result.columns:
        result["پشتیبان"] = ensure_series(result[mentor_column])
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
_MENTOR_ALIAS_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        [
            "mentor_alias_code",
            "mentor_alias_postal_code",
            "mentor_postal_code",
            "alias",
            "alias_norm",
            "alias_normal",
            "جایگزین | alias",
            CANON_EN_TO_FA.get("mentor_alias_code", "کد جایگزین پشتیبان"),
            CANON_EN_TO_FA.get("alias", "جایگزین"),
            CANON_EN_TO_FA.get("postal_code", "کدپستی"),
            "کد جایگزین پشتیبان",
            "کدپستی",
            "کد پستی",
        ]
    )
)


@dataclass(slots=True)
class MentorAliasStats:
    """خلاصهٔ آماری برای نگاشت alias→mentor.

    مثال::

        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "alias": ["1234", "5678"],
        ...     "کد کارمندی پشتیبان": ["EMP-1", ""],
        ... })
        >>> _, stats = build_mentor_alias_map(df)
        >>> stats.as_dict()["alias_rows_with_mentor"]
        1
    """

    total_alias_rows: int = 0
    alias_rows_with_mentor: int = 0
    alias_rows_without_mentor: int = 0
    unique_aliases: int = 0

    def as_dict(self) -> dict[str, int]:
        """دیکشنری JSON-safe از مقادیر شمارشی."""

        return {
            "total_alias_rows": self.total_alias_rows,
            "alias_rows_with_mentor": self.alias_rows_with_mentor,
            "alias_rows_without_mentor": self.alias_rows_without_mentor,
            "unique_aliases": self.unique_aliases,
        }


def _normalize_alias_code(value: Any) -> str:
    """نرمال‌سازی کد جایگزین به digits بدون صفر پیشرو."""

    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    digits = digits[-10:]
    trimmed = digits.lstrip("0")
    return trimmed or digits


def extract_alias_code_series(
    frame: pd.DataFrame, *, alias_columns: Sequence[str] | None = None
) -> pd.Series:
    """استخراج پایدار اولین ستون alias موجود در دیتافریم.

    Args:
        frame: دیتافریم خام Inspactor یا ماتریس.
        alias_columns: فهرست سفارشی ستون‌ها برای جست‌وجو.

    Returns:
        pd.Series: رشتهٔ digit نرمال‌شده (بدون صفر پیشرو) یا رشتهٔ تهی.
    """

    columns = alias_columns or _MENTOR_ALIAS_COLUMNS
    if not columns:
        return pd.Series([""] * len(frame), dtype="string", index=frame.index)
    alias_values = pd.Series([""] * len(frame), dtype="string", index=frame.index)
    for column in columns:
        if column not in frame.columns:
            continue
        series = ensure_series(frame[column]).astype("string")
        normalized = series.map(_normalize_alias_code)
        fill_mask = alias_values.eq("") & normalized.ne("")
        if fill_mask.any():
            alias_values = alias_values.mask(fill_mask, normalized)
        if alias_values.ne("").all():
            break
    return alias_values


def build_mentor_alias_map(
    frame: pd.DataFrame,
    *,
    mentor_column: str = "کد کارمندی پشتیبان",
    alias_series: pd.Series | None = None,
    alias_columns: Sequence[str] | None = None,
) -> tuple[Dict[str, str], MentorAliasStats]:
    """ساخت نگاشت alias→mentor بر مبنای دادهٔ Inspactor.

    Args:
        frame: دیتافریم ورودی پس از canonicalize_pool_frame.
        mentor_column: نام ستون کد کارمندی.
        alias_series: سری از :func:`extract_alias_code_series` (اختیاری).
        alias_columns: در صورت عدم ارائهٔ ``alias_series``، فهرست ستون‌های جایگزین.

    Returns:
        tuple: (دیکشنری نگاشت، آمار نگاشت).
    """

    alias_values = (
        ensure_series(alias_series).astype("string").fillna("")
        if alias_series is not None
        else extract_alias_code_series(frame, alias_columns=alias_columns)
    )
    stats = MentorAliasStats()
    stats.total_alias_rows = int(alias_values.ne("").sum())
    mapping: Dict[str, str] = {}
    if mentor_column not in frame.columns:
        return mapping, stats
    mentor_values = ensure_series(frame[mentor_column]).astype("string").fillna("").str.strip()
    for alias_code, mentor_code in zip(alias_values, mentor_values, strict=False):
        if not alias_code:
            continue
        if not mentor_code:
            stats.alias_rows_without_mentor += 1
            continue
        stats.alias_rows_with_mentor += 1
        mapping.setdefault(alias_code, mentor_code)
    stats.unique_aliases = len(mapping)
    return mapping, stats
