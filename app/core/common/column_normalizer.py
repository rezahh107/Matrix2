"""ابزار نرمال‌سازی نام و مقدار ستون‌های ورودی.

این ماژول در لایهٔ Core قرار دارد تا بدون وابستگی به I/O، DataFrameهای
ورودی از گزارش‌های مختلف (Inspactor، School، Student) را هم‌راستا کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Tuple
import warnings

import pandas as pd

from .columns import ensure_series
from .domain import (
    COL_EDU_CODE,
    COL_FULL_SCHOOL_CODE,
    COL_SCHOOL,
    COL_SCHOOL_CODE_1,
    COL_SCHOOL_CODE_2,
    COL_SCHOOL_CODE_3,
    COL_SCHOOL_CODE_4,
    COL_SCHOOL_NAME,
    COL_SCHOOL_NAME_1,
    COL_SCHOOL_NAME_2,
    COL_SCHOOL_NAME_3,
    COL_SCHOOL_NAME_4,
)
from .normalization import strip_school_code_separators
from .utils import normalize_fa, to_numlike_str

__all__ = ["ColumnNormalizationReport", "normalize_input_columns"]


@dataclass(frozen=True)
class ColumnRule:
    """قانون نرمال‌سازی برای یک ستون مشخص."""

    persian: str
    standard: str
    normalizer: str


@dataclass(frozen=True)
class ColumnNormalizationReport:
    """خلاصهٔ اعمال نرمال‌سازی روی DataFrame ورودی."""

    renamed: Mapping[str, str]
    aliases_added: Tuple[str, ...]
    unmatched: Tuple[str, ...]


_RULES: Tuple[ColumnRule, ...] = (
    ColumnRule(COL_SCHOOL, "school_code", "numlike"),
    ColumnRule(COL_SCHOOL_NAME, "school_name", "text"),
    ColumnRule(COL_SCHOOL_CODE_1, "school_code_1", "numlike"),
    ColumnRule(COL_SCHOOL_CODE_2, "school_code_2", "numlike"),
    ColumnRule(COL_SCHOOL_CODE_3, "school_code_3", "numlike"),
    ColumnRule(COL_SCHOOL_CODE_4, "school_code_4", "numlike"),
    ColumnRule(COL_SCHOOL_NAME_1, "school_name_1", "text"),
    ColumnRule(COL_SCHOOL_NAME_2, "school_name_2", "text"),
    ColumnRule(COL_SCHOOL_NAME_3, "school_name_3", "text"),
    ColumnRule(COL_SCHOOL_NAME_4, "school_name_4", "text"),
    ColumnRule(COL_FULL_SCHOOL_CODE, "full_school_code", "numlike"),
    ColumnRule(COL_EDU_CODE, "edu_code", "numlike"),
)


_LOOKUP: Dict[str, ColumnRule] = {}
for rule in _RULES:
    _LOOKUP.setdefault(normalize_fa(rule.persian), rule)
    _LOOKUP.setdefault(normalize_fa(rule.standard), rule)


_IGNORED_COLUMNS_NORMALIZED: frozenset[str] = frozenset(
    normalize_fa(name)
    for name in (
        "نوع مدرسه",
        "مدیر مدرسه",
        "نام کامل مدرسه",
        "حوزه در خود مدرسه",
        "حوزه در خود مدرسه2",
        "منطقه‌ی آموزش‌وپرورش",
        "ناحیه آموزش و پرورش",
    )
)


def _is_relevant_column(name: str) -> bool:
    """تعیین می‌کند آیا ستون ورودی برای گزارش مدرسه قابل‌توجه است یا خیر.

    ورودی‌هایی که با نام‌های متادیتای مدرسه (مثل «نوع مدرسه»، «مدیر مدرسه»)
    یا نسخه‌های پیشونددار آن‌ها (مثلاً «1حوزه در خود مدرسه») تطبیق داشته باشند
    نادیده گرفته می‌شوند تا هشدار «ستون‌های ناشناخته» فقط برای موارد واقعی
    صادر شود.

    Args:
        name: نام اصلی ستون در DataFrame ورودی.

    Returns:
        ``True`` اگر ستون مرتبط با مدرسه تشخیص داده شود و باید گزارش شود؛ در
        غیر این صورت ``False``.
    """

    normalized = normalize_fa(name)
    normalized_without_prefix = normalize_fa(normalized.lstrip("0123456789"))
    if normalized in _IGNORED_COLUMNS_NORMALIZED or normalized_without_prefix in _IGNORED_COLUMNS_NORMALIZED:
        return False
    return any(token in normalized for token in ("مدرسه", "school", "اموزش", "آموزش"))


def _clean_numlike(value: object) -> str:
    if pd.isna(value):
        return ""
    text = strip_school_code_separators(normalize_fa(value)).replace(",", "")
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except ValueError:
        fallback = to_numlike_str(text)
        return fallback


def _normalize_for_rule(series: pd.Series, mode: str) -> tuple[pd.Series, pd.Series]:
    if mode == "numlike":
        cleaned = series.map(_clean_numlike)
        numeric_raw = pd.to_numeric(cleaned.replace("", pd.NA), errors="coerce")
        if numeric_raw.isna().any():
            numeric = numeric_raw.astype("Int64")
        else:
            numeric = numeric_raw.astype("int64")
        alias = cleaned.astype("string")
        return numeric, alias
    normalized = series.map(lambda v: normalize_fa(v) if not pd.isna(v) else "")
    normalized = normalized.astype("string")
    return normalized, normalized


def _set_column(
    frame: pd.DataFrame, name: str, series: pd.Series, position: int | None
) -> bool:
    if name in frame.columns:
        frame[name] = series
        return False
    if position is None or position >= len(frame.columns):
        frame[name] = series
    else:
        frame.insert(position, name, series)
    return True


def normalize_input_columns(
    df: pd.DataFrame,
    *,
    kind: str = "input",
    include_alias: bool = True,
    report: bool = True,
    collector: Callable[[ColumnNormalizationReport], None] | None = None,
) -> tuple[pd.DataFrame, ColumnNormalizationReport]:
    """نرمال‌سازی نام ستون‌ها و ایجاد معادل استاندارد انگلیسی.

    Args:
        df: DataFrame ورودی برای نرمال‌سازی.
        kind: برچسب توصیفی برای پیام‌های گزارش.
        include_alias: در صورت True ستون‌های انگلیسی استاندارد نیز افزوده می‌شود.
        report: در صورت True، دربارهٔ ستون‌های ناشناخته هشدار می‌دهد.
        collector: تابع اختیاری برای دریافت گزارش نهایی حتی اگر ``report=False`` باشد.

    Returns:
        دوگانهٔ DataFrame نرمال‌شده و گزارش عملیات.
    """

    result = df.copy()
    renamed: Dict[str, str] = {}
    aliases_added: List[str] = []
    unmatched: List[str] = []
    to_drop: set[str] = set()
    positions: Dict[str, int] = {}

    for idx, column in enumerate(df.columns):
        normalized_name = normalize_fa(column)
        rule = _LOOKUP.get(normalized_name)
        if rule is None:
            if _is_relevant_column(column):
                unmatched.append(column)
            continue
        positions.setdefault(rule.persian, idx)
        series = ensure_series(result[column])
        canonical_series, alias_series = _normalize_for_rule(series, rule.normalizer)
        _set_column(result, rule.persian, canonical_series, positions[rule.persian])
        if column != rule.persian and column != rule.standard:
            renamed[column] = rule.persian
            to_drop.add(column)
        alias_position = positions.get(rule.persian, idx) + 1
        if include_alias or rule.standard in result.columns:
            if _set_column(result, rule.standard, alias_series, alias_position):
                aliases_added.append(rule.standard)

    if to_drop:
        result = result.drop(columns=list(to_drop))


    if report and unmatched:
        warnings.warn(
            f"{kind}: ستون‌های ناشناخته یافت شد: {', '.join(sorted(unmatched))}",
            stacklevel=2,
        )

    report_obj = ColumnNormalizationReport(
        renamed=renamed,
        aliases_added=tuple(sorted(set(aliases_added))),
        unmatched=tuple(sorted(unmatched)),
    )
    if collector is not None:
        collector(report_obj)
    return result, report_obj

