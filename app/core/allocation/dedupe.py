from __future__ import annotations

from typing import Iterable

import pandas as pd

from app.core.common.phone_rules import normalize_digits

__all__ = ["dedupe_by_national_id"]

_MISSING_OR_INVALID = "missing_or_invalid_national_code"
_NO_HISTORY_MATCH = "no_history_match"


def _normalize_national_code(value: object) -> str:
    """تبدیل مقدار ورودی به رشتهٔ ده‌رقمی کد ملی یا رشتهٔ خالی.

    - مقادیر None یا NaN و هر مقدار غیر ده‌رقمی به رشتهٔ خالی تبدیل می‌شوند.
    - تنها رقم‌ها پس از نرمال‌سازی ارقام فارسی/عربی نگه داشته می‌شوند.
    """

    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except TypeError:
        # برای مقادیری که isna پشتیبانی نمی‌کند
        pass

    digits_only = normalize_digits(value) or ""
    return digits_only if len(digits_only) == 10 else ""


def _normalize_series(series: pd.Series | None, index: pd.Index | None) -> pd.Series:
    base_index = series.index if series is not None else index
    if base_index is None:
        return pd.Series([], dtype="string")
    if series is None:
        return pd.Series([""] * len(base_index), index=base_index, dtype="string")
    normalized = series.map(_normalize_national_code)
    return normalized.astype("string")


def _first_present_column(df: pd.DataFrame, candidates: Iterable[str]) -> pd.Series | None:
    for column in candidates:
        if column in df.columns:
            return df[column]
    return None


def dedupe_by_national_id(
    students_df: pd.DataFrame, history_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """جداسازی دانش‌آموزان بر اساس وجود کد ملی در سوابق قبلی.

    ورودی‌ها بدون هیچ I/O یا عملیات inplace پردازش می‌شوند و دو دیتافریم برمی‌گردد:

    - ``already_allocated_df``: دانش‌آموزانی که کد ملی نرمال‌شدهٔ آن‌ها در سوابق وجود دارد.
    - ``new_candidates_df``: سایر دانش‌آموزان (کد ملی مفقود، نامعتبر یا بدون تطبیق در سوابق) همراه با
      ستون کمکی ``dedupe_reason``.

    مثال::

        >>> students = pd.DataFrame({"نام": ["الف", "ب"], "کد ملی": ["0012345678", "123"]})
        >>> history = pd.DataFrame({"national_code": ["0012345678"]})
        >>> allocated, new = dedupe_by_national_id(students, history)
        >>> allocated["نام"].tolist()
        ['الف']
        >>> new[["نام", "dedupe_reason"]].values.tolist()
        [['ب', 'missing_or_invalid_national_code']]

    :param students_df: دیتافریم دانش‌آموزان.
    :param history_df: دیتافریم سوابق قبلی که توسط لایهٔ Infra بارگذاری شده است.
    :return: ``(already_allocated_df, new_candidates_df)`` با ترتیب و شاخص اصلی حفظ شده.
    """

    if students_df is None or history_df is None:
        raise ValueError("students_df و history_df نباید None باشند")

    student_series = _first_present_column(
        students_df, ("national_code", "کد ملی")
    )
    history_series = _first_present_column(history_df, ("national_code", "کد ملی"))

    student_norm = _normalize_series(student_series, students_df.index)
    history_norm = _normalize_series(history_series, history_df.index)

    history_codes = set(history_norm[history_norm != ""].unique())
    already_mask = student_norm.ne("") & student_norm.isin(history_codes)

    already_allocated_df = students_df.loc[already_mask].copy()

    reasons = pd.Series(index=students_df.index, dtype="string")
    reasons.loc[student_norm.eq("")] = _MISSING_OR_INVALID
    reasons.loc[student_norm.ne("") & ~student_norm.isin(history_codes)] = (
        _NO_HISTORY_MATCH
    )

    new_candidates_df = students_df.loc[~already_mask].copy()
    if not new_candidates_df.empty:
        new_candidates_df = new_candidates_df.assign(
            dedupe_reason=reasons.loc[new_candidates_df.index].astype("string")
        )

    return already_allocated_df, new_candidates_df
