from __future__ import annotations

from typing import Iterable

import pandas as pd

from app.core.common.phone_rules import (
    fix_guardian_phone_columns,
    normalize_landline_series,
    normalize_mobile_series,
)

__all__ = ["enrich_student_contacts"]

_STUDENT_MOBILE_CANDIDATES: tuple[str, ...] = (
    "student_mobile",
    "student_mobile_raw",
    "student_GF_Mobile",
    "GF_Mobile",
    "student_mobile_number",
)
_GUARDIAN1_CANDIDATES: tuple[str, ...] = (
    "contact1_mobile",
    "student_contact1_mobile",
    "guardian_phone_1",
    "guardian1_mobile",
    "parent_mobile_1",
)
_GUARDIAN2_CANDIDATES: tuple[str, ...] = (
    "contact2_mobile",
    "student_contact2_mobile",
    "guardian_phone_2",
    "guardian2_mobile",
    "parent_mobile_2",
)
_LANDLINE_CANDIDATES: tuple[str, ...] = (
    "student_landline",
    "student_phone",
    "landline",
    "student_home_phone",
    "GF_Landline",
)


def _first_existing(df: pd.DataFrame, candidates: Iterable[str], default: str) -> str:
    for column in candidates:
        if column in df.columns:
            return column
    return default


def _ensure_column(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        df[column] = pd.Series([pd.NA] * len(df), dtype="string", index=df.index)


def enrich_student_contacts(df: pd.DataFrame) -> pd.DataFrame:
    """پاک‌سازی ستون‌های تماس دانش‌آموز جهت مصرف تخصیص/خروجی.

    این مرحله:
        - موبایل دانش‌آموز را با سیاست «09 + 11 رقم» پالایش می‌کند.
        - منطق «رابط دوم فقط در صورت وجود اول» و «حذف تکراری‌ها» را اعمال می‌کند.
        - تلفن ثابت را تنها در صورت شروع با «3» یا «5» نگه می‌دارد تا برای قواعد حکمت آماده شود.
    """

    result = df.copy()
    result.attrs.update(df.attrs)

    student_mobile_column = _first_existing(result, _STUDENT_MOBILE_CANDIDATES, "student_mobile")
    _ensure_column(result, student_mobile_column)
    student_mobile_normalized = normalize_mobile_series(result[student_mobile_column])
    result[student_mobile_column] = student_mobile_normalized
    if student_mobile_column != "student_mobile":
        result["student_mobile"] = student_mobile_normalized

    guardian1_column = _first_existing(result, _GUARDIAN1_CANDIDATES, "contact1_mobile")
    guardian2_column = _first_existing(result, _GUARDIAN2_CANDIDATES, "contact2_mobile")
    _ensure_column(result, guardian1_column)
    _ensure_column(result, guardian2_column)
    result = fix_guardian_phone_columns(
        result,
        guardian1_column,
        guardian2_column,
        canonical1="contact1_mobile",
        canonical2="contact2_mobile",
    )

    landline_column = _first_existing(result, _LANDLINE_CANDIDATES, "student_landline")
    _ensure_column(result, landline_column)
    landline_normalized = normalize_landline_series(result[landline_column])
    result[landline_column] = landline_normalized
    if landline_column != "student_landline":
        result["student_landline"] = landline_normalized

    return result
