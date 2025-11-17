from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from app.core.common.columns import CANON_EN_TO_FA
from app.core.common.phone_rules import (
    apply_hekmat_contact_policy,
    fix_guardian_phone_columns,
    normalize_landline_series,
    normalize_mobile_series,
)

_DIGIT_TRANSLATION = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")

__all__ = [
    "enrich_student_contacts",
    "CONTACT_POLICY_COLUMNS",
    "CONTACT_POLICY_ALIAS_GROUPS",
    "REGISTRATION_STATUS_CANDIDATES",
    "debug_registration_distribution",
]

CONTACT_POLICY_COLUMNS: tuple[str, ...] = (
    "student_mobile",
    "contact1_mobile",
    "contact2_mobile",
    "student_landline",
    "student_registration_status",
    "hekmat_tracking",
)

CONTACT_POLICY_ALIAS_GROUPS: Mapping[str, tuple[str, ...]] = {
    "student_mobile": ("تلفن همراه", "موبایل دانش آموز", "موبایل دانش‌آموز"),
    "contact1_mobile": ("تلفن رابط 1", "موبایل رابط 1"),
    "contact2_mobile": ("تلفن رابط 2", "موبایل رابط 2"),
    "student_landline": ("تلفن ثابت", "تلفن"),
    "student_registration_status": ("وضعیت ثبت نام", "reg_status"),
    "hekmat_tracking": ("کد رهگیری حکمت", "student_hekmat_tracking_code"),
}


def _optional_names(*candidates: str | None) -> tuple[str, ...]:
    return tuple(name for name in candidates if name)

def _alias_names(column: str) -> tuple[str, ...]:
    return CONTACT_POLICY_ALIAS_GROUPS.get(column, ())


def _expand_with_student_prefix(candidates: Iterable[str]) -> tuple[str, ...]:
    expanded: list[str] = []
    for name in candidates:
        expanded.append(name)
        if not str(name).startswith("student_"):
            expanded.append(f"student_{name}")
    return tuple(expanded)


def _unique_preserve(items: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return tuple(unique)


_STUDENT_MOBILE_CANDIDATES: tuple[str, ...] = (
    "student_mobile",
    "student_mobile_raw",
    "student_GF_Mobile",
    "GF_Mobile",
    "student_mobile_number",
    *_optional_names(CANON_EN_TO_FA.get("student_mobile")),
    *_alias_names("student_mobile"),
)
_GUARDIAN1_CANDIDATES: tuple[str, ...] = (
    "contact1_mobile",
    "student_contact1_mobile",
    "guardian_phone_1",
    "guardian1_mobile",
    "parent_mobile_1",
    *_optional_names(CANON_EN_TO_FA.get("contact1_mobile")),
    *_alias_names("contact1_mobile"),
)
_GUARDIAN2_CANDIDATES: tuple[str, ...] = (
    "contact2_mobile",
    "student_contact2_mobile",
    "guardian_phone_2",
    "guardian2_mobile",
    "parent_mobile_2",
    *_optional_names(CANON_EN_TO_FA.get("contact2_mobile")),
    *_alias_names("contact2_mobile"),
)
_LANDLINE_CANDIDATES: tuple[str, ...] = (
    "student_landline",
    "student_phone",
    "landline",
    "student_home_phone",
    "GF_Landline",
    *_optional_names(CANON_EN_TO_FA.get("student_landline")),
    *_alias_names("student_landline"),
)
_STATUS_PRIMARY: tuple[str, ...] = _unique_preserve(
    _expand_with_student_prefix(
        (
            "student_registration_status",
            "reg_status",
            "registration_status",
            *_optional_names(CANON_EN_TO_FA.get("student_registration_status")),
            *_alias_names("student_registration_status"),
            "student_reg_status",
            "وضعیت ثبت نام",
        )
    )
)
_STATUS_FALLBACK: tuple[str, ...] = _unique_preserve(
    _expand_with_student_prefix(
        (
            "student_finance",
            "finance",
            "student_finance_status",
            "student_finance_code",
            *_optional_names(CANON_EN_TO_FA.get("finance")),
        )
    )
)
_TRACKING_EXTRA: tuple[str, ...] = _optional_names(
    CANON_EN_TO_FA.get("hekmat_tracking"),
    *_alias_names("hekmat_tracking"),
)
_TRACKING_CODE_CANDIDATES: tuple[str, ...] = (
    "hekmat_tracking",
    "student_hekmat_tracking_code",
    "student_hekmat_tracking",
    "student_tracking_code",
    *_TRACKING_EXTRA,
)


REGISTRATION_STATUS_CANDIDATES: tuple[str, ...] = _unique_preserve(
    (*_STATUS_PRIMARY, *_STATUS_FALLBACK)
)


def _first_existing(df: pd.DataFrame, candidates: Iterable[str], default: str) -> str:
    for column in candidates:
        if column in df.columns:
            return column
    return default


def _first_present(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    """برگرداندن اولین ستون موجود از بین نام‌های ورودی یا None در صورت نبود.

    مثال::
        >>> df = pd.DataFrame({"a": [1], "b": [2]})
        >>> _first_present(df, ["x", "b", "a"])
        'b'
    """

    for column in candidates:
        if column in df.columns:
            return column
    return None


def _normalize_registration_status(series: pd.Series, *, index: pd.Index) -> pd.Series:
    """تبدیل ستون وضعیت ثبت‌نام به کدهای عددی ۰/۱/۳ با dtype=Int64."""

    status_raw = series.astype("string").str.translate(_DIGIT_TRANSLATION)
    status_numeric = pd.to_numeric(status_raw, errors="coerce")
    return pd.Series(status_numeric, index=index).astype("Int64")


def _select_registration_status(df: pd.DataFrame) -> tuple[str, pd.Series]:
    """انتخاب ستون وضعیت ثبت‌نام با تقدم صریح بر fallback مالی.

    ترتیب انتخاب:
    1. ستون‌های صریح «وضعیت ثبت نام» (و معادل‌های انگلیسی/دانش‌آموزی).
    2. فقط در نبود گزینهٔ ۱، ستون‌های مالی به‌عنوان fallback استفاده می‌شوند.

    مثال::
        >>> frame = pd.DataFrame({
        ...     "student_id": [1, 2],
        ...     "وضعیت ثبت نام": [0, 3],
        ...     "مالی حکمت بنیاد": [0, 0],
        ... })
        >>> column, series = _select_registration_status(frame)
        >>> column
        'وضعیت ثبت نام'
        >>> series.tolist()
        [0, 3]
    """

    primary = _first_present(df, _STATUS_PRIMARY)
    fallback = _first_present(df, _STATUS_FALLBACK)
    source_column = primary or fallback or "student_registration_status"
    _ensure_column(df, source_column)
    normalized = _normalize_registration_status(df[source_column], index=df.index)
    return source_column, normalized


def _ensure_column(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        df[column] = pd.Series([pd.NA] * len(df), dtype="string", index=df.index)


def debug_registration_distribution(
    df: pd.DataFrame, candidate_columns: Iterable[str]
) -> dict[str, Any]:
    """شمارش توزیع وضعیت ثبت‌نام برای دیباگ.

    اولین ستونی را که در بین نام‌های ورودی پیدا کند انتخاب می‌کند و تعداد
    مقادیر غیرخالی، صفرها و سه‌ها را برمی‌گرداند. در صورت نبود ستون، ستون None
    گزارش می‌شود.
    """

    first_column: str | None = None
    for name in candidate_columns:
        if name in df.columns:
            first_column = name
            break

    if first_column is None:
        return {"column": None, "non_null": 0, "zeros": 0, "threes": 0}

    series = pd.to_numeric(df[first_column], errors="coerce")
    non_null = int(series.notna().sum())
    zeros = int((series == 0).sum())
    threes = int((series == 3).sum())
    return {
        "column": first_column,
        "non_null": non_null,
        "zeros": zeros,
        "threes": threes,
    }


def enrich_student_contacts(df: pd.DataFrame) -> pd.DataFrame:
    """پاک‌سازی تلفن‌ها و اعمال سیاست حکمت روی دیتافریم ورودی.

    - تمام ستون‌های موبایل (دانش‌آموز و رابط‌ها) نرمال می‌شوند و ستون‌های استاندارد
      ``student_mobile``، ``contact1_mobile`` و ``contact2_mobile`` همیشه پر هستند.
    - تلفن ثابت در ``student_landline`` فقط مقادیر معتبر (شروع با 3 یا 5) را نگه می‌دارد.
    - ستون‌های وضعیت ثبت‌نام و «کد رهگیری حکمت» در صورت نبود ساخته می‌شوند و سپس قانون
      :func:`apply_hekmat_contact_policy` روی دیتافریم اعمال می‌شود تا fallback تلفن ثابت و
      کد رهگیری ثابت برای ردیف‌های حکمت تنظیم شود.
    همهٔ مصرف‌کننده‌ها باید خروجی این تابع را استفاده کنند تا منطق تلفن سراسری باشد.
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

    status_column, status_numeric = _select_registration_status(result)
    canonical_status_column = "student_registration_status"
    result[canonical_status_column] = status_numeric
    if status_column != canonical_status_column:
        result[status_column] = status_numeric

    tracking_column = _first_existing(result, _TRACKING_CODE_CANDIDATES, _TRACKING_CODE_CANDIDATES[0])
    _ensure_column(result, tracking_column)
    canonical_tracking_column = "hekmat_tracking"
    if tracking_column != canonical_tracking_column:
        result[canonical_tracking_column] = result[tracking_column]
    else:
        canonical_tracking_column = tracking_column

    result = apply_hekmat_contact_policy(
        result,
        status_column=canonical_status_column,
        landline_column="student_landline",
        tracking_code_column=canonical_tracking_column,
    )

    if tracking_column != canonical_tracking_column:
        result[tracking_column] = result[canonical_tracking_column]

    return result
