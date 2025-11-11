"""ابزار استانداردسازی ستون‌ها و تولید هدر دوزبانه برای منابع ورودی.

این ماژول تمام منطق مرتبط با نگاشت نام ستون‌ها، همگرایی تایپ و
تولید هدرهای خروجی دوزبانه را در یک نقطه نگه می‌دارد. منبع داده می‌تواند
یکی از سه مقدار «report»، «inspactor» یا «school» باشد و تمام عملیات‌ها
Case-insensitive و با تکیه بر :func:`normalize_fa` انجام می‌شود تا اختلافات
کاراکتری (نیم‌فاصله، ارقام فارسی، حروف عربی) مدیریت شود.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Mapping, Sequence

import pandas as pd

from app.core.policy_loader import get_policy
from .normalization import normalize_fa, to_numlike_str

__all__ = [
    "CANON_EN_TO_FA",
    "CANON_FA_TO_EN",
    "CANON",
    "REPORT_SYNONYMS",
    "INSPACTOR_SYNONYMS",
    "SCHOOL_SYNONYMS",
    "resolve_aliases",
    "coerce_semantics",
    "canonicalize_headers",
    "collect_aliases_for",
    "accepted_synonyms",
]

Source = Literal["report", "inspactor", "school"]
HeaderMode = Literal["fa", "en", "fa_en"]


# ---------------------------------------------------------------------------
# Canonical dictionaries (EN ↔ FA)
# ---------------------------------------------------------------------------
CANON_EN_TO_FA: Mapping[str, str] = {
    "group_code": "کدرشته",
    "exam_group": "گروه آزمایشی",
    "gender": "جنسیت",
    "graduation_status": "دانش آموز فارغ",
    "center": "مرکز گلستان صدرا",
    "finance": "مالی حکمت بنیاد",
    "school_code": "کد مدرسه",
    "school_name": "نام مدرسه",
    "school_code_1": "کد مدرسه 1",
    "school_code_2": "کد مدرسه 2",
    "school_code_3": "کد مدرسه 3",
    "school_code_4": "کد مدرسه 4",
    "school_name_1": "نام مدرسه 1",
    "school_name_2": "نام مدرسه 2",
    "school_name_3": "نام مدرسه 3",
    "school_name_4": "نام مدرسه 4",
    "postal_code": "کدپستی",
    "schools_covered_count": "تعداد مدارس تحت پوشش",
    "covered_students_count": "تعداد داوطلبان تحت پوشش",
    "capacity_special": "تعداد تحت پوشش خاص",
    "capacity_current": "تعداد داوطلبان تحت پوشش",
    "remaining_capacity": "remaining_capacity",
    "alias": "جایگزین",
    "mentor_id": "کد کارمندی پشتیبان",
    "mentor_name": "پشتیبان",
    "manager_name": "مدیر",
}
CANON: Mapping[str, str] = CANON_EN_TO_FA
CANON_FA_TO_EN: Mapping[str, str] = {
    normalize_fa(value): key for key, value in CANON_EN_TO_FA.items()
}


# ---------------------------------------------------------------------------
# Base synonym dictionaries (raw strings, before normalization)
# ---------------------------------------------------------------------------
REPORT_SYNONYMS: Mapping[str, str] = {
    "وضعیت تحصیلی": "دانش آموز فارغ",
    "مرکز ثبت نام": "مرکز گلستان صدرا",
    # Suggested English equivalents
    "graduated": "دانش آموز فارغ",
    "is_grad": "دانش آموز فارغ",
    "center": "مرکز گلستان صدرا",
    "registration_center": "مرکز گلستان صدرا",
    "کد رشته": "کدرشته",
    "group_code": "کدرشته",
    "major_code": "کدرشته",
}

INSPACTOR_SYNONYMS: Mapping[str, str] = {
    "کد رشته": "کدرشته",
    "کد گروه آزمایشی": "کدرشته",
    "کدپستی": "کدپستی",
    "تعداد مدارس تحت پوشش": "تعداد مدارس تحت پوشش",
    "نام مدرسه 1": "نام مدرسه 1",
    "نام مدرسه 2": "نام مدرسه 2",
    "نام مدرسه 3": "نام مدرسه 3",
    "نام مدرسه 4": "نام مدرسه 4",
    "کد مدرسه": "کد مدرسه",
    "کد کارمندی پشتیبان": "کد کارمندی پشتیبان",
    "mentor_id": "کد کارمندی پشتیبان",
}

SCHOOL_SYNONYMS: Mapping[str, str] = {
    "کد مدرسه": "کد مدرسه",
    "نام مدرسه": "نام مدرسه",
    "کد کامل مدرسه": "کد کامل مدرسه",
    "کد آموزش و پرورش": "کد آموزش و پرورش",
    "school_code": "کد مدرسه",
    "school_name": "نام مدرسه",
}

_BASE_SYNONYMS: Mapping[Source, Mapping[str, str]] = {
    "report": REPORT_SYNONYMS,
    "inspactor": INSPACTOR_SYNONYMS,
    "school": SCHOOL_SYNONYMS,
}


@dataclass(frozen=True)
class _AliasBundle:
    """نگهدارندهٔ نگاشت نرمال‌ شده و فهرست مترادف‌های قابل گزارش."""

    normalized_map: Mapping[str, str]
    report_map: Mapping[str, List[str]]


def _normalize_header(value: str) -> str:
    text = normalize_fa(value)
    text = text.replace("_", " ")
    text = " ".join(text.split())
    if not text:
        raw = str(value).strip().lower().replace("_", " ")
        text = " ".join(raw.split())
    return text.lower()


def _target_to_en(value: str) -> str | None:
    normalized = _normalize_header(value)
    if not normalized:
        return None
    if normalized in CANON_FA_TO_EN:
        return CANON_FA_TO_EN[normalized]
    for key in CANON_EN_TO_FA:
        if normalized == _normalize_header(key):
            return key
    return None


def _combined_aliases(source: Source) -> Mapping[str, str]:
    policy = get_policy()
    policy_aliases = policy.column_aliases.get(source, {})
    merged: Dict[str, str] = {}
    merged.update(_BASE_SYNONYMS.get(source, {}))
    merged.update(policy_aliases)
    return merged


def _build_alias_bundle(source: Source) -> _AliasBundle:
    normalized_map: Dict[str, str] = {}
    report_map: Dict[str, List[str]] = {
        CANON_EN_TO_FA[key]: [] for key in CANON_EN_TO_FA
    }

    raw_aliases = _combined_aliases(source)
    for alias, target in raw_aliases.items():
        canonical = _target_to_en(target)
        if canonical is None:
            continue
        normalized_map[_normalize_header(alias)] = canonical
        report_map.setdefault(CANON_EN_TO_FA[canonical], []).append(alias)

    for en_key, fa_key in CANON_EN_TO_FA.items():
        normalized_map.setdefault(_normalize_header(en_key), en_key)
        normalized_map.setdefault(_normalize_header(fa_key), en_key)

    return _AliasBundle(normalized_map=normalized_map, report_map=report_map)


def collect_aliases_for(source: Source) -> Mapping[str, str]:
    """بازگرداندن نگاشت alias→ستون استاندارد (فارسی) برای پیام‌های خطا."""

    return dict(_combined_aliases(source))


def accepted_synonyms(source: Source, canonical_fa: str) -> Sequence[str]:
    """فهرست مترادف‌های قابل قبول برای ستون هدف را بازمی‌گرداند."""

    bundle = _build_alias_bundle(source)
    synonyms = bundle.report_map.get(canonical_fa, [])
    return sorted(dict.fromkeys(synonyms))


def _find_column(df: pd.DataFrame, *, canonical_en: str) -> str | None:
    """یافتن نام ستونی که با کلید canonical تطابق دارد."""

    fa_name = CANON_EN_TO_FA.get(canonical_en)
    if fa_name and fa_name in df.columns:
        return fa_name
    normalized_targets = {
        _normalize_header(canonical_en),
    }
    if fa_name:
        normalized_targets.add(_normalize_header(fa_name))
    for column in df.columns:
        if _normalize_header(str(column)) in normalized_targets:
            return column
    return fa_name if fa_name in df.columns else None


def resolve_aliases(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    """نام ستون‌ها را براساس مترادف‌های تعریف‌شده به حالت استاندارد فارسی تبدیل می‌کند."""

    bundle = _build_alias_bundle(source)
    rename_map: Dict[str, str] = {}
    for column in df.columns:
        normalized = _normalize_header(str(column))
        canonical_en = bundle.normalized_map.get(normalized)
        if canonical_en is None:
            continue
        rename_map[column] = CANON_EN_TO_FA.get(canonical_en, canonical_en)
    result = df.copy()
    if rename_map:
        result = result.rename(columns=rename_map)
    return result


def _looks_numeric(text: str) -> bool:
    cleaned = text.replace(" ", "")
    if cleaned.startswith("-"):
        cleaned = cleaned[1:]
    parts = cleaned.split(".")
    if len(parts) > 2:
        return False
    return all(part.isdigit() for part in parts if part)


def _identifier_to_string(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    if not text:
        return pd.NA
    numeric_candidate = to_numlike_str(text)
    if numeric_candidate and (_looks_numeric(text) or isinstance(value, (int, float))):
        return numeric_candidate
    return text


def _status_to_int(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int,)):
        return int(value)
    text = str(value).strip()
    if not text:
        return pd.NA
    numeric_candidate = to_numlike_str(text)
    if numeric_candidate and numeric_candidate.isdigit():
        return int(numeric_candidate)
    normalized = normalize_fa(text)
    if "فارغ" in normalized:
        return 1
    return 0


def _coerce_int_column(df: pd.DataFrame, canonical_en: str) -> None:
    column = _find_column(df, canonical_en=canonical_en)
    if column is None:
        return
    column_data = df[column]
    if isinstance(column_data, pd.DataFrame):
        column_data = column_data.iloc[:, 0]
    cleaned = column_data.map(lambda v: to_numlike_str(v) if not pd.isna(v) else "")
    numeric = pd.to_numeric(cleaned.replace("", pd.NA), errors="coerce")
    if numeric.isna().any():
        df[column] = numeric.astype("Int64")
    else:
        df[column] = numeric.astype("int64")


def _coerce_identifier_column(df: pd.DataFrame, canonical_en: str) -> None:
    column = _find_column(df, canonical_en=canonical_en)
    if column is None:
        return
    coerced = df[column].map(_identifier_to_string)
    df[column] = coerced.astype("string").astype(object)


def _coerce_report_semantics(df: pd.DataFrame) -> None:
    grad_col = _find_column(df, canonical_en="graduation_status")
    status_series = df[grad_col] if grad_col is not None else None
    if status_series is not None:
        df[grad_col] = status_series.map(_status_to_int).astype("Int64")
    center_col = _find_column(df, canonical_en="center")
    if center_col is None:
        bundle = _build_alias_bundle("report")
        normalized_target = {
            norm
            for norm, key in bundle.normalized_map.items()
            if key == "center"
        }
        for column in df.columns:
            if _normalize_header(str(column)) in normalized_target:
                center_col = column
                break
    if center_col is not None:
        df[center_col] = df[center_col].map(
            lambda v: normalize_fa(v) if not pd.isna(v) else pd.NA
        )


def _coerce_common_semantics(df: pd.DataFrame, source: Source) -> None:
    _coerce_identifier_column(df, "mentor_id")
    _coerce_identifier_column(df, "alias")
    _coerce_identifier_column(df, "postal_code")
    _coerce_int_column(df, "group_code")
    _coerce_int_column(df, "school_code")
    _coerce_int_column(df, "center")
    _coerce_int_column(df, "finance")
    if source == "report":
        _coerce_int_column(df, "graduation_status")
    if source == "inspactor":
        _coerce_int_column(df, "schools_covered_count")
        _coerce_int_column(df, "covered_students_count")


def coerce_semantics(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    """اعمال تبدیل‌های معنایی (نوع داده، ستونی مشتق‌شده و ...)."""

    result = df.copy()
    if source == "report":
        _coerce_report_semantics(result)
    _coerce_common_semantics(result, source)
    return result


def canonicalize_headers(df: pd.DataFrame, header_mode: HeaderMode) -> pd.DataFrame:
    """تولید هدر خروجی بر اساس تنظیم دوزبانه."""

    if header_mode not in {"fa", "en", "fa_en"}:
        raise ValueError(f"Unsupported header_mode '{header_mode}'")
    rename: Dict[str, str] = {}
    for column in df.columns:
        normalized = _normalize_header(str(column))
        en_key = _target_to_en(str(column))
        if en_key is None:
            continue
        fa_name = CANON_EN_TO_FA[en_key]
        if header_mode == "fa":
            rename[column] = fa_name
        elif header_mode == "en":
            rename[column] = en_key
        else:
            rename[column] = f"{fa_name} | {en_key}"
    if not rename:
        return df.copy()
    return df.rename(columns=rename)
