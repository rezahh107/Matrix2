#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_matrix.py — Eligibility Matrix Builder (SSoT v1.0.2, script v1.0.4)

Summary
-------
- Normal mentors: build BOTH statuses → student=1 AND graduate=0
- School mentors: build ONLY student=1
- Everything else as per SSoT v1.0.2 (capacity gate, crosswalk+synonyms, finance 0/1/3, center mapping, atomic writes)
- Patch: When school code is empty, set "کد مدرسه" to numeric 0 in output rows.
"""
from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from enum import IntEnum, auto
from functools import lru_cache, wraps
from itertools import product
from typing import Any, Callable, Collection, Dict, Iterable, List, Tuple, TypeVar

import numpy as np
import pandas as pd

# =============================================================================
# CONSTANTS
# =============================================================================
__version__ = "1.0.4"  # bumped
# Postal code range for NORMAL mentors
MIN_POSTAL_CODE = 1000
MAX_POSTAL_CODE = 9999
# Sort fallbacks for null values
SCHOOL_CODE_NULL_SORT = 999999
ALIAS_FALLBACK_SORT = 10**12  # push non-numeric aliases last

# Column headers (Persian per SSoT)
CAPACITY_CURRENT_COL = "تعداد داوطلبان تحت پوشش"
CAPACITY_SPECIAL_COL = "تعداد تحت پوشش خاص"
COL_MENTOR_NAME = "نام پشتیبان"
COL_MANAGER_NAME = "نام مدیر"
COL_MENTOR_ID = "کد کارمندی پشتیبان"  # mandatory; only trusted employee code
COL_MENTOR_ROWID = "ردیف پشتیبان"
COL_GROUP = "گروه آزمایشی"
COL_SCHOOL1 = "نام مدرسه 1"
COL_SCHOOL2 = "نام مدرسه 2"
COL_SCHOOL3 = "نام مدرسه 3"
COL_SCHOOL4 = "نام مدرسه 4"
COL_SCHOOL_CODE = "کد مدرسه"
COL_SCHOOL_COUNT = "تعداد مدارس تحت پوشش"
COL_POSTAL = "کدپستی"
COL_CAN_ALLOC = "امکان جذب دانش آموز"
COL_GENDER = "جنسیت"
COL_STATUS_A = "وضعیت تحصیلی"
COL_STATUS_B = "نوع دانش آموز"
# Optional
COL_GROUP_INCLUDED = "شامل گروه های آزمایشی"

# Regex / normalization
_RE_BIDI = re.compile("[\u200c-\u200f\u202a-\u202e]")
_RE_NONWORD = re.compile(r"[^\w\u0600-\u06FF0-9\s]+", flags=re.UNICODE)
_RE_WHITESPACE = re.compile(r"\s+")
_TRANS_PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
_RE_SPLIT_ITEMS = re.compile(r"[,\u060C]\s*")
_RE_RANGE = re.compile(r"^\s*(\d+)\s*[:\-–]\s*(\d+)\s*$")

# Built-in synonyms (keys normalized later)
BUILTIN_SYNONYMS = {
    "دهم علوم انسانی": "دهم انسانی",
    "یازدهم شبکه و نرم‌افزار رایانه": "یازدهم شبکه و نرم افزار رایانه",
    "کل متوسطه 1": "__BUCKET__متوسطه اول",
    "کل متوسطه 2": "__BUCKET__متوسطه دوم",
    "کل دبستان": "__BUCKET__دبستان",
    "کل هنرستان": "__BUCKET__هنرستان",
    "کل کنکوری": "__BUCKET__کنکوری",
    "کنکوریها": "__BUCKET__کنکوری",
}

# =============================================================================
# PROGRESS API
# =============================================================================
ProgressFn = Callable[[int, str], None]


def noop_progress(_: int, __: str) -> None:
    """تابع پیش‌فرض پیشرفت که هیچ کاری انجام نمی‌دهد."""


# =============================================================================
# ENUMS
# =============================================================================
class Gender(IntEnum):
    FEMALE = 0
    MALE = auto()


class Status(IntEnum):
    GRADUATE = 0
    STUDENT = auto()


class Center(IntEnum):
    MARKAZ = 0
    GOLESTAN = auto()
    SADRA = auto()


class Finance(IntEnum):
    NORMAL = 0
    BONYAD = auto()
    HEKMAT = 3  # non-sequential on purpose

# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass(slots=True)
class BuildConfig:
    version: str = __version__
    finance_variants: tuple[int, ...] = (Finance.NORMAL, Finance.BONYAD, Finance.HEKMAT)
    default_status: int = Status.STUDENT
    enable_capacity_gate: bool = True
    center_manager_map: dict[str, int] = field(
        default_factory=lambda: {
            "شهدخت کشاورز": Center.GOLESTAN,
            "آیناز هوشمند": Center.SADRA,
        }
    )
    can_allocate_truthy: tuple[str, ...] = ("بلی", "بله", "Yes", "yes", "1", "true", "True")

# =============================================================================
# NORMALIZATION
# =============================================================================
@lru_cache(maxsize=1024)
def normalize_fa(text: Any) -> str:
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    s = str(text).replace("ي", "ی").replace("ك", "ک")
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = _RE_BIDI.sub("", s)
    s = _RE_NONWORD.sub(" ", s)
    s = s.translate(_TRANS_PERSIAN_DIGITS)
    return _RE_WHITESPACE.sub(" ", s).strip().lower()


_ZERO_WIDTH_CHARS = (
    "\ufeff",
    "\u200b",
    "\u200c",
    "\u200d",
    "\u200e",
    "\u200f",
    "\u202a",
    "\u202b",
    "\u202c",
    "\u202d",
    "\u202e",
    "\u2060",
)
_THOUSAND_SEPARATORS = {",", "\u066c", "\u060c", " ", "\u00a0", "\u202f", "_"}
_RE_THOUSAND_GROUP = re.compile(r"^[+-]?\d{1,3}(,\d{3})*(\.\d+)?$")
_DECIMAL_SEPARATORS = {ord("\u066b"): ".", ord("\u06d4"): "."}
_DIGIT_TRANSLATIONS = {
    **{ord("۰") + i: str(i) for i in range(10)},
    **{ord("٠") + i: str(i) for i in range(10)},
}
_HYPHEN_TRANSLATIONS = {
    ord("−"): "-",
    ord("‐"): "-",
    ord("‑"): "-",
    ord("‒"): "-",
    ord("–"): "-",
    ord("—"): "-",
    ord("―"): "-",
}
_NUMERIC_TRANSLATION_TABLE = {
    **_DIGIT_TRANSLATIONS,
    **_HYPHEN_TRANSLATIONS,
    **_DECIMAL_SEPARATORS,
    **{ord(ch): None for ch in _ZERO_WIDTH_CHARS},
    **{ord(ch): None for ch in _THOUSAND_SEPARATORS},
}

_DefaultT = TypeVar("_DefaultT")


def to_ascii_numeric(value: str) -> str:
    """نرمال‌سازی اعداد به معادل ASCII با حذف جداکننده‌ها و علائم اضافی."""

    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(value))
    translated = normalized.translate(_NUMERIC_TRANSLATION_TABLE)
    return translated.strip()


def parse_int_safe(x: Any, default: _DefaultT | None = None) -> int | _DefaultT | None:
    raw = "" if x is None else str(x)
    normalized = unicodedata.normalize("NFKC", raw)
    normalized = normalized.translate(
        {
            **_DIGIT_TRANSLATIONS,
            **_HYPHEN_TRANSLATIONS,
            **_DECIMAL_SEPARATORS,
            **{ord(ch): None for ch in _ZERO_WIDTH_CHARS},
        }
    ).strip()
    if not normalized:
        return default
    if any(ch in _THOUSAND_SEPARATORS for ch in normalized):
        collapsed = normalized.translate({ord(ch): "," for ch in _THOUSAND_SEPARATORS})
        if not _RE_THOUSAND_GROUP.match(collapsed):
            return default
    s = to_ascii_numeric(normalized)
    if not s:
        return default
    sign = 1
    if s[0] in "+-":
        sign = -1 if s[0] == "-" else 1
        s = s[1:]
    if not s or not s.isascii() or not s.isdigit():
        return default
    return sign * int(s)


def _standardize_numeric_text(value: str) -> str:
    """یکسان‌سازی ارقام فارسی، جداکننده‌ها و علائم اعشاری."""

    sanitized = to_ascii_numeric(value)
    if not sanitized:
        return ""
    sanitized = sanitized.replace("٫", ".")
    sanitized = sanitized.replace("٬", "")
    return sanitized


def _parse_int_from_text(text: str) -> int | None:
    """استخراج مقدار صحیح از رشتهٔ استانداردشده در صورت امکان."""

    if not text:
        return None
    parsed = parse_int_safe(text)
    if parsed is not None:
        return parsed
    digits = text[1:] if text and text[0] in "+-" else text
    if "." in digits:
        integer_part, decimal_part = digits.split(".", 1)
        if integer_part and integer_part.isdigit() and (not decimal_part or set(decimal_part) <= {"0"}):
            sign = -1 if text and text[0] == "-" else 1
            return sign * int(integer_part)
    return None


def _coerce_int_like(value: Any) -> int | None:
    """تبدیل ورودی به int در صورت امکان بدون تبدیل‌های ناخواسته به float."""

    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if math.isnan(value):
            return None
        as_int = int(value)
        if value == as_int:
            return as_int
        return None
    text = _standardize_numeric_text(str(value))
    return _parse_int_from_text(text)


def to_numlike_str(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    normalized = _standardize_numeric_text(raw)
    parsed = _parse_int_from_text(normalized)
    return str(parsed) if parsed is not None else normalized


def ensure_list(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for v in values:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        t = str(v).strip()
        if not t or t == "0":
            continue
        t = t.replace("،", ",").replace("|", ",")
        result.extend(p.strip() for p in t.split(",") if p.strip())
    seen: set[str] = set()
    uniq: list[str] = []
    for x in result:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

# =============================================================================
# DOMAIN HELPERS
# =============================================================================
def norm_gender(value: Any) -> int | None:
    s = normalize_fa(value)
    match s:
        case "1" | "پسر" | "male" | "boy" | "پسرانه":
            return Gender.MALE
        case "0" | "دختر" | "female" | "girl" | "دخترانه":
            return Gender.FEMALE
        case _:
            return None


def gender_text(code: int | None) -> str:
    return {Gender.FEMALE: "دختر", Gender.MALE: "پسر"}.get(int(code), "") if code is not None else ""


def norm_status(value: Any) -> int | None:
    s = normalize_fa(value)
    match s:
        case "1" | "دانش آموز" | "دانش‌آموز" | "student":
            return Status.STUDENT
        case "0" | "فارغ" | "فارغ التحصیل" | "فارغ‌التحصیل" | "graduate":
            return Status.GRADUATE
        case _:
            return None


def status_text(code: int | None) -> str:
    return {Status.STUDENT: "دانش‌آموز", Status.GRADUATE: "فارغ‌التحصیل"}.get(int(code), "") if code is not None else ""


def center_from_manager(manager_name: str, cfg: BuildConfig) -> int:
    name = str(manager_name or "")
    for needle, center_code in cfg.center_manager_map.items():
        if needle and needle in name:
            return int(center_code)
    return Center.MARKAZ


def center_text(code: int) -> str:
    return {Center.MARKAZ: "مرکز", Center.GOLESTAN: "گلستان", Center.SADRA: "صدرا"}.get(int(code), "")

# =============================================================================
# CROSSWALK
# =============================================================================
def validate_dataframe_columns(*required_cols: str):
    def decorator(func):
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns: {missing}")
            return func(df, *args, **kwargs)

        return wrapper

    return decorator


def prepare_crosswalk_mappings(
    crosswalk_groups_df: pd.DataFrame,
    synonyms_df: pd.DataFrame | None = None,
) -> tuple[dict[str, int], dict[int, str], dict[str, list[tuple[str, int]]], dict[str, str]]:
    """ساخت نگاشت‌های موردنیاز از دیتافریم Crosswalk.

    مثال ساده::

        >>> mappings = prepare_crosswalk_mappings(groups_df)  # doctest: +SKIP

    Args:
        crosswalk_groups_df: دیتافریم شیت «پایه تحصیلی (گروه آزمایشی)».
        synonyms_df: دیتافریم شیت «Synonyms» در صورت وجود.

    Returns:
        چهارتایی ``(name_to_code, code_to_name, buckets, synonyms)``.
    """

    required_columns = {"گروه آزمایشی", "کد گروه", "مقطع تحصیلی"}
    missing = required_columns.difference(crosswalk_groups_df.columns)
    if missing:
        raise ValueError(f"ستون‌های الزامی Crosswalk یافت نشد: {sorted(missing)}")

    name_to_code: dict[str, int] = {}
    code_to_name: dict[int, str] = {}
    buckets: dict[str, list[tuple[str, int]]] = {}
    for _, row in crosswalk_groups_df.iterrows():
        gname = str(row["گروه آزمایشی"])
        gcode = int(row["کد گروه"])
        level = str(row["مقطع تحصیلی"])
        name_to_code[normalize_fa(gname)] = gcode
        code_to_name[gcode] = gname
        buckets.setdefault(level, []).append((gname, gcode))

    synonyms = {normalize_fa(k): v for k, v in BUILTIN_SYNONYMS.items()}
    if synonyms_df is not None:
        src_col = next(
            (c for c in synonyms_df.columns if "from" in normalize_fa(c) or "alias" in normalize_fa(c)),
            synonyms_df.columns[0],
        )
        dst_col = next(
            (c for c in synonyms_df.columns if "to" in normalize_fa(c) or "target" in normalize_fa(c)),
            synonyms_df.columns[1] if len(synonyms_df.columns) > 1 else synonyms_df.columns[0],
        )
        for _, row in synonyms_df.iterrows():
            src = normalize_fa(row.get(src_col, ""))
            dst = str(row.get(dst_col, "")).strip()
            if src and dst:
                synonyms[src] = dst

    return name_to_code, code_to_name, buckets, synonyms

# -----------------------------------------------------------------------------
def expand_group_token(
    token: str,
    name_to_code: dict[str, int],
    buckets: dict[str, list[tuple[str, int]]],
    synonyms: dict[str, str],
) -> list[tuple[str, int]]:
    t = normalize_fa(token)
    if not t:
        return []
    out: list[tuple[str, int]] = []
    # 1) Synonym
    if t in synonyms:
        syn = synonyms[t]
        if syn.startswith("__BUCKET__"):
            out.extend(buckets.get(syn.replace("__BUCKET__", ""), []))
        else:
            key = normalize_fa(syn)
            if key in name_to_code:
                out.append((syn, name_to_code[key]))
    # 2) Buckets
    checks = [
        (["متوسطه1", "متوسطه اول"], "متوسطه اول"),
        (["متوسطه2", "متوسطه دوم"], "متوسطه دوم"),
        (["دبستان"], "دبستان"),
        (["هنرستان"], "هنرستان"),
        (["کنکوری"], "کنکوری"),
    ]
    for kws, bucket in checks:
        if any(k in t for k in kws):
            out.extend(buckets.get(bucket, []))
    # 3) grade × major
    for num, name in [("10", "دهم"), ("11", "یازدهم"), ("12", "دوازدهم")]:
        if name in t or num in t:
            for kw, pretty in [
                ("تجربی", "تجربی"),
                ("ریاضی", "ریاضی"),
                ("انسانی", "انسانی"),
                ("علوم و معارف اسلامی", "علوم و معارف اسلامی"),
            ]:
                if normalize_fa(kw) in t:
                    title = f"{name} {pretty}".strip()
                    key = normalize_fa(title)
                    if key in name_to_code:
                        out.append((title, name_to_code[key]))
    # 4) direct
    if not out and t in name_to_code:
        out.append((token, name_to_code[t]))

    # dedup by code
    seen = set()
    uniq: list[tuple[str, int]] = []
    for n, c in out:
        if c not in seen:
            uniq.append((n, c))
            seen.add(c)
    return uniq

# =============================================================================
# SCHOOL MAPPINGS
# =============================================================================
@validate_dataframe_columns(COL_SCHOOL_CODE)
def build_school_maps(schools_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    name_cols = [c for c in schools_df.columns if "نام مدرسه" in c]
    if not name_cols:
        raise ValueError("ستون نام مدرسه در SchoolReport یافت نشد")
    code_to_name = dict(zip(schools_df[COL_SCHOOL_CODE].astype(str), schools_df[name_cols[0]].astype(str)))
    name_to_code: dict[str, str] = {}
    for _, r in schools_df.iterrows():
        code = str(r[COL_SCHOOL_CODE])
        for col in name_cols:
            nm = normalize_fa(r[col])
            if nm:
                name_to_code.setdefault(nm, code)
    return code_to_name, name_to_code


def safe_int_column(df: pd.DataFrame, col: str, default: int = 0) -> pd.Series:
    """تبدیل ستونی از DataFrame به نوع صحیح بدون تبدیل موقت به float."""

    series = df.get(col)
    if series is None:
        return pd.Series([int(default)] * len(df), index=df.index, dtype="Int64")
    coerced = series.map(_coerce_int_like)
    result = pd.Series(coerced, index=series.index, dtype="Int64")
    if default is not None:
        result = result.fillna(int(default))
    return result


def safe_int_value(value: Any, default: int = 0) -> int:
    """تبدیل ورودی به عدد صحیح با مدیریت مقادیر خالی.

    مثال ساده::

        >>> safe_int_value("7")
        7
        >>> safe_int_value("", default=2)
        2

    Args:
        value: مقداری که باید به int تبدیل شود.
        default: مقدار پیش‌فرض در صورت عدم امکان تبدیل.

    Returns:
        مقدار صحیح نرمال‌شده (حداقل برابر با ``default``).
    """

    if value is None:
        return int(default)

    parsed = _coerce_int_like(value)
    return parsed if parsed is not None else int(default)


def normalize_capacity_values(current: Any, special: Any, *, default: int = 0) -> tuple[int, int, int]:
    """نرمال‌سازی ستون‌های ظرفیت و محاسبهٔ ظرفیت باقی‌مانده.

    مثال::

        >>> normalize_capacity_values("5", "12")
        (5, 12, 7)

    Args:
        current: مقدار پوشش فعلی در Inspactor.
        special: ظرفیت ویژهٔ ثبت‌شده برای پشتیبان.
        default: مقدار جایگزین برای ورودی‌های نامعتبر.

    Returns:
        سه‌تایی ``(covered_now, special_limit, remaining_capacity)``.
    """

    covered = max(safe_int_value(current, default=default), 0)
    special_limit = max(safe_int_value(special, default=default), 0)
    remaining = max(special_limit - covered, 0)
    return covered, special_limit, remaining

# =============================================================================
# CAPACITY GATE (R0)
# =============================================================================
def capacity_gate(
    insp: pd.DataFrame,
    *,
    progress: ProgressFn = noop_progress,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    if CAPACITY_CURRENT_COL not in insp.columns or CAPACITY_SPECIAL_COL not in insp.columns:
        progress(10, "capacity columns missing; skipping capacity gate")
        return insp.copy(), pd.DataFrame(), True

    df = insp.copy()
    df["_cap_cur"] = safe_int_column(df, CAPACITY_CURRENT_COL, default=0)
    df["_cap_spec"] = safe_int_column(df, CAPACITY_SPECIAL_COL, default=0)

    removed_mask = ~(df["_cap_cur"] < df["_cap_spec"])

    keep_cols = [COL_MENTOR_NAME, COL_MANAGER_NAME, CAPACITY_CURRENT_COL, CAPACITY_SPECIAL_COL]
    if COL_SCHOOL1 in df.columns:
        keep_cols.append(COL_SCHOOL1)
    if COL_GROUP in df.columns:
        keep_cols.append(COL_GROUP)

    removed = df.loc[removed_mask, keep_cols].copy()
    removed = removed.rename(
        columns={
            CAPACITY_CURRENT_COL: "تحت پوشش فعلی",
            CAPACITY_SPECIAL_COL: "ظرفیت خاص",
            COL_MENTOR_NAME: "پشتیبان",
            COL_MANAGER_NAME: "مدیر",
            COL_SCHOOL1: "مدرسه (خام)",
            COL_GROUP: "گروه آزمایشی (خام)",
        }
    )
    removed["دلیل حذف"] = "تعداد داوطلبان تحت پوشش ≥ تعداد تحت پوشش خاص"

    kept = df.loc[~removed_mask].copy()
    drop_cols = ["_cap_cur", "_cap_spec"]
    kept = kept.drop(columns=drop_cols, errors="ignore")
    removed = removed.drop(columns=drop_cols, errors="ignore")

    progress(20, f"capacity gate kept={len(kept)} removed={len(removed)}")
    return kept, removed, False

# =============================================================================
# SCHOOL CODE EXTRACTION
# =============================================================================
def to_int_str_or_none(value: Any) -> str | None:
    parsed = _coerce_int_like(value)
    if parsed is None or parsed == 0:
        return None
    return str(parsed)


def collect_school_codes_from_row(r: pd.Series, name_to_code: dict[str, str], school_cols: list[str]) -> list[str]:
    codes = [
        code
        for col in school_cols
        if (raw := r.get(col)) and (code := to_int_str_or_none(raw) or name_to_code.get(normalize_fa(raw)))
    ]
    seen: set[str] = set()
    out: list[str] = []
    for c in codes:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out

# =============================================================================
# PROGRESS (optional)
# =============================================================================
try:
    from tqdm import tqdm  # type: ignore

    HAS_TQDM = True
except Exception:
    HAS_TQDM = False


def progress(it, total: int | None = None):
    return tqdm(it, total=total) if HAS_TQDM else it

# =============================================================================
# GROUP CODE PARSING
# =============================================================================
def parse_group_code_spec(
    spec: Any,
    *,
    valid_codes: Collection[int] | None = None,
    invalid_collector: list[int] | None = None,
) -> list[int]:
    """تبدیل رشتهٔ ورودی به فهرست کد گروه‌های معتبر.

    مثال::

        >>> parse_group_code_spec("27,31:35", valid_codes={27, 31, 33, 35})
        [27, 31, 33, 35]

    Args:
        spec: ورودی خام از ستون «شامل گروه های آزمایشی».
        valid_codes: مجموعهٔ کدهای مجاز برای فیلتر کردن خروجی.
        invalid_collector: لیست اختیاری برای ثبت کدهای نامعتبر.

    Returns:
        لیست یکتا از کدهای معتبر به ترتیب مشاهده‌شده.
    """

    if spec is None or (isinstance(spec, float) and math.isnan(spec)):
        return []

    s = str(spec).strip()
    if not s:
        return []

    s = s.translate(_TRANS_PERSIAN_DIGITS)
    parts = _RE_SPLIT_ITEMS.split(s)
    out: list[int] = []
    seen: set[int] = set()
    valid_set = set(valid_codes) if valid_codes is not None else None
    invalid_seen: set[int] = set()

    def _register(value: int) -> None:
        if valid_set is not None and value not in valid_set:
            if invalid_collector is not None and value not in invalid_seen:
                invalid_collector.append(value)
                invalid_seen.add(value)
            return
        if value not in seen:
            out.append(value)
            seen.add(value)

    for tok in parts:
        tok = tok.strip()
        if not tok:
            continue
        m = _RE_RANGE.match(tok)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a > b:
                a, b = b, a
            for value in range(a, b + 1):
                _register(value)
            continue
        if tok.isdigit():
            _register(int(tok))

    return out

# =============================================================================
# ROW GENERATION
# =============================================================================
def generate_row_variants(
    base: dict,
    group_pairs: List[tuple[str, int]],
    genders: List[Any],
    statuses: List[Any],
    schools_raw: List[Any],
    finance_variants: Iterable[int],
    code_to_name_school: Dict[str, str],
) -> List[dict]:
    rows: List[dict] = []

    def _school_lookup(sc_val: Any) -> tuple[str, str, int]:
        """returns (school_code, school_name, is_school_flag)
        Patch: empty -> ("0", "", 0) so output column «کد مدرسه» becomes 0.
        """
        # If empty → set school code to 0 (string to keep type consistent across DataFrame)
        if sc_val is None or str(sc_val).strip() == "":
            return "0", "", 0
        s = str(sc_val).strip()
        if s.isdigit():
            return s, code_to_name_school.get(s, ""), 1
        return "", str(sc_val), 0

    iter_genders = genders or [""]
    iter_statuses = statuses or [""]
    iter_schools = schools_raw if any(schools_raw) else [""]

    for (g_name, g_code), ge, st, fin, sc in product(
        group_pairs, iter_genders, iter_statuses, finance_variants, iter_schools
    ):
        gcode = norm_gender(ge) if ge != "" else None
        stcode = norm_status(st) if st != "" else None
        school_code, school_name, is_school = _school_lookup(sc)

        # Alias rule per SSoT
        alias_val = base["alias"] if is_school == 0 else base["mentor_id"]

        rows.append(
            {
                "جایگزین": int(alias_val) if str(alias_val).strip().isdigit() else str(alias_val),
                "پشتیبان": base["supporter"],
                "کد کارمندی پشتیبان": base["mentor_id"],
                "مدیر": base["manager"],
                "ردیف پشتیبان": int(base["row_id"]) if str(base["row_id"]).strip().isdigit() else "",
                "نام رشته": g_name,
                "کدرشته": int(g_code),
                "جنسیت": int(gcode) if gcode is not None else "",
                "دانش آموز فارغ": int(stcode) if stcode is not None else "",
                "مرکز گلستان صدرا": int(base["center_code"]),
                "مالی حکمت بنیاد": int(fin),
                "کد مدرسه": school_code,
                "عادی مدرسه": "مدرسه‌ای" if is_school else "عادی",
                "نام مدرسه": school_name,
                "جنسیت2": gender_text(gcode),
                "دانش آموز فارغ2": status_text(stcode),
                "مرکز گلستان صدرا3": base["center_text"],
                CAPACITY_CURRENT_COL: int(base.get("capacity_current", 0)),
                CAPACITY_SPECIAL_COL: int(base.get("capacity_special", 0)),
                "remaining_capacity": int(base.get("capacity_remaining", 0)),
            }
        )
    return rows

# =============================================================================
# VECTORIZED MATRIX ASSEMBLY HELPERS
# =============================================================================


def _prepare_base_rows(
    insp: pd.DataFrame,
    *,
    cfg: BuildConfig,
    name_to_code: dict[str, int],
    code_to_name: dict[int, str],
    buckets: dict[str, list[tuple[str, int]]],
    synonyms: dict[str, str],
    school_name_to_code: dict[str, str],
    code_to_name_school: dict[str, str],
    group_cols: list[str],
    school_cols: list[str],
    gender_col: str | None,
    included_col: str | None,
) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    records: list[dict[str, Any]] = []
    unseen_groups: list[dict[str, Any]] = []
    unmatched_schools: list[dict[str, Any]] = []

    finance_variants = list(cfg.finance_variants)

    for row in insp.to_dict(orient="records"):
        mentor_id_raw = row.get(COL_MENTOR_ID, "")
        mentor_id = str(mentor_id_raw).strip()
        if not mentor_id:
            continue

        if COL_CAN_ALLOC in row:
            can_alloc = str(row.get(COL_CAN_ALLOC, "")).strip() in cfg.can_allocate_truthy
            if not can_alloc:
                continue

        mentor_name = str(row.get(COL_MENTOR_NAME, "")).strip()
        manager_name = str(row.get(COL_MANAGER_NAME, "")).strip()

        alias_val = row.get(COL_POSTAL, "")
        alias_num = None
        alias_str = to_numlike_str(alias_val)
        if alias_str.isdigit():
            alias_int = int(alias_str)
            if MIN_POSTAL_CODE <= alias_int <= MAX_POSTAL_CODE:
                alias_num = alias_int

        school_codes = collect_school_codes_from_row(pd.Series(row), school_name_to_code, school_cols)
        school_count = safe_int_value(row.get(COL_SCHOOL_COUNT, 0), default=0)

        covered_now, special_limit, remaining_capacity = normalize_capacity_values(
            row.get(CAPACITY_CURRENT_COL, 0),
            row.get(CAPACITY_SPECIAL_COL, 0),
        )

        genders_raw = ensure_list([row.get(gender_col)]) if gender_col else [""]
        gender_codes: list[int | str] = []
        for token in genders_raw:
            token_str = str(token).strip()
            if not token_str:
                gender_codes.append("")
                continue
            normalized = norm_gender(token_str)
            gender_codes.append(int(normalized))

        raw_groups = ensure_list([row[c] for c in group_cols]) if group_cols else []
        group_pairs: list[tuple[str, int]] = []
        used_included = False
        if included_col:
            invalid_codes: list[int] = []
            codes = parse_group_code_spec(
                row.get(included_col),
                valid_codes=code_to_name.keys(),
                invalid_collector=invalid_codes,
            )
            if codes or invalid_codes:
                used_included = True
            for gc in codes:
                group_pairs.append((code_to_name[gc], gc))
            for gc in invalid_codes:
                unseen_groups.append(
                    {"group_token": f"code:{gc}", "supporter": mentor_name, "manager": manager_name}
                )

        if not used_included:
            expanded: list[tuple[str, int]] = []
            for tok in raw_groups or []:
                ex = expand_group_token(tok, name_to_code, buckets, synonyms)
                if not ex:
                    unseen_groups.append(
                        {"group_token": str(tok), "supporter": mentor_name, "manager": manager_name}
                    )
                expanded.extend(ex)
            seen_codes: set[int] = set()
            for name, code in expanded:
                if code not in seen_codes:
                    group_pairs.append((name, code))
                    seen_codes.add(code)

        if not group_pairs:
            continue

        alias_school = to_numlike_str(mentor_id_raw) or normalize_fa(mentor_id_raw) or mentor_id
        center = center_from_manager(manager_name, cfg=cfg)
        base = {
            "supporter": mentor_name,
            "manager": manager_name,
            "mentor_id": mentor_id,
            "mentor_row_id": row.get(COL_MENTOR_ROWID, ""),
            "center_code": int(center),
            "center_text": center_text(int(center)),
            "group_pairs": group_pairs,
            "genders": gender_codes or [""],
            "school_codes": school_codes,
            "schools_normal": [""],
            "finance": finance_variants,
            "statuses_normal": [Status.STUDENT, Status.GRADUATE],
            "statuses_school": [Status.STUDENT],
            "alias_normal": alias_num,
            "alias_school": alias_school,
            "can_normal": alias_num is not None,
            "can_school": bool(school_codes) or school_count > 0,
            "capacity_current": covered_now,
            "capacity_special": special_limit,
            "capacity_remaining": remaining_capacity,
        }

        for sc in school_codes:
            if sc not in code_to_name_school:
                unmatched_schools.append(
                    {"raw_school": str(sc), "supporter": mentor_name, "manager": manager_name}
                )

        records.append(base)

    base_df = pd.DataFrame(records)
    return base_df, unseen_groups, unmatched_schools


def _explode_rows(
    base: pd.DataFrame,
    *,
    alias_col: str,
    status_col: str,
    school_col: str,
    type_label: str,
    code_to_name_school: dict[str, str],
) -> pd.DataFrame:
    if base.empty:
        return pd.DataFrame()

    df = base.copy()
    df = df.loc[df[alias_col].notna()]
    if df.empty:
        return pd.DataFrame()

    df = df.assign(finance_list=df["finance"], school_list=df[school_col])
    df = df.drop(columns=["finance", school_col])
    df = df.explode("group_pairs")
    gp = pd.DataFrame(df.pop("group_pairs").tolist(), columns=["group_name", "group_code"], index=df.index)
    df = df.join(gp)
    df = df.explode("genders").explode(status_col).explode("school_list").explode("finance_list")

    if df.empty:
        return pd.DataFrame()

    def _optional_int(value: Any) -> int | type(pd.NA):
        if pd.isna(value):
            return pd.NA
        if isinstance(value, str) and not value.strip():
            return pd.NA
        parsed = _coerce_int_like(value)
        return parsed if parsed is not None else pd.NA

    gender_series = df["genders"]
    df["gender_code"] = gender_series.map(_optional_int).astype("Int64")

    status_series = df[status_col]
    df["status_code"] = status_series.map(_optional_int).astype("Int64")

    blank_school_mask = df["school_list"].map(lambda v: pd.isna(v) or (isinstance(v, str) and not v.strip()))
    school_codes = df["school_list"].map(_coerce_int_like)
    df["کد مدرسه"] = pd.Series(school_codes, index=df.index).fillna(0).astype("int64")
    df.loc[blank_school_mask.fillna(True), "کد مدرسه"] = 0
    df["کد مدرسه"] = df["کد مدرسه"].astype("int64")
    df["نام مدرسه"] = df["کد مدرسه"].astype(str).map(code_to_name_school).fillna("")

    alias_series = df[alias_col]
    df["جایگزین"] = alias_series.map(to_numlike_str)

    df = df.drop(columns=["genders", status_col, "school_list", alias_col])
    df["عادی مدرسه"] = type_label

    df = df.rename(
        columns={
            "supporter": "پشتیبان",
            "mentor_id": "کد کارمندی پشتیبان",
            "manager": "مدیر",
            "mentor_row_id": "ردیف پشتیبان",
            "group_name": "نام رشته",
            "group_code": "کدرشته",
            "center_code": "مرکز گلستان صدرا",
            "finance_list": "مالی حکمت بنیاد",
        }
    )

    df[CAPACITY_CURRENT_COL] = safe_int_column(df, "capacity_current")
    df[CAPACITY_SPECIAL_COL] = safe_int_column(df, "capacity_special")
    df["remaining_capacity"] = safe_int_column(df, "capacity_remaining")

    df["مالی حکمت بنیاد"] = safe_int_column(df, "مالی حکمت بنیاد")
    df["جنسیت"] = df["gender_code"].astype("Int64")
    df["دانش آموز فارغ"] = df["status_code"].astype("Int64")
    df["جنسیت2"] = df["جنسیت"].map(lambda v: gender_text(v) if pd.notna(v) else "")
    df["دانش آموز فارغ2"] = df["دانش آموز فارغ"].map(lambda v: status_text(v) if pd.notna(v) else "")
    df["مرکز گلستان صدرا3"] = df["center_text"]

    df = df.drop(
        columns=[
            "gender_code",
            "status_code",
            "center_text",
            "capacity_current",
            "capacity_special",
            "capacity_remaining",
        ],
        errors="ignore",
    )
    ordered_columns = [
        "جایگزین",
        "پشتیبان",
        "کد کارمندی پشتیبان",
        "مدیر",
        "ردیف پشتیبان",
        "نام رشته",
        "کدرشته",
        "جنسیت",
        "دانش آموز فارغ",
        "مرکز گلستان صدرا",
        "مالی حکمت بنیاد",
        "کد مدرسه",
        "نام مدرسه",
        "عادی مدرسه",
        "جنسیت2",
        "دانش آموز فارغ2",
        "مرکز گلستان صدرا3",
        CAPACITY_CURRENT_COL,
        CAPACITY_SPECIAL_COL,
        "remaining_capacity",
    ]
    return df[ordered_columns]


# =============================================================================
# BUILD MATRIX
# =============================================================================
def build_matrix(
    insp_df: pd.DataFrame,
    schools_df: pd.DataFrame,
    crosswalk_groups_df: pd.DataFrame,
    *,
    crosswalk_synonyms_df: pd.DataFrame | None = None,
    cfg: BuildConfig = BuildConfig(),
    progress: ProgressFn = noop_progress,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """ساخت ماتریس اهلیت بر مبنای دیتافریم‌های ورودی.

    مثال ساده::

        >>> matrix, *_ = build_matrix(insp_df, schools_df, crosswalk_df)  # doctest: +SKIP

    Args:
        insp_df: دیتافریم خام گزارش Inspactor.
        schools_df: دیتافریم نگاشت مدارس.
        crosswalk_groups_df: دیتافریم گروه‌های آزمایشی.
        crosswalk_synonyms_df: دیتافریم نگاشت نام‌های مترادف.
        cfg: پیکربندی ساخت ماتریس.
        progress: تابع پیشرفت تزریق‌شده از لایهٔ زیرساخت.

    Returns:
        شش‌تایی دیتافریم شامل ماتریس و جداول کنترلی.
    """
    progress(5, "preparing crosswalk mappings")
    name_to_code, code_to_name, buckets, synonyms = prepare_crosswalk_mappings(
        crosswalk_groups_df,
        crosswalk_synonyms_df,
    )
    code_to_name_school, school_name_to_code = build_school_maps(schools_df)

    insp = insp_df.copy()

    if cfg.enable_capacity_gate:
        insp, removed_mentors, r0_skipped = capacity_gate(insp, progress=progress)
    else:
        removed_mentors = pd.DataFrame()
        r0_skipped = True
        progress(15, "capacity gate disabled by config")

    # detect columns
    gender_col = COL_GENDER if COL_GENDER in insp.columns else None
    included_col = next(
        (c for c in insp.columns if normalize_fa(c) == normalize_fa(COL_GROUP_INCLUDED)),
        next((c for c in insp.columns if all(k in normalize_fa(c) for k in ("شامل", "گروه", "آزمایشی"))), None),
    )
    group_cols = [c for c in insp.columns if ("گروه آزمایشی" in str(c)) and (c != included_col)]
    school_cols = [c for c in [COL_SCHOOL1, COL_SCHOOL2, COL_SCHOOL3, COL_SCHOOL4] if c in insp.columns]

    # generate rows
    progress(30, "preparing vectorized base rows")
    mentor_id_series = insp[COL_MENTOR_ID].astype(str).str.strip()
    invalid_mask = mentor_id_series.eq("") | insp[COL_MENTOR_ID].isna()
    invalid_mentors_df = pd.DataFrame(
        {
            "row_index": (insp.index[invalid_mask] + 1),
            "پشتیبان": insp.loc[invalid_mask, COL_MENTOR_NAME],
            "مدیر": insp.loc[invalid_mask, COL_MANAGER_NAME],
            "reason": "missing mentor employee code",
        }
    )
    if invalid_mentors_df.empty:
        invalid_mentors_df = pd.DataFrame(columns=["row_index", "پشتیبان", "مدیر", "reason"])

    insp_valid = insp.loc[~invalid_mask].copy()

    base_df, unseen_groups, unmatched_schools = _prepare_base_rows(
        insp_valid,
        cfg=cfg,
        name_to_code=name_to_code,
        code_to_name=code_to_name,
        buckets=buckets,
        synonyms=synonyms,
        school_name_to_code=school_name_to_code,
        code_to_name_school=code_to_name_school,
        group_cols=group_cols,
        school_cols=school_cols,
        gender_col=gender_col,
        included_col=included_col,
    )

    progress(55, "assembling matrix variants")
    normal_df = _explode_rows(
        base_df.loc[base_df["can_normal"]],
        alias_col="alias_normal",
        status_col="statuses_normal",
        school_col="schools_normal",
        type_label="عادی",
        code_to_name_school=code_to_name_school,
    )
    school_df = _explode_rows(
        base_df.loc[base_df["can_school"]],
        alias_col="alias_school",
        status_col="statuses_school",
        school_col="school_codes",
        type_label="مدرسه‌ای",
        code_to_name_school=code_to_name_school,
    )

    matrix = pd.concat([normal_df, school_df], ignore_index=True, sort=False)
    if matrix.empty:
        matrix = pd.DataFrame(
            columns=[
                "جایگزین",
                "پشتیبان",
                "کد کارمندی پشتیبان",
                "مدیر",
                "ردیف پشتیبان",
                "نام رشته",
                "کدرشته",
                "جنسیت",
                "دانش آموز فارغ",
                "مرکز گلستان صدرا",
                "مالی حکمت بنیاد",
                "کد مدرسه",
                "عادی مدرسه",
                "نام مدرسه",
                "جنسیت2",
                "دانش آموز فارغ2",
                "مرکز گلستان صدرا3",
                CAPACITY_CURRENT_COL,
                CAPACITY_SPECIAL_COL,
                "remaining_capacity",
            ]
        )

    unseen_groups_df = pd.DataFrame(unseen_groups).drop_duplicates() if unseen_groups else pd.DataFrame()
    unmatched_schools_df = (
        pd.DataFrame(unmatched_schools).drop_duplicates() if unmatched_schools else pd.DataFrame()
    )

    if not matrix.empty:
        matrix["ردیف پشتیبان"] = matrix["ردیف پشتیبان"].map(_coerce_int_like)
        matrix["کد مدرسه"] = pd.Series(
            matrix["کد مدرسه"].map(_coerce_int_like), index=matrix.index
        ).fillna(0).astype("int64")
        matrix["جایگزین"] = matrix["جایگزین"].map(to_numlike_str)
        matrix["_school_sort"] = matrix["کد مدرسه"].map(
            lambda v: safe_int_value(v, default=SCHOOL_CODE_NULL_SORT)
        )
        matrix["_alias_sort"] = matrix["جایگزین"].map(
            lambda v: safe_int_value(v, default=ALIAS_FALLBACK_SORT)
        )
        matrix = matrix.sort_values(
            by=["مرکز گلستان صدرا", "کدرشته", "_school_sort", "_alias_sort"],
            kind="stable",
        )
        matrix = matrix.drop(columns=["_school_sort", "_alias_sort"])

    matrix.insert(0, "counter", range(1, len(matrix) + 1))

    validation = pd.DataFrame(
        [
            {
                "total_rows": len(matrix),
                "distinct_supporters": matrix["پشتیبان"].nunique() if not matrix.empty else 0,
                "school_based_rows": int((matrix["عادی مدرسه"] == "مدرسه‌ای").sum()) if not matrix.empty else 0,
                "finance_0_rows": int((matrix["مالی حکمت بنیاد"] == Finance.NORMAL).sum()) if not matrix.empty else 0,
                "finance_1_rows": int((matrix["مالی حکمت بنیاد"] == Finance.BONYAD).sum()) if not matrix.empty else 0,
                "finance_3_rows": int((matrix["مالی حکمت بنیاد"] == Finance.HEKMAT).sum()) if not matrix.empty else 0,
                "removed_mentors": 0 if removed_mentors is None else len(removed_mentors),
                "r0_skipped": 1 if r0_skipped else 0,
            }
        ]
    )

    removed_df = removed_mentors
    progress(90, "matrix assembly complete")
    return matrix, validation, removed_df, unmatched_schools_df, unseen_groups_df, invalid_mentors_df


# =============================================================================
# VALIDATION vs StudentReport (optional)
# =============================================================================
def infer_students_gender_from_hint(source: str | None) -> int | None:
    s = str(source or "")
    if "3570" in s:
        return Gender.MALE
    if "3730" in s:
        return Gender.FEMALE
    return None


def resolve_students_gender_series(
    stud_df: pd.DataFrame,
    *,
    source_hint: str | None = None,
    mode: str = "auto",
) -> pd.Series:
    mode = (mode or "auto").lower()
    n = len(stud_df)
    if mode == "male":
        return pd.Series([Gender.MALE] * n, index=stud_df.index)
    if mode == "female":
        return pd.Series([Gender.FEMALE] * n, index=stud_df.index)
    hint = infer_students_gender_from_hint(source_hint)
    if hint is not None:
        return pd.Series([hint] * n, index=stud_df.index)
    if "جنسیت" in stud_df.columns:
        return stud_df["جنسیت"].apply(norm_gender).fillna(Gender.MALE)
    return pd.Series([Gender.MALE] * n, index=stud_df.index)


def validate_with_students(
    students_df: pd.DataFrame,
    matrix_df: pd.DataFrame,
    schools_df: pd.DataFrame,
    crosswalk_groups_df: pd.DataFrame,
    *,
    crosswalk_synonyms_df: pd.DataFrame | None = None,
    students_gender_mode: str = "auto",
    students_source_hint: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    stud_raw = students_df.copy()
    schools = schools_df.copy()
    _, school_name_to_code = build_school_maps(schools)
    name_to_code, _, _, _ = prepare_crosswalk_mappings(
        crosswalk_groups_df,
        crosswalk_synonyms_df,
    )

    # Support "کد پستی" OR "کد جایگزین"
    std_alias_col = "کد پستی" if "کد پستی" in stud_raw.columns else ("کد جایگزین" if "کد جایگزین" in stud_raw.columns else None)
    if std_alias_col is None:
        raise ValueError("در StudentReport ستونی با عنوان «کد پستی» یا «کد جایگزین» یافت نشد.")

    stud = pd.DataFrame(
        {
            "student_postal": stud_raw[std_alias_col].apply(to_numlike_str),
            "alias_norm": stud_raw[std_alias_col].apply(to_numlike_str),
            "mentor_name": stud_raw["نام پشتیبان"].astype(str).str.strip(),
            "manager": stud_raw["مدیر"].astype(str).str.strip(),
            "group_code": stud_raw["گروه آزمایشی"].apply(lambda x: name_to_code.get(normalize_fa(x), None)),
            "school_code": stud_raw[COL_SCHOOL1].apply(
                lambda x: school_name_to_code.get(normalize_fa(x), "")
            )
            if COL_SCHOOL1 in stud_raw.columns
            else "",
        }
    )
    stud["status_code"] = Status.STUDENT
    stud["gender_code"] = resolve_students_gender_series(
        stud_raw,
        source_hint=students_source_hint,
        mode=students_gender_mode,
    ).values

    mat = matrix_df.copy()
    mat["alias_norm"] = mat["جایگزین"].apply(to_numlike_str)
    mat["school_code"] = mat["کد مدرسه"].astype(str).str.strip()

    def _student_type_from_postal(v: str) -> str:
        if not v:
            return "normal_by_alias"
        try:
            iv = int(v)
            if iv < MIN_POSTAL_CODE:
                return "school_by_schoolcode"
            if MIN_POSTAL_CODE <= iv <= MAX_POSTAL_CODE:
                return "normal_by_alias"
            return "school_by_mentorid"
        except Exception:
            return "school_by_mentorid"

    def _sub_by_type(row: pd.Series) -> tuple[pd.DataFrame, str | None]:
        stype = _student_type_from_postal(row["student_postal"])
        if stype == "normal_by_alias":
            sub = mat[(mat["عادی مدرسه"] == "عادی") & (mat["alias_norm"] == row["alias_norm"])]
            return (sub, None if not sub.empty else "no normal alias match")
        if stype == "school_by_mentorid":
            sub = mat[(mat["عادی مدرسه"] == "مدرسه‌ای") & (mat["alias_norm"] == row["alias_norm"])]
            return (sub, None if not sub.empty else "no mentor-id school match")
        sub = mat[(mat["عادی مدرسه"] == "مدرسه‌ای")]
        if row["school_code"]:
            sub = sub[sub["school_code"].astype(str) == str(row["school_code"])]
        return (sub, None if not sub.empty else "no school-code match")

    def _check_gender(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        sub2 = sub[sub["جنسیت"] == row["gender_code"]]
        return (sub2, None if not sub2.empty else "gender mismatch")

    def _check_status(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        sub2 = sub[sub["دانش آموز فارغ"] == row["status_code"]]
        return (sub2, None if not sub2.empty else "status mismatch")

    def _check_center(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        def _center_from_manager(n: str) -> int:
            if "شهدخت" in str(n) and "کشاورز" in str(n):
                return Center.GOLESTAN
            if "آیناز" in str(n) and "هوشمند" in str(n):
                return Center.SADRA
            return Center.MARKAZ

        expected = _center_from_manager(row["manager"])
        sub2 = sub[sub["مرکز گلستان صدرا"] == expected]
        return (sub2, None if not sub2.empty else "center mismatch (manager-based)")

    def _check_group(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        if row["group_code"] is None:
            return (sub, None)
        sub2 = sub[sub["کدرشته"] == row["group_code"]]
        return (sub2, None if not sub2.empty else "group_code mismatch")

    def first_fail_reason(row: pd.Series) -> str:
        sub, err = _sub_by_type(row)
        if err:
            return err
        for checker in (_check_gender, _check_status, _check_center, _check_group):
            sub, err = checker(row, sub)
            if err:
                return err
        return "MATCHED"

    stud["reason"] = stud.apply(first_fail_reason, axis=1)
    stud["match"] = stud["reason"].eq("MATCHED")

    breakdown = stud["reason"].value_counts().reset_index()
    breakdown.columns = ["reason", "count"]
    summary = {"total": int(len(stud)), "matched": int(stud["match"].sum()), "unmatched": int((~stud["match"]).sum())}

    return stud, breakdown, summary

