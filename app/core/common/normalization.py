# -*- coding: utf-8 -*-
"""
A compact, fail-safe Persian normalization helper (Python 3.10+).

Public API:
- normalize_fa(text: Any) -> str
- to_numlike_str(value: Any) -> str
- ensure_list(values: Iterable[Any]) -> List[str]
- strip_school_code_separators(text: str) -> str

Design notes:
- Side-effect free on import and on inputs.
- Deterministic; no exceptions escape to callers.
- Core string normalization cached with @lru_cache(maxsize=1024).
- Only standard library; no external deps.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Set, Dict, Tuple, Mapping, MutableMapping, Optional
import re
import unicodedata
from functools import lru_cache
import math
from logging import Logger, getLogger

import pandas as pd

LOGGER = getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & Regex Patterns (internal)
# ---------------------------------------------------------------------------

# Only these explicit BIDI control code points; no ranges allowed.
_RE_BIDI = re.compile(r"[\u200c\u200d\u200e\u200f\u202a\u202b\u202c\u202d\u202e]")

# Non-word: anything that's not a Persian letter block, an ASCII digit, or whitespace.
# NOTE: Uses '+' quantifier to batch replacements (performance).
_RE_NONWORD = re.compile(r"[^0-9\u0600-\u06FF\s]+")

# Collapse any whitespace run to a single space.
_RE_WHITESPACE = re.compile(r"\s+")

# List separators: comma, pipe, Arabic comma, Arabic semicolon.
_SEP_SPLIT = re.compile(r"[,\|،؛]+")

_ARABIC_TO_PERSIAN = str.maketrans({"ي": "ی", "ك": "ک"})
_PERSIAN_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

_SCHOOL_CODE_SEPARATOR_TRANSLATION = str.maketrans(
    {
        "-": " ",
        "−": " ",  # minus sign
        "‑": " ",  # non-breaking hyphen
        "–": " ",  # en dash
        "—": " ",  # em dash
        "―": " ",  # horizontal bar
        "﹘": " ",  # small em dash
        "﹣": " ",  # small hyphen-minus
        "／": " ",  # full-width slash
        "/": " ",
        "\\": " ",
        "⁄": " ",
        "ـ": "",  # kashida
    }
)

# Arabic-Indic (0660–0669) and Extended Arabic-Indic (06F0–06F9) → ASCII digits
_DIGIT_TRANSLATION: Dict[int, int] = {
    **{ord(chr(0x0660 + i)): ord(str(i)) for i in range(10)},
    **{ord(chr(0x06F0 + i)): ord(str(i)) for i in range(10)},
}

# Numeric symbol translation for number-like normalization contexts.
_NUM_SYM_TRANSLATION: Dict[int, int | None] = {
    ord("\u2212"): ord("-"),  # MINUS SIGN → hyphen-minus
    ord("\u066B"): ord("."),  # ARABIC DECIMAL SEPARATOR → '.'
    # Thousand separators to remove in numeric contexts
    ord("\u066C"): None,      # ARABIC THOUSANDS SEPARATOR
    ord(","): None,
    ord("،"): None,
    ord("_"): None,
    ord("\u00A0"): None,      # NBSP
    ord(" "): None,           # plain space (numeric path only)
}


def strip_school_code_separators(text: str) -> str:
    """حذف جداکننده‌های متداول «کد مدرسه» پیش از تبدیل به مقدار عددی.

    مثال::

        >>> strip_school_code_separators("35-81")
        '35 81'
    """

    return text.translate(_SCHOOL_CODE_SEPARATOR_TRANSLATION)

# Minimal Arabic → Persian letter fixes
_AR2FA_MAP: Dict[str, str] = {
    "ي": "ی",
    "ى": "ی",
    "ك": "ک",
    "ة": "ه",
}

# Detect repeated 'ل' tokens separated by spaces (to collapse after special "الله" mapping).
_RE_ALLAH_MULTI_L = re.compile(r"^(?:ل\s+)+ل$")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_nan_like(x: Any) -> bool:
    """Return True for None/empty/NaN-like inputs. Fail-safe."""
    try:
        if x is None:
            return True
        if isinstance(x, float):
            return not math.isfinite(x) or math.isnan(x)
        if isinstance(x, (bytes, bytearray)):
            try:
                s = x.decode("utf-8", "ignore")
            except Exception:
                return True
        else:
            s = str(x)
        s = s.strip().lower()
        return s in {"", "nan", "none", "null", "nil", "na", "n/a"}
    except Exception:
        return True


def _trim_fraction_zeros(num_str: str) -> str:
    """Remove trailing fractional zeros; fix '.5'→'0.5' and '10.'→'10'."""
    try:
        if "." not in num_str:
            return num_str
        sign = ""
        if num_str.startswith("-"):
            sign, num_str = "-", num_str[1:]
        int_part, _, frac_part = num_str.partition(".")
        frac_part = frac_part.rstrip("0")
        if not int_part:
            int_part = "0"
        if frac_part:
            return f"{sign}{int_part}.{frac_part}"
        return f"{sign}{int_part}"
    except Exception:
        return num_str or ""


def _format_float_stable(val: float) -> str:
    """Deterministic float to string with up to 12 significant digits; non-finite → '0'."""
    try:
        if not math.isfinite(val) or math.isnan(val):
            return "0"
        s = format(val, ".12g")
        if s in {"-0", "-0.0"}:
            s = "0"
        s = _trim_fraction_zeros(s)
        return s
    except Exception:
        return "0"


def _to_stable_str(value: Any, _seen: Set[int] | None = None) -> str:
    """
    Deterministic, cycle-safe conversion of any value to a stable string.
    - dict: sort by the stable-string of keys
    - list/tuple: preserve order; include length for stability
    - set: sort by the stable-string of elements
    - bytes: UTF-8 decode with errors='ignore'
    - float: 12 significant digits; NaN/Inf → '0'
    - cycles: return '<cycle>' sentinel
    """
    try:
        if _seen is None:
            _seen = set()
        obj_id = id(value)
        if obj_id in _seen:
            return "<cycle>"
        _seen.add(obj_id)

        if _is_nan_like(value):
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (bytes, bytearray)):
            try:
                return value.decode("utf-8", "ignore")
            except Exception:
                return ""
        if isinstance(value, float):
            return _format_float_stable(value)
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, dict):
            items: List[Tuple[str, Any]] = []
            for k in value.keys():
                ks = _to_stable_str(k, _seen)
                items.append((ks, k))
            items.sort(key=lambda pair: pair[0])
            parts = []
            for ks, k in items:
                vs = _to_stable_str(value[k], _seen)
                parts.append(f"{ks}:{vs}")
            return "{" + ",".join(parts) + "}"
        if isinstance(value, (list, tuple)):
            parts = [_to_stable_str(v, _seen) for v in value]
            return "[" + str(len(parts)) + "|" + ",".join(parts) + "]"
        if isinstance(value, set):
            parts = sorted((_to_stable_str(v, _seen) for v in value))
            return "{" + ",".join(parts) + "}"

        name = type(value).__name__
        s = str(value)
        if " at 0x" in s:
            return f"<{name}>"
        return s
    except Exception:
        return ""


def _remove_combining(s: str) -> str:
    """Drop all combining marks in categories Mn/Mc/Me."""
    try:
        return "".join(ch for ch in s if unicodedata.category(ch) not in {"Mn", "Mc", "Me"})
    except Exception:
        return ""


def _apply_arabic_to_persian_maps(s: str) -> str:
    """Arabic→Persian letter normalization (ي/ى→ی, ك→ک, ة→ه)."""
    try:
        return s.translate(str.maketrans(_AR2FA_MAP))
    except Exception:
        return s


def _apply_allah_special(s: str) -> str:
    """Map 'ﷲ' (U+FDF2) and 'الله' to a single 'ل'."""
    try:
        if not s:
            return s
        s = s.replace("\ufdf2", "ل")
        s = s.replace("الله", "ل")
        return s
    except Exception:
        return s


def _translate_digits_and_symbols_for_text(s: str) -> str:
    """
    Convert Arabic/Persian digits to ASCII and normalize U+2212→'-', U+066B→'.' for text paths.
    Thousand separators are handled by _RE_NONWORD, so not removed here.
    """
    try:
        table = _DIGIT_TRANSLATION | {ord("\u2212"): ord("-"), ord("\u066B"): ord(".")}
        return s.translate(table)
    except Exception:
        return s


def _numlike_ascii_cleanup(s: str) -> str:
    """
    Normalize a possibly number-like string to ASCII-only:
    - digits and at most one '.'
    - optional leading '-'
    - remove thousands separators (U+066C, ',', '،', space, NBSP, '_')
    - keep only the first '.' if multiple
    - trim leading zeros in integer part and trailing zeros in fractional part
    - avoid '-0'
    """
    try:
        if not s:
            return ""
        s = s.translate(_DIGIT_TRANSLATION | _NUM_SYM_TRANSLATION)
        if not s:
            return ""
        keep: List[str] = []
        first_dot_used = False
        is_negative = False
        for ch in s:
            if ch == "-":
                is_negative = True
                continue
            if ch == ".":
                if not first_dot_used:
                    keep.append(".")
                    first_dot_used = True
                continue
            if "0" <= ch <= "9":
                keep.append(ch)
        if not keep:
            return ""
        num = "".join(keep)
        if "." in num:
            int_part, _, frac_part = num.partition(".")
        else:
            int_part, frac_part = num, ""
        int_part = int_part.lstrip("0") or "0"
        frac_part = frac_part.rstrip("0")
        num = f"{int_part}.{frac_part}" if frac_part else int_part
        if is_negative and not (int_part == "0" and not frac_part):
            num = "-" + num
        if num.startswith("."):
            num = "0" + num
        return num
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Core normalization (cached)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1024)
def _normalize_core(s: str) -> str:
    """
    Cached core Persian normalization pipeline.

    Steps:
    1) NFKD
    2) Remove Mn/Mc/Me
    3) Arabic→Persian maps (ي/ى/ك/ة)
    4) Remove explicit BIDI controls (by replacement with a single space)
    5) Special-case: 'ﷲ' and 'الله' → 'ل'
    6) Digits & numeric symbols: Arabic/Persian digits→ASCII; U+2212→'-'; U+066B→'.'
    7) Replace non-(Persian letter | ASCII digit | whitespace) with space (batched '+')
    8) Collapse whitespace, strip, lower()
    9) If string is just repeated 'ل' tokens, collapse to a single 'ل'
    """
    try:
        s = unicodedata.normalize("NFKD", s)
        s = _remove_combining(s)
        s = _apply_arabic_to_persian_maps(s)
        s = _RE_BIDI.sub(" ", s)
        s = _apply_allah_special(s)
        s = _translate_digits_and_symbols_for_text(s)
        s = _RE_NONWORD.sub(" ", s)
        s = _RE_WHITESPACE.sub(" ", s).strip().lower()
        if _RE_ALLAH_MULTI_L.fullmatch(s):
            return "ل"
        return s
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_fa(text: Any) -> str:
    """
    Normalize arbitrary input to a stable Persian string.
    Returns empty string on any error.

    Doctests (acceptance):
    >>> normalize_fa("كريم ياسر ١٢۳") == "کریم یاسر 123"
    True
    >>> normalize_fa("س\\u200cلام") == "س لام"
    True
    >>> normalize_fa("الله ﷲ") == "ل"
    True
    """
    try:
        s = _to_stable_str(text)
        if not s:
            return ""
        return _normalize_core(s)
    except Exception:
        return ""


def to_numlike_str(value: Any) -> str:
    """
    Convert number-like inputs to canonical ASCII representation.
    Returns "" for None/NaN-like/invalid values.

    Rules:
    - Arabic/Persian digits → ASCII; U+066B→'.'; U+2212→'-'
    - Thousand separators (U+066C, ',', '،', space, NBSP, '_') are removed
    - Only digits, '.', and a single leading '-' allowed; multiple '.' → keep first
    - Trim leading zeros in integer part and trailing zeros in fractional part
    - Avoid '-0'

    Doctests (acceptance):
    >>> to_numlike_str("١٬٢٣٤٫٥٠") == "1234.5"
    True
    >>> to_numlike_str("-000") == "0"
    True
    >>> to_numlike_str("-000.00") == "0"
    True
    """
    try:
        if _is_nan_like(value):
            return ""
        s = _to_stable_str(value)
        if not s:
            return ""
        return _numlike_ascii_cleanup(s)
    except Exception:
        return ""


def ensure_list(values: Iterable[Any]) -> List[str]:
    """
    Split and normalize a heterogeneous list of values into a unique, order-preserving list of strings.
    - Splits strings by any of [, | ، ؛]+
    - Drops empty/NaN-like tokens, "0"/"۰", and tokens where to_numlike_str(...) == "0"
    - Each token is normalize_fa(...) first; tokens empty after normalization are dropped
    - Uniqueness is preserved by first occurrence

    Doctests (acceptance):
    >>> ensure_list([" الف،ب ", "الف|ج", None, "۰"]) == ["الف", "ب", "ج"]
    True
    """
    out: List[str] = []
    seen: Set[str] = set()
    try:
        if isinstance(values, (str, bytes, bytearray)):
            values_iter = [values]
        else:
            values_iter = values  # type: ignore[assignment]
        for item in values_iter:
            if _is_nan_like(item):
                continue
            s = _to_stable_str(item)
            if not s:
                continue
            parts = [p for p in _SEP_SPLIT.split(s) if p != ""]
            if not parts:
                parts = [s]
            for token in parts:
                t = normalize_fa(token)
                if not t:
                    continue
                if t in {"0", "۰"}:
                    continue
                if to_numlike_str(t) == "0":
                    continue
                if t not in seen:
                    seen.add(t)
                    out.append(t)
        return out
    except Exception:
        return out


def normalize_header(name: Any) -> str:
    """نرمال‌سازی عنوان ستون با حذف نیم‌فاصله، حروف عربی و فاصله‌های اضافه."""

    try:
        text = str(name or "").strip()
    except Exception:
        text = ""
    if not text:
        return ""
    text = text.replace("\u200c", "")
    text = text.translate(_ARABIC_TO_PERSIAN)
    text = text.translate(_PERSIAN_DIGITS)
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = text.replace("_", " ")
    text = _RE_WHITESPACE.sub(" ", text)
    return text


def parse_int_safe(value: Any) -> Optional[int]:
    """تبدیل امن مقدار به عدد صحیح؛ فقط اعداد درست یا اعشاری با صفر اعشار پذیرفته می‌شوند."""

    candidate = to_numlike_str(value)
    if not candidate:
        return None
    sign = 1
    if candidate[0] in "+-":
        sign = -1 if candidate[0] == "-" else 1
        candidate = candidate[1:]
    if not candidate:
        return None
    if "." in candidate:
        integer_part, decimal_part = candidate.split(".", 1)
        if not integer_part or not integer_part.isdigit() or set(decimal_part) - {"0"}:
            return None
        candidate = integer_part
    if not candidate.isdigit():
        return None
    try:
        return sign * int(candidate)
    except Exception:
        return None


def resolve_group_code(
    row: "pd.Series",
    group_map: Mapping[str, int],
    *,
    major_column: str,
    group_column: str,
    prefer_major_code: bool = True,
    stats: MutableMapping[str, int] | None = None,
    logger: Logger | None = None,
) -> Optional[int]:
    """تعیین کد رشتهٔ دانش‌آموز با اولویت «کد رشته» و سپس نگاشت Crosswalk."""

    def _bump(key: str) -> None:
        if stats is None:
            return
        stats[key] = stats.get(key, 0) + 1

    major_raw = row.get(major_column)
    major_code = parse_int_safe(major_raw)

    group_name_raw = row.get(group_column)
    group_code = None
    if group_name_raw is not None and not pd.isna(group_name_raw):
        normalized_name = normalize_fa(group_name_raw)
        if normalized_name:
            group_code = group_map.get(normalized_name)

    if prefer_major_code and major_code is not None:
        if group_code is not None and group_code != major_code:
            _bump("mismatch_major_vs_group")
            active_logger = logger or LOGGER
            student_ref = row.get("student_id") or row.get("student_postal") or row.name
            active_logger.warning(
                "student %s: mismatch between major_code=%s and group name mapping=%s -> using major_code",
                student_ref,
                major_code,
                group_code,
            )
        _bump("resolved_by_major_code")
        return major_code

    if group_code is not None:
        _bump("resolved_by_crosswalk")
        return group_code

    _bump("unresolved_group_code")
    return None


def safe_int_value(x, default: int = 0) -> int:
    """تبدیل امن مقدار به عدد صحیح بدون پذیرش اعشار ساختگی."""

    s = to_numlike_str(x).strip()
    if s and s.lstrip("-").isdigit():
        try:
            return int(s)
        except Exception:
            pass
    return int(default)


_BIDI_REMOVALS = {
    0x200E,  # LRM
    0x200F,  # RLM
    0x202A,
    0x202B,
    0x202C,
    0x202D,
    0x202E,
}
_ZWJ = "\u200d"
_ZWNJ = "\u200c"
_PERSIAN_DIGIT_TABLE = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def sanitize_bidi(text: object) -> str:
    """حذف کاراکترهای کنترل جهت و فشرده‌سازی نیم‌فاصله."""

    if text is None:
        return ""
    result: list[str] = []
    previous_zwnj = False
    for char in str(text):
        codepoint = ord(char)
        if codepoint in _BIDI_REMOVALS or char == _ZWJ:
            continue
        if char == _ZWNJ:
            if previous_zwnj:
                continue
            previous_zwnj = True
            result.append(char)
            continue
        previous_zwnj = False
        if unicodedata.category(char) in {"Cc", "Cf"} and char not in {"\t", "\n", "\r"}:
            continue
        result.append(char)
    return "".join(result)


def fa_digitize(text: object) -> str:
    """تبدیل ارقام لاتین به فارسی برای نمایش."""

    if text is None:
        return ""
    sanitized = sanitize_bidi(text)
    return sanitized.translate(_PERSIAN_DIGIT_TABLE)


def safe_truncate(text: object, max_len: int) -> str:
    """ترانکیشن یونیکد-ایمن با حفظ نیم‌فاصله و افزودن «…» در صورت نیاز."""

    if max_len <= 0:
        return ""
    value = fa_digitize(text)
    if len(value) <= max_len:
        return value
    if max_len == 1:
        return "…"
    limit = max_len - 1
    trimmed = value[:limit]
    while trimmed and unicodedata.combining(trimmed[-1]):
        trimmed = trimmed[:-1]
    return f"{trimmed}…"


def extract_ascii_digits(value: Any) -> str:
    """استخراج تنها ارقام انگلیسی از ورودی متنی/عددی."""

    try:
        if _is_nan_like(value):
            return ""
        text = _to_stable_str(value)
        if not text:
            return ""
        translated = text.translate(_PERSIAN_DIGITS)
        digits = "".join(ch for ch in translated if ch.isdigit())
        return digits
    except Exception:
        return ""


def normalize_persian_text(text: Any) -> str:
    """نرمال‌سازی ملایم متن فارسی برای مصرف در هسته و خروجی.

    - حذف کاراکترهای کنترل جهت و نیم‌فاصله‌های تکراری
    - یکسان‌سازی حروف عربی/فارسی (ی/ى، ک/ك)
    - تبدیل ارقام عربی/فارسی به لاتین جهت محاسبه/اکسل
    - حفظ نشانه‌گذاری و خط‌تیره‌ها برای متن‌های ترکیبی

    مثال::
        >>> normalize_persian_text("كريم ياسر ۱۲۳\u200c")
        'کریم یاسر 123'
    """

    sanitized = sanitize_bidi(text)
    if not sanitized:
        return ""

    sanitized = unicodedata.normalize("NFKC", sanitized)
    sanitized = _remove_combining(sanitized)
    sanitized = _apply_arabic_to_persian_maps(sanitized)
    sanitized = _apply_allah_special(sanitized)
    sanitized = _translate_digits_and_symbols_for_text(sanitized)
    sanitized = sanitized.replace(_ZWNJ, " ")
    sanitized = _RE_WHITESPACE.sub(" ", sanitized)
    return sanitized.strip()


def normalize_persian_label(text: Any) -> str:
    """نرمال‌سازی برچسب برای کلیدهای join و مقایسهٔ پایدار.

    مثال::
        >>> normalize_persian_label("  مدرسه‌ي نمونه")
        'مدرسه ی نمونه'
    """

    normalized = normalize_persian_text(text)
    return normalized


def normalize_ascii_digits(text: Any) -> str:
    """تبدیل ارقام فارسی/عربی به لاتین با حذف نویز جهت پردازش عددی.

    مثال::
        >>> normalize_ascii_digits("۱۲۳۴۵۶۷۸۹")
        '123456789'
    """

    cleaned = sanitize_bidi(text)
    translated = cleaned.translate(_PERSIAN_DIGITS)
    translated = _RE_WHITESPACE.sub(" ", translated)
    return translated.strip()


__all__ = [
    "normalize_fa",
    "normalize_header",
    "to_numlike_str",
    "ensure_list",
    "parse_int_safe",
    "resolve_group_code",
    "safe_int_value",
    "sanitize_bidi",
    "fa_digitize",
    "safe_truncate",
    "strip_school_code_separators",
    "extract_ascii_digits",
    "normalize_persian_text",
    "normalize_persian_label",
    "normalize_ascii_digits",
]
