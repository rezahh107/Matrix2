"""ابزار استانداردسازی ستون‌ها و اعمال اجباری سیاست ستون‌ها."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Collection, Dict, Iterable, List, Literal, Mapping, Sequence

import pandas as pd

from app.core.policy_loader import get_policy
from .normalization import normalize_fa, strip_school_code_separators, to_numlike_str

__all__ = [
    "CANON_EN_TO_FA",
    "CANON_FA_TO_EN",
    "CANON",
    "resolve_aliases",
    "ensure_required_columns",
    "coerce_semantics",
    "canonicalize_headers",
    "ensure_series",
    "collect_aliases_for",
    "accepted_synonyms",
    "sanitize_digits",
    "to_int64",
    "normalize_bool_like",
    "enforce_join_key_types",
    "enrich_school_columns_en",
]

Source = Literal["report", "inspactor", "school", "matrix"]
HeaderMode = Literal["fa", "en", "fa_en"]


def ensure_series(values: pd.Series | pd.DataFrame) -> pd.Series:
    """تضمین می‌کند ورودی ستونی به‌صورت Series یک‌بعدی بازگردد.

    این تابع برای سناریوهایی مفید است که بر اثر ستون‌های تکراری یا خروجی‌های
    چندستونه، انتخاب یک ستون با نام مشخص `DataFrame` بازمی‌گرداند. در این حالت
    تنها نخستین ستون حفظ می‌شود تا توابعی نظیر :func:`pandas.to_numeric`
    ورودی سازگار دریافت کنند.

    مثال::

        >>> import pandas as pd
        >>> df = pd.DataFrame([[1, 2]], columns=["x", "x"])
        >>> ensure_series(df["x"]).tolist()
        [1]

    Args:
        values: Series یا DataFrame انتخاب‌شده از روی ستون.

    Returns:
        pd.Series: ستون یک‌بعدی هم‌تراز با index ورودی.
    """

    if isinstance(values, pd.DataFrame):
        if values.shape[1] == 0:
            return pd.Series([pd.NA] * len(values), index=values.index, dtype="object")
        return values.iloc[:, 0]
    return values


# ---------------------------------------------------------------------------
# Canonical headers (EN ↔ FA)
# ---------------------------------------------------------------------------
CANON_EN_TO_FA: Mapping[str, str] = {
    "group_code": "کدرشته",
    "exam_group": "گروه آزمایشی",
    "gender": "جنسیت",
    "graduation_status": "دانش آموز فارغ",
    "national_id": "کد ملی",
    "student_national_code": "کدملی دانش‌آموز",
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
    "mentor_alias_code": "کد جایگزین پشتیبان",
    "mentor_id": "کد کارمندی پشتیبان",
    "mentor_name": "نام پشتیبان",
    "manager_name": "نام مدیر",
}
CANON = CANON_EN_TO_FA
CANON_FA_TO_EN: Mapping[str, str] = {
    normalize_fa(value): key for key, value in CANON_EN_TO_FA.items()
}


# ---------------------------------------------------------------------------
# Base alias maps (Policy-first; extensible via policy.column_aliases)
# ---------------------------------------------------------------------------
_INSPACTOR_ALIASES: Mapping[str, str] = {
    "کد رشته": "کدرشته",
    "کد گروه آزمایشی": "کدرشته",
    "کدپستی": "کدپستی",
    "کد پستی": "کدپستی",
    "mentor": "نام پشتیبان",
    "mentor name": "نام پشتیبان",
    "mentor_name": "نام پشتیبان",
    "کد کارمندی پشتیبان": "کد کارمندی پشتیبان",
    "mentor_id": "کد کارمندی پشتیبان",
    "پشتیبان": "نام پشتیبان",
    "نام و نام خانوادگی پشتیبان": "نام پشتیبان",
    "مدیر": "نام مدیر",
    "manager": "نام مدیر",
    "manager name": "نام مدیر",
    "manager_name": "نام مدیر",
    "نام و نام خانوادگی مدیر": "نام مدیر",
    "نام مدرسه 1": "نام مدرسه 1",
    "نام مدرسه 2": "نام مدرسه 2",
    "نام مدرسه 3": "نام مدرسه 3",
    "نام مدرسه 4": "نام مدرسه 4",
    "کد مدرسه 1": "کد مدرسه 1",
    "کد مدرسه 2": "کد مدرسه 2",
    "کد مدرسه 3": "کد مدرسه 3",
    "کد مدرسه 4": "کد مدرسه 4",
    "کد کامل مدرسه": "کد کامل مدرسه",
    "تعداد مدارس تحت پوشش": "تعداد مدارس تحت پوشش",
    "تعداد داوطلبان تحت پوشش": "تعداد داوطلبان تحت پوشش",
    "تعداد تحت پوشش خاص": "تعداد تحت پوشش خاص",
}

ALIASES_DEFAULT: Mapping[Source, Mapping[str, str]] = {
    "report": {
        "وضعیت تحصیلی": "دانش آموز فارغ",
        "وضیعت تحصیلی": "دانش آموز فارغ",
        "وضعیت‌تحصیلی": "دانش آموز فارغ",
        "مرکز ثبت نام": "مرکز گلستان صدرا",
        "مرکز ثبت‌نام": "مرکز گلستان صدرا",
        "کد رشته": "کدرشته",
        "گروه آزمایشی نهایی": "کدرشته",
        "گروه آزمایشی": "گروه آزمایشی",
        "group_code": "کدرشته",
        "major_code": "کدرشته",
        "مدرسه نهایی": "کد مدرسه",
        "مدرسه‌ نهایی": "کد مدرسه",
        "مدرسه نهايی": "کد مدرسه",
        "school final": "کد مدرسه",
        "school_code": "کد مدرسه",
    },
    "inspactor": _INSPACTOR_ALIASES,
    "school": {
        "کد مدرسه": "کد مدرسه",
        "کد‌مدرسه": "کد مدرسه",
        "school_code": "کد مدرسه",
        "مدرسه نهایی": "کد مدرسه",
        "مدرسه‌ نهایی": "کد مدرسه",
        "مدرسه نهايی": "کد مدرسه",
        "school final": "کد مدرسه",
        "school id": "کد مدرسه",
        "نام مدرسه": "نام مدرسه",
        "نام‌مدرسه": "نام مدرسه",
        "school_name": "نام مدرسه",
    },
    "matrix": _INSPACTOR_ALIASES,
}

_STRING_COLUMNS: Mapping[str, Sequence[str]] = {
    "common": ("alias", "mentor_id", "postal_code"),
}

_INT_COLUMNS_BASE: Sequence[str] = (
    "group_code",
    "school_code",
    "school_code_1",
    "school_code_2",
    "school_code_3",
    "school_code_4",
)

_INT_COLUMNS_INSPACTOR: Sequence[str] = _INT_COLUMNS_BASE + (
    "graduation_status",
    "center",
    "finance",
    "schools_covered_count",
    "covered_students_count",
    "capacity_special",
    "capacity_current",
)

_INT_COLUMNS_BY_SOURCE: Mapping[Source, Sequence[str]] = {
    "report": _INT_COLUMNS_BASE + ("graduation_status", "center", "finance"),
    "inspactor": _INT_COLUMNS_INSPACTOR,
    "matrix": _INT_COLUMNS_INSPACTOR,
    "school": _INT_COLUMNS_BASE,
}


_BIDI_PATTERN = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]")
_DIGIT_TRANSLATION = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
_TRUTHY_TOKENS = {
    "1",
    "true",
    "yes",
    "y",
    "t",
    "مدرسه‌ای",
    "مدرسه اي",
    "مدرسه",
    "school",
    "school-based",
}
_FALSY_TOKENS = {"0", "false", "no", "n", "f", "عادی", "normal"}
_GENDER_TOKEN_MAP = {
    "پسر": 1,
    "مرد": 1,
    "male": 1,
    "boy": 1,
    "آقا": 1,
    "دختر": 0,
    "زن": 0,
    "female": 0,
    "girl": 0,
    "خانم": 0,
}


def sanitize_digits(series: pd.Series | None) -> pd.Series:
    """پاک‌سازی ایمن ارقام فارسی/عربی و حذف کنترل‌های جهت متن.

    مثال::

        >>> import pandas as pd
        >>> sanitize_digits(pd.Series(["\u200f۶۶۳ ", "٠١"])).tolist()
        ['663', '01']
    """

    if series is None:
        return pd.Series(dtype="string")
    text = series.astype("string")
    text = text.fillna("")
    text = text.str.replace(_BIDI_PATTERN, "", regex=True)
    text = text.str.translate(_DIGIT_TRANSLATION)
    text = text.str.replace(" ", "", regex=False)
    text = text.str.strip()
    return text.astype("string")


def to_int64(series: pd.Series | None) -> pd.Series:
    """تبدیل ستون به نوع عددی «Int64» پس از پاک‌سازی ارقام.

    مقدار خالی یا نامعتبر → «<NA>».
    """

    if series is None:
        return pd.Series(dtype="Int64")
    if pd.api.types.is_bool_dtype(series):
        return series.astype("Int64")
    sanitized = sanitize_digits(series)
    numeric = pd.to_numeric(sanitized.replace("", pd.NA), errors="coerce")
    return numeric.astype("Int64")


def normalize_bool_like(series: pd.Series | None) -> pd.Series:
    """نرمال‌سازی مقادیر متنی/عددی به ۰ و ۱ (Int64).

    ورودی‌های نامعتبر به ۰ نگاشت می‌شوند تا رفتار قطعی حفظ گردد.
    """

    if series is None:
        return pd.Series(dtype="Int64")
    text = sanitize_digits(series).str.lower()
    result = pd.Series(0, index=text.index, dtype="Int64")
    mask_truthy = text.isin(_TRUTHY_TOKENS)
    mask_falsy = text.isin(_FALSY_TOKENS)
    numeric = pd.to_numeric(text.replace("", pd.NA), errors="coerce")
    result = result.astype("Int64")
    result.loc[mask_truthy] = 1
    result.loc[mask_falsy] = 0
    if numeric.notna().any():
        result.loc[numeric == 1] = 1
        result.loc[numeric == 0] = 0
    return result.fillna(0).astype("Int64")


def enforce_join_key_types(df: pd.DataFrame, join_keys: Sequence[str]) -> pd.DataFrame:
    """تضمین تبدیل ستون‌های کلید join به نوع «Int64».

    هرگونه تغییر در سیاست join keys باید از طریق Policy منتقل شود و این تابع
    با دریافت همان فهرست، نسخهٔ کپی‌شده از DataFrame را بازگردانی می‌کند که در آن
    مقادیر ستون‌های مذکور به‌صورت قطعی به `Int64` تبدیل شده‌اند. در صورت فقدان
    هر ستون، تغییری اعمال نمی‌شود تا سناریوهای خطای کنترل‌شده باقی بمانند.
    """

    if not join_keys:
        return df

    result = df.copy()
    gender_normalized = normalize_fa(CANON_EN_TO_FA.get("gender", "جنسیت"))
    for column in dict.fromkeys(join_keys):
        if column not in result.columns:
            continue
        series = result[column]
        normalized_name = normalize_fa(column)
        if normalized_name == gender_normalized:
            series = _replace_gender_tokens(series)
        if pd.api.types.is_bool_dtype(series):
            result[column] = series.astype("Int64")
            continue
        if series.dtype == "object":
            series = series.replace({True: 1, False: 0})
        result[column] = to_int64(series)
    return result


def _replace_gender_tokens(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip().str.lower()
    mapped = text.map(_GENDER_TOKEN_MAP)
    if mapped.notna().any():
        return series.mask(mapped.notna(), mapped)
    return series


def enrich_school_columns_en(
    df: pd.DataFrame, *, empty_as_zero: bool = False
) -> pd.DataFrame:
    """تولید ستون‌های خام و نرمال مدرسه روی DataFrame انگلیسی.

    ستون‌های اضافه‌شده:
        - ``school_code_raw``: مقدار متنی اولیه (trim شده)
        - ``school_code`` و ``school_code_norm``: مقدار Int64 پس از پاک‌سازی ارقام
        - ``school_status_resolved``: نتیجهٔ نهایی تشخیص مدرسه (Int64: ۰ یا ۱)

    Args:
        df: دیتافریم منبع با نام ستون‌های انگلیسی یا فارسی.
        empty_as_zero: در صورت True، مقادیر نامعتبر/خالی به ۰ نگاشت می‌شوند تا با
            سیاست «school_code_empty_as_zero» هم‌سو باشد.
    """

    result = df.copy()
    index = result.index

    raw_column = result.get("school_code_raw")
    if isinstance(raw_column, pd.DataFrame):
        raw_column = raw_column.iloc[:, 0]
    school_code_column = result.get("school_code")
    if isinstance(school_code_column, pd.DataFrame):
        school_code_column = school_code_column.iloc[:, 0]

    if raw_column is not None:
        raw = raw_column.astype("string").str.strip()
    elif school_code_column is not None:
        raw = school_code_column.astype("string").str.strip()
    else:
        raw = pd.Series(pd.NA, dtype="string", index=index)
    raw = raw.map(lambda value: strip_school_code_separators(value) if isinstance(value, str) else value)
    # حذف نسخه‌های قدیمی برای جلوگیری از ستون‌های تکراری
    result = result.drop(columns=[col for col in result.columns if col == "school_code_raw"], errors="ignore")
    result = result.drop(columns=[col for col in result.columns if col == "school_code"], errors="ignore")

    result["school_code_raw"] = raw
    normalized = to_int64(raw)
    if empty_as_zero:
        normalized = normalized.fillna(0)
    normalized = normalized.astype("Int64")
    result["school_code_norm"] = normalized
    result["school_code"] = normalized

    flag_series: pd.Series | None = None
    for candidate in ("school_flag", "is_school"):
        if candidate in result.columns:
            flag_series = normalize_bool_like(result[candidate])
            break
    if flag_series is None:
        flag_series = pd.Series(0, dtype="Int64", index=index)

    status = (normalized.fillna(0) > 0) | (flag_series.fillna(0) == 1)
    result["school_status_resolved"] = status.astype("Int64")
    return result


@dataclass(frozen=True)
class _AliasBundle:
    """نگه‌دارندهٔ نگاشت نرمال‌شده به کلید کاننیکال و فهرست سینونیم‌ها."""

    normalized_map: Mapping[str, str]
    report_map: Mapping[str, List[str]]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_header(value: object) -> str:
    text = normalize_fa(value)
    text = text.replace("_", " ")
    text = " ".join(text.split())
    if not text:
        raw = str(value).strip().lower().replace("_", " ")
        text = " ".join(raw.split())
    return text.lower()


def _split_bilingual_header(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    text = str(value)
    if "|" not in text:
        return ()
    parts = [part.strip() for part in text.split("|")]
    return tuple(part for part in parts if part)


def _normalized_header_tokens(value: object) -> tuple[str, ...]:
    base = _normalize_header(value)
    tokens: list[str] = []
    if base:
        tokens.append(base)
    bilingual = _split_bilingual_header(value)
    if bilingual:
        padded = " | ".join(bilingual)
        padded_normalized = _normalize_header(padded)
        if padded_normalized and padded_normalized not in tokens:
            tokens.append(padded_normalized)
    for part in bilingual:
        normalized = _normalize_header(part)
        if normalized and normalized not in tokens:
            tokens.append(normalized)
    return tuple(tokens)


def _policy_aliases(source: Source) -> Mapping[str, str]:
    policy = get_policy()
    return policy.column_aliases.get(source, {})


def _build_alias_bundle(source: Source) -> _AliasBundle:
    base = dict(ALIASES_DEFAULT.get(source, {}))
    base.update(_policy_aliases(source))

    normalized_map: Dict[str, str] = {}
    report_map: Dict[str, List[str]] = {
        CANON_EN_TO_FA[key]: [] for key in CANON_EN_TO_FA
    }

    for alias, target in base.items():
        normalized = _normalize_header(alias)
        target_key = CANON_FA_TO_EN.get(_normalize_header(target))
        if not normalized or not target_key:
            continue
        normalized_map[normalized] = target_key
        report_map.setdefault(CANON_EN_TO_FA[target_key], []).append(alias)

    for en_key, fa_value in CANON_EN_TO_FA.items():
        normalized_map.setdefault(_normalize_header(en_key), en_key)
        normalized_map.setdefault(_normalize_header(fa_value), en_key)
        report_map.setdefault(fa_value, []).append(fa_value)

    for en_key, fa_value in CANON_EN_TO_FA.items():
        bilingual = f"{fa_value} | {en_key}"
        normalized_bilingual = _normalize_header(bilingual)
        if normalized_bilingual not in normalized_map:
            normalized_map[normalized_bilingual] = en_key
        report_map.setdefault(fa_value, [])
        if bilingual not in report_map[fa_value]:
            report_map[fa_value].append(bilingual)

    return _AliasBundle(normalized_map=normalized_map, report_map=report_map)


def collect_aliases_for(source: Source) -> Mapping[str, str]:
    """برگرداندن نگاشت alias→نام کاننیکال (فارسی) برای پیام‌های خطا."""

    bundle = _build_alias_bundle(source)
    aliases: Dict[str, str] = {}
    for normalized, en_key in bundle.normalized_map.items():
        fa_name = CANON_EN_TO_FA.get(en_key, en_key)
        aliases[normalized] = fa_name
    return aliases


def accepted_synonyms(source: Source, canonical_fa: str) -> Sequence[str]:
    """فهرست سینونیم‌های قابل قبول برای ستونی مشخص."""

    bundle = _build_alias_bundle(source)
    normalized_key = normalize_fa(canonical_fa)
    target_en = CANON_FA_TO_EN.get(normalized_key)
    if target_en is None:
        return ()

    canonical_name = CANON_EN_TO_FA[target_en]
    synonyms = list(bundle.report_map.get(canonical_name, []))
    synonyms.append(canonical_name)

    # نسخه‌های انگلیسی (با و بدون زیرخط) برای پیام خطا
    english_name = target_en
    synonyms.append(english_name)
    english_spaced = english_name.replace("_", " ")
    if english_spaced != english_name:
        synonyms.append(english_spaced)

    # نسخهٔ حاوی زیرخط برای معادل فارسی (مانند «کد_مدرسه»)
    persian_underscored = canonical_name.replace(" ", "_")
    if persian_underscored != canonical_name:
        synonyms.append(persian_underscored)

    return tuple(dict.fromkeys(filter(None, synonyms)))


def _match_column(df: pd.DataFrame, *, canonical_en: str, bundle: _AliasBundle) -> str | None:
    canonical_fa = CANON_EN_TO_FA.get(canonical_en, canonical_en)
    if canonical_fa in df.columns:
        return canonical_fa
    normalized_targets = {
        key for key, target in bundle.normalized_map.items() if target == canonical_en
    }
    for column in df.columns:
        if _normalize_header(column) in normalized_targets:
            return str(column)
    return None


def _identifier_to_string(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    if not text:
        return pd.NA
    num_candidate = to_numlike_str(text)
    if num_candidate:
        cleaned = text.replace(" ", "")
        if cleaned.endswith(".0") and num_candidate.isdigit():
            return num_candidate
        cleaned = cleaned.lstrip("+-")
        cleaned = cleaned.replace(".", "", 1)
        if cleaned.isdigit() and num_candidate:
            return num_candidate
    return text


def _clean_numeric(value: object) -> str:
    if pd.isna(value):
        return ""
    text = strip_school_code_separators(normalize_fa(value))
    candidate = to_numlike_str(text)
    return candidate if candidate is not None else ""


def _coerce_numeric_column(df: pd.DataFrame, canonical_en: str, bundle: _AliasBundle) -> None:
    column = _match_column(df, canonical_en=canonical_en, bundle=bundle)
    if column is None:
        return
    cleaned = df[column].map(_clean_numeric)
    numeric = pd.to_numeric(cleaned.replace("", pd.NA), errors="coerce")
    if numeric.isna().any():
        df[column] = numeric.astype("Int64")
    else:
        df[column] = numeric.astype("int64")


def _coerce_string_column(df: pd.DataFrame, canonical_en: str, bundle: _AliasBundle) -> None:
    column = _match_column(df, canonical_en=canonical_en, bundle=bundle)
    if column is None:
        return
    coerced = df[column].map(_identifier_to_string)
    df[column] = coerced.astype("string")


def _ensure_column(df: pd.DataFrame, canonical_en: str, bundle: _AliasBundle) -> str | None:
    column = _match_column(df, canonical_en=canonical_en, bundle=bundle)
    if column is None:
        return None
    canonical_fa = CANON_EN_TO_FA.get(canonical_en, canonical_en)
    if column != canonical_fa:
        df[canonical_fa] = df[column]
        column = canonical_fa
    return column


def _status_to_int(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int,)):
        return int(value)
    text = normalize_fa(value)
    if not text:
        return pd.NA
    num_candidate = to_numlike_str(text)
    if num_candidate and num_candidate.isdigit():
        return int(num_candidate)
    return 1 if "فارغ" in text else 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_aliases(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    """هم‌نام‌سازی ستون‌های ورودی بر اساس سیاست و سینونیم‌ها."""

    bundle = _build_alias_bundle(source)
    rename_map: Dict[str, str] = {}
    seen: set[str] = set()
    for column in df.columns:
        canonical_en: str | None = None
        for normalized in _normalized_header_tokens(column):
            canonical_en = bundle.normalized_map.get(normalized)
            if canonical_en is not None:
                break
        if canonical_en is None:
            continue
        canonical_fa = CANON_EN_TO_FA.get(canonical_en, canonical_en)
        if canonical_fa in seen:
            continue
        seen.add(canonical_fa)
        if str(column) != canonical_fa:
            rename_map[str(column)] = canonical_fa
    result = df.rename(columns=rename_map, errors="ignore")
    result = result.loc[:, ~pd.Index(result.columns).duplicated(keep="first")]
    return result


def ensure_required_columns(
    df: pd.DataFrame,
    required: Collection[str],
    source: Source,
) -> pd.DataFrame:
    """تضمین حضور ستون‌های حیاتی با استفاده از سینونیم‌ها.

    مثال ساده::

        >>> import pandas as pd
        >>> df = pd.DataFrame({"school_code": [101]})
        >>> ensured = ensure_required_columns(
        ...     df,
        ...     {CANON_EN_TO_FA["school_code"]},
        ...     "inspactor",
        ... )
        >>> list(ensured.columns)
        ['کد مدرسه']

    Args:
        df: دیتافریم ورودی که باید ستون‌های ضروری را داشته باشد.
        required: فهرست ستون‌های فارسی که Policy اجبار کرده است.
        source: نوع ورودی (report/inspactor/school) برای اعمال نگاشت صحیح.

    Returns:
        دیتافریم با نام‌گذاری استاندارد در صورت امکان. در صورت نقص ستون‌ها، خطا می‌دهد.
    """

    if not required:
        return df

    existing = set(map(str, df.columns))
    missing = [column for column in required if column not in existing]
    if not missing:
        return df

    resolved = resolve_aliases(df, source)
    existing = set(map(str, resolved.columns))
    missing = [column for column in required if column not in existing]
    if not missing:
        return resolved

    accepted: Dict[str, Sequence[str]] = {}
    for column in missing:
        synonyms = list(accepted_synonyms(source, column))
        if column not in synonyms:
            synonyms.insert(0, column)
        accepted[column] = tuple(dict.fromkeys(synonyms))

    raise ValueError(f"Missing columns: {missing} — accepted synonyms: {accepted}")


def coerce_semantics(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    """اعمال قواعد معنایی (تایپ، مقادیر مشتق) پس از هم‌نام‌سازی."""

    bundle = _build_alias_bundle(source)
    result = df.copy()

    if source == "report":
        status_col = _ensure_column(result, "graduation_status", bundle)
        if status_col is not None:
            coerced = result[status_col].map(_status_to_int)
            result[status_col] = coerced.astype("Int64")
        center_col = _ensure_column(result, "center", bundle)
        if center_col is not None:
            cleaned = result[center_col].map(_clean_numeric)
            numeric = pd.to_numeric(cleaned.replace("", pd.NA), errors="coerce")
            result[center_col] = numeric.astype("Int64")
    else:
        _ensure_column(result, "center", bundle)
        _ensure_column(result, "graduation_status", bundle)

    for key in _STRING_COLUMNS["common"]:
        _coerce_string_column(result, key, bundle)

    for key in _INT_COLUMNS_BY_SOURCE[source]:
        _coerce_numeric_column(result, key, bundle)

    return result


def canonicalize_headers(df: pd.DataFrame, header_mode: HeaderMode) -> pd.DataFrame:
    """تبدیل نام ستون‌ها به فارسی، انگلیسی یا دوزبانه."""

    if header_mode not in {"fa", "en", "fa_en"}:
        raise ValueError(f"Unsupported header_mode '{header_mode}'")

    rename: Dict[str, str] = {}
    for column in df.columns:
        en_key: str | None = None
        for normalized in _normalized_header_tokens(column):
            en_key = CANON_FA_TO_EN.get(normalized)
            if en_key is not None:
                break
            for candidate_en in CANON_EN_TO_FA:
                if normalized == _normalize_header(candidate_en):
                    en_key = candidate_en
                    break
            if en_key is not None:
                break
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


# ---------------------------------------------------------------------------
# Doctest-style examples (برای اسناد داخلی)
# ---------------------------------------------------------------------------

def _example_usage() -> None:  # pragma: no cover - documentation helper
    """نمونهٔ فشرده برای دفترچه توسعه‌دهندگان."""

    data = pd.DataFrame(
        {
            "وضعیت تحصیلی": ["فارغ التحصیل"],
            "مرکز ثبت نام": ["مرکز 12"],
            "کد رشته": ["4001"],
            "کد کارمندی پشتیبان": ["1205"],
        }
    )
    resolved = resolve_aliases(data, "report")
    coerced = coerce_semantics(resolved, "report")
    _ = canonicalize_headers(coerced, header_mode="fa_en")

