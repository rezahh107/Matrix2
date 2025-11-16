"""موتور شمارندهٔ دانش‌آموز بر اساس Policy و روسترها."""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import blake2b
from typing import Dict, Optional, Sequence

import pandas as pd

from app.core.common.columns import ensure_series
from app.core.common.ranking import natural_key
from app.core.policy_loader import GenderCodes, get_policy

__all__ = [
    "assert_unique_student_ids",
    "assign_counters",
    "build_registration_id",
    "detect_academic_year_from_counters",
    "find_duplicate_student_id_groups",
    "find_max_sequence_by_prefix",
    "gender_to_mid3",
    "infer_year_strict",
    "normalize_digits",
    "pick_counter_sheet_name",
    "stable_counter_hash",
    "strip_hidden_chars",
    "validate_counter",
    "year_to_yy",
]


_DIGIT_MAP = str.maketrans(
    "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
    "01234567890123456789",
)

_ZERO_WIDTH = {
    ord("\u200c"): None,
    ord("\u200d"): None,
    ord("\ufeff"): None,
}

_COUNTER_SHEET_CANDIDATES = [
    "شمارنده",
    "کد ثبت نام",
    "کد_ثبت_نام",
    "کدثبتنام",
    "counters",
    "counter",
    "studentreport",
]


def pick_counter_sheet_name(sheet_names: Sequence[str]) -> str | None:
    """انتخاب شیت مناسب برای شمارنده بر اساس نام‌های رایج."""

    if not sheet_names:
        return None

    normalized: Dict[str, str] = {
        str(name).strip().lower(): str(name) for name in sheet_names
    }
    for candidate in _COUNTER_SHEET_CANDIDATES:
        key = candidate.lower()
        if key in normalized:
            return normalized[key]

    for candidate in _COUNTER_SHEET_CANDIDATES:
        needle = candidate.lower()
        for original in sheet_names:
            label = str(original).strip().lower()
            if needle in label:
                return str(original)

    return str(sheet_names[0])


def _normalize_nat_id(value: object) -> str:
    """نرمال‌سازی کدملی به رشتهٔ ده‌رقمی بدون کاراکتر اضافی."""

    text = normalize_digits(("" if value is None else str(value)).strip())
    digits = re.sub(r"\D", "", text)
    return digits.zfill(10) if digits else ""


def find_duplicate_student_id_groups(series: pd.Series) -> dict[str, list[object]]:
    """گروه‌بندی ردیف‌های دارای student_id تکراری بر اساس مقدار."""

    if series.empty:
        return {}

    series_string = series.astype("string")
    mask = series_string.duplicated(keep=False)
    duplicates = series_string[mask & series_string.notna()]
    if duplicates.empty:
        return {}

    groups: dict[str, list[object]] = {}
    for index_label, value in duplicates.items():
        groups.setdefault(str(value), []).append(index_label)
    return groups


def assert_unique_student_ids(series: pd.Series) -> None:
    """اعتبارسنجی یکتایی شناسه‌ها و ارائهٔ نمونه در صورت تکرار."""

    groups = find_duplicate_student_id_groups(series)
    if not groups:
        return

    samples: list[str] = []
    for student_id, index_list in groups.items():
        for index_label in index_list:
            samples.append(f"index={index_label}, student_id={student_id}")
            if len(samples) >= 3:
                break
        if len(samples) >= 3:
            break
    sample_text = "; ".join(samples)
    raise ValueError(
        "student_id تکراری در خروجی شمارنده یافت شد؛ نمونه‌ها: " + sample_text
    )


def year_to_yy(academic_year: int) -> int:
    """محاسبهٔ بخش YY از سال تحصیلی.

    مثال::

        >>> year_to_yy(1404)
        54
    """

    yy = academic_year - 1350
    if not 0 <= yy <= 99:
        raise ValueError("سال تحصیلی خارج از بازهٔ معتبر است")
    return yy


@dataclass(frozen=True)
class GenderPolicy:
    """نگاشت جنسیت به کد شمارنده طبق policy."""

    male_value: int
    female_value: int
    male_mid3: str
    female_mid3: str


def _load_gender_policy() -> GenderPolicy:
    """استخراج تنظیمات جنسیت از policy و تبدیل به ساختار مصرفی."""

    policy = get_policy()
    gender_codes: GenderCodes = policy.gender_codes
    male_mid3 = str(gender_codes.male.counter_code).zfill(3)
    female_mid3 = str(gender_codes.female.counter_code).zfill(3)
    return GenderPolicy(
        male_value=int(gender_codes.male.value),
        female_value=int(gender_codes.female.value),
        male_mid3=male_mid3,
        female_mid3=female_mid3,
    )


def gender_to_mid3(gender_value: int, mapping: GenderPolicy) -> str:
    """برگرداندن کد MID3 متناسب با مقدار جنسیت.

    مثال::

        >>> mapping = GenderPolicy(1, 0, "357", "373")
        >>> gender_to_mid3(1, mapping)
        '357'
    """

    if gender_value == mapping.male_value:
        return mapping.male_mid3
    if gender_value == mapping.female_value:
        return mapping.female_mid3
    raise ValueError("مقدار جنسیت با policy هم‌خوان نیست")


def build_registration_id(yy: int, mid3: str, sequence: int) -> str:
    """ساخت شناسهٔ ۹رقمی دانش‌آموز.

    مثال::

        >>> build_registration_id(54, "357", 12)
        '543570012'
    """

    if not 0 <= yy <= 99:
        raise ValueError("YY باید بین 0 تا 99 باشد")
    if not re.fullmatch(r"\d{3}", str(mid3)):
        raise ValueError("MID3 نامعتبر است")
    if not 0 <= sequence <= 9999:
        raise ValueError("sequence خارج از بازهٔ 0..9999 است")
    return f"{yy:02d}{int(mid3):03d}{int(sequence):04d}"


def _extract_sequence(counter_value: object) -> Optional[int]:
    text = normalize_digits(("" if counter_value is None else str(counter_value)).strip())
    if re.fullmatch(r"\d{9}", text):
        return int(text[-4:])
    return None


def _pick_counter_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "student_id",
        "counter",
        "شمارنده",
        "کد ثبت نام",
        "کد_ثبت_نام",
        "کدثبتنام",
        "کدرهنگامثبتنام",
    ]
    lowered = {str(column).lower(): column for column in df.columns}
    for name in candidates:
        key = name.lower()
        if key in lowered:
            return lowered[key]
    for column in df.columns:
        label = str(column).lower()
        if "counter" in label or "ثبت" in label:
            return column
    return None


def _pick_nat_id_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["national_id", "کد ملی", "کدملی", "شماره ملی"]
    lowered = {str(column).lower(): column for column in df.columns}
    for name in candidates:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _normalized_counter_series(df: pd.DataFrame, column: str) -> pd.Series:
    """تهیهٔ سری شمارنده با strip و تبدیل ارقام به لاتین.

    مثال::

        >>> frame = pd.DataFrame({"counter": [" ۱۲۳۴۵۶۷۸۹ "]})
        >>> _normalized_counter_series(frame, "counter").iloc[0]
        '123456789'
    """

    return (
        ensure_series(df[column])
        .astype("string")
        .fillna("")
        .str.strip()
        .map(normalize_digits)
    )


def detect_academic_year_from_counters(
    current_roster_df: pd.DataFrame | None,
) -> Optional[int]:
    """تشخیص سال تحصیلی از روی شناسه‌های ۹رقمی موجود در روستر."""

    if current_roster_df is None or current_roster_df.empty:
        return None

    column = _pick_counter_column(current_roster_df)
    if column is None:
        return None

    series = _normalized_counter_series(current_roster_df, column)
    for value in series:
        if value and re.fullmatch(r"\d{9}", value):
            return int(value[:2]) + 1350
    return None


def infer_year_strict(current_roster_df: pd.DataFrame | None) -> Optional[int]:
    """استخراج سال تحصیلی در صورت یکتایی پیشوندهای YY."""

    if current_roster_df is None or current_roster_df.empty:
        return None

    column = _pick_counter_column(current_roster_df)
    if column is None:
        return None

    series = _normalized_counter_series(current_roster_df, column)

    prefixes = {
        value[:2]
        for value in series
        if value and re.fullmatch(r"\d{9}", value)
    }

    if len(prefixes) != 1:
        return None

    prefix = prefixes.pop()
    return int(prefix) + 1350


def find_max_sequence_by_prefix(current_roster_df: pd.DataFrame | None, prefix: str) -> int:
    """جستجوی بزرگ‌ترین sequence برای پیشوند مشخص.

    مثال::

        >>> df = pd.DataFrame({"student_id": ["543570009", "543730015"]})
        >>> find_max_sequence_by_prefix(df, "54357")
        9
    """

    if current_roster_df is None or current_roster_df.empty:
        return 0
    column = _pick_counter_column(current_roster_df)
    if column is None:
        return 0
    series = _normalized_counter_series(current_roster_df, column)
    mask = series.str.startswith(prefix, na=False)
    sequences = [seq for seq in series[mask].map(_extract_sequence) if seq is not None]
    return max(sequences) if sequences else 0


def _prior_map(
    prior_roster_df: pd.DataFrame | None,
) -> tuple[Dict[str, str], Dict[str, list[str]]]:
    """ساخت نگاشت روستر سال قبل با حذف برخورد شناسه‌های تکراری."""

    if prior_roster_df is None or prior_roster_df.empty:
        return {}, {}
    nat_col = _pick_nat_id_column(prior_roster_df)
    counter_col = _pick_counter_column(prior_roster_df)
    if nat_col is None or counter_col is None:
        return {}, {}
    mapping: Dict[str, str] = {}
    conflicts: Dict[str, list[str]] = {}
    counter_to_owner: Dict[str, str] = {}
    for nat, counter_value in zip(prior_roster_df[nat_col], prior_roster_df[counter_col]):
        normalized = _normalize_nat_id(nat)
        if not normalized:
            continue
        raw_counter = ("" if counter_value is None else str(counter_value)).strip()
        try:
            counter_clean = validate_counter(raw_counter)
        except ValueError:
            continue
        owner = counter_to_owner.get(counter_clean)
        if owner is None:
            counter_to_owner[counter_clean] = normalized
            mapping[normalized] = counter_clean
            continue
        if owner == normalized:
            mapping[normalized] = counter_clean
            continue
        conflict_peers = conflicts.setdefault(counter_clean, [owner])
        if normalized not in conflict_peers:
            conflict_peers.append(normalized)
    return mapping, conflicts


def assign_counters(
    students_df: pd.DataFrame,
    *,
    prior_roster_df: pd.DataFrame | None,
    current_roster_df: pd.DataFrame | None,
    academic_year: int,
) -> pd.Series:
    """تخصیص شمارندهٔ ۹رقمی به دانش‌آموزان بر اساس policy.

    - اگر دانش‌آموز در روستر سال قبل باشد، همان شمارنده برگردانده می‌شود.
    - در غیر این صورت، شمارندهٔ جدید بر اساس سال و جنسیت ساخته می‌شود.

    مثال::

        >>> students = pd.DataFrame({"national_id": ["1"], "gender": [1]})
        >>> assign_counters(students, prior_roster_df=None, current_roster_df=None, academic_year=1404)
        0    543570001
        Name: student_id, dtype: string
    """

    if students_df is None or students_df.empty:
        return pd.Series([], dtype="string", name="student_id")

    policy = _load_gender_policy()
    yy = year_to_yy(int(academic_year))
    yy_prefix = f"{yy:02d}"

    if "national_id" not in students_df.columns or "gender" not in students_df.columns:
        raise ValueError("students_df باید ستون‌های national_id و gender داشته باشد")

    nat_values = ensure_series(students_df["national_id"])
    gender_values = ensure_series(students_df["gender"])

    work = pd.DataFrame(
        {
            "national_id": nat_values.astype("string"),
            "gender": gender_values,
        },
        index=students_df.index,
    )
    work["__nat__"] = work["national_id"].map(_normalize_nat_id)
    work["__gender__"] = pd.to_numeric(work["gender"], errors="coerce")

    if work["__nat__"].eq("").any():
        raise ValueError("کد ملی نامعتبر در students_df وجود دارد")
    if work["__gender__"].isna().any():
        raise ValueError("مقدار gender نامعتبر است")

    order = sorted(
        range(len(work)), key=lambda idx: (natural_key(work.iloc[idx]["__nat__"]), idx)
    )
    result = pd.Series(index=students_df.index, dtype="string", name="student_id")

    prior_mapping, prior_conflicts = _prior_map(prior_roster_df)
    male_max = find_max_sequence_by_prefix(
        current_roster_df, yy_prefix + policy.male_mid3
    )
    female_max = find_max_sequence_by_prefix(
        current_roster_df, yy_prefix + policy.female_mid3
    )
    next_male = male_max + 1
    next_female = female_max + 1

    assigned_mapping: Dict[str, str] = {}
    reused_count = 0
    new_male_count = 0
    new_female_count = 0

    def _assign(index_label: object, counter_value: object) -> None:
        normalized_counter = validate_counter(counter_value)
        result.at[index_label] = normalized_counter

    for position in order:
        index_label = students_df.index[position]
        normalized_nat = work.iloc[position]["__nat__"]
        gender_value = int(work.iloc[position]["__gender__"])

        if normalized_nat in assigned_mapping:
            _assign(index_label, assigned_mapping[normalized_nat])
            reused_count += 1
            continue

        if normalized_nat in prior_mapping:
            counter_value = prior_mapping[normalized_nat]
            _assign(index_label, counter_value)
            assigned_mapping[normalized_nat] = validate_counter(counter_value)
            reused_count += 1
            continue

        mid3 = gender_to_mid3(gender_value, policy)
        if gender_value == policy.male_value:
            sequence = next_male
            if sequence > 9999:
                raise ValueError("sequence پسران از 9999 عبور کرده است")
            next_male += 1
            new_male_count += 1
        elif gender_value == policy.female_value:
            sequence = next_female
            if sequence > 9999:
                raise ValueError("sequence دختران از 9999 عبور کرده است")
            next_female += 1
            new_female_count += 1
        else:  # pragma: no cover - حالت غیرمنتظره پس از اعتبارسنجی
            raise ValueError("جنسیت پشتیبانی‌نشده")

        counter_value = build_registration_id(yy, mid3, sequence)
        _assign(index_label, counter_value)
        assigned_mapping[normalized_nat] = validate_counter(counter_value)

    conflict_count = sum(max(len(peers) - 1, 0) for peers in prior_conflicts.values())
    conflict_samples = sorted(prior_conflicts.keys())[:3]

    result.attrs["counter_summary"] = {
        "reused_count": reused_count,
        "new_male_count": new_male_count,
        "new_female_count": new_female_count,
        "next_male_start": next_male,
        "next_female_start": next_female,
        "prior_conflict_counter_count": conflict_count,
        "prior_conflict_counter_samples": conflict_samples,
    }

    return result
def strip_hidden_chars(value: str) -> str:
    """حذف کاراکترهای صفرعرض و BOM از متن ورودی."""

    return value.translate(_ZERO_WIDTH)


def normalize_digits(value: str) -> str:
    """تبدیل تمامی ارقام فارسی/عربی به لاتین."""

    return value.translate(_DIGIT_MAP)


def validate_counter(value: object) -> str:
    """اعتبارسنجی شمارندهٔ ۹رقمی بر اساس SSoT."""

    text = strip_hidden_chars(normalize_digits(str(value or "").strip()))
    text = re.sub(r"\s+", "", text)
    if not re.fullmatch(r"\d{9}", text):
        raise ValueError("فرمت شمارنده معتبر نیست.")
    return text


def stable_counter_hash(counter_value: object) -> int:
    """هش پایدار ۶۴بیتی بر اساس شمارندهٔ معتبر."""

    normalized = validate_counter(counter_value)
    digest = blake2b(normalized.encode("utf-8"), digest_size=8)
    return int.from_bytes(digest.digest(), "big")
