# -*- coding: utf-8 -*-
"""
Domain models and logic for Eligibility Matrix → Allocation system.
Python 3.10+, stdlib only, no I/O, no side-effects on import.
Deterministic and fail-safe, adhering to Policy v1.0.3.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Mapping, Literal, TypedDict, final, TypeGuard

from .errors import DataMissingError, InvalidCenterMappingError, InvalidGenderValueError
from .normalization import normalize_fa, to_numlike_str

# ---------------------------------------------------------------------------
# Type Aliases
# ---------------------------------------------------------------------------
# StudentRow: TypeAlias = Mapping[str, Any]  # Not using TypeAlias for stdlib compatibility
StudentRow = Mapping[str, Any]
JoinKeyDict = dict[str, int]
MentorDict = dict[str, Any]



# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

@final
class MentorType(Enum):
    """Type of mentor based on postal code and school assignment."""
    NORMAL = "normal"
    SCHOOL = "school"
    DUAL = "dual"


@final
class Status(IntEnum):
    """Student graduation status."""
    STUDENT = 1
    GRADUATE = 0


@final
class Gender(IntEnum):
    """Gender codes."""
    MALE = 1
    FEMALE = 2


@final
class FinanceCode(IntEnum):
    """Valid finance codes."""
    NORMAL = 0
    FOUNDATION = 1
    HEKMAT = 3


# ---------------------------------------------------------------------------
# Column name constants
# ---------------------------------------------------------------------------

COL_GROUP = "کدرشته"
COL_GENDER = "جنسیت"
COL_STATUS = "دانش آموز فارغ"
COL_CENTER = "مرکز گلستان صدرا"
COL_FINANCE = "مالی حکمت بنیاد"
COL_SCHOOL = "کد مدرسه"
COL_SCHOOL_NAME = "نام مدرسه"
COL_SCHOOL_CODE_1 = "کد مدرسه 1"
COL_SCHOOL_CODE_2 = "کد مدرسه 2"
COL_SCHOOL_CODE_3 = "کد مدرسه 3"
COL_SCHOOL_CODE_4 = "کد مدرسه 4"
COL_SCHOOL_NAME_1 = "نام مدرسه 1"
COL_SCHOOL_NAME_2 = "نام مدرسه 2"
COL_SCHOOL_NAME_3 = "نام مدرسه 3"
COL_SCHOOL_NAME_4 = "نام مدرسه 4"
COL_FULL_SCHOOL_CODE = "کد کامل مدرسه"
COL_EDU_CODE = "کد آموزش و پرورش"
COL_ALIAS = "جایگزین"
COL_MENTOR = "پشتیبان"
COL_MANAGER = "مدیر"
COL_MENTOR_ID = "کد کارمندی پشتیبان"
COL_MENTOR_ROWID = "ردیف پشتیبان"
# New column for output schema
COL_MENTOR_TYPE = "عادی مدرسه"


# ---------------------------------------------------------------------------
# Internal helpers (fail-safe)
# ---------------------------------------------------------------------------

# Status normalization constants
_STATUS_GRADUATE_EN = frozenset({"0", "graduate", "grad"})
_STATUS_STUDENT_EN = frozenset({"1", "student", "pupil"})
_STATUS_GRADUATE_FA = frozenset({"فارغ", "فارغ التحصیل"})
_STATUS_STUDENT_FA = frozenset({"دانش آموز", "دانشجو", "دانش اموز"})

# Gender normalization constants
_GENDER_MALE_EN = frozenset({"1", "male", "m", "boy", "♂"})
_GENDER_FEMALE_EN = frozenset({"2", "female", "f", "girl", "♀"})
_GENDER_MALE_FA = frozenset({"پسر", "مذکر"})
_GENDER_FEMALE_FA = frozenset({"دختر", "مونث"})
_GENDER_MALE_FA_NORMALIZED = frozenset(normalize_fa(tok) for tok in _GENDER_MALE_FA)
_GENDER_FEMALE_FA_NORMALIZED = frozenset(normalize_fa(tok) for tok in _GENDER_FEMALE_FA)


def _num_to_int_safe(x: Any) -> int:
    """تبدیل امن مقدار به int بدون ایجاد استثناء غیرمنتظره.

    مثال::

        >>> _num_to_int_safe("12.7")
        12

    """

    s = to_numlike_str(x)
    if not s or s == "-":
        return 0
    sign = -1 if s.startswith("-") else 1
    digits = s[1:] if sign == -1 else s
    int_part = digits.split(".")[0]
    if not int_part:
        return 0
    if int_part.isdigit():
        return sign * int(int_part)
    return 0


def _coerce_center_id(val: Any, default_zero: int = 0) -> int:
    """تبدیل مقدار ورودی به شناسهٔ مرکز غیرمنفی."""

    n = _num_to_int_safe(val)
    return n if n >= 0 else default_zero


def _coerce_finance(val: Any, *, cfg: BuildConfig) -> int:
    """بازگرداندن کد مالی معتبر مطابق تنظیمات."""

    v = _num_to_int_safe(val)
    if v in cfg.finance_variants:
        return v
    return cfg.finance_variants[0] if cfg.finance_variants else 0


def _normalize_map_keys(m: Mapping[str, int]) -> dict[str, int]:
    """نرمال‌سازی کلیدهای نگاشت مراکز با رعایت wildcard."""

    out: dict[str, int] = {}
    for k, v in m.items():
        normalized_key = "*" if k == "*" else normalize_fa(k)
        if normalized_key:
            out[normalized_key] = _num_to_int_safe(v)
    return out


def _postal_valid(num_str: str, *, cfg: BuildConfig) -> bool:
    """اعتبارسنجی بازهٔ کدپستی مطابق پیکربندی."""

    n = _num_to_int_safe(num_str)
    min_val, max_val = cfg.postal_valid_range
    return min_val <= n <= max_val


def is_valid_postal_code(postal_code: Any) -> TypeGuard[str]:
    """
    TypeGuard to check if a value is a string of digits (potential postal code).
    This is a basic check before further validation.
    """
    return isinstance(postal_code, str) and postal_code.isdigit()


def _compute_school_alias(mentor_id: Any) -> str:
    """تولید کد جایگزین برای ردیف‌های مدرسه‌ای (همیشه شناسهٔ پشتیبان)."""

    return to_numlike_str(mentor_id) or normalize_fa(mentor_id) or ""


def _compute_normal_or_dual_alias(postal_code: Any, mentor_id: Any, cfg: BuildConfig) -> str:
    """کد جایگزین برای ردیف‌های عادی یا دوگانه (کدپستی معتبر یا شناسهٔ پشتیبان)."""

    postal_str = to_numlike_str(postal_code)
    if _postal_valid(postal_str, cfg=cfg):
        return postal_str
    return to_numlike_str(mentor_id) or normalize_fa(mentor_id) or ""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@final
@dataclass(frozen=True, slots=True)
class BuildConfig:
    """
    Build-time configuration for the allocation system.

    Attributes:
        version: Version string
        postal_valid_range: Min/max for valid postal codes
        finance_variants: Valid finance codes
        center_map: Manager name → center ID mapping
        school_code_empty_as_zero: Treat empty school codes as 0
        alias_rule_normal: Alias rule for NORMAL mentors
        alias_rule_school: Alias rule for SCHOOL mentors
    """
    version: str = "1.0.3"
    postal_valid_range: tuple[int, int] = (1000, 9999)
    finance_variants: tuple[int, ...] = (0, 1, 3)
    center_map: dict[str, int] = field(default_factory=lambda: {"*": 0})
    school_code_empty_as_zero: bool = True
    alias_rule_normal: str = "postal_or_fallback_mentor_id"
    alias_rule_school: str = "mentor_id"
    _center_map_norm: dict[str, int] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization."""
        if len(self.postal_valid_range) != 2 or self.postal_valid_range[0] > self.postal_valid_range[1]:
            raise ValueError(f"Invalid postal range: {self.postal_valid_range}")

    def center_map_norm(self) -> dict[str, int]:
        """
        Get normalized center_map with keys normalized using normalize_fa.
        Cached after first call.
        """
        if not self._center_map_norm:
            object.__setattr__(self, "_center_map_norm", _normalize_map_keys(self.center_map))
        return self._center_map_norm


# ---------------------------------------------------------------------------
# Domain functions
# ---------------------------------------------------------------------------

def norm_status(x: Any) -> int:
    """نرمال‌سازی وضعیت تحصیلی به کد ۰/۱.

    مثال::

        >>> norm_status("فارغ")
        0

    """

    raw = str(x or "").strip().lower()
    if raw in _STATUS_GRADUATE_EN:
        return 0
    if raw in _STATUS_STUDENT_EN:
        return 1

    normalized = normalize_fa(x)
    if any(token in normalized for token in _STATUS_GRADUATE_FA):
        return 0
    if any(token in normalized for token in _STATUS_STUDENT_FA):
        return 1
    return 1


def norm_gender(x: Any, strict: bool = False) -> Gender:
    """نرمال‌سازی جنسیت به مقادیر دامنه‌ای.

    Args:
        x: مقدار خام ورودی.
        strict: در صورت `True` برای مقادیر ناشناخته استثناء می‌اندازد.

    Returns:
        عضو :class:`Gender` متناظر. در حالت غیرسخت‌گیر مقدار پیش‌فرض
        :data:`Gender.MALE` برگردانده می‌شود.

    Raises:
        InvalidGenderValueError: اگر `strict=True` و مقدار ورودی قابل نگاشت
            نباشد.
    """

    raw = str(x or "").strip().lower()
    if raw in _GENDER_MALE_EN:
        return Gender.MALE
    if raw in _GENDER_FEMALE_EN:
        return Gender.FEMALE

    normalized = normalize_fa(x)
    normalized_padded = f" {normalized} " if normalized else ""
    if normalized_padded:
        if any(f" {token} " in normalized_padded for token in _GENDER_MALE_FA_NORMALIZED):
            return Gender.MALE
        if any(
            f" {token} " in normalized_padded for token in _GENDER_FEMALE_FA_NORMALIZED
        ):
            return Gender.FEMALE

    numeric = _num_to_int_safe(raw or normalized)
    if numeric == int(Gender.FEMALE):
        return Gender.FEMALE

    if strict:
        raise InvalidGenderValueError(
            func="norm_gender",
            column=COL_GENDER,
            value=x,
        )

    return Gender.MALE


def center_from_manager(name: Any, *, cfg: BuildConfig) -> int:
    """استخراج شناسهٔ مرکز از نام مدیر با استفاده از نگاشت پیکربندی.

    مثال::

        >>> cfg = BuildConfig(center_map={"مدیر الف": 2, "*": 0})
        >>> center_from_manager("مدیر الف", cfg=cfg)
        2

    """

    s = normalize_fa(name)
    cmap = cfg.center_map_norm()
    wildcard = cmap.get('*')

    if s:
        if s in cmap:
            return cmap[s]

        for key, val in cmap.items():
            if key != '*' and key in s:
                return val

    if wildcard is not None:
        return wildcard

    raise InvalidCenterMappingError(func='center_from_manager', value=name)


def mentor_type(postal_code: Any, school_count: int | None, *, cfg: BuildConfig) -> MentorType:
    """تعیین نوع پشتیبان بر اساس کدپستی و تعداد مدارس.

    مثال::

        >>> mentor_type('12345', 0, cfg=BuildConfig())
        <MentorType.NORMAL: 'normal'>

    """

    has_postal = _postal_valid(to_numlike_str(postal_code), cfg=cfg)
    has_school = (school_count or 0) > 0

    if has_postal and has_school:
        return MentorType.DUAL
    if has_school:
        return MentorType.SCHOOL
    return MentorType.NORMAL


def compute_alias(row_type: MentorType, postal_code: Any, mentor_id: Any, *, cfg: BuildConfig) -> str:
    """تولید مقدار ستون «جایگزین» براساس نوع ردیف.

    مثال::

        >>> compute_alias(MentorType.NORMAL, '12345', 'EMP-1', cfg=BuildConfig())
        '12345'

    """

    if row_type is MentorType.SCHOOL:
        return _compute_school_alias(mentor_id)
    return _compute_normal_or_dual_alias(postal_code, mentor_id, cfg)


def compute_mentor_type_str(row_type: MentorType) -> str:
    """تبدیل نوع پشتیبان به متن فارسی استاندارد.

    مثال::

        >>> compute_mentor_type_str(MentorType.SCHOOL)
        'مدرسه‌ای'

    """

    mapping = {
        MentorType.NORMAL: "عادی",
        MentorType.SCHOOL: "مدرسه‌ای",
        MentorType.DUAL: "دوگانه",
    }
    return mapping.get(row_type, "عادی")


# ---------------------------------------------------------------------------
# Join Key
# ---------------------------------------------------------------------------

@final
@dataclass(frozen=True, slots=True)
class JoinKey:
    """
    Six-field join key for matching students to mentor matrix rows.

    Fields: major, gender, status, center, finance, school_code
    """
    major: int
    gender: int
    status: int
    center: int
    finance: int
    school_code: int

    def __repr__(self) -> str:
        """Provide a clear string representation for debugging."""
        return (
            f"JoinKey(major={self.major}, gender={self.gender}, status={self.status}, "
            f"center={self.center}, finance={self.finance}, school_code={self.school_code})"
        )

    @staticmethod
    def from_student_row(row: StudentRow, *, cfg: BuildConfig) -> "JoinKey":
        """ساخت کلید الحاق از سطر دانش‌آموز.

        مثال::

            >>> row = {
            ...     "کدرشته": 1201,
            ...     "جنسیت": 1,
            ...     "دانش آموز فارغ": 0,
            ...     "مرکز گلستان صدرا": 1,
            ...     "مالی حکمت بنیاد": 0,
            ...     "کد مدرسه": 3581,
            ... }
            >>> JoinKey.from_student_row(row, cfg=BuildConfig())
            JoinKey(major=1201, gender=1, status=1, center=1, finance=0, school_code=3581)

        """

        required = {COL_GROUP, COL_GENDER, COL_STATUS, COL_FINANCE}
        missing = [col for col in required if col not in row]
        if missing:
            raise DataMissingError(func='JoinKey.from_student_row', column=','.join(missing), value=None)

        major = _num_to_int_safe(row.get(COL_GROUP, 0))
        gender = norm_gender(row.get(COL_GENDER, 1))
        status = norm_status(row.get(COL_STATUS, 1))

        center_val = row.get(COL_CENTER, "")
        center = _coerce_center_id(center_val, default_zero=0)
        if center == 0:
            manager_name = row.get(COL_MANAGER, "")
            center = center_from_manager(manager_name, cfg=cfg)

        finance = _coerce_finance(row.get(COL_FINANCE, 0), cfg=cfg)

        school_val = row.get(COL_SCHOOL, "")
        school_str = to_numlike_str(school_val)
        school_code = _num_to_int_safe(school_str) if school_str and school_str != '0' else 0

        return JoinKey(
            major=major,
            gender=gender,
            status=status,
            center=center,
            finance=finance,
            school_code=school_code,
        )


    def as_dict(self) -> JoinKeyDict:
        """
        Convert to dict with Persian column names.

        Returns:
            A mapping: {COL_GROUP: major, COL_GENDER: gender, ...}
        """
        return {
            COL_GROUP: self.major,
            COL_GENDER: self.gender,
            COL_STATUS: self.status,
            COL_CENTER: self.center,
            COL_FINANCE: self.finance,
            COL_SCHOOL: self.school_code,
        }


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

@final
@dataclass(frozen=True, slots=True)
class MentorIdentity:
    """Mentor identity: ID, name, and manager name."""
    mentor_id: str
    mentor_name: str
    manager_name: str

    def __repr__(self) -> str:
        """Provide a clear string representation for debugging."""
        return f"MentorIdentity(mentor_id='{self.mentor_id}', mentor_name='{self.mentor_name}', manager_name='{self.manager_name}')"


@final
@dataclass(frozen=True, slots=True)
class Capacity:
    """
    Mentor capacity tracking.

    Attributes:
        covered_now: Current coverage count
        special_limit: Capacity limit for this mentor
        allocations_new: Number of new allocations made (default 0)
    """
    covered_now: int
    special_limit: int
    allocations_new: int = 0

    def __post_init__(self):
        """Validate capacity values after initialization."""
        if self.special_limit < 0:
             raise ValueError(f"special_limit must be non-negative, got {self.special_limit}")
        if self.covered_now < 0:
             raise ValueError(f"covered_now must be non-negative, got {self.covered_now}")
        if self.allocations_new < 0:
             raise ValueError(f"allocations_new must be non-negative, got {self.allocations_new}")

    def __repr__(self) -> str:
        """Provide a clear string representation for debugging."""
        return f"Capacity(covered_now={self.covered_now}, special_limit={self.special_limit}, allocations_new={self.allocations_new})"

    def occupancy_ratio(self) -> float:
        """نسبت اشغال فعلی را محاسبه می‌کند."""

        denominator = max(1, int(self.special_limit))
        numerator = max(int(self.covered_now) + int(self.allocations_new), 0)
        return float(numerator) / float(denominator)


@final
@dataclass(frozen=True, slots=True)
class MatrixRow:
    """
    A single row from the eligibility matrix.

    Represents one mentor with their eligibility criteria and metadata.
    """
    alias: str
    mentor: MentorIdentity
    major: int
    gender: int
    status: int
    center: int
    finance: int
    school_code: int
    row_type: MentorType
    mentor_row_id: int | str
    # New field for output schema
    mentor_type_str: str = field(init=False)

    def __post_init__(self):
        """Calculate mentor_type_str after initialization."""
        object.__setattr__(self, "mentor_type_str", compute_mentor_type_str(self.row_type))

    def __repr__(self) -> str:
        """Provide a clear string representation for debugging."""
        return f"MatrixRow(alias='{self.alias}', mentor={self.mentor}, row_type={self.row_type.value})"


@final
@dataclass(frozen=True, slots=True)
class ImportToSabtRow:
    """
    Output row for import to Sabt system.

    Contains postal code and mentor name for assignment.
    """
    postal_code: str
    mentor_name: str

    def __repr__(self) -> str:
        """Provide a clear string representation for debugging."""
        return f"ImportToSabtRow(postal_code='{self.postal_code}', mentor_name='{self.mentor_name}')"


# ---------------------------------------------------------------------------
# Trace types
# ---------------------------------------------------------------------------

DecisionReason = Literal[
    "no_candidate",
    "capacity_full",
    "gender_mismatch",
    "center_mismatch",
    "school_mismatch",
    "finance_mismatch",
    "status_policy"
]


class TraceDict(TypedDict, total=False):
    """
    Trace dictionary for allocation decisions.

    Required key: 'key' (dict with join keys)
    Optional keys: 'candidates' (int), 'reason' (str), 'top5' (list of mentor dicts)
    """
    key: JoinKeyDict  # Six join keys
    candidates: int
    reason: DecisionReason
    top5: list[MentorDict]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "COL_GROUP", "COL_GENDER", "COL_STATUS", "COL_CENTER", "COL_FINANCE", "COL_SCHOOL",
    "COL_SCHOOL_NAME", "COL_SCHOOL_CODE_1", "COL_SCHOOL_CODE_2", "COL_SCHOOL_CODE_3", "COL_SCHOOL_CODE_4",
    "COL_SCHOOL_NAME_1", "COL_SCHOOL_NAME_2", "COL_SCHOOL_NAME_3", "COL_SCHOOL_NAME_4",
    "COL_FULL_SCHOOL_CODE", "COL_EDU_CODE",
    "COL_ALIAS", "COL_MENTOR", "COL_MANAGER", "COL_MENTOR_ID", "COL_MENTOR_ROWID", "COL_MENTOR_TYPE",
    "MentorType", "Status", "Gender", "FinanceCode",
    "BuildConfig", "JoinKey", "MentorIdentity", "Capacity", "MatrixRow", "ImportToSabtRow",
    "norm_status", "norm_gender", "center_from_manager", "mentor_type", "compute_alias", "compute_mentor_type_str",
    "DecisionReason", "TraceDict",
    "StudentRow", "JoinKeyDict", "MentorDict",
]
