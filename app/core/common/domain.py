# -*- coding: utf-8 -*-
"""
Domain models and logic for Eligibility Matrix → Allocation system.
Python 3.10+, stdlib only, no I/O, no side-effects on import.
Deterministic and fail-safe, adhering to Policy v1.0.3.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Mapping, Literal, TypedDict, final, TypeGuard

from .normalization import normalize_fa, to_numlike_str

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type Aliases
# ---------------------------------------------------------------------------
# StudentRow: TypeAlias = Mapping[str, Any]  # Not using TypeAlias for stdlib compatibility
StudentRow = Mapping[str, Any]
JoinKeyDict = dict[str, int]
MentorDict = dict[str, Any]


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------
class DomainError(Exception):
    """Base exception for domain logic errors."""
    pass


class InvalidFinanceCodeError(DomainError):
    """Raised when a finance code is not in the allowed variants."""
    pass


class InvalidPostalCodeError(DomainError):
    """Raised when a postal code is out of the valid range."""
    pass


class InvalidGenderValueError(DomainError):
    """Raised when a gender value cannot be normalized to MALE or FEMALE."""
    pass


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


def _num_to_int_safe(x: Any) -> int:
    """
    Convert any value to int safely using to_numlike_str.
    Empty/invalid → 0. Decimals → truncate to integer part.
    """
    try:
        s = to_numlike_str(x)
        if not s or s == "-":
            return 0
        # Handle negative numbers
        if s.startswith("-"):
            sign = -1
            s = s[1:]
        else:
            sign = 1
        # Take integer part only
        if "." in s:
            s = s.split(".")[0]
        if not s:
            return 0
        return sign * int(s)
    except Exception:
        logger.exception(f"Error converting value to int: {x}")
        return 0


def _coerce_center_id(val: Any, default_zero: int = 0) -> int:
    """
    Coerce value to non-negative center ID.
    Only accept non-negative integers; error → default_zero.
    """
    try:
        n = _num_to_int_safe(val)
        return n if n >= 0 else default_zero
    except Exception:
        logger.exception(f"Error coercing center ID from value: {val}")
        return default_zero


def _coerce_finance(val: Any, *, cfg: BuildConfig) -> int:
    """
    Coerce finance value to a valid code from cfg.finance_variants.
    If not in variants, return the first variant (typically 0) or 0 if empty.
    """
    v = _num_to_int_safe(val)
    if v in cfg.finance_variants:
        return v
    logger.warning(f"Invalid finance code: {val}. Coercing to default: {cfg.finance_variants[0]}")
    return cfg.finance_variants[0] if cfg.finance_variants else 0


def _normalize_map_keys(m: Mapping[str, int]) -> dict[str, int]:
    """
    Normalize all keys in mapping using normalize_fa.
    Preserves "*" wildcard key. Values are safely coerced to int.
    """
    try:
        out: dict[str, int] = {}
        for k, v in m.items():
            # More robust handling of non-string keys for k
            nk = "*" if k == "*" else normalize_fa(k)
            if nk:
                out[nk] = _num_to_int_safe(v)
        return out
    except Exception:
        logger.exception(f"Error normalizing map keys: {dict(m)}")
        return {}


def _postal_valid(num_str: str, *, cfg: BuildConfig) -> bool:
    """
    Check if postal code string is in valid range.
    Uses cfg.postal_valid_range.
    """
    try:
        n = _num_to_int_safe(num_str)
        min_val, max_val = cfg.postal_valid_range
        is_valid = min_val <= n <= max_val
        logger.debug(f"Postal code validation: '{num_str}' (int: {n}) -> {is_valid} (range: {min_val}-{max_val})")
        return is_valid
    except Exception:
        logger.exception(f"Error validating postal code: {num_str}")
        return False


def is_valid_postal_code(postal_code: Any) -> TypeGuard[str]:
    """
    TypeGuard to check if a value is a string of digits (potential postal code).
    This is a basic check before further validation.
    """
    return isinstance(postal_code, str) and postal_code.isdigit()


def _compute_school_alias(mentor_id: Any) -> str:
    """
    Compute alias for SCHOOL mentors: always mentor_id.
    """
    alias = to_numlike_str(mentor_id) or normalize_fa(mentor_id) or ""
    logger.debug(f"Computed SCHOOL alias for mentor_id '{mentor_id}': '{alias}'")
    return alias


def _compute_normal_or_dual_alias(postal_code: Any, mentor_id: Any, cfg: BuildConfig) -> str:
    """
    Compute alias for NORMAL/DUAL mentors: postal if valid, else mentor_id.
    """
    postal_str = to_numlike_str(postal_code)
    if _postal_valid(postal_str, cfg=cfg):
        logger.debug(f"Using valid postal code as alias: '{postal_str}'")
        return postal_str
    else:
        alias = to_numlike_str(mentor_id) or normalize_fa(mentor_id) or ""
        logger.debug(f"Postal code '{postal_str}' invalid. Using mentor_id as alias: '{alias}'")
        return alias


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
    """
    Normalize status value to Status enum (0=graduate, 1=student).
    Supports both English tokens and Persian equivalents.
    Default on ambiguity: 1 (STUDENT).

    Args:
        x: The input value to normalize.

    Returns:
        The corresponding Status enum value (1 for STUDENT, 0 for GRADUATE).
        Defaults to 1 (STUDENT) if normalization fails.
    """
    try:
        # Path 1: English/numeric tokens on raw lowercased string
        raw = str(x).strip().lower()
        if raw in _STATUS_GRADUATE_EN:
            logger.debug(f"Normalized status '{x}' to GRADUATE (0) using English token.")
            return 0
        if raw in _STATUS_STUDENT_EN:
            logger.debug(f"Normalized status '{x}' to STUDENT (1) using English token.")
            return 1

        # Path 2: Persian equivalents on normalized string
        s = normalize_fa(x)
        for token in _STATUS_GRADUATE_FA:
            if token in s:
                logger.debug(f"Normalized status '{x}' to GRADUATE (0) using Persian token '{token}'.")
                return 0
        for token in _STATUS_STUDENT_FA:
            if token in s:
                logger.debug(f"Normalized status '{x}' to STUDENT (1) using Persian token '{token}'.")
                return 1

        # Default
        logger.warning(f"Could not normalize status '{x}'. Defaulting to STUDENT (1).")
        return 1
    except Exception:
        logger.exception(f"Error normalizing status value: {x}")
        return 1


def norm_gender(x: Any, strict: bool = False) -> int:
    """
    Normalize gender value to Gender enum (1=male, 2=female).
    Supports both English tokens and Persian equivalents.
    Default on ambiguity: 1 (MALE).

    Args:
        x: The input value to normalize.
        strict: If True, raises InvalidGenderValueError on unrecognized values.

    Returns:
        The corresponding Gender enum value (1 for MALE, 2 for FEMALE).
        Defaults to 1 (MALE) if normalization fails and strict is False.

    Raises:
        InvalidGenderValueError: If strict is True and the gender value cannot be normalized.
    """
    try:
        # Path 1: English/numeric tokens on raw lowercased string
        raw = str(x).strip().lower()
        if raw in _GENDER_MALE_EN:
            logger.debug(f"Normalized gender '{x}' to MALE (1) using English token.")
            return 1
        if raw in _GENDER_FEMALE_EN:
            logger.debug(f"Normalized gender '{x}' to FEMALE (2) using English token.")
            return 2

        # Path 2: Persian equivalents on normalized string
        s = normalize_fa(x)
        # Use 'any' for more flexible matching (contains check)
        if any(tok in s for tok in _GENDER_MALE_FA):
            logger.debug(f"Normalized gender '{x}' to MALE (1) using Persian token.")
            return 1
        if any(tok in s for tok in _GENDER_FEMALE_FA):
            logger.debug(f"Normalized gender '{x}' to FEMALE (2) using Persian token.")
            return 2

        # Path 3: Try numeric conversion
        n = _num_to_int_safe(raw or s)
        result = 2 if n == 2 else 1
        logger.debug(f"Normalized gender '{x}' to {result} using numeric conversion.")

        if strict and result == 1: # If default was returned
            # Check if it was truly invalid
            all_known_tokens = _GENDER_MALE_EN | _GENDER_FEMALE_EN | {normalize_fa(tok) for tok in _GENDER_MALE_FA | _GENDER_FEMALE_FA}
            if normalize_fa(x) not in all_known_tokens and raw not in all_known_tokens:
                logger.error(f"Strict gender normalization failed for value: {x}")
                raise InvalidGenderValueError(f"Invalid gender value: {x}")
        return result
    except InvalidGenderValueError:
        logger.error(f"Strict gender normalization failed for value: {x}")
        raise # Re-raise if we raised it ourselves
    except Exception:
        if strict:
            logger.exception(f"Unexpected error in strict gender normalization for value: {x}")
            raise InvalidGenderValueError(f"Invalid gender value: {x}")
        logger.exception(f"Error normalizing gender value: {x}")
        return 1 # In non-strict mode, still return default


def center_from_manager(name: Any, *, cfg: BuildConfig) -> int:
    """
    Extract center ID from manager name using cfg.center_map_norm().
    First tries exact match, then contains match, then "*" wildcard, else 0.

    Args:
        name: The manager's name.
        cfg: The BuildConfig instance containing the center map.

    Returns:
        The corresponding center ID (0, 1, 2, ...).
    """
    try:
        s = normalize_fa(name)
        if not s:
            logger.warning(f"Manager name '{name}' normalizes to empty string. Using wildcard center ID.")
            return cfg.center_map_norm().get("*", 0)

        cmap = cfg.center_map_norm()
        logger.debug(f"Looking for center ID for manager name: '{s}' in map: {cmap}")

        # Exact match
        if s in cmap:
            center_id = cmap[s]
            logger.debug(f"Found exact match for manager '{s}': center ID {center_id}")
            return center_id

        # Contains match
        for key, val in cmap.items():
            if key != "*" and key in s:
                logger.debug(f"Found partial match for manager '{s}' with key '{key}': center ID {val}")
                return val

        # Wildcard
        wildcard_id = cmap.get("*", 0)
        logger.debug(f"No match found for manager '{s}'. Using wildcard center ID: {wildcard_id}")
        return wildcard_id
    except Exception:
        logger.exception(f"Error extracting center ID from manager name: {name}")
        return 0


def mentor_type(postal_code: Any, school_count: int | None, *, cfg: BuildConfig) -> MentorType:
    """
    Determine mentor type based on postal code validity and school count.

    - Valid postal → NORMAL
    - school_count > 0 → SCHOOL
    - Both → DUAL
    - Neither → NORMAL (default)

    Args:
        postal_code: The mentor's postal code.
        school_count: The number of schools the mentor covers.
        cfg: The BuildConfig instance.

    Returns:
        The corresponding MentorType.
    """
    try:
        postal_str = to_numlike_str(postal_code)
        has_postal = _postal_valid(postal_str, cfg=cfg)
        # More robust handling of school_count
        has_school = (_num_to_int_safe(school_count) > 0) if (school_count is not None) else False

        logger.debug(f"Evaluating mentor type: postal='{postal_code}' (valid: {has_postal}), school_count={school_count} (has_school: {has_school})")

        if has_postal and has_school:
            logger.debug("Mentor type determined as DUAL.")
            return MentorType.DUAL
        if has_school:
            logger.debug("Mentor type determined as SCHOOL.")
            return MentorType.SCHOOL
        # has_postal or neither → NORMAL
        logger.debug("Mentor type determined as NORMAL.")
        return MentorType.NORMAL
    except Exception:
        logger.exception(f"Error determining mentor type for postal: {postal_code}, school_count: {school_count}")
        return MentorType.NORMAL


def compute_alias(row_type: MentorType, postal_code: Any, mentor_id: Any, *, cfg: BuildConfig) -> str:
    """
    Compute alias for a mentor row.

    Rules:
    - SCHOOL: always mentor_id
    - NORMAL/DUAL: use postal if valid, else mentor_id

    Output is stable ASCII string. Falls back to normalized text if numeric conversion fails.

    Args:
        row_type: The MentorType (SCHOOL, NORMAL, DUAL).
        postal_code: The mentor's postal code.
        mentor_id: The mentor's ID.
        cfg: The BuildConfig instance.

    Returns:
        The computed alias string.
    """
    try:
        logger.debug(f"Computing alias for row_type: {row_type}, postal: {postal_code}, mentor_id: {mentor_id}")
        if row_type == MentorType.SCHOOL:
            alias = _compute_school_alias(mentor_id)
            logger.debug(f"Computed SCHOOL alias: '{alias}'")
            return alias

        # NORMAL or DUAL: prefer postal if valid
        alias = _compute_normal_or_dual_alias(postal_code, mentor_id, cfg)
        logger.debug(f"Computed {row_type.value} alias: '{alias}'")
        return alias
    except Exception:
        logger.exception(f"Error computing alias for row_type: {row_type}, postal: {postal_code}, mentor_id: {mentor_id}")
        return ""


def compute_mentor_type_str(row_type: MentorType) -> str:
    """
    Convert MentorType enum to its string representation for output schema.

    Args:
        row_type: The MentorType enum value.

    Returns:
        The string representation ('عادی', 'مدرسه ای', 'دوقلو').
    """
    # Note: Using 'مدرسه ای' with a space for consistency with the spec's column name
    # Adjust the mapping if the exact output string format is different.
    type_map = {
        MentorType.NORMAL: "عادی",
        MentorType.SCHOOL: "مدرسه ای", # Matches column name COL_MENTOR_TYPE
        MentorType.DUAL: "دوقلو"
    }
    result = type_map.get(row_type, "نامشخص")
    logger.debug(f"Converted MentorType '{row_type}' to string '{result}'")
    return result


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
        """
        Build JoinKey from student row mapping.

        - All numeric fields converted safely via to_numlike_str → int
        - gender normalized via norm_gender
        - status normalized via norm_status
        - center: if empty/invalid, extract from manager using cfg.center_map_norm()
        - school_code: empty/invalid → 0
        - finance: coerced to valid variant using _coerce_finance

        Args:
            row: A mapping representing a student row.
            cfg: The BuildConfig instance.

        Returns:
            A JoinKey instance.
        """
        try:
            major = _num_to_int_safe(row.get(COL_GROUP, 0))
            gender = norm_gender(row.get(COL_GENDER, 1))
            status = norm_status(row.get(COL_STATUS, 1))

            # Center: try direct value first, else extract from manager
            center_val = row.get(COL_CENTER, "")
            center = _coerce_center_id(center_val, default_zero=0)
            if center == 0:
                manager_name = row.get(COL_MANAGER, "")
                center = center_from_manager(manager_name, cfg=cfg)

            # Finance: coerce to valid variant
            finance = _coerce_finance(row.get(COL_FINANCE, 0), cfg=cfg)

            # School code: empty/invalid → 0
            school_val = row.get(COL_SCHOOL, "")
            school_str = to_numlike_str(school_val)
            if not school_str or school_str == "0":
                school_code = 0
            else:
                school_code = _num_to_int_safe(school_str)

            logger.debug(f"Created JoinKey from row: {dict(row)} -> {JoinKey(major=major, gender=gender, status=status, center=center, finance=finance, school_code=school_code)}")
            return JoinKey(
                major=major,
                gender=gender,
                status=status,
                center=center,
                finance=finance,
                school_code=school_code
            )
        except Exception:
            logger.exception(f"Error creating JoinKey from row: {dict(row)}")
            return JoinKey(major=0, gender=1, status=1, center=0, finance=0, school_code=0)

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
        """
        Calculate occupancy ratio as (covered_now + allocations_new) / max(1, special_limit).
        Fail-safe: denominator always >= 1.

        Returns:
            float in [0.0, inf), typically [0.0, 1.0+]
        """
        try:
            den = max(int(self.special_limit), 1)
            num = max(int(self.covered_now) + int(self.allocations_new), 0)
            ratio = float(num) / float(den)
            logger.debug(f"Calculated occupancy ratio for capacity {self}: {ratio}")
            return ratio
        except Exception:
            logger.exception(f"Error calculating occupancy ratio for capacity: {self}")
            return 0.0


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
    "COL_ALIAS", "COL_MENTOR", "COL_MANAGER", "COL_MENTOR_ID", "COL_MENTOR_ROWID", "COL_MENTOR_TYPE",
    "MentorType", "Status", "Gender", "FinanceCode",
    "BuildConfig", "JoinKey", "MentorIdentity", "Capacity", "MatrixRow", "ImportToSabtRow",
    "norm_status", "norm_gender", "center_from_manager", "mentor_type", "compute_alias", "compute_mentor_type_str",
    "DecisionReason", "TraceDict",
    "DomainError", "InvalidFinanceCodeError", "InvalidPostalCodeError", "InvalidGenderValueError",
    "StudentRow", "JoinKeyDict", "MentorDict",
]
