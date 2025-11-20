"""Policy Loader (Core) — سبک، کش‌شونده و متکی بر SSoT.

نکتهٔ معماری: اگر قرار باشد I/O از Core خارج شود، کافی است دادهٔ JSON خوانده‌شده
در Infra به تابع :func:`parse_policy_dict` پاس داده شود. این ماژول هر دو مسیر
را فراهم می‌کند.
"""

from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
from typing import Literal, cast

from app.core.policy.config import AllocationChannelConfig
from app.core.policy.loader import compute_schema_hash, validate_policy_columns

VersionMismatchMode = Literal["raise", "warn", "migrate"]

DEFAULT_POLICY_VERSION = "1.0.3"
_EXPECTED_JOIN_KEYS_COUNT = 6
_EXPECTED_RANKING_ITEMS_COUNT = 4

_DEFAULT_VIRTUAL_ALIAS_RANGES: tuple[tuple[int, int], ...] = ((7000, 7999),)
_DEFAULT_VIRTUAL_NAME_PATTERNS: tuple[str, ...] = (
    r"در\s+انتظار\s+تخصیص",
    "فراگیر",
)
_DEFAULT_COVERAGE_THRESHOLD = 0.95
_DEFAULT_DEDUP_REMOVED_RATIO_THRESHOLD = 0.05
_DEFAULT_SCHOOL_LOOKUP_MISMATCH_THRESHOLD = 0.0
_DEFAULT_JOIN_KEY_DUPLICATE_THRESHOLD = 0
_DEFAULT_EXCEL_OPTIONS: Mapping[str, object] = {
    "rtl": True,
    "font_name": "Tahoma",
    "font_size": 8,
    "header_mode_internal": "en",
    "header_mode_write": "fa_en",
}
_DEFAULT_REASON_TRACE_LABELS: tuple[str, ...] = (
    "جنسیت",
    "مدرسه",
    "گروه/رشته",
    "سیاست رتبه‌بندی",
)
_DEFAULT_SELECTION_REASON_OPTIONS: Mapping[str, object] = {
    "enabled": True,
    "sheet_name": "دلایل انتخاب پشتیبان",
    "template": (
        "دانش‌آموز {gender_label} — مدرسه {school_name} (پس‌مدرسه‌ای={is_after_school}) — "
        "رشته/گروه {track_label} — طبق سیاست رتبه‌بندی: {ranking_chain} — پشتیبان: {mentor_id} / {mentor_name}"
    ),
    "trace_stage_labels": _DEFAULT_REASON_TRACE_LABELS,
    "locale": "fa",
    "labels": {
        "reason": {
            "gender": ("جنسیت",),
            "school": ("مدرسه",),
            "track": ("رشته/گروه",),
            "capacity": ("ظرفیت",),
            "result": ("نتیجه",),
            "tiebreak": ("سیاست رتبه‌بندی",),
        }
    },
    "columns": (
        "شمارنده",
        "کدملی",
        "نام",
        "نام خانوادگی",
        "شناسه پشتیبان",
        "دلیل انتخاب پشتیبان",
    ),
}

_TRACE_STAGE_ORDER: tuple[str, ...] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
    "capacity_gate",
)

_LEGACY_TRACE_DEFAULTS: Mapping[str, str] = {
    "type": "کدرشته",
    "group": "گروه آزمایشی",
    "gender": "جنسیت",
    "graduation_status": "دانش آموز فارغ",
    "center": "مرکز گلستان صدرا",
    "finance": "مالی حکمت بنیاد",
    "school": "کد مدرسه",
    "capacity_gate": "remaining_capacity",
}

_RANKING_RULE_LIBRARY: Mapping[str, tuple[str, bool]] = {
    "min_occupancy_ratio": ("occupancy_ratio", True),
    "max_remaining_capacity": ("remaining_capacity_desc", True),
    "min_allocations_new": ("allocations_new", True),
    "min_mentor_id": ("mentor_sort_key", True),
}

_VALID_FAIRNESS_STRATEGIES: tuple[str, ...] = (
    "none",
    "deterministic_jitter",
    "round_robin",
)


@dataclass(frozen=True)
class PolicyColumns:
    """تعریف ستون‌های سیاست که باید از Policy خوانده شوند."""

    postal_code: str
    school_count: str
    school_code: str
    capacity_current: str
    capacity_special: str
    remaining_capacity: str


@dataclass(frozen=True)
class PolicyAliasRule:
    """قوانین تعیین alias برای ردیف‌های عادی و مدرسه‌ای."""

    normal: str
    school: str


@dataclass(frozen=True)
class GenderCode:
    """تعریف نگاشت جنسیت به مقدار و کد شمارنده."""

    value: int
    counter_code: str


@dataclass(frozen=True)
class GenderCodes:
    """مجموعهٔ نگاشت جنسیت بر اساس policy."""

    male: GenderCode
    female: GenderCode


@dataclass(frozen=True)
class CenterConfig:
    """تعریف یک مرکز ثبت‌نام با متادیتای کامل."""

    id: int
    name: str
    default_manager: str | None = None
    description: str = ""

    def manager_list(self) -> tuple[str, ...]:
        """لیست پایدار مدیران پیش‌فرض (صفر یا یک نفر)."""

        if self.default_manager:
            return (self.default_manager,)
        return tuple()


@dataclass(frozen=True)
class CenterManagementConfig:
    """تنظیمات جامع مدیریت مراکز برای تخصیص."""

    enabled: bool
    centers: tuple[CenterConfig, ...]
    priority_order: tuple[int, ...]
    strict_manager_validation: bool
    default_center_for_invalid: int | None
    school_student_column: str

    def center_ids(self) -> tuple[int, ...]:
        """لیست پایدار شناسه‌های مراکز تعریف‌شده."""

        return tuple(center.id for center in self.centers)

    def get_center(self, center_id: int) -> CenterConfig | None:
        """یافتن تعریف مرکز براساس شناسه (در صورت وجود)."""

        return next((center for center in self.centers if center.id == center_id), None)

    def get_center_name(self, center_id: int) -> str:
        """بازیابی نام مرکز برای نمایش در UI/Logs."""

        center = self.get_center(center_id)
        return center.name if center else f"مرکز {center_id}"

    def validate_priority_order(self) -> bool:
        """صحت ترتیب اولویت را نسبت به مراکز تعریف‌شده بررسی می‌کند."""

        if not self.priority_order:
            return False
        defined = {center.id for center in self.centers}
        return all(center_id in defined for center_id in self.priority_order)


@dataclass(frozen=True)
class ExcelOptions:
    """تنظیمات خروجی Excel (جهت، فونت و حالت هدر)."""

    rtl: bool
    font_name: str
    font_size: int
    header_mode_internal: str
    header_mode_write: str

    @property
    def header_mode(self) -> str:
        """سازگاری رو به عقب: حالت هدر خروجی."""

        return self.header_mode_write


@dataclass(frozen=True)
class SelectionReasonOptions:
    """تنظیمات شیت دلایل انتخاب پشتیبان."""

    enabled: bool
    sheet_name: str
    template: str
    trace_stage_labels: tuple[str, ...]
    locale: str
    labels: Mapping[str, tuple[str, ...]]
    columns: tuple[str, ...]
    schema_hash: str


@dataclass(frozen=True)
class EmissionOptions:
    """گزینه‌های انتشار خروجی‌های توضیحی."""

    selection_reasons: SelectionReasonOptions


@dataclass(frozen=True)
class MatrixCoverageOptions:
    """گزینه‌های سیاست پوشش ماتریس برای کنترل مخرج پوشش."""

    denominator_mode: str
    require_student_presence: bool
    include_blocked_candidates_in_denominator: bool


@dataclass(frozen=True)
class MentorSchoolBindingPolicy:
    """سیاست اتصال مدرسه به پشتیبان برای تعیین global/restricted.

    Attributes
    ----------
    global_mode:
        مقدار متنی برای ردیف‌های بدون الزام مدرسه‌ای.
    restricted_mode:
        مقدار متنی برای ردیف‌های مقید به مدرسه.
    empty_tokens:
        مقادیر خالی/بی‌اثر که به‌عنوان نبود مدرسه تفسیر می‌شوند.
    """

    global_mode: str = "global"
    restricted_mode: str = "restricted"
    empty_tokens: tuple[str, ...] = ("", "0", "-", "—", "_", "nan", "NaN")

    @property
    def empty_placeholders(self) -> tuple[str, ...]:
        """سازگاری عقبرو برای نام قدیمی فیلد مقادیر تهی."""

        return self.empty_tokens

    def binding_mode(self, has_reference: bool) -> str:
        """حالت اتصال را بر اساس وجود مرجع مدرسه برمی‌گرداند."""

        return self.restricted_mode if has_reference else self.global_mode

    def is_empty_value(self, value: object) -> bool:
        """آیا مقدار ورودی نمایانگر نبود مدرسه است؟"""

        if value is None:
            return True
        text = str(value).strip()
        return text in self.empty_tokens


class MentorStatus(str, Enum):
    """وضعیت پشتیبان برای حاکمیت استخر."""

    ACTIVE = "active"
    INACTIVE = "inactive"

    @classmethod
    def from_value(cls, value: object) -> "MentorStatus":
        """تبدیل مقدار متنی به Enum؛ در صورت مقدار ناشناخته خطا می‌دهد."""

        text = str(value).strip().lower()
        for item in cls:
            if item.value == text:
                return item
        raise ValueError(f"Unknown mentor status '{value}'")


@dataclass(frozen=True)
class MentorPoolGovernanceConfig:
    """تنظیمات حاکمیت استخر پشتیبان‌ها بر اساس Policy."""

    default_status: MentorStatus
    mentor_status_map: Mapping[int, MentorStatus]
    allowed_statuses: tuple[MentorStatus, ...]

    @property
    def disabled_mentors(self) -> tuple[int, ...]:
        """شناسهٔ منتورهایی که در Policy غیرفعال هستند."""

        return tuple(
            sorted(
                mentor_id
                for mentor_id, status in self.mentor_status_map.items()
                if status != MentorStatus.ACTIVE
            )
        )

    def status_for(self, mentor_id: int | float | str | None) -> MentorStatus:
        """وضعیت مؤثر منتور بر اساس Policy را برمی‌گرداند."""

        if mentor_id is None:
            return self.default_status
        try:
            normalized_id = int(mentor_id)
        except (TypeError, ValueError):
            return self.default_status
        return self.mentor_status_map.get(normalized_id, self.default_status)


@dataclass(frozen=True)
class PolicyConfig:
    """ساختار دادهٔ فقط‌خواندنی برای نگهداری سیاست بارگذاری‌شده."""

    version: str
    normal_statuses: List[int]
    school_statuses: List[int]
    join_keys: List[str]
    required_student_fields: List[str]
    ranking_rules: List["RankingRule"]
    trace_stages: List["TraceStageDefinition"]
    gender_codes: GenderCodes
    postal_valid_range: tuple[int, int]
    finance_variants: tuple[int, ...]
    center_map: Mapping[str, int]
    school_code_empty_as_zero: bool
    prefer_major_code: bool
    coverage_threshold: float
    dedup_removed_ratio_threshold: float
    school_lookup_mismatch_threshold: float
    join_key_duplicate_threshold: int
    alias_rule: PolicyAliasRule
    columns: PolicyColumns
    column_aliases: Mapping[str, Dict[str, str]]
    excel: ExcelOptions
    virtual_alias_ranges: Tuple[Tuple[int, int], ...]
    virtual_name_patterns: Tuple[str, ...]
    emission: EmissionOptions
    fairness_strategy: str
    center_management: CenterManagementConfig
    allocation_channels: AllocationChannelConfig = field(
        default_factory=AllocationChannelConfig.empty
    )
    coverage_options: MatrixCoverageOptions = field(
        default_factory=lambda: MatrixCoverageOptions(
            denominator_mode="mentors",
            require_student_presence=False,
            include_blocked_candidates_in_denominator=False,
        )
    )
    mentor_school_binding: MentorSchoolBindingPolicy = field(
        default_factory=MentorSchoolBindingPolicy
    )
    mentor_pool_governance: MentorPoolGovernanceConfig = field(
        default_factory=lambda: MentorPoolGovernanceConfig(
            default_status=MentorStatus.ACTIVE,
            mentor_status_map={},
            allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
        )
    )

    @property
    def ranking(self) -> List[str]:
        """ترتیب قوانین رتبه‌بندی بر اساس نام قانون."""

        return [rule.name for rule in self.ranking_rules]

    @property
    def trace_stage_names(self) -> tuple[str, ...]:
        """نام مراحل تریس به‌ترتیب تعریف Policy."""

        return tuple(stage.stage for stage in self.trace_stages)

    def stage_column(self, stage: str) -> str:
        """نام ستون متناظر با مرحلهٔ تریس را برمی‌گرداند."""

        for item in self.trace_stages:
            if item.stage == stage:
                return item.column
        raise KeyError(f"Stage '{stage}' is not defined in policy trace stages")

    @property
    def capacity_column(self) -> str:
        """ستون ظرفیت را از روی تعریف مرحلهٔ capacity_gate استخراج می‌کند."""

        return self.stage_column("capacity_gate")

    @property
    def default_center_for_invalid(self) -> int | None:
        """شناسهٔ مرکز fallback برای مقادیر نامعتبر ستون مرکز."""

        return self.center_management.default_center_for_invalid

    @property
    def join_stage_columns(self) -> List[str]:
        """لیست ستون‌های فیلتر join به ترتیب تعریف‌شده در Policy."""

        return [item.column for item in self.trace_stages if item.stage != "capacity_gate"]


@dataclass(frozen=True)
class RankingRule:
    """تعریف قانون مرتب‌سازی از روی Policy."""

    name: str
    column: str
    ascending: bool


@dataclass(frozen=True)
class TraceStageDefinition:
    """تعریف مرحلهٔ تریس به‌صورت فقط‌خواندنی."""

    stage: str
    column: str


def _normalize_policy_payload(data: Mapping[str, object]) -> Mapping[str, object]:
    data = _apply_schema_defaults(dict(data))
    required = [
        "version",
        "normal_statuses",
        "school_statuses",
        "join_keys",
        "gender_codes",
        "postal_valid_range",
        "finance_variants",
        "center_map",
        "school_code_empty_as_zero",
        "prefer_major_code",
        "alias_rule",
        "columns",
        "virtual_alias_ranges",
        "virtual_name_patterns",
    ]
    missing = [key for key in required if key not in data]
    if "ranking_rules" not in data and "ranking" not in data:
        missing.append("ranking")
    if missing:
        raise ValueError(f"Policy keys missing: {missing}")

    version = str(data["version"])
    normal_statuses = _ensure_int_sequence("normal_statuses", data["normal_statuses"])
    school_statuses = _ensure_int_sequence("school_statuses", data["school_statuses"])
    join_keys = _normalize_join_keys(data["join_keys"])
    required_student_fields = _normalize_required_student_fields(
        data.get("required_student_fields"), join_keys
    )
    ranking_rules = _normalize_ranking_rules(data["ranking_rules"] if "ranking_rules" in data else data["ranking"])
    trace_stages = _normalize_trace_stages(data.get("trace_stages"), join_keys)
    postal_valid_range = _normalize_postal_valid_range(data["postal_valid_range"])
    finance_variants = _normalize_finance_variants(data["finance_variants"])
    center_map = _normalize_center_map(data["center_map"])
    center_management = _normalize_center_management(
        data.get("center_management"), center_map
    )
    school_code_empty_as_zero = _ensure_bool("school_code_empty_as_zero", data["school_code_empty_as_zero"])
    prefer_major_code = _ensure_bool("prefer_major_code", data["prefer_major_code"])
    coverage_threshold = _normalize_coverage_threshold(
        data.get("coverage_threshold", _DEFAULT_COVERAGE_THRESHOLD)
    )
    dedup_removed_ratio_threshold = _normalize_ratio_value(
        "dedup_removed_ratio_threshold",
        data.get(
            "dedup_removed_ratio_threshold",
            _DEFAULT_DEDUP_REMOVED_RATIO_THRESHOLD,
        ),
    )
    school_lookup_mismatch_threshold = _normalize_ratio_value(
        "school_lookup_mismatch_threshold",
        data.get(
            "school_lookup_mismatch_threshold",
            _DEFAULT_SCHOOL_LOOKUP_MISMATCH_THRESHOLD,
        ),
    )
    join_key_duplicate_threshold = _normalize_join_key_duplicate_threshold(
        data.get(
            "join_key_duplicate_threshold",
            _DEFAULT_JOIN_KEY_DUPLICATE_THRESHOLD,
        )
    )
    alias_rule = _normalize_alias_rule(data["alias_rule"])
    gender_codes = data["gender_codes"]
    if not isinstance(gender_codes, Mapping):
        raise TypeError("gender_codes must be a mapping")
    columns = _normalize_columns(data["columns"])
    column_aliases = _normalize_column_aliases(data.get("column_aliases", {}))
    excel = _normalize_excel_options(data.get("excel"))
    virtual_alias_ranges = _normalize_virtual_alias_ranges(data["virtual_alias_ranges"])
    virtual_name_patterns = _normalize_virtual_name_patterns(data["virtual_name_patterns"])
    fairness_strategy = _normalize_fairness_strategy(
        data.get("fairness_strategy") or data.get("fairness")
    )
    coverage_options = _normalize_coverage_options(
        (data.get("matrix") or {}).get("coverage", {})
    )
    mentor_pool_governance = _normalize_mentor_pool_governance(
        data.get("mentor_pool_governance")
    )
    allocation_channels = _normalize_allocation_channels(
        data.get("allocation_channels"), normal_statuses=normal_statuses
    )

    return {
        "version": version,
        "normal_statuses": normal_statuses,
        "school_statuses": school_statuses,
        "join_keys": join_keys,
        "required_student_fields": required_student_fields,
        "ranking_rules": ranking_rules,
        "trace_stages": trace_stages,
        "postal_valid_range": postal_valid_range,
        "finance_variants": finance_variants,
        "center_map": center_map,
        "center_management": center_management,
        "school_code_empty_as_zero": school_code_empty_as_zero,
        "prefer_major_code": prefer_major_code,
        "coverage_threshold": coverage_threshold,
        "dedup_removed_ratio_threshold": dedup_removed_ratio_threshold,
        "school_lookup_mismatch_threshold": school_lookup_mismatch_threshold,
        "join_key_duplicate_threshold": join_key_duplicate_threshold,
        "alias_rule": alias_rule,
        "gender_codes": gender_codes,
        "columns": columns,
        "column_aliases": column_aliases,
        "excel": excel,
        "virtual_alias_ranges": virtual_alias_ranges,
        "virtual_name_patterns": virtual_name_patterns,
        "emission": data.get("emission", {}),
        "fairness_strategy": fairness_strategy,
        "coverage_options": coverage_options,
        "mentor_pool_governance": mentor_pool_governance,
        "allocation_channels": allocation_channels,
        "mentor_pool_governance": mentor_pool_governance,
    }


def _ensure_int_sequence(name: str, value: object) -> List[int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence of ints")
    result: List[int] = []
    for item in value:
        if not isinstance(item, int):
            raise TypeError(f"All {name} items must be int")
        result.append(int(item))
    return result


def _normalize_postal_valid_range(value: object) -> tuple[int, int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 2:
        raise TypeError("postal_valid_range must be a sequence of two ints")
    start, end = value
    if not isinstance(start, int) or not isinstance(end, int):
        raise TypeError("postal_valid_range items must be int")
    if start > end:
        raise ValueError("postal_valid_range start must be <= end")
    return int(start), int(end)


def _normalize_finance_variants(value: object) -> tuple[int, ...]:
    items = _ensure_int_sequence("finance_variants", value)
    unique: list[int] = []
    seen: set[int] = set()
    for item in items:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    required = {0, 1, 3}
    if not required.issubset(seen):
        missing = sorted(required.difference(seen))
        raise ValueError(f"finance_variants missing required codes: {missing}")
    return tuple(unique)


def _normalize_ratio_value(name: str, value: object) -> float:
    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{name} must be a number") from exc
    if numeric > 1:
        numeric /= 100.0
    if not 0.0 <= numeric <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1 (inclusive)")
    return float(numeric)


def _normalize_coverage_threshold(value: object) -> float:
    return _normalize_ratio_value("coverage_threshold", value)


def _normalize_join_key_duplicate_threshold(value: object) -> int:
    try:
        threshold = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise TypeError("join_key_duplicate_threshold must be an integer") from exc
    if threshold < 0:
        raise ValueError("join_key_duplicate_threshold must be >= 0")
    return threshold


def _normalize_fairness_strategy(value: object) -> str:
    if value is None:
        return "none"
    candidate: object
    if isinstance(value, Mapping):
        candidate = value.get("strategy", "none")
    else:
        candidate = value
    text = str(candidate).strip().lower()
    if text not in _VALID_FAIRNESS_STRATEGIES:
        raise ValueError(
            "fairness_strategy must be one of " + ", ".join(_VALID_FAIRNESS_STRATEGIES)
        )
    return text


def _normalize_center_map(value: object) -> Mapping[str, int]:
    if not isinstance(value, Mapping):
        raise TypeError("center_map must be a mapping of manager name to center id")
    normalized: dict[str, int] = {}
    for key, raw in value.items():
        if not isinstance(key, str):
            raise TypeError("center_map keys must be strings")
        if not isinstance(raw, int):
            raise TypeError("center_map values must be integers")
        normalized[key.strip()] = int(raw)
    if "*" not in normalized:
        normalized["*"] = 0
    return normalized


def _normalize_center_management(
    value: object, center_map: Mapping[str, int]
) -> Mapping[str, object]:
    if value is None:
        value = {}
    if not isinstance(value, Mapping):
        raise TypeError("center_management must be a mapping of options")
    enabled = bool(value.get("enabled", True))
    strict_validation = bool(value.get("strict_manager_validation", False))
    default_invalid = value.get("default_center_for_invalid")
    if default_invalid is None or default_invalid == "":
        fallback_center = center_map.get("*")
    else:
        try:
            fallback_center = int(default_invalid)
        except (TypeError, ValueError) as exc:
            raise TypeError("default_center_for_invalid must be an integer or null") from exc

    centers_payload = value.get("centers")
    if centers_payload is None:
        centers_payload = _infer_centers_from_map(center_map)
    elif not isinstance(centers_payload, Sequence):
        raise TypeError("center_management.centers must be a list of definitions")

    centers: list[Mapping[str, object]] = []
    seen_ids: set[int] = set()
    for entry in centers_payload:
        if not isinstance(entry, Mapping):
            raise TypeError("center definition must be a mapping")
        if "id" not in entry:
            raise ValueError("center definition missing 'id'")
        try:
            center_id = int(entry["id"])
        except (TypeError, ValueError) as exc:
            raise TypeError("center id must be an integer") from exc
        if center_id in seen_ids:
            raise ValueError(f"duplicate center id detected: {center_id}")
        seen_ids.add(center_id)
        name_raw = entry.get("name")
        name = str(name_raw).strip() if name_raw is not None else f"مرکز {center_id}"
        if not name:
            name = f"مرکز {center_id}"
        managers_raw = entry.get("default_managers")
        if managers_raw is None:
            managers_raw = entry.get("default_manager")
        default_manager: str | None = None
        if isinstance(managers_raw, (list, tuple)):
            for candidate in managers_raw:
                text = str(candidate).strip()
                if text:
                    default_manager = text
                    break
        elif managers_raw is not None:
            text = str(managers_raw).strip()
            if text:
                default_manager = text
        centers.append({
            "id": center_id,
            "name": name,
            "default_manager": default_manager,
            "description": str(entry.get("description", "")),
        })

    if not centers:
        centers = [
            {
                "id": 1,
                "name": "گلستان",
                "default_manager": "شهدخت کشاورز",
                "description": "مرکز گلستان",
            },
            {
                "id": 2,
                "name": "صدرا",
                "default_manager": "آیناز هوشمند",
                "description": "مرکز صدرا",
            },
            {
                "id": 0,
                "name": "مرکزی",
                "default_manager": None,
                "description": "مرکز اصلی",
            },
        ]

    priority_payload = value.get("priority_order")
    if priority_payload is None:
        priority = [center["id"] for center in centers]
    elif not isinstance(priority_payload, Sequence):
        raise TypeError("center_management.priority_order must be a list")
    else:
        priority = []
        seen_priority: set[int] = set()
        for item in priority_payload:
            try:
                center_id = int(item)
            except (TypeError, ValueError) as exc:
                raise TypeError("center priority items must be integers") from exc
            if center_id in seen_priority:
                continue
            priority.append(center_id)
            seen_priority.add(center_id)
        for center in centers:
            if center["id"] not in seen_priority:
                priority.append(center["id"])
                seen_priority.add(center["id"])

    school_student_column = str(value.get("school_student_column", "is_school_student"))

    return {
        "enabled": enabled,
        "strict_manager_validation": strict_validation,
        "default_center_for_invalid": fallback_center,
        "priority_order": priority,
        "centers": centers,
        "school_student_column": school_student_column,
    }


def _infer_centers_from_map(center_map: Mapping[str, int]) -> list[Mapping[str, object]]:
    inferred: list[Mapping[str, object]] = []
    seen_ids: set[int] = set()
    for manager_name, center_id in center_map.items():
        if manager_name == "*":
            continue
        if center_id in seen_ids:
            continue
        seen_ids.add(center_id)
        inferred.append(
            {
                "id": int(center_id),
                "name": f"مرکز {center_id}",
                "default_manager": str(manager_name).strip(),
                "description": f"مرکز {center_id}",
            }
        )
    wildcard = center_map.get("*")
    if wildcard is not None and wildcard not in seen_ids:
        inferred.append(
            {
                "id": int(wildcard),
                "name": f"مرکز {wildcard}",
                "default_manager": None,
                "description": f"مرکز {wildcard}",
            }
        )
    return inferred


def _normalize_allocation_channels(
    payload: object, *, normal_statuses: Sequence[int]
) -> AllocationChannelConfig:
    if payload is None:
        return AllocationChannelConfig.empty()
    if not isinstance(payload, Mapping):
        raise TypeError("allocation_channels must be a mapping")

    school_codes_raw = payload.get("school_codes", [])
    school_codes = tuple(
        _ensure_int_sequence("allocation_channels.school_codes", school_codes_raw)
    )

    center_payload = payload.get("center_channels", {})
    if not isinstance(center_payload, Mapping):
        raise TypeError("allocation_channels.center_channels must be a mapping")
    center_channels: Dict[str, Tuple[int, ...]] = {}
    for name, values in center_payload.items():
        normalized_name = str(name or "").strip().upper()
        if not normalized_name:
            raise ValueError("center channel name must be a non-empty string")
        value_list = _ensure_int_sequence(
            f"allocation_channels.center_channels.{normalized_name}", values
        )
        center_channels[normalized_name] = tuple(dict.fromkeys(value_list))

    registration_column_raw = payload.get("registration_center_column")
    registration_column: str | None
    if isinstance(registration_column_raw, (str, bytes)):
        registration_column = str(registration_column_raw).strip() or None
    else:
        registration_column = None

    educational_status_column_raw = payload.get("educational_status_column")
    educational_status_column: str | None
    if isinstance(educational_status_column_raw, (str, bytes)):
        educational_status_column = str(educational_status_column_raw).strip() or None
    else:
        educational_status_column = None

    active_status_values_raw = payload.get("active_status_values")
    if active_status_values_raw is None:
        active_status_values = tuple(int(item) for item in normal_statuses)
    else:
        active_status_values = tuple(
            _ensure_int_sequence(
                "allocation_channels.active_status_values", active_status_values_raw
            )
        )

    return AllocationChannelConfig(
        school_codes=school_codes,
        center_channels=center_channels,
        registration_center_column=registration_column,
        educational_status_column=educational_status_column,
        active_status_values=active_status_values,
    )


def _ensure_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return bool(value)


def _normalize_alias_rule(value: object) -> Mapping[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError("alias_rule must be a mapping")
    required_keys = {"normal", "school"}
    missing = [key for key in required_keys if key not in value]
    if missing:
        raise ValueError(f"alias_rule missing keys: {missing}")
    normalized: dict[str, str] = {}
    for key in required_keys:
        item = value[key]
        if not isinstance(item, (str, bytes)) or not str(item).strip():
            raise ValueError(f"alias_rule['{key}'] must be a non-empty string")
        normalized[key] = str(item).strip()
    return normalized


def _normalize_columns(value: object) -> Mapping[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError("columns must be a mapping")
    required = {
        "postal_code",
        "school_count",
        "school_code",
        "capacity_current",
        "capacity_special",
        "remaining_capacity",
    }
    missing = [key for key in required if key not in value]
    if missing:
        raise ValueError(f"columns mapping missing keys: {missing}")
    normalized: dict[str, str] = {}
    for key in required:
        item = value[key]
        if not isinstance(item, (str, bytes)) or not str(item).strip():
            raise ValueError(f"columns['{key}'] must be a non-empty string")
        normalized[key] = str(item).strip()
    return normalized


def _normalize_column_aliases(value: object) -> Mapping[str, Dict[str, str]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("column_aliases must be a mapping from source to alias map")
    normalized: Dict[str, Dict[str, str]] = {}
    for source, alias_map in value.items():
        if not isinstance(source, str):
            raise TypeError("column_aliases keys must be strings")
        if not isinstance(alias_map, Mapping):
            raise TypeError("column_aliases values must be mappings")
        normalized[source] = {
            str(k): str(v)
            for k, v in alias_map.items()
            if isinstance(k, (str, bytes)) and isinstance(v, (str, bytes)) and str(v).strip()
        }
    return normalized

def _normalize_virtual_alias_ranges(raw: object) -> Tuple[Tuple[int, int], ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("virtual_alias_ranges must be a sequence of [start, end]")
    ranges: list[Tuple[int, int]] = []
    for item in raw:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)) or len(item) != 2:
            raise ValueError("Each virtual_alias_range must be a pair [start, end]")
        start_raw, end_raw = item
        try:
            start = int(start_raw)
            end = int(end_raw)
        except (ValueError, TypeError) as exc:
            raise ValueError("virtual_alias_ranges values must be integers") from exc
        if start > end:
            start, end = end, start
        ranges.append((start, end))
    if not ranges:
        raise ValueError("virtual_alias_ranges must define at least one range")
    return tuple(ranges)


def _normalize_virtual_name_patterns(raw: object) -> Tuple[str, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("virtual_name_patterns must be a sequence of strings")
    patterns: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, (str, bytes)):
            raise TypeError("virtual_name_patterns items must be strings")
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        patterns.append(text)
    if not patterns:
        raise ValueError("virtual_name_patterns must define at least one regex pattern")
    return tuple(patterns)


def _normalize_join_keys(raw: object) -> List[str]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("join_keys must be a sequence of strings")
    join_keys = [str(item).strip() for item in raw]
    if len(join_keys) != _EXPECTED_JOIN_KEYS_COUNT:
        raise ValueError(
            f"join_keys must contain exactly {_EXPECTED_JOIN_KEYS_COUNT} entries",
        )
    if any(not key for key in join_keys):
        raise ValueError("join_keys must be non-empty strings")
    if len(set(join_keys)) != len(join_keys):
        seen: set[str] = set()
        duplicates: set[str] = set()
        for key in join_keys:
            if key in seen:
                duplicates.add(key)
            else:
                seen.add(key)
        raise ValueError(
            "join_keys must be unique. Duplicate keys found: "
            f"{sorted(duplicates)}"
        )
    return join_keys


def _normalize_required_student_fields(
    raw: object | None, join_keys: Sequence[str]
) -> List[str]:
    """نرمال‌سازی فهرست ستون‌های ضروری دانش‌آموز از Policy."""

    if raw is None:
        candidates: Sequence[object] = join_keys
    else:
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise TypeError("required_student_fields must be a sequence of strings")
        candidates = raw

    normalized: list[str] = []
    seen: set[str] = set()

    def _push(value: object) -> None:
        if not isinstance(value, (str, bytes)):
            raise TypeError("required_student_fields items must be strings")
        text = str(value).strip()
        if not text:
            return
        if text not in seen:
            normalized.append(text)
            seen.add(text)

    for item in candidates:
        _push(item)

    for fallback in join_keys:
        _push(fallback)

    if not normalized:
        raise ValueError("required_student_fields must define at least one column")

    return normalized


def _normalize_ranking_rules(raw: object) -> List[Mapping[str, object]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("ranking must be a sequence of rules")
    ranking_items = list(raw)
    if not ranking_items:
        raise ValueError("ranking must contain at least one rule")
    if len(ranking_items) != _EXPECTED_RANKING_ITEMS_COUNT:
        raise ValueError(
            f"ranking must contain exactly {_EXPECTED_RANKING_ITEMS_COUNT} items",
        )
    normalized: List[Mapping[str, object]] = []
    ranking_names: List[str] = []
    for item in ranking_items:
        if isinstance(item, Mapping):
            if "name" not in item or "column" not in item:
                raise ValueError("Ranking rule must define 'name' and 'column'")
            name = str(item["name"])
            column = str(item["column"])
            ascending_value = item.get("ascending", True)
            if not isinstance(ascending_value, bool):
                raise TypeError("ranking rule 'ascending' must be boolean")
            ascending = bool(ascending_value)
        else:
            name = str(item)
            if name not in _RANKING_RULE_LIBRARY:
                raise ValueError(f"Unknown ranking rule '{name}'")
            column, ascending = _RANKING_RULE_LIBRARY[name]
        ranking_names.append(name)
        normalized.append({"name": name, "column": column, "ascending": ascending})
    if len(set(ranking_names)) != len(ranking_names):
        raise ValueError("ranking items must be unique")
    return normalized


def _normalize_trace_stages(raw: object | None, join_keys: Sequence[str]) -> List[Mapping[str, str]]:
    if raw is None:
        stages = [
            {"stage": stage, "column": _LEGACY_TRACE_DEFAULTS[stage]}
            for stage in _TRACE_STAGE_ORDER
        ]
    else:
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise TypeError("trace_stages must be a sequence of mappings")
        if len(raw) != len(_TRACE_STAGE_ORDER):
            raise ValueError(
                "trace_stages must define exactly eight stages including capacity_gate",
            )
        stages = []
        seen_stage_names: set[str] = set()
        for expected, item in zip(_TRACE_STAGE_ORDER, raw, strict=True):
            if not isinstance(item, Mapping):
                raise TypeError("Each trace stage must be a mapping")
            stage_name = str(item.get("stage", "")).strip()
            column_value = item.get("column")
            if not stage_name:
                raise ValueError("Trace stage missing 'stage' value")
            if stage_name in seen_stage_names:
                raise ValueError("Trace stage names must be unique")
            if stage_name != expected:
                raise ValueError(
                    f"Trace stage order mismatch: expected '{expected}' got '{stage_name}'",
                )
            if not isinstance(column_value, (str, bytes)) or not str(column_value).strip():
                raise ValueError("Trace stage 'column' must be a non-empty string")
            seen_stage_names.add(stage_name)
            stages.append({"stage": stage_name, "column": str(column_value)})

    stage_columns = {stage["column"] for stage in stages}
    missing_from_trace = [key for key in join_keys if key not in stage_columns]
    if missing_from_trace:
        raise ValueError(
            "All join_keys must appear in trace_stages columns: "
            + ", ".join(missing_from_trace),
        )
    return stages


def _parse_semver(value: str) -> tuple[int, int, int]:
    try:
        major, minor, patch = value.split(".")
        return int(major), int(minor), int(patch)
    except (ValueError, TypeError) as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid semantic version: '{value}'") from exc


def _version_gate(
    loaded_version: str,
    expected_version: Optional[str],
    on_version_mismatch: VersionMismatchMode,
) -> None:
    if expected_version is None:
        return
    if loaded_version == expected_version:
        return

    loaded_semver = _parse_semver(loaded_version)
    expected_semver = _parse_semver(expected_version)
    message = (
        f"Policy version mismatch: loaded='{loaded_version}' "
        f"expected='{expected_version}'"
    )

    if loaded_semver[0] != expected_semver[0]:
        raise ValueError(message + " (major incompatible)")

    if on_version_mismatch == "raise":
        raise ValueError(message)
    if on_version_mismatch == "warn":
        warnings.warn(message, RuntimeWarning, stacklevel=3)


def _to_config(data: Mapping[str, object]) -> PolicyConfig:
    allocation_channels_obj = cast(AllocationChannelConfig, data["allocation_channels"])

    return PolicyConfig(
        version=str(data["version"]),
        normal_statuses=[int(item) for item in data["normal_statuses"]],  # type: ignore[index]
        school_statuses=[int(item) for item in data["school_statuses"]],  # type: ignore[index]
        join_keys=[str(item) for item in data["join_keys"]],  # type: ignore[index]
        required_student_fields=[
            str(item) for item in data["required_student_fields"]
        ],
        ranking_rules=[_to_ranking_rule(item) for item in data["ranking_rules"]],
        trace_stages=[_to_trace_stage(item) for item in data["trace_stages"]],
        gender_codes=_to_gender_codes(data["gender_codes"]),
        postal_valid_range=tuple(int(item) for item in data["postal_valid_range"]),
        finance_variants=tuple(int(item) for item in data["finance_variants"]),
        center_map={str(k): int(v) for k, v in data["center_map"].items()},
        school_code_empty_as_zero=bool(data["school_code_empty_as_zero"]),
        prefer_major_code=bool(data["prefer_major_code"]),
        coverage_threshold=float(data["coverage_threshold"]),
        dedup_removed_ratio_threshold=float(
            data["dedup_removed_ratio_threshold"]
        ),
        school_lookup_mismatch_threshold=float(
            data["school_lookup_mismatch_threshold"]
        ),
        join_key_duplicate_threshold=int(data["join_key_duplicate_threshold"]),
        alias_rule=PolicyAliasRule(
            normal=str(data["alias_rule"]["normal"]),
            school=str(data["alias_rule"]["school"]),
        ),
        columns=PolicyColumns(
            postal_code=str(data["columns"]["postal_code"]),
            school_count=str(data["columns"]["school_count"]),
            school_code=str(data["columns"]["school_code"]),
            capacity_current=str(data["columns"]["capacity_current"]),
            capacity_special=str(data["columns"]["capacity_special"]),
            remaining_capacity=str(data["columns"]["remaining_capacity"]),
        ),
        column_aliases={str(source): {str(k): str(v) for k, v in aliases.items()}
                        for source, aliases in data["column_aliases"].items()},
        excel=ExcelOptions(**_normalize_excel_options(data["excel"])),
        virtual_alias_ranges=tuple(
            (int(start), int(end)) for start, end in data["virtual_alias_ranges"]
        ),
        virtual_name_patterns=tuple(str(item) for item in data["virtual_name_patterns"]),
        emission=_to_emission_options(data.get("emission", {})),
        fairness_strategy=str(data.get("fairness_strategy", "none")),
        center_management=_to_center_management_config(data["center_management"]),
        coverage_options=MatrixCoverageOptions(
            denominator_mode=str(data["coverage_options"]["denominator_mode"]),
            require_student_presence=bool(
                data["coverage_options"]["require_student_presence"]
            ),
            include_blocked_candidates_in_denominator=bool(
                data["coverage_options"]["include_blocked_candidates_in_denominator"]
            ),
        ),
        mentor_pool_governance=_to_mentor_pool_governance(
            data.get("mentor_pool_governance", {})
        ),
        mentor_school_binding=_to_mentor_school_binding(
            data.get("mentor_school_binding", {})
        ),
        allocation_channels=allocation_channels_obj,
    )


def _to_ranking_rule(item: Mapping[str, object]) -> RankingRule:
    return RankingRule(
        name=str(item["name"]),
        column=str(item["column"]),
        ascending=bool(item.get("ascending", True)),
    )


def _to_trace_stage(item: Mapping[str, object]) -> TraceStageDefinition:
    return TraceStageDefinition(stage=str(item["stage"]), column=str(item["column"]))


def _to_center_management_config(data: Mapping[str, object]) -> CenterManagementConfig:
    centers_payload = data.get("centers") or []
    centers: list[CenterConfig] = []
    for entry in centers_payload:
        centers.append(
            CenterConfig(
                id=int(entry["id"]),
                name=str(entry["name"]),
                default_manager=(
                    None
                    if entry.get("default_manager") in (None, "")
                    else str(entry.get("default_manager"))
                ),
                description=str(entry.get("description", "")),
            )
        )
    priority_order = tuple(int(item) for item in data.get("priority_order", []))
    default_invalid = data.get("default_center_for_invalid")
    fallback = None if default_invalid is None else int(default_invalid)
    return CenterManagementConfig(
        enabled=bool(data.get("enabled", True)),
        centers=tuple(centers),
        priority_order=priority_order,
        strict_manager_validation=bool(data.get("strict_manager_validation", False)),
        default_center_for_invalid=fallback,
        school_student_column=str(data.get("school_student_column", "is_school_student")),
    )


def _to_mentor_school_binding(data: Mapping[str, object]) -> MentorSchoolBindingPolicy:
    tokens = data.get("empty_tokens", data.get("empty_placeholders", ("", "0", "-", "—", "_", "nan", "NaN")))
    return MentorSchoolBindingPolicy(
        global_mode=str(data.get("global_mode", "global")),
        restricted_mode=str(data.get("restricted_mode", "restricted")),
        empty_tokens=tuple(str(item) for item in tokens),
    )


def _to_mentor_pool_governance(
    data: Mapping[str, object]
) -> MentorPoolGovernanceConfig:
    allowed_raw = data.get("allowed_statuses") or (
        MentorStatus.ACTIVE.value,
        MentorStatus.INACTIVE.value,
    )
    allowed = tuple(MentorStatus.from_value(item) for item in allowed_raw)
    default_status = MentorStatus.from_value(
        data.get("default_status", MentorStatus.ACTIVE.value)
    )
    if default_status not in allowed:
        raise ValueError("default_status must be included in allowed_statuses")

    status_map_raw = data.get("mentor_status_map") or {}
    if not isinstance(status_map_raw, Mapping):
        raise TypeError("mentor_status_map must be a mapping of mentor_id to status")
    mentor_status_map: dict[int, MentorStatus] = {}
    for key, raw_status in status_map_raw.items():
        try:
            mentor_id = int(key)
        except (TypeError, ValueError) as exc:
            raise ValueError("mentor_status_map keys must be convertible to int") from exc
        status = MentorStatus.from_value(raw_status)
        if status not in allowed:
            raise ValueError("mentor_status_map contains status outside allowed_statuses")
        mentor_status_map[mentor_id] = status

    return MentorPoolGovernanceConfig(
        default_status=default_status,
        mentor_status_map=mentor_status_map,
        allowed_statuses=allowed,
    )


def _to_gender_codes(payload: Mapping[str, Mapping[str, object]]) -> GenderCodes:
    """تبدیل ساختار gender_codes در policy به GenderCodes."""

    try:
        male_payload = payload["male"]
        female_payload = payload["female"]
    except KeyError as exc:  # pragma: no cover - نگهبان مهاجرت
        raise ValueError("Policy missing gender_codes.male/female definitions") from exc

    def _parse(entry: Mapping[str, object]) -> GenderCode:
        if "value" not in entry or "counter_code" not in entry:
            raise ValueError("Gender code entry must include 'value' and 'counter_code'")
        value = int(entry["value"])
        counter_code = str(entry["counter_code"]).strip()
        if not counter_code:
            raise ValueError("Gender counter_code must be non-empty")
        if not re.fullmatch(r"\d{3}", counter_code):
            raise ValueError("Gender counter_code must be exactly three digits")
        return GenderCode(value=value, counter_code=counter_code)

    male_code = _parse(male_payload)
    female_code = _parse(female_payload)
    return GenderCodes(male=male_code, female=female_code)


def parse_policy_dict(
    data: Mapping[str, object],
    expected_version: Optional[str] = DEFAULT_POLICY_VERSION,
    on_version_mismatch: VersionMismatchMode = "raise",
) -> PolicyConfig:
    """مسیر خالص برای تبدیل dict به :class:`PolicyConfig`."""

    prepared = _prepare_policy_payload(data, expected_version, on_version_mismatch)
    normalized = _normalize_policy_payload(prepared)
    config = _to_config(normalized)
    _version_gate(config.version, expected_version, on_version_mismatch)
    return config


def _apply_schema_defaults(data: Dict[str, object]) -> Dict[str, object]:
    """تزریق کلیدهای ضروری در صورت فقدان برای مهاجرت نسخه."""

    data.setdefault("virtual_alias_ranges", list(_DEFAULT_VIRTUAL_ALIAS_RANGES))
    data.setdefault("virtual_name_patterns", list(_DEFAULT_VIRTUAL_NAME_PATTERNS))

    excel_options = data.get("excel") or {}
    if not isinstance(excel_options, Mapping):
        excel_options = dict(_DEFAULT_EXCEL_OPTIONS)
    else:
        excel_options = dict(_DEFAULT_EXCEL_OPTIONS) | {str(k): v for k, v in excel_options.items()}
    data["excel"] = excel_options

    if "column_aliases" not in data or not isinstance(data["column_aliases"], Mapping):
        data["column_aliases"] = {}

    emission = data.get("emission") or {}
    if not isinstance(emission, Mapping):
        emission = {}
    selection_reasons = emission.get("selection_reasons") or {}
    selection_payload = dict(_DEFAULT_SELECTION_REASON_OPTIONS)
    if isinstance(selection_reasons, Mapping):
        for key, value in selection_reasons.items():
            if key == "trace_stage_labels":
                selection_payload["trace_stage_labels"] = _normalize_reason_trace_labels(value)
            elif key == "labels":
                selection_payload["labels"] = _normalize_reason_labels(value)
            else:
                selection_payload[str(key)] = value
    if not isinstance(selection_payload.get("trace_stage_labels"), tuple):
        selection_payload["trace_stage_labels"] = _normalize_reason_trace_labels(
            selection_payload.get("trace_stage_labels")
        )
    if not isinstance(selection_payload.get("labels"), Mapping):
        selection_payload["labels"] = _normalize_reason_labels(
            selection_payload.get("labels")
        )
    data["emission"] = {"selection_reasons": selection_payload}

    matrix_section = data.get("matrix", {})
    if not isinstance(matrix_section, Mapping):
        matrix_section = {}

    coverage_section = matrix_section.get("coverage", {})
    if not isinstance(coverage_section, Mapping):
        coverage_section = {}

    if "mentor_pool_governance" not in data or not isinstance(
        data.get("mentor_pool_governance"), Mapping
    ):
        data["mentor_pool_governance"] = {}

    if "allocation_channels" not in data or not isinstance(
        data.get("allocation_channels"), Mapping
    ):
        data["allocation_channels"] = {}

    coverage_defaults = {
        "denominator_mode": "mentors",
        "require_student_presence": False,
        "include_blocked_candidates_in_denominator": False,
    }
    normalized_coverage = {str(k): v for k, v in coverage_section.items()}
    merged_coverage = {**coverage_defaults, **normalized_coverage}
    data["matrix"] = {**matrix_section, "coverage": merged_coverage}

    return data


def _normalize_excel_options(payload: Mapping[str, object]) -> Dict[str, object]:
    """اعتبارسنجی و نرمال‌سازی گزینه‌های Excel."""

    rtl = bool(payload.get("rtl", _DEFAULT_EXCEL_OPTIONS["rtl"]))
    font_name = str(payload.get("font_name", _DEFAULT_EXCEL_OPTIONS["font_name"]))
    font_size_raw = payload.get("font_size", _DEFAULT_EXCEL_OPTIONS["font_size"])
    try:
        font_size = int(font_size_raw)
    except (ValueError, TypeError) as exc:  # pragma: no cover - defensive branch
        raise TypeError("excel.font_size must be an integer") from exc
    if font_size <= 0:
        raise ValueError("excel.font_size must be a positive integer")
    internal = str(payload.get("header_mode_internal", _DEFAULT_EXCEL_OPTIONS["header_mode_internal"]))
    write = str(payload.get("header_mode_write", _DEFAULT_EXCEL_OPTIONS["header_mode_write"]))
    return {
        "rtl": rtl,
        "font_name": font_name,
        "font_size": font_size,
        "header_mode_internal": internal,
        "header_mode_write": write,
    }


def _normalize_coverage_options(payload: Mapping[str, object]) -> Dict[str, object]:
    """اعتبارسنجی تنظیمات پوشش ماتریس."""

    if not isinstance(payload, Mapping):
        raise TypeError("matrix.coverage must be a mapping")
    denominator_mode = str(payload.get("denominator_mode", "mentors"))
    allowed_modes = {
        "mentors",
        "mentors_students_intersection",
        "mentors_students_union",
    }
    if denominator_mode not in allowed_modes:
        raise ValueError(
            "matrix.coverage.denominator_mode must be one of "
            "mentors, mentors_students_intersection, mentors_students_union"
        )
    require_student_presence = bool(payload.get("require_student_presence", False))
    include_blocked = bool(
        payload.get("include_blocked_candidates_in_denominator", False)
    )
    return {
        "denominator_mode": denominator_mode,
        "require_student_presence": require_student_presence,
        "include_blocked_candidates_in_denominator": include_blocked,
    }


def _parse_status_value(value: object) -> str:
    status = MentorStatus.from_value(value)
    return status.value


def _normalize_mentor_pool_governance(raw: object | None) -> Mapping[str, object]:
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise TypeError("mentor_pool_governance must be a mapping of options")

    allowed_raw = raw.get("allowed_statuses") or (
        MentorStatus.ACTIVE.value,
        MentorStatus.INACTIVE.value,
    )
    if not isinstance(allowed_raw, Sequence) or isinstance(allowed_raw, (str, bytes)):
        raise TypeError("allowed_statuses must be a sequence of strings")
    allowed_statuses: list[str] = []
    seen: set[str] = set()
    for item in allowed_raw:
        status_value = _parse_status_value(item)
        if status_value not in seen:
            allowed_statuses.append(status_value)
            seen.add(status_value)

    default_status = _parse_status_value(
        raw.get("default_status", MentorStatus.ACTIVE.value)
    )
    if default_status not in seen:
        raise ValueError("default_status must be part of allowed_statuses")

    mentors_raw = raw.get("mentors") or []
    if not isinstance(mentors_raw, Sequence) or isinstance(mentors_raw, (str, bytes)):
        raise TypeError("mentor_pool_governance.mentors must be a sequence of objects")
    mentor_status_map: dict[int, str] = {}
    for entry in mentors_raw:
        if not isinstance(entry, Mapping):
            raise TypeError("each mentor entry must be a mapping")
        if "mentor_id" not in entry or "status" not in entry:
            raise ValueError("mentor entry must include mentor_id and status")
        try:
            mentor_id = int(entry["mentor_id"])
        except (TypeError, ValueError) as exc:
            raise ValueError("mentor_id must be convertible to integer") from exc
        status_value = _parse_status_value(entry["status"])
        if status_value not in seen:
            raise ValueError("mentor status must be listed in allowed_statuses")
        mentor_status_map[mentor_id] = status_value

    return {
        "default_status": default_status,
        "allowed_statuses": tuple(allowed_statuses),
        "mentor_status_map": mentor_status_map,
    }


def _normalize_reason_trace_labels(value: object) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        ordered = [
            value.get("gender"),
            value.get("school"),
            value.get("track"),
            value.get("ranking"),
        ]
        return _finalize_reason_trace_labels(ordered)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return _finalize_reason_trace_labels(list(value))
    return _DEFAULT_REASON_TRACE_LABELS


def _finalize_reason_trace_labels(values: Sequence[object]) -> tuple[str, ...]:
    normalized: list[str] = []
    for idx in range(len(_DEFAULT_REASON_TRACE_LABELS)):
        if idx < len(values):
            item = str(values[idx] or "").strip()
            normalized.append(item or _DEFAULT_REASON_TRACE_LABELS[idx])
        else:
            normalized.append(_DEFAULT_REASON_TRACE_LABELS[idx])
    return tuple(normalized[: len(_DEFAULT_REASON_TRACE_LABELS)])


def _normalize_reason_labels(value: object) -> Mapping[str, tuple[str, ...]]:
    default_reason = _DEFAULT_SELECTION_REASON_OPTIONS["labels"].get("reason", {})
    resolved: Dict[str, tuple[str, ...]] = {}
    source: Mapping[str, object] | None = None
    if isinstance(value, Mapping):
        candidate = value.get("reason") if "reason" in value else value
        if isinstance(candidate, Mapping):
            source = candidate
    if source is None:
        source = {}

    for key, fallback in default_reason.items():
        resolved[key] = _normalize_label_tuple(source.get(key), fallback)
    return resolved


def _normalize_reason_columns(value: object) -> tuple[str, ...]:
    candidates: list[str] = []
    if isinstance(value, (list, tuple)):
        candidates = [str(item).strip() for item in value if str(item or "").strip()]
    elif isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            candidates = [cleaned]

    if not candidates:
        defaults = _DEFAULT_SELECTION_REASON_OPTIONS.get("columns", ())
        candidates = [str(item) for item in defaults]

    return validate_policy_columns(candidates)


def _normalize_label_tuple(value: object, fallback: object) -> tuple[str, ...]:
    options: list[str] = []
    if isinstance(value, (list, tuple)):
        options = [str(item).strip() for item in value if str(item or "").strip()]
    elif isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            options = [cleaned]
    if not options:
        if isinstance(fallback, (list, tuple)):
            options = [str(item).strip() for item in fallback if str(item or "").strip()]
        elif isinstance(fallback, str):
            cleaned = fallback.strip()
            if cleaned:
                options = [cleaned]
    return tuple(options) if options else ("",)


def _to_selection_reason_options(data: Mapping[str, object]) -> SelectionReasonOptions:
    enabled = bool(data.get("enabled", True))
    sheet_name = str(data.get("sheet_name", _DEFAULT_SELECTION_REASON_OPTIONS["sheet_name"]))
    template = str(data.get("template", _DEFAULT_SELECTION_REASON_OPTIONS["template"]))
    labels = _normalize_reason_trace_labels(data.get("trace_stage_labels"))
    locale = str(data.get("locale", _DEFAULT_SELECTION_REASON_OPTIONS["locale"]))
    reason_labels = _normalize_reason_labels(data.get("labels"))
    columns = _normalize_reason_columns(data.get("columns"))
    schema_hash = compute_schema_hash(columns)
    return SelectionReasonOptions(
        enabled=enabled,
        sheet_name=sheet_name,
        template=template,
        trace_stage_labels=labels,
        locale=locale,
        labels=reason_labels,
        columns=columns,
        schema_hash=schema_hash,
    )


def _to_emission_options(data: Mapping[str, object]) -> EmissionOptions:
    if not isinstance(data, Mapping):
        data = {}
    selection = data.get("selection_reasons", {})
    if not isinstance(selection, Mapping):
        selection = {}
    return EmissionOptions(selection_reasons=_to_selection_reason_options(selection))


def _prepare_policy_payload(
    data: Mapping[str, object],
    expected_version: Optional[str],
    mode: VersionMismatchMode,
) -> Mapping[str, object]:
    """آماده‌سازی اولیهٔ دادهٔ Policy با لحاظ مهاجرت و هشدار نسخه."""

    payload = _apply_schema_defaults(dict(data))
    if expected_version is None:
        return payload

    version = str(payload.get("version", ""))
    if not version:
        raise ValueError("Policy payload missing 'version'")

    if version == expected_version:
        return payload

    loaded_semver = _parse_semver(version)
    expected_semver = _parse_semver(expected_version)
    message = (
        f"Policy version mismatch: loaded='{version}' expected='{expected_version}'"
    )

    if loaded_semver[0] != expected_semver[0]:
        raise ValueError(message + " (major incompatible)")

    if mode == "raise":
        raise ValueError(message)

    if mode == "warn":
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        return payload

    if mode != "migrate":
        raise ValueError(f"Unsupported version mismatch mode: {mode}")

    warnings.warn(
        "Policy schema migrated in-memory to match expected version. Persist the updated policy.json to avoid runtime migrations.",
        RuntimeWarning,
        stacklevel=3,
    )

    migrated = dict(payload)
    migrated["version"] = expected_version

    if "trace_stages" not in migrated or not migrated["trace_stages"]:
        migrated["trace_stages"] = [
            {"stage": stage, "column": column} for stage, column in _LEGACY_TRACE_DEFAULTS.items()
        ]

    if "ranking_rules" not in migrated:
        ranking_names = migrated.get("ranking") or [rule for rule in _RANKING_RULE_LIBRARY]
        ranking_rules = []
        for name in ranking_names:
            if name not in _RANKING_RULE_LIBRARY:
                raise ValueError(f"Unknown ranking rule '{name}' in legacy policy")
            column, ascending = _RANKING_RULE_LIBRARY[name]
            ranking_rules.append({"name": name, "column": column, "ascending": ascending})
        migrated["ranking_rules"] = ranking_rules

    return migrated


@lru_cache(maxsize=8)
def _load_policy_cached(
    resolved_path: str,
    raw: str,
    mtime_ns: int,
    expected_version: Optional[str],
    on_version_mismatch: VersionMismatchMode,
) -> PolicyConfig:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in policy file: {resolved_path}") from exc
    prepared = _prepare_policy_payload(data, expected_version, on_version_mismatch)
    normalized = _normalize_policy_payload(prepared)
    config = _to_config(normalized)
    _version_gate(config.version, expected_version, on_version_mismatch)
    return config


def load_policy(
    path: str | Path = "config/policy.json",
    *,
    expected_version: Optional[str] = DEFAULT_POLICY_VERSION,
    on_version_mismatch: VersionMismatchMode = "raise",
) -> PolicyConfig:
    """بارگذاری سیاست از فایل JSON و بازگشت ساختار کش‌شونده."""

    policy_path = Path(path)
    try:
        raw = policy_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:  # pragma: no cover - پیام واضح برای مصرف‌کننده
        raise FileNotFoundError(f"Policy file not found: {policy_path}") from exc

    try:
        mtime_ns = policy_path.stat().st_mtime_ns
    except FileNotFoundError as exc:  # pragma: no cover - race condition guard
        raise FileNotFoundError(f"Policy file not found: {policy_path}") from exc

    resolved = str(policy_path.resolve())
    return _load_policy_cached(
        resolved,
        raw,
        mtime_ns,
        expected_version,
        on_version_mismatch,
    )


load_policy.cache_clear = _load_policy_cached.cache_clear  # type: ignore[attr-defined]
load_policy.cache_info = _load_policy_cached.cache_info  # type: ignore[attr-defined]


def get_policy() -> PolicyConfig:
    """دسترسی ساده برای دریافت Policy کش‌شده."""

    return load_policy()
