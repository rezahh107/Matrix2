"""Policy Loader (Core) — سبک، کش‌شونده و متکی بر SSoT.

نکتهٔ معماری: اگر قرار باشد I/O از Core خارج شود، کافی است دادهٔ JSON خوانده‌شده
در Infra به تابع :func:`parse_policy_dict` پاس داده شود. این ماژول هر دو مسیر
را فراهم می‌کند.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence
from typing import Literal

VersionMismatchMode = Literal["raise", "warn", "ignore"]

DEFAULT_POLICY_VERSION = "1.0.3"
_EXPECTED_JOIN_KEYS_COUNT = 6
_EXPECTED_RANKING_ITEMS_COUNT = 3

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
    "min_allocations_new": ("allocations_new", True),
    "min_mentor_id": ("mentor_sort_key", True),
}


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
class ExcelOptions:
    """تنظیمات خروجی Excel (جهت، فونت و حالت هدر)."""

    rtl: bool
    font_name: str
    header_mode: str


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
    postal_valid_range: tuple[int, int]
    finance_variants: tuple[int, ...]
    center_map: Mapping[str, int]
    school_code_empty_as_zero: bool
    prefer_major_code: bool
    alias_rule: PolicyAliasRule
    columns: PolicyColumns
    column_aliases: Mapping[str, Dict[str, str]]
    excel: ExcelOptions

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
    required = [
        "version",
        "normal_statuses",
        "school_statuses",
        "join_keys",
        "postal_valid_range",
        "finance_variants",
        "center_map",
        "school_code_empty_as_zero",
        "prefer_major_code",
        "alias_rule",
        "columns",
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
    school_code_empty_as_zero = _ensure_bool("school_code_empty_as_zero", data["school_code_empty_as_zero"])
    prefer_major_code = _ensure_bool("prefer_major_code", data["prefer_major_code"])
    alias_rule = _normalize_alias_rule(data["alias_rule"])
    columns = _normalize_columns(data["columns"])
    column_aliases = _normalize_column_aliases(data.get("column_aliases", {}))
    excel = _normalize_excel_options(data.get("excel"))

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
        "school_code_empty_as_zero": school_code_empty_as_zero,
        "prefer_major_code": prefer_major_code,
        "alias_rule": alias_rule,
        "columns": columns,
        "column_aliases": column_aliases,
        "excel": excel,
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


def _normalize_excel_options(value: object | None) -> ExcelOptions:
    if value is None:
        return ExcelOptions(rtl=True, font_name="Vazirmatn", header_mode="fa")
    if not isinstance(value, Mapping):
        raise TypeError("excel must be a mapping")
    rtl = _ensure_bool("excel.rtl", value.get("rtl", True))
    font_raw = value.get("font_name", "Vazirmatn")
    font_name = str(font_raw or "Vazirmatn")
    header_mode = str(value.get("header_mode", "fa"))
    if header_mode not in {"fa", "en", "fa_en"}:
        raise ValueError("excel.header_mode must be one of {'fa','en','fa_en'}")
    return ExcelOptions(rtl=rtl, font_name=font_name, header_mode=header_mode)


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
    except Exception as exc:  # pragma: no cover - defensive guard
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
        postal_valid_range=tuple(int(item) for item in data["postal_valid_range"]),
        finance_variants=tuple(int(item) for item in data["finance_variants"]),
        center_map={str(k): int(v) for k, v in data["center_map"].items()},
        school_code_empty_as_zero=bool(data["school_code_empty_as_zero"]),
        prefer_major_code=bool(data["prefer_major_code"]),
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
        excel=data["excel"],
    )


def _to_ranking_rule(item: Mapping[str, object]) -> RankingRule:
    return RankingRule(
        name=str(item["name"]),
        column=str(item["column"]),
        ascending=bool(item.get("ascending", True)),
    )


def _to_trace_stage(item: Mapping[str, object]) -> TraceStageDefinition:
    return TraceStageDefinition(stage=str(item["stage"]), column=str(item["column"]))


def parse_policy_dict(
    data: Mapping[str, object],
    expected_version: Optional[str] = DEFAULT_POLICY_VERSION,
    on_version_mismatch: VersionMismatchMode = "raise",
) -> PolicyConfig:
    """مسیر خالص برای تبدیل dict به :class:`PolicyConfig`."""

    normalized = _normalize_policy_payload(data)
    config = _to_config(normalized)
    _version_gate(config.version, expected_version, on_version_mismatch)
    return config


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
    normalized = _normalize_policy_payload(data)
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
