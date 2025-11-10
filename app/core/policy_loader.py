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
from typing import List, Mapping, Optional, Sequence
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
class PolicyConfig:
    """ساختار دادهٔ فقط‌خواندنی برای نگهداری سیاست بارگذاری‌شده."""

    version: str
    normal_statuses: List[int]
    school_statuses: List[int]
    join_keys: List[str]
    ranking_rules: List["RankingRule"]
    trace_stages: List["TraceStageDefinition"]

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
    required = ["version", "normal_statuses", "school_statuses", "join_keys"]
    missing = [key for key in required if key not in data]
    if "ranking_rules" not in data and "ranking" not in data:
        missing.append("ranking")
    if missing:
        raise ValueError(f"Policy keys missing: {missing}")

    version = str(data["version"])
    normal_statuses = _ensure_int_sequence("normal_statuses", data["normal_statuses"])
    school_statuses = _ensure_int_sequence("school_statuses", data["school_statuses"])
    join_keys = _normalize_join_keys(data["join_keys"])
    ranking_rules = _normalize_ranking_rules(data["ranking_rules"] if "ranking_rules" in data else data["ranking"])
    trace_stages = _normalize_trace_stages(data.get("trace_stages"), join_keys)

    return {
        "version": version,
        "normal_statuses": normal_statuses,
        "school_statuses": school_statuses,
        "join_keys": join_keys,
        "ranking_rules": ranking_rules,
        "trace_stages": trace_stages,
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
        duplicates: List[str] = []
        for key in join_keys:
            if key in seen and key not in duplicates:
                duplicates.append(key)
            else:
                seen.add(key)
        raise ValueError(
            "join_keys must be unique. Duplicate keys found: "
            f"{duplicates}"
        )
    return join_keys


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
        ranking_rules=[_to_ranking_rule(item) for item in data["ranking_rules"]],
        trace_stages=[_to_trace_stage(item) for item in data["trace_stages"]],
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
