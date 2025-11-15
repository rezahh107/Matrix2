"""ساخت زنجیرهٔ تریس ۸ مرحله‌ای برای تصمیمات تخصیص (Core-only).

این ماژول هیچ I/O انجام نمی‌دهد و تنها شمارش کاندیدها پس از هر فیلتر را
بر اساس سیاست رسمی انجام می‌دهد.

مثال::

    >>> import pandas as pd
    >>> from app.core.common.trace import build_allocation_trace
    >>> student = {"کدرشته": 101, "جنسیت": 1, "دانش_آموز_فارغ": 0}
    >>> pool = pd.DataFrame({
    ...     "کدرشته": [101],
    ...     "گروه آزمایشی": ["تجربی"],
    ...     "جنسیت": [1],
    ...     "دانش آموز فارغ": [0],
    ...     "مرکز گلستان صدرا": [1],
    ...     "مالی حکمت بنیاد": [0],
    ...     "کد مدرسه": [500],
    ...     "remaining_capacity": [2],
    ... })
    >>> build_allocation_trace(student, pool)[-1]["total_after"]
    1
"""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Number
from typing import Any, Iterable, List, Mapping, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
from .filters import filter_school_by_value, resolve_student_school_code
from .columns import normalize_bool_like, to_int64
from .rules import Rule, RuleContext, apply_rule, default_stage_rule_map
from .types import StudentRow, TraceStageLiteral, TraceStageRecord

__all__ = [
    "TraceStagePlan",
    "build_trace_plan",
    "build_stage_rule_map",
    "build_allocation_trace",
]


_CANONICAL_TRACE_ORDER: tuple[TraceStageLiteral, ...] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
    "capacity_gate",
)


@dataclass(frozen=True)
class TraceStagePlan:
    """برنامهٔ فیلتر یک مرحله از تریس."""

    stage: TraceStageLiteral
    column: str


def _normalize_student_key(column: str) -> str:
    return column.replace(" ", "_")


def _student_value(student: Mapping[str, object], column: str) -> object:
    normalized = _normalize_student_key(column)
    if column in student:
        return student[column]
    if normalized in student:
        return student[normalized]
    raise KeyError(f"Student row missing value for '{column}'")


def build_trace_plan(
    policy: PolicyConfig,
    *,
    capacity_column: str = "remaining_capacity",
) -> List[TraceStagePlan]:
    """ساخت برنامهٔ پیش‌فرض مراحل تریس از روی Policy."""

    if policy.trace_stage_names != _CANONICAL_TRACE_ORDER:
        raise ValueError(
            "Policy trace stages must match the canonical 8-stage order",
        )

    plan: List[TraceStagePlan] = []
    for definition in policy.trace_stages:
        column = (
            capacity_column
            if definition.stage == "capacity_gate"
            else definition.column
        )
        plan.append(TraceStagePlan(stage=definition.stage, column=column))
    return plan


def _ensure_columns(pool: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [col for col in columns if col not in pool.columns]
    if missing:
        raise KeyError(f"Missing columns in candidate pool: {missing}")


def build_stage_rule_map(_: PolicyConfig | None = None) -> Mapping[TraceStageLiteral, Rule]:
    """برگرداندن نگاشت مرحله→Rule پیش‌فرض."""

    return default_stage_rule_map()


def _apply_stage_rule(
    record: TraceStageRecord,
    stage_rules: Mapping[TraceStageLiteral, Rule],
    student: Mapping[str, object],
) -> None:
    rule = stage_rules.get(record["stage"])
    if rule is None:
        return
    context = RuleContext(stage_record=record, student=student)
    result = apply_rule(rule, context)
    extras = dict(record.get("extras") or {})
    extras["rule_reason_code"] = result.reason.code
    extras["rule_reason_text"] = result.reason.message_fa
    extras["rule_passed"] = result.passed
    if result.details:
        extras["rule_details"] = dict(result.details)
    record["extras"] = extras


def _filter_stage(frame: pd.DataFrame, column: str, value: object) -> pd.DataFrame:
    mask = frame[column] == value
    return frame.loc[mask]


def _candidate_join_value(frame: pd.DataFrame, column: str) -> object | None:
    """استخراج اولین مقدار معتبر ستون از استخر فعلی کاندیداها."""

    if column not in frame.columns or frame.empty:
        return None
    try:
        series = frame[column]
    except KeyError:  # pragma: no cover - محافظ همگام‌سازی ستون
        return None
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    value = cleaned.iloc[0]
    try:
        if value is pd.NA:  # type: ignore[comparison-overlap]
            return None
    except Exception:  # pragma: no cover - برای انواع غیرپشتیبان
        pass
    return value


def _string_or_none(value: object) -> str | None:
    if value is None or value is pd.NA:
        return None
    text = str(value)
    text = text.strip()
    return text or None


def _coerce_optional_int(value: object) -> int | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(value)
        except Exception:
            return None
    try:
        numeric = to_int64(pd.Series([value])).iloc[0]
    except Exception:
        return None
    if pd.isna(numeric):
        return None
    return int(numeric)


def _resolve_school_status(student: Mapping[str, object], norm_value: int | None) -> bool:
    status_value = student.get("school_status_resolved")
    if status_value is not None and status_value is not pd.NA:
        try:
            normalized = normalize_bool_like(pd.Series([status_value])).iloc[0]
            if not pd.isna(normalized):
                return bool(int(normalized))
        except Exception:
            pass
    fallback = norm_value is not None and norm_value > 0
    flag_value = None
    for candidate in ("school_flag", "is_school"):
        if candidate in student:
            flag_value = student[candidate]
            break
    if flag_value is not None and flag_value is not pd.NA:
        try:
            normalized_flag = normalize_bool_like(pd.Series([flag_value])).iloc[0]
            if not pd.isna(normalized_flag):
                return bool(int(normalized_flag)) or fallback
        except Exception:
            return fallback
    return fallback


def _school_stage_filter(
    frame: pd.DataFrame,
    column: str,
    student: Mapping[str, object],
    policy: PolicyConfig,
) -> tuple[pd.DataFrame, dict[str, Any], object]:
    code = resolve_student_school_code(student, policy)
    norm_value = code.value
    status = _resolve_school_status(student, norm_value)
    raw = _string_or_none(student.get("school_code_raw"))

    filter_applied = False
    if code.wildcard or code.missing:
        filtered = frame
    elif status:
        if norm_value is not None:
            filtered, filter_applied = filter_school_by_value(frame, column, int(norm_value))
        else:
            numeric = pd.to_numeric(frame[column], errors="coerce").fillna(0)
            mask = numeric > 0
            filter_applied = bool(mask.any())
            filtered = frame.loc[mask] if filter_applied else frame
    else:
        numeric = pd.to_numeric(frame[column], errors="coerce").fillna(0)
        mask = numeric == 0
        filter_applied = bool(mask.any())
        filtered = frame.loc[mask] if filter_applied else frame

    extras = {
        "school_code_raw": raw,
        "school_code_norm": norm_value,
        "school_status_resolved": bool(status),
        "school_filter_applied": filter_applied,
    }
    return filtered, extras, norm_value


def build_allocation_trace(
    student: StudentRow,
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    stage_plan: Sequence[TraceStagePlan] | None = None,
    capacity_column: str = "remaining_capacity",
    stage_rules: Mapping[TraceStageLiteral, Rule] | None = None,
) -> List[TraceStageRecord]:
    """ایجاد تریس ۸ مرحله‌ای مطابق Policy."""

    if policy is None:
        policy = load_policy()

    if stage_plan is None:
        stage_plan = build_trace_plan(policy, capacity_column=capacity_column)

    resolved_rules = (
        dict(stage_rules) if stage_rules is not None else dict(build_stage_rule_map(policy))
    )

    non_capacity_plan = [plan for plan in stage_plan if plan.stage != "capacity_gate"]
    capacity_stage = next((plan for plan in stage_plan if plan.stage == "capacity_gate"), None)
    if capacity_stage is None:
        capacity_stage = TraceStagePlan(stage="capacity_gate", column=capacity_column)

    columns_needed = [plan.column for plan in non_capacity_plan] + [capacity_stage.column]
    _ensure_columns(candidate_pool, columns_needed)

    trace: List[TraceStageRecord] = []
    current = candidate_pool
    for plan in non_capacity_plan:
        before = int(current.shape[0])
        expected_value: object
        expected_op: str | None = "="
        expected_threshold: object | None = None
        stage_extras: dict[str, Any] = {}
        mentor_join_value = _candidate_join_value(current, plan.column)
        if plan.stage == "school":
            filtered, school_extras, norm_value = _school_stage_filter(
                current, plan.column, student, policy
            )
            expected_value = norm_value
            expected_op = ">"
            expected_threshold = 0
            stage_extras.update(school_extras)
            stage_extras.setdefault("join_value_raw", school_extras.get("school_code_raw"))
            stage_extras.setdefault("join_value_norm", school_extras.get("school_code_norm"))
        else:
            value = _student_value(student, plan.column)
            filtered = _filter_stage(current, plan.column, value)
            expected_value = value
            stage_extras["join_value_raw"] = value
            stage_extras["join_value_norm"] = _coerce_optional_int(value)
        if mentor_join_value is not None:
            mentor_raw: object | None = mentor_join_value
            if isinstance(mentor_join_value, Number) and not isinstance(mentor_join_value, bool):
                try:
                    if pd.isna(mentor_join_value):  # type: ignore[arg-type]
                        mentor_raw = None
                    else:
                        mentor_raw = int(mentor_join_value)
                except Exception:
                    mentor_raw = None
            if mentor_raw is not None:
                stage_extras["mentor_value_raw"] = mentor_raw
            mentor_norm = _coerce_optional_int(mentor_join_value)
            if mentor_norm is not None:
                stage_extras["mentor_value_norm"] = mentor_norm
        stage_extras["expected_op"] = expected_op
        stage_extras["expected_threshold"] = expected_threshold
        trace.append(
            TraceStageRecord(
                stage=plan.stage,
                column=plan.column,
                expected_value=expected_value,
                total_before=before,
                total_after=int(filtered.shape[0]),
                matched=bool(filtered.shape[0]),
                expected_op=expected_op,
                expected_threshold=expected_threshold,
                extras=stage_extras,
            )
        )
        _apply_stage_rule(trace[-1], resolved_rules, student)
        current = filtered

    before_capacity = int(current.shape[0])
    capacity_filtered = current.loc[current[capacity_stage.column] > 0]
    capacity_extras: dict[str, Any] = {
        "expected_op": ">",
        "expected_threshold": 0,
        "capacity_before": before_capacity,
        "capacity_after": int(capacity_filtered.shape[0]),
        "join_value_raw": None,
        "join_value_norm": None,
    }
    trace.append(
        TraceStageRecord(
            stage="capacity_gate",
            column=capacity_stage.column,
            expected_value=">0",
            total_before=before_capacity,
            total_after=int(capacity_filtered.shape[0]),
            matched=bool(capacity_filtered.shape[0]),
            expected_op=">",
            expected_threshold=0,
            extras=capacity_extras,
        )
    )
    _apply_stage_rule(trace[-1], resolved_rules, student)
    return trace
