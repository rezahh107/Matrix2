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
from typing import Any, Iterable, List, Mapping, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
from .columns import normalize_bool_like, to_int64
from .types import StudentRow, TraceStageLiteral, TraceStageRecord

__all__ = ["TraceStagePlan", "build_trace_plan", "build_allocation_trace"]


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


def _filter_stage(frame: pd.DataFrame, column: str, value: object) -> pd.DataFrame:
    mask = frame[column] == value
    return frame.loc[mask]


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
) -> tuple[pd.DataFrame, dict[str, Any], object]:
    value = _student_value(student, column)
    norm_candidate = student.get("school_code_norm", value)
    norm_value = _coerce_optional_int(norm_candidate)
    status = _resolve_school_status(student, norm_value)
    raw = _string_or_none(student.get("school_code_raw"))

    if status:
        if norm_value is not None:
            filtered = frame.loc[frame[column] == norm_value]
        else:
            filtered = frame.loc[frame[column] > 0]
    else:
        filtered = frame.loc[frame[column] == 0]

    extras = {
        "school_code_raw": raw,
        "school_code_norm": norm_value,
        "school_status_resolved": bool(status),
    }
    return filtered, extras, norm_value


def build_allocation_trace(
    student: StudentRow,
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    stage_plan: Sequence[TraceStagePlan] | None = None,
    capacity_column: str = "remaining_capacity",
) -> List[TraceStageRecord]:
    """ایجاد تریس ۸ مرحله‌ای مطابق Policy."""

    if policy is None:
        policy = load_policy()

    if stage_plan is None:
        stage_plan = build_trace_plan(policy, capacity_column=capacity_column)

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
        expected_op: str | None = None
        expected_threshold: object | None = None
        extras: Mapping[str, Any] | None = None
        if plan.stage == "school":
            filtered, extras, norm_value = _school_stage_filter(current, plan.column, student)
            expected_value = norm_value
            expected_op = ">"
            expected_threshold = 0
        else:
            value = _student_value(student, plan.column)
            filtered = _filter_stage(current, plan.column, value)
            expected_value = value
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
                extras=extras,
            )
        )
        current = filtered

    before_capacity = int(current.shape[0])
    capacity_filtered = current.loc[current[capacity_stage.column] > 0]
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
            extras=None,
        )
    )
    return trace
