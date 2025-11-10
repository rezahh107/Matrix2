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
from typing import Iterable, List, Mapping, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
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
        value = _student_value(student, plan.column)
        filtered = _filter_stage(current, plan.column, value)
        trace.append(
            TraceStageRecord(
                stage=plan.stage,
                column=plan.column,
                expected_value=value,
                total_before=before,
                total_after=int(filtered.shape[0]),
                matched=bool(filtered.shape[0]),
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
        )
    )
    return trace
