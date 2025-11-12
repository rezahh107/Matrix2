"""ماژول تخصیص دانش‌آموز به پشتیبان مطابق Policy-First."""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Number
from typing import Callable, Dict, List, Mapping, Sequence

import pandas as pd

from .common.column_normalizer import normalize_input_columns
from .common.columns import (
    CANON_EN_TO_FA,
    canonicalize_headers,
    coerce_semantics,
    enrich_school_columns_en,
    resolve_aliases,
)
from .common.filters import apply_join_filters
from .common.ids import build_mentor_id_map, inject_mentor_id
from .common.normalization import to_numlike_str
from .common.ranking import apply_ranking_policy, build_mentor_state, consume_capacity
from .common.trace import TraceStagePlan, build_allocation_trace, build_trace_plan
from .common.types import AllocationLogRecord, JoinKeyValues, TraceStageRecord
from .policy_loader import PolicyConfig, load_policy

ProgressFn = Callable[[int, str], None]

__all__ = ["ProgressFn", "AllocationResult", "allocate_student", "allocate_batch"]


def _noop_progress(_: int, __: str) -> None:
    """تابع پیش‌فرض progress که کاری انجام نمی‌دهد."""


@dataclass(frozen=True)
class AllocationResult:
    """خروجی تخصیص یک دانش‌آموز."""

    mentor_row: pd.Series | None
    trace: List[TraceStageRecord]
    log: AllocationLogRecord


def _resolve_capacity_column(policy: PolicyConfig, override: str | None) -> str:
    if override:
        return override
    try:
        return policy.stage_column("capacity_gate")
    except KeyError:
        return policy.columns.remaining_capacity


def _coerce_int(value: object) -> int:
    if value is None:
        raise ValueError("DATA_MISSING")
    if isinstance(value, Number):
        if pd.isna(value):  # type: ignore[arg-type]
            raise ValueError("DATA_MISSING")
        return int(value)
    text = to_numlike_str(value).strip()
    if not text:
        raise ValueError("DATA_MISSING")
    try:
        return int(float(text))
    except ValueError as exc:
        raise ValueError("DATA_MISSING") from exc


def _student_value(student: Mapping[str, object], column: str) -> object:
    if column in student:
        return student[column]
    normalized = column.replace(" ", "_")
    if normalized in student:
        return student[normalized]
    raise KeyError(f"Student row missing value for '{column}'")


def _build_log_base(student: Mapping[str, object], policy: PolicyConfig) -> AllocationLogRecord:
    join_map = {
        column.replace(" ", "_"): _coerce_int(_student_value(student, column))
        for column in policy.join_keys
    }
    log: AllocationLogRecord = {
        "row_index": -1,
        "student_id": str(student.get("student_id", "")),
        "allocation_status": "failed",
        "mentor_selected": None,
        "mentor_id": None,
        "occupancy_ratio": None,
        "join_keys": JoinKeyValues(join_map),
        "candidate_count": 0,
        "selection_reason": None,
        "tie_breakers": {},
        "error_type": None,
        "detailed_reason": None,
        "suggested_actions": [],
        "capacity_before": None,
        "capacity_after": None,
    }
    return log


def _normalize_students(df: pd.DataFrame, policy: PolicyConfig) -> pd.DataFrame:
    normalized = resolve_aliases(df, "report")
    school_fa = CANON_EN_TO_FA["school_code"]
    if school_fa in normalized.columns:
        pre_normal_raw = normalized[school_fa].astype("string").str.strip()
    else:
        pre_normal_raw = pd.Series([pd.NA] * len(normalized), dtype="string", index=normalized.index)
    normalized = coerce_semantics(normalized, "report")
    normalized, _ = normalize_input_columns(
        normalized, kind="StudentReport", include_alias=True, report=False
    )
    normalized_en = canonicalize_headers(normalized, header_mode="en")
    if "school_code_raw" not in normalized_en.columns:
        normalized_en["school_code_raw"] = pre_normal_raw.reindex(normalized_en.index)
    normalized_en = enrich_school_columns_en(normalized_en)
    normalized = canonicalize_headers(normalized_en, header_mode="fa")
    default_index = normalized_en.index
    normalized["school_code_raw"] = normalized_en.get(
        "school_code_raw", pd.Series([pd.NA] * len(default_index), dtype="string", index=default_index)
    )
    normalized["school_code_norm"] = normalized_en.get(
        "school_code_norm",
        pd.Series([pd.NA] * len(default_index), dtype="Int64", index=default_index),
    )
    normalized["school_status_resolved"] = normalized_en.get(
        "school_status_resolved",
        pd.Series([0] * len(default_index), dtype="Int64", index=default_index),
    )
    school_fa = CANON_EN_TO_FA["school_code"]
    if school_fa in normalized.columns:
        normalized[school_fa] = normalized["school_code_norm"].astype("Int64")
    missing = [column for column in policy.join_keys if column not in normalized.columns]
    if missing:
        raise KeyError(f"Student data missing columns: {missing}")
    required_fields = set(policy.required_student_fields)
    missing_required = []
    for field in required_fields:
        canonical = CANON_EN_TO_FA.get(field, field)
        if field not in normalized.columns and canonical not in normalized.columns:
            missing_required.append(field)
    if missing_required:
        raise ValueError(f"Missing columns: {missing_required}")
    return normalized


def _normalize_pool(df: pd.DataFrame, policy: PolicyConfig) -> pd.DataFrame:
    normalized = resolve_aliases(df, "inspactor")
    normalized = coerce_semantics(normalized, "inspactor")
    normalized, _ = normalize_input_columns(
        normalized, kind="MentorPool", include_alias=True, report=False
    )
    required = set(policy.join_keys) | {"کد کارمندی پشتیبان"}
    missing = [column for column in required if column not in normalized.columns]
    if missing:
        raise KeyError(f"Pool data missing columns: {missing}")

    capacity_alias = policy.columns.remaining_capacity
    if capacity_alias in normalized.columns and "remaining_capacity" not in normalized.columns:
        normalized["remaining_capacity"] = normalized[capacity_alias]

    for column_name in {
        capacity_alias,
        "remaining_capacity",
    }:
        if column_name in normalized.columns:
            normalized[column_name] = (
                pd.to_numeric(normalized[column_name], errors="coerce")
                .fillna(0)
                .astype("Int64")
            )
    return normalized


def allocate_student(
    student: Mapping[str, object],
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    progress: ProgressFn = _noop_progress,
    capacity_column: str | None = None,
    trace_plan: Sequence[TraceStagePlan] | None = None,
    state: Dict[object, Dict[str, int]] | None = None,
    pool_state_view: pd.DataFrame | None = None,
) -> AllocationResult:
    """تخصیص تک‌دانش‌آموز با حفظ Trace و لاگ کامل."""
    if policy is None:
        policy = load_policy()
    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)
    if trace_plan is None:
        trace_plan = build_trace_plan(policy, capacity_column=resolved_capacity_column)

    progress(5, "prefilter")
    eligible = apply_join_filters(candidate_pool, student, policy=policy)
    trace = build_allocation_trace(
        student,
        candidate_pool,
        policy=policy,
        stage_plan=trace_plan,
        capacity_column=resolved_capacity_column,
    )

    log = _build_log_base(student, policy)
    log["candidate_count"] = int(eligible.shape[0])

    if eligible.empty:
        log.update(
            {
                "detailed_reason": "No candidates matched join keys",
                "error_type": "ELIGIBILITY_NO_MATCH",
                "suggested_actions": ["بازبینی دادهٔ ورودی", "تطبیق join keys"],
            }
        )
        return AllocationResult(None, trace, log)

    progress(30, "capacity")
    state_frame = pool_state_view if pool_state_view is not None else candidate_pool
    state_view_en = canonicalize_headers(state_frame, header_mode="en")

    capacity_candidates: list[str] = []
    if "remaining_capacity" in state_view_en.columns:
        capacity_candidates.append("remaining_capacity")
    capacity_candidates.append(resolved_capacity_column)
    derived_name = canonicalize_headers(
        pd.DataFrame(columns=[resolved_capacity_column]), header_mode="en"
    ).columns[0]
    if derived_name not in capacity_candidates:
        capacity_candidates.append(derived_name)

    capacity_column_name: str | None = None
    for candidate in capacity_candidates:
        if candidate in state_view_en.columns:
            capacity_column_name = candidate
            break
    if capacity_column_name is None:
        raise KeyError(
            f"Capacity column '{resolved_capacity_column}' not found after canonicalization"
        )

    capacity_series = state_view_en.loc[eligible.index, capacity_column_name]
    capacity_numeric = pd.to_numeric(capacity_series, errors="coerce").fillna(0).astype(int)
    capacity_mask = capacity_numeric > 0
    capacity_filtered = eligible.loc[capacity_mask.values]

    if capacity_filtered.empty:
        log.update(
            {
                "detailed_reason": "No capacity among matched candidates",
                "error_type": "CAPACITY_FULL",
                "suggested_actions": ["افزایش ظرفیت", "بازنگری محدودیت‌ها"],
            }
        )
        return AllocationResult(None, trace, log)

    progress(60, "ranking")
    ranking_input = capacity_filtered.copy()
    ranking_input["__candidate_index__"] = capacity_filtered.index

    active_state = (
        state
        if state is not None
        else build_mentor_state(
            state_view_en, capacity_column=capacity_column_name, policy=policy
        )
    )
    ranked = apply_ranking_policy(ranking_input, state=active_state, policy=policy)

    chosen_row = ranked.iloc[0].copy()
    chosen_index = chosen_row["__candidate_index__"]
    ranked = ranked.drop(columns=["__candidate_index__"], errors="ignore")
    ranked_en = canonicalize_headers(ranked, header_mode=policy.excel.header_mode_internal)
    chosen_en = ranked_en.iloc[0]

    mentor_identifier = chosen_row.get("mentor_id_en", chosen_en.get("mentor_id"))
    state_entry_snapshot = active_state.get(mentor_identifier, {}) if active_state else {}
    capacity_before = int(state_entry_snapshot.get("remaining", 0))
    capacity_after = capacity_before
    occupancy_value = float(chosen_row.get("occupancy_ratio", 0.0))

    try:
        capacity_before, capacity_after, occupancy_value = consume_capacity(
            active_state, mentor_identifier
        )
    except KeyError as exc:
        log.update(
            {
                "allocation_status": "failed",
                "mentor_selected": None,
                "mentor_id": None,
                "error_type": "INTERNAL_ERROR",
                "detailed_reason": str(exc),
                "suggested_actions": [
                    "بازسازی state ظرفیت",
                    "بررسی داده‌های استخر",
                ],
            }
        )
        return AllocationResult(None, trace, log)
    except ValueError as exc:
        error_code = str(exc) or "CAPACITY_UNDERFLOW"
        log.update(
            {
                "allocation_status": "failed",
                "mentor_selected": None,
                "mentor_id": None,
                "error_type": error_code,
                "detailed_reason": "Mentor capacity underflow detected",
                "suggested_actions": [
                    "بازبینی ظرفیت ورودی",
                    "اجرای مجدد sanitize pool",
                ],
            }
        )
        return AllocationResult(None, trace, log)
    mentor_name = chosen_row.get("پشتیبان", chosen_row.get("mentor_name", ""))
    mentor_id_text = chosen_row.get("کد کارمندی پشتیبان", chosen_en.get("mentor_id", ""))
    tie_breakers = {
        "stage1": {
            "metric": "occupancy_ratio",
            "value": float(chosen_row.get("occupancy_ratio", 0.0)),
        },
        "stage2": {
            "metric": "allocations_new",
            "value": int(chosen_row.get("allocations_new", 0)),
        },
        "stage3": {
            "metric": "natural mentor_id",
            "value": list(chosen_row.get("mentor_sort_key", ())),
        },
    }

    log.update(
        {
            "row_index": int(chosen_index) if chosen_index is not None else 0,
            "allocation_status": "success",
            "mentor_selected": str(mentor_name),
            "mentor_id": str(mentor_id_text),
            "occupancy_ratio": float(occupancy_value),
            "selection_reason": "policy: min occ → min alloc → natural mentor_id",
            "tie_breakers": tie_breakers,
            "capacity_before": int(capacity_before),
            "capacity_after": int(capacity_after),
        }
    )
    return AllocationResult(capacity_filtered.loc[chosen_index], trace, log)


def allocate_batch(
    students: pd.DataFrame,
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    progress: ProgressFn = _noop_progress,
    capacity_column: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """تخصیص دسته‌ای دانش‌آموزان و بازگشت خروجی‌های چهارتایی."""
    if policy is None:
        policy = load_policy()

    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)
    capacity_internal = canonicalize_headers(
        pd.DataFrame(columns=[resolved_capacity_column]),
        header_mode=policy.excel.header_mode_internal,
    ).columns[0]

    students_norm = _normalize_students(students, policy)
    pool_norm = _normalize_pool(candidate_pool, policy)
    pool_with_ids = inject_mentor_id(pool_norm, build_mentor_id_map(pool_norm))
    if "allocations_new" not in pool_with_ids.columns:
        pool_with_ids["allocations_new"] = 0
    if "occupancy_ratio" not in pool_with_ids.columns:
        pool_with_ids["occupancy_ratio"] = 0.0

    pool_internal = canonicalize_headers(pool_with_ids, header_mode="en")
    if capacity_internal not in pool_internal.columns:
        pool_internal[capacity_internal] = 0
    if "allocations_new" not in pool_internal.columns:
        pool_internal["allocations_new"] = 0
    if "occupancy_ratio" not in pool_internal.columns:
        pool_internal["occupancy_ratio"] = 0.0
    if "mentor_id" not in pool_internal.columns:
        raise KeyError("Pool must contain 'mentor_id' column after canonicalization")

    mentor_state = build_mentor_state(
        pool_internal, capacity_column=capacity_internal, policy=policy
    )

    allocations: List[Mapping[str, object]] = []
    logs: List[AllocationLogRecord] = []
    trace_rows: List[Mapping[str, object]] = []

    total = max(int(students_norm.shape[0]), 1)
    trace_plan = build_trace_plan(policy, capacity_column=resolved_capacity_column)

    progress(0, "start")
    for idx, (_, student_row) in enumerate(students_norm.iterrows(), start=1):
        student_dict = student_row.to_dict()
        progress(int(idx * 100 / total), f"allocating {idx}/{total}")
        result = allocate_student(
            student_dict,
            pool_with_ids,
            policy=policy,
            progress=_noop_progress,
            capacity_column=resolved_capacity_column,
            trace_plan=trace_plan,
            state=mentor_state,
            pool_state_view=pool_internal,
        )
        logs.append(result.log)
        for stage in result.trace:
            trace_rows.append({"student_id": result.log["student_id"], **stage})

        if result.mentor_row is not None:
            chosen_index = result.mentor_row.name
            mentor_row_en = canonicalize_headers(
                result.mentor_row.to_frame().T, header_mode=policy.excel.header_mode_internal
            ).iloc[0]
            mentor_identifier = mentor_row_en.get("mentor_id")
            state_entry = mentor_state.get(mentor_identifier)
            if state_entry is None:
                raise KeyError(f"Mentor '{mentor_identifier}' missing from state after allocation")
            pool_internal.loc[chosen_index, capacity_internal] = state_entry["remaining"]
            if (
                capacity_internal != "remaining_capacity"
                and "remaining_capacity" in pool_internal.columns
            ):
                pool_internal.loc[chosen_index, "remaining_capacity"] = state_entry[
                    "remaining"
                ]
            pool_internal.loc[chosen_index, "allocations_new"] = state_entry["alloc_new"]
            initial_value = max(int(state_entry.get("initial", 0)), 1)
            pool_internal.loc[chosen_index, "occupancy_ratio"] = (
                (int(state_entry.get("initial", 0)) - state_entry["remaining"]) / initial_value
            )
            pool_with_ids.loc[chosen_index, resolved_capacity_column] = state_entry[
                "remaining"
            ]
            if (
                resolved_capacity_column != "remaining_capacity"
                and "remaining_capacity" in pool_with_ids.columns
            ):
                pool_with_ids.loc[chosen_index, "remaining_capacity"] = state_entry[
                    "remaining"
                ]
            pool_with_ids.loc[chosen_index, "allocations_new"] = state_entry["alloc_new"]
            pool_with_ids.loc[chosen_index, "occupancy_ratio"] = pool_internal.loc[
                chosen_index, "occupancy_ratio"
            ]

            allocations.append(
                {
                    "student_id": student_dict.get("student_id", ""),
                    "mentor": result.mentor_row.get("پشتیبان", ""),
                    "mentor_id": mentor_row_en.get("mentor_id", ""),
                }
            )

    progress(100, "done")

    allocations_df = pd.DataFrame(allocations)
    logs_df = pd.DataFrame(logs)
    trace_df = pd.DataFrame(trace_rows)

    pool_output = pool_with_ids.copy()
    original_columns = list(candidate_pool.columns)
    for column in original_columns:
        if column not in pool_output.columns:
            pool_output[column] = pd.NA
    pool_output = pool_output.loc[:, original_columns]

    for column in original_columns:
        if column in candidate_pool.columns:
            try:
                pool_output[column] = pool_output[column].astype(candidate_pool[column].dtype)
            except (TypeError, ValueError):
                continue

    for entry in mentor_state.values():
        if entry["remaining"] < 0:
            raise ValueError("Negative remaining capacity detected after allocation")

    internal_remaining = pd.to_numeric(pool_internal[capacity_internal], errors="coerce").fillna(0)
    if (internal_remaining < 0).any():
        raise ValueError("Pool capacity column contains negative values after allocation")

    return allocations_df, pool_output, logs_df, trace_df
