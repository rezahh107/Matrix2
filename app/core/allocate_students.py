"""موتور تخصیص دانش‌آموز به پشتیبان مطابق Policy v1.0.3 (Core-only).

این ماژول هیچ I/O انجام نمی‌دهد و برای استفاده در لایه‌های Application/Infra
طراحی شده است. مراحل اصلی:

1. اعمال «Allocation 7-Pack» روی استخر کاندیدها.
2. تولید Trace هشت‌مرحله‌ای جهت Explainability.
3. اعمال دروازهٔ ظرفیت و رتبه‌بندی دترمینیستیک با sort پایدار.
4. ثبت لاگ استاندارد و به‌روزرسانی ظرفیت در نسخهٔ کپی شده از استخر.

Progress API تزریق‌پذیر است و به‌صورت پیش‌فرض عمل ناپ (بدون خروجی) دارد.

مثال ساده::

    >>> import pandas as pd
    >>> from app.core.allocate_students import allocate_batch
    >>> students = pd.DataFrame([
    ...     {
    ...         "student_id": "STD-001",
    ...         "کدرشته": 1201,
    ...         "گروه_آزمایشی": "تجربی",
    ...         "جنسیت": 1,
    ...         "دانش_آموز_فارغ": 0,
    ...         "مرکز_گلستان_صدرا": 1,
    ...         "مالی_حکمت_بنیاد": 0,
    ...         "کد_مدرسه": 3581, # تغییر نام داده شد
    ...     }
    ... ])
    >>> pool = pd.DataFrame({
    ...     "پشتیبان": ["زهرا"],
    ...     "کد کارمندی پشتیبان": ["EMP-001"],
    ...     "کدرشته": [1201],
    ...     "گروه آزمایشی": ["تجربی"],
    ...     "جنسیت": [1],
    ...     "دانش آموز فارغ": [0],
    ...     "مرکز گلستان صدرا": [1],
    ...     "مالی حکمت بنیاد": [0],
    ...     "کد مدرسه": [3581],
    ...     "remaining_capacity": [1],
    ...     "occupancy_ratio": [0.2],
    ...     "allocations_new": [0],
    ... })
    >>> alloc_df, updated_pool, logs_df, trace_df = allocate_batch(students, pool)
    >>> alloc_df.iloc[0]["mentor_id"]
    'EMP-001'
"""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Number
from typing import Callable, Collection, Dict, List, Mapping, Sequence

import pandas as pd
import numpy as np # اضافه شده برای چک کردن NaN
from pandas.api import types as pd_types

from .common.columns import (
    CANON,
    CANON_FA_TO_EN,
    accepted_synonyms,
    coerce_semantics,
    resolve_aliases,
)
from .common.column_normalizer import normalize_input_columns
from .common.filters import apply_join_filters
from .common.ids import build_mentor_id_map, inject_mentor_id
from .common.ranking import apply_ranking_policy
from .common.trace import TraceStagePlan, build_allocation_trace, build_trace_plan
from .common.types import (
    AllocationLogRecord,
    JoinKeyValues,
    StudentRow,
    TraceStageRecord,
)
from .common.normalization import normalize_fa, to_numlike_str
from .policy_adapter import policy as policy_adapter
from .policy_loader import PolicyConfig, load_policy

ProgressFn = Callable[[int, str], None]

__all__ = [
    "ProgressFn",
    "AllocationResult",
    "allocate_student",
    "allocate_batch",
]

def _canonical_student_column(name: str) -> str:
    """نام ستون را به صورت کاننیکال فارسی برمی‌گرداند."""

    text = str(name or "").strip()
    if not text:
        return ""

    key = text.lower().replace(" ", "_")
    if key in CANON:
        return CANON[key]

    normalized = normalize_fa(text)
    normalized = normalized.replace("_", " ")
    normalized = " ".join(normalized.split())
    if normalized in CANON_FA_TO_EN:
        return CANON[CANON_FA_TO_EN[normalized]]

    if normalized in CANON.values():
        return normalized

    direct = " ".join(text.replace("_", " ").split())
    if direct in CANON.values():
        return direct

    return text


def _required_student_columns_from_policy(policy: PolicyConfig) -> frozenset[str]:
    """ستون‌های ضروری دانش‌آموز را براساس Policy استخراج می‌کند."""

    required: set[str] = set()
    for column in list(policy.required_student_fields) + list(policy.join_keys):
        canonical = _canonical_student_column(column)
        if canonical:
            required.add(canonical)
    return frozenset(required)

REQUIRED_POOL_BASE_COLUMNS = {CANON["mentor_id"]}


def _noop_progress(_: int, __: str) -> None:
    """توابع پیش‌فرض Progress که هیچ‌ کاری انجام نمی‌دهد."""


@dataclass(frozen=True)
class AllocationResult:
    """خروجی تخصیص یک دانش‌آموز."""

    mentor_row: pd.Series | None
    trace: List[TraceStageRecord]
    log: AllocationLogRecord


def _student_value(student: Mapping[str, object], column: str) -> object:
    # تلاش برای پیدا کردن مقدار با استفاده از نام کاننیکال
    canonical = _canonical_student_column(column)
    if canonical in student:
        return student[canonical]
    # تلاش برای پیدا کردن مقدار با نام اصلی
    if column in student:
        return student[column]
    # تلاش برای پیدا کردن مقدار با نام جایگزین فاصله‌دار
    normalized = column.replace(" ", "_")
    if normalized in student:
        return student[normalized]
    raise KeyError(f"Student row missing value for '{column}'")


def _int_value(student: Mapping[str, object], column: str) -> int:
    raw = _student_value(student, column)
    # چک کردن اینکه آیا raw یک عدد است
    if isinstance(raw, Number):
        # اگر NaN بود، یک مقدار پیش‌فرض برگردانده می‌شود
        if pd.isna(raw):
            return 0 # یا هر مقدار پیش‌فرض منطقی دیگر
        return int(raw)
    # اگر عدد نبود، به رشته تبدیل می‌کنیم
    text = to_numlike_str(raw).strip()
    # چک کردن اینکه آیا رشته خالی یا '-' است
    if not text or text == "-" or text.lower() == "nan" or text.lower() == "none":
        # در این موارد، مقدار پیش‌فرض را برمی‌گردانیم
        return 0 # یا هر مقدار پیش‌فرض منطقی دیگر
    # چک کردن اینکه آیا رشته عددی است
    if not text.lstrip("-").isdigit():
        raise ValueError(f"DATA_MISSING: '{column}' in student row")
    return int(text)


def _build_log_base(student: Mapping[str, object], policy: PolicyConfig) -> AllocationLogRecord:
    join_key_values = JoinKeyValues(
        {
            column.replace(" ", "_"): _int_value(student, column)
            for column in policy.join_keys
        }
    )
    log: AllocationLogRecord = {
        "row_index": -1,
        "student_id": str(student.get("student_id", "")),
        "mentor_selected": None,
        "mentor_id": None,
        "occupancy_ratio": None,
        "join_keys": join_key_values,
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


def _require_columns(df: pd.DataFrame, required: Collection[str], source: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if not missing:
        return
    accepted: Dict[str, List[str]] = {}
    for col in missing:
        synonyms = list(accepted_synonyms(source, col))
        if col not in synonyms:
            synonyms.insert(0, col)
        accepted[col] = synonyms
    raise ValueError(f"Missing columns: {missing} — accepted synonyms: {accepted}")


def _resolve_capacity_column(policy: PolicyConfig, override: str | None) -> str:
    if override:
        return override
    column = policy_adapter.stage_column("capacity_gate")
    if column:
        return column
    return policy.columns.remaining_capacity

# --- اصلاح تابع safe_int_value ---
def safe_int_value(value, default: int = 0) -> int:
    """تبدیل یک مقدار داده‌ای به عدد صحیح با مقدار پیش‌فرض در صورت ناموفق بودن."""
    # چک کردن اگر ورودی یک Series یا DataFrame تک‌مقداری است
    if isinstance(value, (pd.Series, pd.DataFrame)):
        if value.size == 0:
            return default
        # فقط اولین مقدار را در نظر می‌گیریم
        scalar_value = value.iloc[0] if isinstance(value, pd.Series) else value.iloc[0, 0]
    else:
        scalar_value = value

    # چک کردن اگر مقدار NaN یا None است
    if pd.isna(scalar_value) or scalar_value is None:
        return default

    # چک کردن اگر مقدار عدد است
    if isinstance(scalar_value, Number):
        return int(scalar_value)

    # تبدیل به رشته و سپس تلاش برای تبدیل به عدد صحیح
    try:
        text = to_numlike_str(scalar_value).strip()
        if not text or text == "-" or text.lower() == "nan" or text.lower() == "none":
            return default
        if not text.lstrip("-").isdigit():
            return default # یا می‌توانید خطا صادر کنید
        return int(text)
    except (ValueError, TypeError):
        return default

# --- پایان اصلاح تابع safe_int_value ---


def allocate_student(
    student: StudentRow | Mapping[str, object],
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    progress: ProgressFn = _noop_progress,
    capacity_column: str | None = None,
    trace_plan: Sequence[TraceStagePlan] | None = None,
) -> AllocationResult:
    """تخصیص یک دانش‌آموز براساس ۷ فیلتر، ظرفیت و رتبه‌بندی سیاست."""

    if policy is None:
        policy = load_policy()
    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)
    if trace_plan is None:
        trace_plan = build_trace_plan(policy, capacity_column=resolved_capacity_column)

    progress(5, "prefilter")
    eligible_after_join = apply_join_filters(candidate_pool, student, policy=policy)
    trace = build_allocation_trace(
        student,
        candidate_pool,
        policy=policy,
        stage_plan=trace_plan,
        capacity_column=resolved_capacity_column,
    )

    if eligible_after_join.empty:
        log = _build_log_base(student, policy)
        log.update(
            {
                "allocation_status": "failed",
                "error_type": "ELIGIBILITY_NO_MATCH",
                "detailed_reason": "No candidates matched join keys",
                "suggested_actions": ["بررسی داده‌های ورودی", "تطبیق کلیدهای join"],
            }
        )
        return AllocationResult(None, trace, log)

    progress(25, "capacity")
    capacity_mask = eligible_after_join[resolved_capacity_column] > 0
    capacity_filtered = eligible_after_join.loc[capacity_mask]

    if capacity_filtered.empty:
        log = _build_log_base(student, policy)
        log.update(
            {
                "allocation_status": "failed",
                "candidate_count": int(eligible_after_join.shape[0]),
                "error_type": "CAPACITY_FULL",
                "detailed_reason": "No capacity among matched candidates",
                "suggested_actions": ["افزایش ظرفیت", "بازنگری محدودیت‌ها"],
            }
        )
        return AllocationResult(None, trace, log)

    progress(55, "ranking")
    ranked = apply_ranking_policy(capacity_filtered, policy=policy)
    top_row = ranked.iloc[0]

    log = _build_log_base(student, policy)
    log.update(
        {
            "row_index": int(top_row.name) if hasattr(top_row, "name") else 0,
            "allocation_status": "success",
            "mentor_selected": str(top_row.get("پشتیبان", "")),
            "mentor_id": str(top_row.get("کد کارمندی پشتیبان", "")),
            "occupancy_ratio": float(top_row.get("occupancy_ratio", 0.0)),
            "candidate_count": int(capacity_filtered.shape[0]),
            "selection_reason": "policy: min occ → min alloc → min mentor_id",
            "tie_breakers": {"stage3": "natural mentor_id"},
        }
    )
    return AllocationResult(top_row, trace, log)


def allocate_batch(
    students: pd.DataFrame,
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    progress: ProgressFn = _noop_progress,
    capacity_column: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """تخصیص دسته‌ای با خروجی چهارگانه (allocations, pool, logs, trace)."""

    if policy is None:
        policy = load_policy()
    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)

    # --- حذف نرمال‌سازی اولیه ---
    # این بخش فرض می‌کند که داده‌ها قبلاً نرمال‌سازی شده‌اند.
    # students = resolve_aliases(students, "report")
    # students = coerce_semantics(students, "report")
    # students, _ = normalize_input_columns(
    #     students, kind="StudentReport", include_alias=True, report=False
    # )
    #
    # candidate_pool = resolve_aliases(candidate_pool, "inspactor")
    # candidate_pool = coerce_semantics(candidate_pool, "inspactor")
    # candidate_pool, _ = normalize_input_columns(
    #     candidate_pool, kind="MentorPool", include_alias=True, report=False
    # )

    # --- حذف ایجاد ستون از شاخص ---
    # فرض می‌شود فایل ورودی دارای ستون‌های مورد نیاز است.
    # required_student_columns = _required_student_columns_from_policy(policy)
    # required_pool_columns = set(policy.join_keys) | REQUIRED_POOL_BASE_COLUMNS
    # required_columns_map_by_index = {
    #     5: "کد مدرسه",  # نام واقعی در فایل ورودی یا نام قابل قبول
    #     6: "جنسیت",
    #     7: "وضعیت تحصیلی",
    #     8: "کد ملی", # فرض بر این است که این ستون مربوط به 'مرکز گلستان صدرا' است
    #     9: "کد پستی", # فرض بر این است که این ستون مربوط به 'مالی حکمت بنیاد' است
    #     10: "آدرس", # فرض بر این است که این ستون مربوط به 'کد مدرسه' است
    # }
    # for col_idx, col_name in required_columns_map_by_index.items():
    #     if col_name not in candidate_pool.columns and col_idx < candidate_pool.shape[1]:
    #         candidate_pool[col_name] = candidate_pool.iloc[:, col_idx]
    #     if col_name not in students.columns and col_idx < students.shape[1]:
    #         students[col_name] = students.iloc[:, col_idx]

    # بعد از ایجاد ستون‌های ضروری، نرمال‌سازی مجدد انجام نمی‌دهیم تا از بروز خطا جلوگیری شود
    # توجه: اگر نام ستون‌های ایجاد شده با نام‌های پذیرفته شده در policy مطابقت نداشته باشد,
    # باید در فایل policy یا در مکانیزم نرمال‌سازی اولیه تغییرات لازم اعمال شود.
    # students = resolve_aliases(students, "report")
    # students = coerce_semantics(students, "report")
    # students, _ = normalize_input_columns(
    #     students, kind="StudentReport", include_alias=True, report=False
    # )

    # candidate_pool = resolve_aliases(candidate_pool, "inspactor")
    # candidate_pool = coerce_semantics(candidate_pool, "inspactor")
    # candidate_pool, _ = normalize_input_columns(
    #     candidate_pool, kind="MentorPool", include_alias=True, report=False
    # )

    # حالا دوباره بررسی می‌کنیم که آیا ستون‌های مورد نیاز وجود دارند
    required_student_columns = _required_student_columns_from_policy(policy)
    _require_columns(students, required_student_columns, "report")
    
    required_pool_columns = set(policy.join_keys) | REQUIRED_POOL_BASE_COLUMNS
    _require_columns(candidate_pool, required_pool_columns, "inspactor")
    
    if resolved_capacity_column not in candidate_pool.columns:
        raise KeyError(f"Missing capacity column '{resolved_capacity_column}'")

    progress(0, "start")
    pool = candidate_pool.copy()
    original_capacity_dtype = pool[resolved_capacity_column].dtype
    # استفاده از map به جای apply
    converted_capacity = pool[resolved_capacity_column].map(lambda v: safe_int_value(v, default=0))
    if pd_types.is_integer_dtype(original_capacity_dtype):
        pool[resolved_capacity_column] = converted_capacity.astype(original_capacity_dtype)
    else:
        pool[resolved_capacity_column] = converted_capacity.astype("Int64")
    mentor_col = CANON["mentor_id"]
    pool[mentor_col] = (
        pool[mentor_col].astype("string").str.strip().fillna("").astype(object)
    )
    id_map = build_mentor_id_map(pool)
    pool = inject_mentor_id(pool, id_map)

    allocations: list[Mapping[str, object]] = []
    logs: list[AllocationLogRecord] = []
    trace_rows: list[Mapping[str, object]] = []

    total = max(int(students.shape[0]), 1)
    trace_plan = build_trace_plan(policy, capacity_column=resolved_capacity_column)

    for idx, (_, student_row) in enumerate(students.iterrows(), start=1):
        student_dict = student_row.to_dict()
        step_pct = int(idx * 100 / total)
        progress(step_pct, f"allocating {idx}/{total}")

        result = allocate_student(
            student_dict,
            pool,
            policy=policy,
            progress=_noop_progress,
            capacity_column=resolved_capacity_column,
            trace_plan=trace_plan,
        )
        logs.append(result.log)
        for stage in result.trace:
            trace_rows.append({"student_id": result.log["student_id"], **stage})

        if result.mentor_row is not None:
            mentor_index = result.mentor_row.name
            if mentor_index in pool.index:
                # استفاده از تابع اصلاح شده
                previous_capacity = safe_int_value(
                    pool.loc[mentor_index, resolved_capacity_column], default=0
                )
                updated_capacity = max(previous_capacity - 1, 0)
                pool.loc[mentor_index, resolved_capacity_column] = updated_capacity
                logs[-1]["capacity_before"] = previous_capacity
                logs[-1]["capacity_after"] = updated_capacity
            allocations.append(
                {
                    "student_id": student_dict.get("student_id", ""),
                    "mentor": result.mentor_row.get("پشتیبان", ""),
                    "mentor_id": result.mentor_row.get("کد کارمندی پشتیبان", ""),
                }
            )

    progress(100, "done")

    allocations_df = pd.DataFrame(allocations)
    if not allocations_df.empty and "mentor_id" in allocations_df.columns:
        allocations_df["mentor_id"] = allocations_df["mentor_id"].astype("string").str.strip().fillna("")
    logs_df = pd.DataFrame(logs)
    trace_df = pd.DataFrame(trace_rows)
    return allocations_df, pool, logs_df, trace_df
