"""ماژول تخصیص دانش‌آموز به پشتیبان مطابق Policy-First."""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Number
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple

import pandas as pd

from .canonical_frames import canonicalize_pool_frame, canonicalize_students_frame
from .common.column_normalizer import normalize_input_columns
from .common.columns import (
    CANON_EN_TO_FA,
    CANON_FA_TO_EN,
    canonicalize_headers,
    coerce_semantics,
    enrich_school_columns_en,
    ensure_series,
    enforce_join_key_types,
    resolve_aliases,
)
from .common.filters import (
    StudentSchoolCode,
    apply_join_filters,
    resolve_student_school_code,
)
from .common.ids import build_mentor_id_map, inject_mentor_id, natural_key
from .common.normalization import normalize_fa, to_numlike_str
from .common.ranking import apply_ranking_policy, build_mentor_state, consume_capacity
from .common.reasons import ReasonCode, build_reason
from .common.rules import Rule, default_stage_rule_map
from .common.trace import TraceStagePlan, build_allocation_trace, build_trace_plan
from .common.types import (
    AllocationAlertRecord,
    AllocationLogRecord,
    JoinKeyValues,
    TraceStageLiteral,
    TraceStageRecord,
)
from .counter import normalize_digits, strip_hidden_chars
from .policy_loader import PolicyConfig, load_policy
from .reason.selection_reason import build_selection_reason_rows as _build_selection_reason_rows

ProgressFn = Callable[[int, str], None]

__all__ = [
    "ProgressFn",
    "AllocationResult",
    "allocate_student",
    "allocate_batch",
    "build_selection_reason_rows",
]

_STUDENT_NATIONAL_KEYS: Tuple[str, ...] = (
    "student_national_code",
    "student_national_id",
    "national_id",
    "کدملی دانش‌آموز",
    "کدملی",
    "کد ملی",
)
_MENTOR_ALIAS_KEYS: Tuple[str, ...] = (
    "mentor_alias_code",
    "mentor_alias_postal_code",
    "mentor_postal_code",
    "alias",
    "alias_norm",
    "alias_normal",
    "جایگزین",
    "جایگزین | alias",
    "کد جایگزین پشتیبان",
    "کدپستی",
    "کد پستی",
)

_JOIN_STAGE_FAILURE_ORDER: Tuple[TraceStageLiteral, ...] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
)

_STAGE_LABEL_FA: Dict[str, str] = {
    "type": CANON_EN_TO_FA.get("group_code", "type"),
    "group": CANON_EN_TO_FA.get("exam_group", "group"),
    "gender": CANON_EN_TO_FA.get("gender", "gender"),
    "graduation_status": CANON_EN_TO_FA.get("graduation_status", "graduation_status"),
    "center": CANON_EN_TO_FA.get("center", "center"),
    "finance": CANON_EN_TO_FA.get("finance", "finance"),
    "school": CANON_EN_TO_FA.get("school_code", "school"),
    "capacity_gate": "capacity",
}


def _normalize_digit_code(value: object, *, length: int | None = None, pad: bool = False) -> str:
    """نرمال‌سازی ورودی‌های عددی به رشتهٔ digits پایدار برای خروجی اکسل."""

    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except TypeError:
        pass
    text = strip_hidden_chars(normalize_digits(str(value).strip()))
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    if length is not None:
        if len(digits) > length:
            digits = digits[-length:]
        if pad:
            digits = digits.zfill(length)
        elif len(digits) < length:
            return ""
    return digits


def _extract_student_national_code(student: Mapping[str, object]) -> str:
    """بازیابی امن کد ملی دانش‌آموز از کلیدهای چندزبانهٔ ورودی."""

    for key in _STUDENT_NATIONAL_KEYS:
        value = student.get(key)
        normalized = _normalize_digit_code(value, length=10, pad=True)
        if normalized:
            return normalized
    return ""


def _extract_mentor_alias_code(mentor_row: Mapping[str, object] | pd.Series) -> str:
    """دریافت کد جایگزین/پستی پشتیبان با حذف نویز ورودی."""

    for key in _MENTOR_ALIAS_KEYS:
        value = mentor_row.get(key)
        normalized = _normalize_digit_code(value, length=10, pad=True)
        if normalized:
            return normalized
    return ""


def _normalize_mentor_identifier(value: object) -> object | None:
    """تبدیل امن شناسهٔ پشتیبان به مقدار قابل جست‌وجو در state.

    مثال::

        >>> _normalize_mentor_identifier(" EMP-7 ")
        'EMP-7'
    """

    if value is None:
        return None
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        return _normalize_mentor_identifier(value.iloc[0])
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return None
        return _normalize_mentor_identifier(value.iloc[0])
    if isinstance(value, (list, tuple, set, dict)):
        return None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except TypeError:
        pass
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return value


def _resolve_mentor_identifier(
    result: AllocationResult, *, policy: PolicyConfig
) -> object:
    """بازیابی شناسهٔ پشتیبان با اولویت: log → سطر فارسی → سطر canonical.

    مثال::

        >>> _resolve_mentor_identifier(result, policy=policy)
        'EMP-101'
    """

    mentor_identifier_logged = _normalize_mentor_identifier(result.log.get("mentor_id"))
    if mentor_identifier_logged is not None:
        result.log["mentor_id"] = mentor_identifier_logged
        return mentor_identifier_logged

    if result.mentor_row is None:
        raise KeyError("Mentor identifier missing: row not provided")

    mentor_identifier = _normalize_mentor_identifier(
        result.mentor_row.get("کد کارمندی پشتیبان")
    )
    if mentor_identifier is not None:
        result.log["mentor_id"] = mentor_identifier
        return mentor_identifier

    mentor_row_en = canonicalize_headers(
        result.mentor_row.to_frame().T,
        header_mode=policy.excel.header_mode_internal,
    ).iloc[0]
    mentor_identifier = _normalize_mentor_identifier(mentor_row_en.get("mentor_id"))
    if mentor_identifier is not None:
        result.log["mentor_id"] = mentor_identifier
        return mentor_identifier

    raise KeyError("Mentor identifier missing from allocation log and row")


def _noop_progress(_: int, __: str) -> None:
    """تابع پیش‌فرض progress که کاری انجام نمی‌دهد."""


@dataclass(frozen=True)
class AllocationResult:
    """خروجی تخصیص یک دانش‌آموز."""

    mentor_row: pd.Series | None
    trace: List[TraceStageRecord]
    log: AllocationLogRecord


def _maybe_int_from_text(value: object) -> int | None:
    """Try to coerce heterogeneous inputs to a stable integer identifier."""

    try:
        numeric = pd.to_numeric([value], errors="coerce")[0]
    except Exception:
        return None
    if isinstance(numeric, Number):
        if isinstance(numeric, float):
            if float(numeric).is_integer():
                return int(numeric)
            return None
        return int(numeric)
    if pd.isna(numeric):  # type: ignore[arg-type]
        return None
    return None


def _resolve_mentor_state_entry(
    mentor_state: Mapping[Any, Mapping[str, int]],
    identifier: object,
) -> tuple[Any | None, Mapping[str, int] | None]:
    """Resolve mentor state entry while tolerating dtype mismatches."""

    if isinstance(identifier, pd.Series):
        if identifier.empty:
            identifier = None
        else:
            identifier = identifier.iloc[0]
    if identifier is None:
        return None, None
    try:
        if pd.isna(identifier):  # type: ignore[arg-type]
            return None, None
    except TypeError:
        pass

    candidates: List[Any] = []
    seen: set[str] = set()

    def _push(value: object) -> None:
        if value is None:
            return
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return
        except TypeError:
            pass
        marker = repr(value)
        if marker in seen:
            return
        seen.add(marker)
        candidates.append(value)

    _push(identifier)
    if isinstance(identifier, str):
        stripped = identifier.strip()
        if stripped and stripped != identifier:
            _push(stripped)
        numeric_candidate = _maybe_int_from_text(stripped)
        if numeric_candidate is not None:
            _push(numeric_candidate)
    else:
        numeric_candidate = _maybe_int_from_text(identifier)
        if numeric_candidate is not None:
            _push(numeric_candidate)
        _push(str(identifier))

    for candidate in candidates:
        entry = mentor_state.get(candidate)
        if entry is not None:
            return candidate, entry
    return None, None


class JoinKeyDataMissingError(ValueError):
    """خطای اختصاصی برای کمبود دادهٔ کلیدهای Join در ورودی دانش‌آموز."""

    def __init__(
        self, missing_columns: Sequence[str], join_map: Mapping[str, int]
    ) -> None:
        super().__init__("DATA_MISSING")
        self.missing_columns: Tuple[str, ...] = tuple(missing_columns)
        self.join_map: Dict[str, int] = dict(join_map)


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


def _collect_join_key_map(
    student: Mapping[str, object], policy: PolicyConfig
) -> tuple[Dict[str, int], Tuple[str, ...]]:
    join_map: Dict[str, int] = {}
    missing_columns: list[str] = []
    school_column = policy.columns.school_code
    school_code_resolved: StudentSchoolCode | None = None
    for column in policy.join_keys:
        normalized = column.replace(" ", "_")
        allow_zero = policy.school_code_empty_as_zero and column == school_column
        if allow_zero:
            if school_code_resolved is None:
                school_code_resolved = resolve_student_school_code(student, policy)
            school_code = school_code_resolved
            if school_code.missing:
                join_map[normalized] = -1
                missing_columns.append(column)
            else:
                join_map[normalized] = int(school_code.value or 0)
            continue
        try:
            value = _student_value(student, column)
        except KeyError:
            join_map[normalized] = -1
            missing_columns.append(column)
            continue

        try:
            join_map[normalized] = _coerce_int(value)
        except ValueError:
            join_map[normalized] = -1
            missing_columns.append(column)
    return join_map, tuple(missing_columns)


def _build_log_from_join_map(
    student: Mapping[str, object], join_map: Mapping[str, int]
) -> AllocationLogRecord:
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
        "rule_reason_code": None,
        "rule_reason_text": None,
        "rule_reason_details": None,
        "fairness_reason_code": None,
        "fairness_reason_text": None,
        "alerts": [],
    }
    return log


def _normalize_rule_details(payload: object) -> Mapping[str, object] | None:
    if isinstance(payload, Mapping):
        return dict(payload)
    return None


def _derive_rule_reason(
    trace: Sequence[TraceStageRecord],
) -> tuple[str, str, Mapping[str, object] | None]:
    """تعیین کد/متن دلیل بر اساس اولین مرحلهٔ رد."""

    fallback = build_reason(ReasonCode.OK)
    if not trace:
        return fallback.code, fallback.message_fa, None
    for record in trace:
        extras = record.get("extras") or {}
        code = extras.get("rule_reason_code")
        message = extras.get("rule_reason_text")
        details = _normalize_rule_details(extras.get("rule_details"))
        after = int(record.get("total_after", 0))
        if code and (not record.get("matched") or after == 0):
            return str(code), str(message or fallback.message_fa), details
    tail_extras = trace[-1].get("extras") or {}
    code = tail_extras.get("rule_reason_code")
    message = tail_extras.get("rule_reason_text")
    details = _normalize_rule_details(tail_extras.get("rule_details"))
    if code:
        return (str(code), str(message or fallback.message_fa), details)
    return fallback.code, fallback.message_fa, None


def _display_expected_value(value: object) -> str:
    """تبدیل مقدار مورد انتظار به متن قابل‌گزارش."""

    if value is None:
        return "نامشخص"
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return "نامشخص"
    except Exception:
        pass
    text = str(value).strip()
    return text or "نامشخص"


def _format_alert_message(stage: str, record: TraceStageRecord | None) -> str:
    """ساخت پیام فارسی برای هشدار حذف کاندید در یک مرحله."""

    label = _STAGE_LABEL_FA.get(stage, stage)
    expected_value = record.get("expected_value") if record else None
    if stage == "capacity_gate":
        column = record.get("column") if record else None
        column_text = str(column or "remaining_capacity")
        return f"ظرفیت فعال در ستون {column_text} صفر است؛ هیچ منتوری باقی نماند."
    value_text = _display_expected_value(expected_value)
    return f"فیلتر {label} با مقدار {value_text} هیچ کاندیدی باقی نگذاشت."


def _derive_failure_alerts(
    stage_candidate_counts: Mapping[str, int],
    trace: Sequence[TraceStageRecord],
    *,
    error_type: str,
) -> List[AllocationAlertRecord]:
    """استخراج هشدارهای ساخت‌یافته براساس stage و trace."""

    if not stage_candidate_counts:
        return []
    if error_type == "ELIGIBILITY_NO_MATCH":
        stage_sequence: Tuple[str, ...] = _JOIN_STAGE_FAILURE_ORDER
    elif error_type == "CAPACITY_FULL":
        stage_sequence = ("capacity_gate",)
    else:
        return []
    failing_stage = next(
        (stage for stage in stage_sequence if stage_candidate_counts.get(stage) == 0),
        None,
    )
    if failing_stage is None:
        return []
    record = next((item for item in trace if item.get("stage") == failing_stage), None)
    message = _format_alert_message(failing_stage, record)
    context: Dict[str, Any] = {}
    if record is not None:
        context = {
            "column": record.get("column"),
            "expected_value": record.get("expected_value"),
            "total_before": record.get("total_before"),
            "total_after": record.get("total_after"),
        }
        extras = record.get("extras") or {}
        if extras:
            context["extras"] = dict(extras)
    alert: AllocationAlertRecord = {
        "code": str(error_type),
        "stage": str(failing_stage),
        "message": message,
        "context": context,
    }
    return [alert]


def _emit_alert_progress(
    alerts: Sequence[AllocationAlertRecord], alert_progress: ProgressFn | None
) -> None:
    """ارسال پیام هشدار به progress hook برای مشاهدهٔ لحظه‌ای."""

    if not alerts or alert_progress in (None, _noop_progress):
        return
    for alert in alerts:
        stage = str(alert.get("stage") or "join")
        pct = 30 if stage == "capacity_gate" else 5
        context = alert.get("context") or {}
        expected = context.get("expected_value")
        try:
            if expected is not None and pd.isna(expected):  # type: ignore[arg-type]
                expected = None
        except Exception:
            pass
        column = context.get("column")
        hints: List[str] = []
        if expected not in (None, ""):
            hints.append(f"مقدار={expected}")
        if column:
            hints.append(f"ستون={column}")
        hint_text = f" ({' | '.join(hints)})" if hints else ""
        message = alert.get("message") or "هشدار"
        alert_progress(pct, f"⚠️ {alert.get('code', 'WARNING')} - {message}{hint_text}")


def _build_log_base(
    student: Mapping[str, object],
    policy: PolicyConfig,
    *,
    join_map: Mapping[str, int] | None = None,
    missing: Sequence[str] | None = None,
) -> AllocationLogRecord:
    """ساخت لاگ پایه با استفاده از نگاشت ازپیش‌محاسبه‌شدهٔ کلیدهای join."""

    if join_map is None or missing is None:
        join_map, missing = _collect_join_key_map(student, policy)
    if missing:
        raise JoinKeyDataMissingError(missing, join_map)
    return _build_log_from_join_map(student, join_map)


def _normalize_students(df: pd.DataFrame, policy: PolicyConfig) -> pd.DataFrame:
    """نرمال‌سازی قاب دانش‌آموز برای ورودی تابع allocate_batch."""

    return canonicalize_students_frame(df, policy=policy)


def _normalize_pool(df: pd.DataFrame, policy: PolicyConfig) -> pd.DataFrame:
    """تبدیل قاب استخر به نمای canonical بدون پاک‌سازی تهاجمی."""

    return canonicalize_pool_frame(
        df,
        policy=policy,
        sanitize_pool=False,
        pool_source="inspactor",
    )


def _ensure_students_canonical(
    df: pd.DataFrame, policy: PolicyConfig
) -> pd.DataFrame:
    students = df.copy(deep=True)
    missing = [column for column in policy.join_keys if column not in students.columns]
    if missing:
        raise ValueError(f"Canonical student frame missing columns: {missing}")
    student_id = students.get("student_id")
    if student_id is not None:
        student_id_series = ensure_series(student_id)
        empty_mask = student_id_series.astype("string").str.strip().eq("")
        if empty_mask.any():
            raise ValueError("Canonical student frame contains empty student_id values")
    for column in policy.join_keys:
        series = pd.to_numeric(students[column], errors="coerce")
        if series.isna().any():
            raise ValueError(f"Canonical student join key '{column}' has invalid values")
    return students


def _ensure_pool_canonical(
    df: pd.DataFrame,
    policy: PolicyConfig,
    capacity_column: str,
) -> pd.DataFrame:
    """اعتبارسنجی قاب استخر canonical و تضمین ستون‌های حیاتی.

    Raises:
        ValueError: اگر هر یک از ستون‌های join یا ظرفیت در دیتافریم موجود نباشد.
    """

    pool = df.copy(deep=True)
    required = set(policy.join_keys) | {
        "کد کارمندی پشتیبان",
        "mentor_id",
        "remaining_capacity",
        "allocations_new",
        "occupancy_ratio",
    }
    missing = [column for column in required if column not in pool.columns]
    if missing:
        raise ValueError(f"Canonical pool frame missing columns: {missing}")
    numeric_candidates = {
        capacity_column,
        policy.columns.remaining_capacity,
        "remaining_capacity",
    }
    for column in numeric_candidates:
        if column in pool.columns:
            numeric = pd.to_numeric(pool[column], errors="coerce")
            if numeric.isna().any():
                raise ValueError(f"Canonical pool column '{column}' has non-numeric values")
    return pool


def allocate_student(
    student: Mapping[str, object],
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    progress: ProgressFn = _noop_progress,
    capacity_column: str | None = None,
    trace_plan: Sequence[TraceStagePlan] | None = None,
    stage_rules: Mapping[TraceStageLiteral, Rule] | None = None,
    state: Dict[object, Dict[str, int]] | None = None,
    pool_state_view: pd.DataFrame | None = None,
    alert_progress: ProgressFn | None = None,
) -> AllocationResult:
    """تخصیص تک‌دانش‌آموز با حفظ Trace و لاگ کامل."""
    if policy is None:
        policy = load_policy()
    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)
    if trace_plan is None:
        trace_plan = build_trace_plan(policy, capacity_column=resolved_capacity_column)
    if stage_rules is None:
        stage_rules = default_stage_rule_map()
    if alert_progress is None:
        alert_progress = progress

    join_map, missing_columns = _collect_join_key_map(student, policy)

    progress(5, "prefilter")
    stage_candidate_counts: Dict[str, int] = {}

    def _record_stage(stage: str, count: int) -> None:
        stage_candidate_counts[stage] = int(count)

    eligible = apply_join_filters(
        candidate_pool,
        student,
        policy=policy,
        student_join_map=join_map,
        tracker=_record_stage,
    )
    stage_candidate_counts.setdefault("capacity_gate", 0)
    trace = build_allocation_trace(
        student,
        candidate_pool,
        policy=policy,
        stage_plan=trace_plan,
        capacity_column=resolved_capacity_column,
        stage_rules=stage_rules,
    )
    rule_reason_code, rule_reason_text, rule_details = _derive_rule_reason(trace)

    try:
        log = _build_log_base(
            student,
            policy,
            join_map=join_map,
            missing=missing_columns,
        )
    except JoinKeyDataMissingError as exc:
        log = _build_log_from_join_map(student, exc.join_map)
        log.update(
            {
                "rule_reason_code": rule_reason_code,
                "rule_reason_text": rule_reason_text,
                "rule_reason_details": rule_details,
            }
        )
        log["candidate_count"] = int(eligible.shape[0])
        log["stage_candidate_counts"] = dict(stage_candidate_counts)
        missing_text = ", ".join(exc.missing_columns)
        log.update(
            {
                "error_type": "DATA_MISSING",
                "detailed_reason": f"Missing student join key data: {missing_text}",
                "suggested_actions": [
                    "تکمیل دادهٔ دانش‌آموز",
                    "بازبینی StudentReport",
                ],
            }
        )
        return AllocationResult(None, trace, log)

    def _fail_allocation(
        detailed_reason: str,
        *,
        error_type: str = "INTERNAL_ERROR",
        suggested_actions: Sequence[str] | None = None,
        extra_updates: Mapping[str, object] | None = None,
    ) -> AllocationResult:
        payload = {
            "detailed_reason": detailed_reason,
            "error_type": error_type,
            "suggested_actions": list(suggested_actions or []),
        }
        alerts = _derive_failure_alerts(
            stage_candidate_counts,
            trace,
            error_type=error_type,
        )
        if alerts:
            existing = log.get("alerts")
            if isinstance(existing, list):
                existing.extend(alerts)
            else:
                log["alerts"] = list(alerts)
            _emit_alert_progress(alerts, alert_progress)
        if extra_updates:
            payload.update(extra_updates)
        log.update(payload)
        return AllocationResult(None, trace, log)

    log["candidate_count"] = int(eligible.shape[0])
    log["stage_candidate_counts"] = dict(stage_candidate_counts)
    log["rule_reason_code"] = rule_reason_code
    log["rule_reason_text"] = rule_reason_text
    log["rule_reason_details"] = rule_details

    if eligible.empty:
        return _fail_allocation(
            "No candidates matched join keys",
            error_type="ELIGIBILITY_NO_MATCH",
            suggested_actions=["بازبینی دادهٔ ورودی", "تطبیق join keys"],
        )

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

    capacity_series = ensure_series(state_view_en.loc[eligible.index, capacity_column_name])
    capacity_numeric = pd.to_numeric(capacity_series, errors="coerce").fillna(0).astype(int)
    capacity_mask = capacity_numeric > 0
    capacity_filtered = eligible.loc[capacity_mask.values]
    stage_candidate_counts["capacity_gate"] = int(capacity_mask.sum())
    log["stage_candidate_counts"] = dict(stage_candidate_counts)

    if capacity_filtered.empty:
        return _fail_allocation(
            "No capacity among matched candidates",
            error_type="CAPACITY_FULL",
            suggested_actions=["افزایش ظرفیت", "بازنگری محدودیت‌ها"],
        )

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
    fairness_reason = ranked.attrs.get("fairness_reason")
    if fairness_reason is not None:
        fairness_code = getattr(fairness_reason, "code", None)
        fairness_message = getattr(fairness_reason, "message_fa", None)
        log["fairness_reason_code"] = fairness_code
        if fairness_code and fairness_message:
            formatted = f"[{fairness_code}] {fairness_message}"
        else:
            formatted = fairness_message
        log["fairness_reason_text"] = formatted

    if ranked.empty:
        return _fail_allocation(
            "Ranking policy returned no candidates",
            suggested_actions=[
                "بازبینی دادهٔ استخر پشتیبان",
                "بررسی قوانین رتبه‌بندی",
            ],
        )

    try:
        first_ranked = ranked.head(1)
    except Exception:  # pragma: no cover - defensive, head() should not fail
        first_ranked = ranked.iloc[:1]
    if first_ranked.empty:
        return _fail_allocation(
            "Ranked candidates lost during extraction",
            suggested_actions=[
                "بازبینی خروجی apply_ranking_policy",
                "بررسی دادهٔ استخر پس از رتبه‌بندی",
            ],
        )

    try:
        chosen_row = first_ranked.iloc[0].copy()
    except IndexError:
        return _fail_allocation(
            "Ranked candidates missing despite non-empty frame",
            suggested_actions=[
                "بازبینی منطق رتبه‌بندی",
                "بررسی فیلترهای capacity",
            ],
        )

    chosen_index = chosen_row["__candidate_index__"]
    ranked = ranked.drop(columns=["__candidate_index__"], errors="ignore")
    ranked_en = canonicalize_headers(ranked, header_mode=policy.excel.header_mode_internal)
    if ranked_en.empty:
        return _fail_allocation(
            "Canonicalization returned empty ranked view",
            suggested_actions=[
                "بازبینی canonicalize_headers",
                "هماهنگی schema استخر با Policy",
            ],
        )
    try:
        chosen_en = ranked_en.iloc[0]
    except IndexError:
        return _fail_allocation(
            "Unable to read ranked row after canonicalization",
            suggested_actions=[
                "بازبینی stage رتبه‌بندی",
                "بررسی canonicalize_headers",
            ],
        )

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
            "mentor_id": mentor_id_text,
            "occupancy_ratio": float(occupancy_value),
            "selection_reason": "policy: min occ → min alloc → natural mentor_id",
            "tie_breakers": tie_breakers,
            "capacity_before": int(capacity_before),
            "capacity_after": int(capacity_after),
            "stage_candidate_counts": dict(stage_candidate_counts),
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
    frames_already_canonical: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """تخصیص دسته‌ای دانش‌آموزان و بازگشت خروجی‌های چهارتایی.

    Raises:
        ValueError: زمانی که قاب‌های canonical قرارداد ستون‌ها را رعایت نکرده باشند.
        ValueError("DATA_MISSING"): نسخهٔ سازگار با CLI برای خطاهای داده‌ای.
    """
    if policy is None:
        policy = load_policy()

    resolved_capacity_column = _resolve_capacity_column(policy, capacity_column)
    capacity_internal = canonicalize_headers(
        pd.DataFrame(columns=[resolved_capacity_column]),
        header_mode=policy.excel.header_mode_internal,
    ).columns[0]

    def _validate_pool(frame: pd.DataFrame) -> pd.DataFrame:
        try:
            return _ensure_pool_canonical(
                frame, policy, resolved_capacity_column
            )
        except ValueError as exc:
            if frames_already_canonical:
                raise ValueError("DATA_MISSING") from exc
            raise

    if frames_already_canonical:
        students_norm = _ensure_students_canonical(students, policy)
        pool_norm = _validate_pool(candidate_pool)
    else:
        students_norm = _normalize_students(students, policy)
        pool_norm = _validate_pool(_normalize_pool(candidate_pool, policy))
    extra_columns = [
        column for column in pool_norm.columns if column not in candidate_pool.columns
    ]
    pool_with_ids = inject_mentor_id(pool_norm, build_mentor_id_map(pool_norm))
    if "allocations_new" not in pool_with_ids.columns:
        pool_with_ids["allocations_new"] = 0
    if "occupancy_ratio" not in pool_with_ids.columns:
        pool_with_ids["occupancy_ratio"] = 0.0

    pool_internal = canonicalize_headers(pool_with_ids, header_mode="en")
    pool_internal = pool_internal.loc[:, ~pool_internal.columns.duplicated(keep="first")]
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
    stage_rules = default_stage_rule_map()

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
            stage_rules=stage_rules,
            state=mentor_state,
            pool_state_view=pool_internal,
            alert_progress=progress,
        )
        logs.append(result.log)
        for stage in result.trace:
            trace_rows.append({"student_id": result.log["student_id"], **stage})

        if result.mentor_row is not None:
            chosen_index = result.mentor_row.name
            mentor_identifier = _resolve_mentor_identifier(result, policy=policy)
            resolved_identifier, state_entry = _resolve_mentor_state_entry(
                mentor_state, mentor_identifier
            )
            if state_entry is None:
                raise KeyError(
                    f"Mentor '{mentor_identifier}' missing from state after allocation"
                )
            pool_internal.loc[chosen_index, capacity_internal] = state_entry["remaining"]
            if (
                capacity_internal != "remaining_capacity"
                and "remaining_capacity" in pool_internal.columns
            ):
                pool_internal.loc[chosen_index, "remaining_capacity"] = state_entry["remaining"]
            pool_internal.loc[chosen_index, "allocations_new"] = state_entry["alloc_new"]
            initial_value = max(int(state_entry.get("initial", 0)), 1)
            pool_internal.loc[chosen_index, "occupancy_ratio"] = (
                (int(state_entry.get("initial", 0)) - state_entry["remaining"]) / initial_value
            )
            pool_with_ids.loc[chosen_index, resolved_capacity_column] = state_entry["remaining"]
            if (
                resolved_capacity_column != "remaining_capacity"
                and "remaining_capacity" in pool_with_ids.columns
            ):
                pool_with_ids.loc[chosen_index, "remaining_capacity"] = state_entry["remaining"]
            pool_with_ids.loc[chosen_index, "allocations_new"] = state_entry["alloc_new"]
            pool_with_ids.loc[chosen_index, "occupancy_ratio"] = pool_internal.loc[
                chosen_index, "occupancy_ratio"
            ]

            mentor_id_display = result.log.get("mentor_id")
            if mentor_id_display is None:
                mentor_id_display = resolved_identifier
            student_national_code = _extract_student_national_code(student_dict)
            mentor_alias_code = _extract_mentor_alias_code(result.mentor_row)
            allocations.append(
                {
                    "student_id": student_dict.get("student_id", ""),
                    "student_national_code": student_national_code,
                    "mentor": result.mentor_row.get("پشتیبان", ""),
                    "mentor_id": "" if mentor_id_display is None else str(mentor_id_display),
                    "mentor_alias_code": mentor_alias_code,
                }
            )

    progress(100, "done")

    allocations_df = pd.DataFrame(allocations)
    logs_df = pd.DataFrame(logs)
    trace_df = pd.DataFrame(trace_rows)

    pool_output = pool_with_ids.copy()
    original_columns = list(candidate_pool.columns)
    desired_columns = original_columns + [
        column for column in extra_columns if column not in original_columns
    ]
    for column in desired_columns:
        if column not in pool_output.columns:
            pool_output[column] = pd.NA
    pool_output = pool_output.loc[:, desired_columns]

    for column in original_columns:
        if column in candidate_pool.columns:
            try:
                pool_output[column] = pool_output[column].astype(candidate_pool[column].dtype)
            except (TypeError, ValueError):
                continue

    for entry in mentor_state.values():
        if entry["remaining"] < 0:
            raise ValueError("Negative remaining capacity detected after allocation")

    internal_remaining = pd.to_numeric(
        ensure_series(pool_internal[capacity_internal]), errors="coerce"
    ).fillna(0)
    if (internal_remaining < 0).any():
        raise ValueError("Pool capacity column contains negative values after allocation")

    return allocations_df, pool_output, logs_df, trace_df


def build_selection_reason_rows(
    allocations: pd.DataFrame,
    students: pd.DataFrame,
    mentors: pd.DataFrame,
    *,
    policy: PolicyConfig,
    logs: pd.DataFrame | None = None,
    trace: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """واسطهٔ سازگار برای ساخت شیت دلایل انتخاب پشتیبان."""

    return _build_selection_reason_rows(
        allocations,
        students,
        mentors,
        policy=policy,
        logs=logs,
        trace=trace,
    )



