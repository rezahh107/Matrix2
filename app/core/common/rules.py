"""Rule/Reason system برای اتصال Trace و گزارش Explainability.

تمامی Ruleها باید pure باشند و فقط روی رکورد مرحله یا دادهٔ دانش‌آموز کار
کنند. این ماژول هیچ وابستگی به pandas/I-O ندارد و صرفاً ReasonCode مناسب را
بر اساس شمارش کاندیدا و دادهٔ ورودی برمی‌گرداند.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Integral, Real
from typing import Any, Mapping, Protocol

from .reasons import LocalizedReason, ReasonCode, build_reason
from .types import CANONICAL_TRACE_ORDER, TraceStageLiteral, TraceStageRecord

__all__ = [
    "Rule",
    "RuleContext",
    "RuleResult",
    "CandidateStageRule",
    "StageRecordGuard",
    "apply_rule",
    "compose_rules",
    "default_stage_rule_map",
]


@dataclass(frozen=True, slots=True)
class RuleContext:
    """کانتکست مینیمال موردنیاز برای اجرای یک Rule.

    مثال::

        >>> ctx = RuleContext(stage_record={"stage": "gender", ...})
        >>> apply_rule(default_stage_rule_map()["gender"], ctx).reason.code
        <ReasonCode.OK: 'OK'>
    """

    stage_record: TraceStageRecord
    student: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class RuleResult:
    """خروجی استاندارد Rule شامل وضعیت و پیام محلی."""

    stage: TraceStageLiteral
    passed: bool
    reason: LocalizedReason
    details: Mapping[str, object] | None = None


class Rule(Protocol):
    """پروتکل عمومی اجرای Rule بدون I/O و DataFrame."""

    def __call__(self, context: RuleContext) -> RuleResult:
        ...


def apply_rule(rule: Rule, context: RuleContext) -> RuleResult:
    """اجرای ایمن یک Rule و تضمین نوع خروجی."""

    result = rule(context)
    if not isinstance(result, RuleResult):  # pragma: no cover - محافظ تزریق اشتباه
        raise TypeError("rule must return RuleResult")
    return result


def compose_rules(*rules: Rule) -> Rule:
    """ترکیب چند Rule با short-circuit بر روی اولین خطا."""

    def _combined(context: RuleContext) -> RuleResult:
        last_result: RuleResult | None = None
        for rule in rules:
            last_result = rule(context)
            if not last_result.passed:
                return last_result
        if last_result is None:
            raise ValueError("compose_rules requires at least one rule")
        return last_result

    return _combined


@dataclass(frozen=True, slots=True)
class StageRecordGuard:
    """نگهبان سازگار با Policy برای بررسی صحت رکورد مرحله قبل از Rule اصلی.

    مثال::

        >>> record = {"stage": "gender", "total_before": 5, "total_after": 5}
        >>> guard = StageRecordGuard(stage="gender")
        >>> guard(RuleContext(stage_record=record)).passed
        True
    """

    stage: TraceStageLiteral
    allowed_stages: tuple[TraceStageLiteral, ...] = CANONICAL_TRACE_ORDER

    def __call__(self, context: RuleContext) -> RuleResult:
        record = context.stage_record
        record_stage = record.get("stage")
        before = _coerce_detail_int(record.get("total_before"))
        after = _coerce_detail_int(record.get("total_after"))
        issue: str | None = None

        if record_stage not in self.allowed_stages:
            issue = "invalid_stage"
        elif record_stage != self.stage:
            issue = "stage_mismatch"
        elif before is None or after is None:
            issue = "missing_totals"
        elif before < after:
            issue = "before_lt_after"
        elif after < 0:
            issue = "negative_total_after"

        if issue is not None:
            details: dict[str, object] = {
                "issue": issue,
                "record_stage": record_stage,
                "expected_stage": self.stage,
                "total_before": record.get("total_before"),
                "total_after": record.get("total_after"),
            }
            if issue == "invalid_stage":
                details["allowed_stages"] = self.allowed_stages
            return RuleResult(
                stage=self.stage,
                passed=False,
                reason=build_reason(ReasonCode.INTERNAL_ERROR),
                details=details,
            )

        return RuleResult(
            stage=self.stage,
            passed=True,
            reason=build_reason(ReasonCode.OK),
            details={
                "total_before": before,
                "total_after": after,
            },
        )


@dataclass(frozen=True, slots=True)
class CandidateStageRule:
    """Rule عمومی بر اساس تعداد کاندیدای باقی‌مانده در یک مرحله.

    پارامتر `detail_keys` اجازه می‌دهد اطلاعات کلیدی از `record['extras']`
    به خروجی منتقل شود تا selection_reason بتواند پیام دقیق بسازد.
    """

    stage: TraceStageLiteral
    failure_code: ReasonCode
    detail_keys: tuple[str, ...] = ()

    def __call__(self, context: RuleContext) -> RuleResult:
        record = context.stage_record
        after = int(record["total_after"])
        passed = after > 0
        code = ReasonCode.OK if passed else self.failure_code
        extras = {
            "column": record["column"],
            "total_before": int(record["total_before"]),
            "total_after": after,
            "expected_value": record.get("expected_value"),
            "stage": record.get("stage"),
        }
        source_extras = record.get("extras") or {}
        for key in self.detail_keys:
            if key in source_extras:
                extras[key] = source_extras[key]
        if (not passed) and _should_augment_join_details(self.stage):
            join_details = _extract_join_detail_values(record)
            if join_details:
                extras.update(join_details)
        return RuleResult(
            stage=self.stage,
            passed=passed,
            reason=build_reason(code),
            details=extras,
        )


_COMMON_DETAIL_KEYS = (
    "join_value_raw",
    "join_value_norm",
    "expected_op",
    "expected_threshold",
)


_SENSITIVE_JOIN_STAGES = frozenset({"gender", "center", "school"})


def _should_augment_join_details(stage: TraceStageLiteral) -> bool:
    return stage in _SENSITIVE_JOIN_STAGES


def _coerce_detail_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        numeric = float(value)
        if math.isnan(numeric):  # pragma: no cover - محافظ NaN
            return None
        if numeric.is_integer():
            return int(numeric)
        return None
    return None


def _extract_join_detail_values(record: TraceStageRecord) -> Mapping[str, Any] | None:
    extras = record.get("extras") or {}
    student_value = None
    mentor_value = None
    for key in ("join_value_norm", "school_code_norm"):
        if key in extras:
            student_value = _coerce_detail_int(extras.get(key))
        if student_value is not None:
            break
    for key in ("mentor_value_norm", "mentor_value_raw"):
        if key in extras:
            mentor_value = _coerce_detail_int(extras.get(key))
        if mentor_value is not None:
            break
    if student_value is None and mentor_value is None:
        return None
    details: dict[str, Any] = {}
    if student_value is not None:
        details["student_value"] = student_value
    if mentor_value is not None:
        details["mentor_value"] = mentor_value
    if student_value is not None and mentor_value is not None:
        details["normalize_diff"] = student_value - mentor_value
    return details or None


_DEFAULT_RULE_CODES: Mapping[TraceStageLiteral, tuple[ReasonCode, tuple[str, ...]]] = {
    "type": (ReasonCode.TYPE_MISMATCH, _COMMON_DETAIL_KEYS),
    "group": (ReasonCode.GROUP_MISMATCH, _COMMON_DETAIL_KEYS),
    "gender": (ReasonCode.GENDER_MISMATCH, _COMMON_DETAIL_KEYS),
    "graduation_status": (ReasonCode.GRADUATION_STATUS_MISMATCH, _COMMON_DETAIL_KEYS),
    "center": (ReasonCode.CENTER_MISMATCH, _COMMON_DETAIL_KEYS),
    "finance": (ReasonCode.FINANCE_MISMATCH, _COMMON_DETAIL_KEYS),
    "school": (
        ReasonCode.SCHOOL_STATUS_MISMATCH,
        _COMMON_DETAIL_KEYS
        + (
            "school_code_raw",
            "school_code_norm",
            "school_status_resolved",
            "school_filter_applied",
        ),
    ),
    "capacity_gate": (
        ReasonCode.CAPACITY_FULL,
        (
            "expected_op",
            "expected_threshold",
            "capacity_before",
            "capacity_after",
        ),
    ),
}


def default_stage_rule_map() -> Mapping[TraceStageLiteral, Rule]:
    """تولید Ruleهای پیش‌فرض مطابق ترتیب ۸ مرحله‌ای."""

    rules: dict[TraceStageLiteral, Rule] = {}
    for stage, (code, detail_keys) in _DEFAULT_RULE_CODES.items():
        guard = StageRecordGuard(stage=stage)
        candidate_rule = CandidateStageRule(
            stage=stage,
            failure_code=code,
            detail_keys=detail_keys,
        )
        rules[stage] = compose_rules(guard, candidate_rule)
    return rules
