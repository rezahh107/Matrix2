"""Rule/Reason system برای اتصال Trace و گزارش Explainability.

تمامی Ruleها باید pure باشند و فقط روی رکورد مرحله یا دادهٔ دانش‌آموز کار
کنند. این ماژول هیچ وابستگی به pandas/I-O ندارد و صرفاً ReasonCode مناسب را
بر اساس شمارش کاندیدا و دادهٔ ورودی برمی‌گرداند.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from .reasons import LocalizedReason, ReasonCode, build_reason
from .types import TraceStageLiteral, TraceStageRecord

__all__ = [
    "Rule",
    "RuleContext",
    "RuleResult",
    "CandidateStageRule",
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
        return RuleResult(
            stage=self.stage,
            passed=passed,
            reason=build_reason(code),
            details=extras,
        )


_DEFAULT_RULE_CODES: Mapping[TraceStageLiteral, tuple[ReasonCode, tuple[str, ...]]] = {
    "type": (ReasonCode.TYPE_MISMATCH, ()),
    "group": (ReasonCode.GROUP_MISMATCH, ()),
    "gender": (ReasonCode.GENDER_MISMATCH, ()),
    "graduation_status": (ReasonCode.GRADUATION_STATUS_MISMATCH, ()),
    "center": (ReasonCode.CENTER_MISMATCH, ()),
    "finance": (ReasonCode.FINANCE_MISMATCH, ()),
    "school": (
        ReasonCode.SCHOOL_STATUS_MISMATCH,
        ("school_code_raw", "school_code_norm", "school_status_resolved"),
    ),
    "capacity_gate": (ReasonCode.CAPACITY_FULL, ()),
}


def default_stage_rule_map() -> Mapping[TraceStageLiteral, Rule]:
    """تولید Ruleهای پیش‌فرض مطابق ترتیب ۸ مرحله‌ای."""

    return {
        stage: CandidateStageRule(stage=stage, failure_code=code, detail_keys=detail_keys)
        for stage, (code, detail_keys) in _DEFAULT_RULE_CODES.items()
    }
