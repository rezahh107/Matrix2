from app.core.common.reasons import ReasonCode
from app.core.common.rules import (
    CandidateStageRule,
    RuleContext,
    apply_rule,
    default_stage_rule_map,
)


def _stage_record(stage: str, *, after: int) -> dict:
    return {
        "stage": stage,
        "column": "ستون",
        "expected_value": 1,
        "total_before": 2,
        "total_after": after,
        "matched": after > 0,
        "expected_op": None,
        "expected_threshold": None,
        "extras": None,
    }


def test_candidate_stage_rule_failure_sets_reason() -> None:
    rule = CandidateStageRule(stage="gender", failure_code=ReasonCode.GENDER_MISMATCH)
    record = _stage_record("gender", after=0)
    result = apply_rule(rule, RuleContext(stage_record=record))
    assert not result.passed
    assert result.reason.code == ReasonCode.GENDER_MISMATCH


def test_default_stage_rules_report_ok_on_success() -> None:
    mapping = default_stage_rule_map()
    record = _stage_record("gender", after=3)
    result = apply_rule(mapping["gender"], RuleContext(stage_record=record))
    assert result.passed
    assert result.reason.code == ReasonCode.OK
