from app.core.common.reasons import ReasonCode
from app.core.common.rules import CandidateStageRule, RuleContext, default_stage_rule_map


def _stage_record(stage: str, *, after: int, extras: dict | None = None) -> dict:
    return {
        "stage": stage,
        "column": "ستون",
        "expected_value": 1,
        "total_before": 2,
        "total_after": after,
        "matched": after > 0,
        "expected_op": None,
        "expected_threshold": None,
        "extras": extras,
    }


def test_gender_rule_failure_sets_specific_reason() -> None:
    mapping = default_stage_rule_map()
    result = mapping["gender"](RuleContext(stage_record=_stage_record("gender", after=0)))
    assert result.reason.code is ReasonCode.GENDER_MISMATCH
    assert not result.passed


def test_school_rule_copies_extra_details() -> None:
    mapping = default_stage_rule_map()
    extras = {
        "school_code_raw": "۱۲۳",
        "school_code_norm": 123,
        "school_status_resolved": True,
        "school_filter_applied": True,
        "join_value_raw": "۱۲۳",
        "join_value_norm": 123,
        "expected_op": ">",
        "expected_threshold": 0,
    }
    context = RuleContext(stage_record=_stage_record("school", after=0, extras=extras))
    result = mapping["school"](context)
    assert result.details["school_code_raw"] == "۱۲۳"
    assert result.details["expected_op"] == ">"
    assert result.reason.code is ReasonCode.SCHOOL_STATUS_MISMATCH


def test_candidate_stage_rule_ok_when_after_positive() -> None:
    rule = CandidateStageRule(stage="center", failure_code=ReasonCode.CENTER_MISMATCH)
    result = rule(RuleContext(stage_record=_stage_record("center", after=5)))
    assert result.passed
    assert result.reason.code is ReasonCode.OK


def test_common_detail_keys_forwarded_from_extras() -> None:
    mapping = default_stage_rule_map()
    extras = {
        "join_value_raw": "A",
        "join_value_norm": None,
        "expected_op": "=",
        "expected_threshold": None,
    }
    context = RuleContext(stage_record=_stage_record("gender", after=0, extras=extras))
    result = mapping["gender"](context)
    assert result.details["join_value_raw"] == "A"
    assert result.details["expected_op"] == "="
    assert result.reason.code is ReasonCode.GENDER_MISMATCH
