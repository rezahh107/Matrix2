from app.core.common.reasons import ReasonCode
from app.core.common.rules import (
    CandidateStageRule,
    CenterPriorityRule,
    RuleContext,
    RuleEngine,
    SchoolStudentPriorityGuard,
    default_stage_rule_map,
)


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


def test_sensitive_stage_failure_reports_join_differences() -> None:
    mapping = default_stage_rule_map()
    extras = {
        "join_value_norm": 2,
        "mentor_value_norm": 1,
    }
    context = RuleContext(stage_record=_stage_record("center", after=0, extras=extras))
    result = mapping["center"](context)
    assert result.details["student_value"] == 2
    assert result.details["mentor_value"] == 1
    assert result.details["normalize_diff"] == 1


def test_stage_guard_rejects_missing_totals_before_candidate_rule() -> None:
    mapping = default_stage_rule_map()
    record = _stage_record("center", after=1)
    record["total_after"] = None
    context = RuleContext(stage_record=record)
    result = mapping["center"](context)
    assert result.reason.code is ReasonCode.INTERNAL_ERROR
    assert not result.passed
    assert result.details["issue"] == "missing_totals"
    assert result.details["record_stage"] == "center"


def test_center_priority_rule_enforces_allowed_centers() -> None:
    rule = CenterPriorityRule((1, 2, 0), center_column="center")
    student = {"center": 1, "is_school_student": False}
    mentor = {"allowed_centers": [1, 2]}
    assert rule.evaluate(student, mentor) is None
    mentor_mismatch = {"allowed_centers": [2]}
    assert rule.evaluate(student, mentor_mismatch) is ReasonCode.CENTER_MISMATCH
    assert rule.evaluate({"center": None, "is_school_student": False}, mentor) is ReasonCode.INVALID_CENTER_VALUE
    assert rule.evaluate(student, None) is ReasonCode.NO_MANAGER_FOR_CENTER
    school_student = {"center": 5, "is_school_student": True}
    assert rule.evaluate(school_student, mentor_mismatch) is None


def test_school_student_priority_guard_counts_students() -> None:
    guard = SchoolStudentPriorityGuard("is_school_student")
    payload = [True, False, 1, 0, "1"]
    result = guard.before_stage("school_phase_start", payload)
    assert result["school_student_count"] == 3
    assert result["center_student_count"] == 2
    assert "مدرسه‌ای" in result["message"]


def test_rule_engine_records_stage_entries_and_pair_reasons() -> None:
    guard = SchoolStudentPriorityGuard("is_school_student")
    rule = CenterPriorityRule((1, 2), center_column="center")
    engine = RuleEngine(stage_guards=(guard,), pair_rules=(rule,))
    students = [
        {"is_school_student": True},
        {"is_school_student": False},
    ]
    entries = engine.run_stage("school_phase_start", students, extras={"message": "شروع"})
    assert entries[0]["school_student_count"] == 1
    trace: list[dict[str, object]] = []
    engine.apply_stage_rules("school_phase_start", students, trace)
    assert trace
    reason = engine.evaluate_pair(
        {"center": 2, "is_school_student": False},
        {"allowed_centers": [1]},
    )
    assert reason is ReasonCode.CENTER_MISMATCH
