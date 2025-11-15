"""آزمون یکپارچهٔ جریان مدرسه برای Trace."""

from __future__ import annotations

import pandas as pd

from app.core.allocate_students import _normalize_students
from app.core.common.reasons import ReasonCode
from app.core.common.trace import build_allocation_trace
from app.core.policy_loader import load_policy


def _student_df(code: str | None) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "مدرسه نهایی": [code],
            "کدرشته": [101],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
        }
    )


def _pool_df(school_code: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "کدرشته": [101],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [school_code],
            "remaining_capacity": [1],
        }
    )


def _school_stage(trace: list[dict[str, object]]) -> dict[str, object]:
    return next(stage for stage in trace if stage["stage"] == "school")


def _gender_stage(trace: list[dict[str, object]]) -> dict[str, object]:
    return next(stage for stage in trace if stage["stage"] == "gender")


def test_trace_records_raw_and_normalized_school_code() -> None:
    policy = load_policy()
    students = _normalize_students(_student_df("۶۶۳"), policy)
    student = students.iloc[0].to_dict()
    pool = _pool_df(663)

    trace = build_allocation_trace(student, pool, policy=policy)
    stage = _school_stage(trace)

    assert stage["expected_op"] == ">"
    assert stage["expected_threshold"] == 0
    assert stage["expected_value"] == 663
    extras = stage["extras"]
    expected = {
        "school_code_raw": "۶۶۳",
        "school_code_norm": 663,
        "school_status_resolved": True,
        "school_filter_applied": True,
    }
    for key, value in expected.items():
        assert extras[key] == value
    assert extras["join_value_norm"] == 663
    assert extras["mentor_value_norm"] == 663
    assert extras["expected_op"] == ">"
    assert extras["rule_details"]["school_code_norm"] == 663
    assert extras["rule_reason_code"] == ReasonCode.OK


def test_trace_for_normal_student_marks_false() -> None:
    policy = load_policy()
    students = _normalize_students(_student_df("0"), policy)
    student = students.iloc[0].to_dict()
    pool = _pool_df(0)

    trace = build_allocation_trace(student, pool, policy=policy)
    stage = _school_stage(trace)

    assert stage["expected_op"] == ">"
    assert stage["expected_threshold"] == 0
    assert stage["expected_value"] == 0
    extras = stage["extras"]
    expected = {
        "school_code_raw": "0",
        "school_code_norm": 0,
        "school_status_resolved": False,
        "school_filter_applied": False,
    }
    for key, value in expected.items():
        assert extras[key] == value
    assert extras["join_value_norm"] == 0
    assert extras["mentor_value_norm"] == 0
    assert extras["expected_op"] == ">"
    assert extras["rule_details"]["school_code_norm"] == 0
    assert extras["rule_reason_code"] == ReasonCode.OK


def test_gender_stage_failure_reports_join_details() -> None:
    policy = load_policy()
    student_df = _student_df("۶۶۳")
    student_df["جنسیت"] = [policy.gender_codes.female.value]
    students = _normalize_students(student_df, policy)
    student = students.iloc[0].to_dict()
    pool = _pool_df(663)
    pool["جنسیت"] = [policy.gender_codes.male.value]

    trace = build_allocation_trace(student, pool, policy=policy)
    stage = _gender_stage(trace)

    assert stage["total_after"] == 0
    extras = stage["extras"]
    assert extras["mentor_value_norm"] == policy.gender_codes.male.value
    assert extras["rule_details"]["student_value"] == policy.gender_codes.female.value
    assert extras["rule_details"]["mentor_value"] == policy.gender_codes.male.value
    assert (
        extras["rule_details"]["normalize_diff"]
        == policy.gender_codes.female.value - policy.gender_codes.male.value
    )
