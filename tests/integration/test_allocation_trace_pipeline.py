import pandas as pd

from app.core.common.reasons import ReasonCode
from app.core.common.trace import build_allocation_trace
from app.core.policy_loader import load_policy


def test_trace_records_include_reason_codes() -> None:
    policy = load_policy()
    student = {
        "کدرشته": 101,
        "گروه آزمایشی": "ریاضی",
        "جنسیت": policy.gender_codes.male.value,
        "دانش آموز فارغ": 0,
        "مرکز گلستان صدرا": 1,
        "مالی حکمت بنیاد": 0,
        "کد مدرسه": 1000,
    }
    candidate_pool = pd.DataFrame(
        {
            "کدرشته": [101],
            "گروه آزمایشی": ["ریاضی"],
            "جنسیت": [policy.gender_codes.female.value],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [1],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [1000],
            "remaining_capacity": [0],
        }
    )
    trace = build_allocation_trace(student, candidate_pool, policy=policy)
    gender_stage = next(stage for stage in trace if stage["stage"] == "gender")
    assert gender_stage["extras"]["rule_reason_code"] == ReasonCode.GENDER_MISMATCH
    capacity_stage = trace[-1]
    assert capacity_stage["extras"]["rule_reason_code"] == ReasonCode.CAPACITY_FULL
