import pandas as pd

from app.core.allocate_students import build_selection_reason_rows
from app.core.policy_loader import load_policy
from app.infra.excel.exporter import write_selection_reasons_sheet


def test_selection_reason_sheet_enriches_reason_text() -> None:
    policy = load_policy()
    allocations = pd.DataFrame(
        {
            "student_id": ["S-1"],
            "mentor_id": ["201"],
            policy.capacity_column: [5],
            "occupancy_ratio": [0.2],
            "allocations_new": [0],
            "counter": ["543570001"],
        }
    )
    students = pd.DataFrame(
        {
            "student_id": ["S-1"],
            "کدملی": ["0012345678"],
            "نام": ["زهرا"],
            "نام خانوادگی": ["محمدی"],
            "کدرشته": [101],
            "گروه آزمایشی": ["ریاضی"],
            "جنسیت": [policy.gender_codes.female.value],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [1],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [1000],
        }
    )
    mentors = pd.DataFrame({"mentor_id": ["201"], "mentor_name": ["منتور تست"]})
    logs = pd.DataFrame(
        {
            "student_id": ["S-1"],
            "rule_reason_code": ["GENDER_MISMATCH"],
            "rule_reason_text": [""],
            "rule_reason_details": [
                {
                    "stage": "gender",
                    "join_value_norm": policy.gender_codes.female.value,
                    "expected_op": "=",
                    "student_value": policy.gender_codes.female.value,
                    "mentor_value": policy.gender_codes.male.value,
                    "normalize_diff": policy.gender_codes.female.value
                    - policy.gender_codes.male.value,
                }
            ],
            "fairness_reason_code": ["FAIRNESS_ORDER"],
            "fairness_reason_text": ["[FAIRNESS_ORDER] بازچینش عدالت"],
        }
    )
    reasons = build_selection_reason_rows(
        allocations,
        students,
        mentors,
        policy=policy,
        logs=logs,
        trace=None,
    )
    reason_text = reasons.iloc[0]["دلیل انتخاب پشتیبان"]
    assert "دلیل Policy" in reason_text
    assert "جزئیات Policy" in reason_text
    assert "join_value_norm" in reason_text
    assert "student_value" in reason_text
    assert "mentor_value" in reason_text
    assert "normalize_diff" in reason_text
    assert "عدالت" in reason_text
    assert "[GENDER_MISMATCH]" in reason_text
    sheet_name, sanitized = write_selection_reasons_sheet(reasons, writer=None, policy=policy)
    assert sheet_name == policy.emission.selection_reasons.sheet_name
    assert sanitized.attrs["schema_hash"] == policy.emission.selection_reasons.schema_hash
