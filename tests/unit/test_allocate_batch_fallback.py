import pandas as pd

from app.core import allocate_students as allocator
from app.core.policy_loader import load_policy


def _sample_students() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            }
        ]
    )


def _sample_pool() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "پشتیبان": "منتور الف",
                "mentor_name": "منتور الف",
                "alias": 101,
                "remaining_capacity": 2,
                "allocations_new": 0,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": "EMP-101",
            }
        ]
    )


def test_allocate_batch_recovers_missing_log_identifier(monkeypatch):
    policy = load_policy()
    students = _sample_students()
    pool = _sample_pool()

    original = allocator.allocate_student

    def _wrapped_allocate_student(*args, **kwargs):
        result = original(*args, **kwargs)
        result.log["mentor_id"] = pd.NA
        return result

    monkeypatch.setattr(allocator, "allocate_student", _wrapped_allocate_student)

    allocations, _, logs, _ = allocator.allocate_batch(students, pool, policy=policy)

    assert allocations.iloc[0]["mentor_id"] == "EMP-101"
    assert logs.iloc[0]["mentor_id"] == "EMP-101"


def test_resolve_mentor_identifier_uses_canonical_headers():
    policy = load_policy()
    mentor_row = pd.Series({" Mentor Id ": "EMP-202"})
    result = allocator.AllocationResult(mentor_row=mentor_row, trace=[], log={"mentor_id": pd.NA})

    resolved = allocator._resolve_mentor_identifier(result, policy=policy)

    assert resolved == "EMP-202"
    assert result.log["mentor_id"] == "EMP-202"
