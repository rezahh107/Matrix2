from __future__ import annotations

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy
from app.infra.cli import _sanitize_pool_for_allocation


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
            },
            {
                "student_id": "STD-2",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
        ]
    )


def _sample_pool() -> pd.DataFrame:
    raw = pd.DataFrame(
        [
            {
                "mentor_name": "منتور الف",
                "alias": 101,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 101,
            },
            {
                "mentor_name": "منتور ب",
                "alias": 102,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 102,
            },
        ]
    )
    policy = load_policy()
    return _sanitize_pool_for_allocation(raw, policy=policy)


def test_capacity_decrements_and_never_negative() -> None:
    policy = load_policy()
    students = _sample_students()
    pool = _sample_pool()

    _, updated_pool, logs, trace = allocate_batch(students, pool, policy=policy)

    success_logs = logs.loc[logs["allocation_status"] == "success"]
    assert not success_logs.empty
    diff = success_logs["capacity_before"].astype(int) - success_logs["capacity_after"].astype(int)
    assert diff.eq(1).all()

    remaining = pd.to_numeric(updated_pool["remaining_capacity"], errors="coerce").fillna(0)
    assert (remaining >= 0).all()

    per_student = trace.groupby("student_id")
    assert all(len(group) == 8 for _, group in per_student)
