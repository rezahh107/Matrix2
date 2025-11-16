from __future__ import annotations

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy


def test_allocate_prefers_mentor_with_larger_remaining_capacity() -> None:
    policy = load_policy()
    students = pd.DataFrame(
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
    pool = pd.DataFrame(
        [
            {
                "پشتیبان": "Mentor Large",
                "کد کارمندی پشتیبان": "EMP-LARGE",
                "remaining_capacity": 5,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
            {
                "پشتیبان": "Mentor Small",
                "کد کارمندی پشتیبان": "EMP-SMALL",
                "remaining_capacity": 1,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
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

    allocations, updated_pool, logs, _ = allocate_batch(students, pool, policy=policy)

    assert allocations["mentor_id"].iloc[0] == "EMP-LARGE"
    assert int(updated_pool.loc[0, "remaining_capacity"]) == 4
    assert logs.loc[0, "allocation_status"] == "success"
