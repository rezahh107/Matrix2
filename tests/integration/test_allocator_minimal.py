from __future__ import annotations

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy
from app.infra.cli import _sanitize_pool_for_allocation


def test_allocator_respects_capacity_and_filters_virtual() -> None:
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
            {
                "student_id": "STD-3",
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

    pool_raw = pd.DataFrame(
        [
            {
                "mentor_name": "منتور الف",
                "alias": 101,
                "remaining_capacity": 2,
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
            {
                "mentor_name": "در انتظار تخصیص",
                "alias": 7501,
                "remaining_capacity": 5,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 7501,
            },
        ]
    )

    policy = load_policy()
    pool = _sanitize_pool_for_allocation(pool_raw, policy=policy)
    assert "mentor_name" in pool.columns

    allocations, updated_pool, logs, _ = allocate_batch(students, pool, policy=policy)

    assert len(allocations) == 3
    assert not allocations["mentor_id"].astype(str).str.contains("7501").any()
    assert int(updated_pool.loc[0, "remaining_capacity"]) == 0
    assert int(updated_pool.loc[1, "remaining_capacity"]) == 0

    success_logs = logs.loc[logs["allocation_status"] == "success"]
    assert all(success_logs["capacity_before"] > success_logs["capacity_after"])
    assert success_logs["occupancy_ratio"].iloc[-1] > 0
