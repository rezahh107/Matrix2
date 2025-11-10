from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.allocate_students import allocate_batch


@pytest.fixture()
def _students_10() -> pd.DataFrame:
    rows = []
    for i in range(10):
        rows.append(
            {
                "student_id": f"STD-{i+1:03d}",
                "کدرشته": 1201,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 0,
                "مرکز_گلستان_صدرا": 1,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 3581,
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture()
def _candidate_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": ["زهرا", "علی"],
            "کد کارمندی پشتیبان": ["EMP-001", "EMP-002"],
            "کدرشته": [1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی"],
            "جنسیت": [1, 1],
            "دانش آموز فارغ": [0, 0],
            "مرکز گلستان صدرا": [1, 1],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [3581, 3581],
            "remaining_capacity": [5, 3],
            "occupancy_ratio": [0.2, 0.2],
            "allocations_new": [0, 0],
        }
    )


def test_allocation_rate_and_determinism(_students_10: pd.DataFrame, _candidate_pool: pd.DataFrame) -> None:
    alloc1, pool1, logs1, trace1 = allocate_batch(_students_10, _candidate_pool)
    alloc2, pool2, logs2, trace2 = allocate_batch(_students_10, _candidate_pool)

    rate = alloc1.shape[0] / _students_10.shape[0]
    assert rate >= 0.8

    pd.testing.assert_frame_equal(alloc1.sort_index(), alloc2.sort_index())
    pd.testing.assert_frame_equal(pool1.sort_index(), pool2.sort_index())
    pd.testing.assert_frame_equal(
        logs1.sort_index(axis=1).sort_index(),
        logs2.sort_index(axis=1).sort_index(),
    )
    pd.testing.assert_frame_equal(
        trace1.sort_index(axis=1).sort_index(),
        trace2.sort_index(axis=1).sort_index(),
    )

    assert (logs1["allocation_status"] == "success").sum() >= 8
