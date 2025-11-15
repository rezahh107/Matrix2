import math
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.allocate_students import allocate_batch, _normalize_pool, _normalize_students
from app.core.policy_loader import PolicyConfig, load_policy


@pytest.fixture(scope="module")
def _policy() -> PolicyConfig:
    return load_policy()


@pytest.fixture()
def _students_alias_join() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "student_id": "STD-001",
                "کدرشته": 33,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 111,
            },
            {
                "student_id": "STD-002",
                "کدرشته": 33,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 0,
            },
            {
                "student_id": "STD-003",
                "کدرشته": 5,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش_آموز_فارغ": 0,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 1081,
            },
            {
                "student_id": "STD-004",
                "کدرشته": 5,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش_آموز_فارغ": 0,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 1081,
            },
            {
                "student_id": "STD-005",
                "کدرشته": 3,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 286,
            },
            {
                "student_id": "STD-006",
                "کدرشته": 3,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 286,
            },
            {
                "student_id": "STD-007",
                "کدرشته": 21,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 663,
            },
            {
                "student_id": "STD-008",
                "کدرشته": 21,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 663,
            },
            {
                "student_id": "STD-009",
                "کدرشته": 35,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 1,
                "مرکز_گلستان_صدرا": 0,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 43,
            },
        ]
    )


@pytest.fixture()
def _candidate_pool_alias() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": [
                "Mentor-1",
                "Mentor-2",
                "Mentor-3",
                "Mentor-4",
                "Mentor-5",
            ],
            "کد کارمندی پشتیبان": [
                "EMP-001",
                "EMP-002",
                "EMP-003",
                "EMP-004",
                "EMP-005",
            ],
            "کدرشته|group code": ["33", "33", "5", "3", "21"],
            "گروه آزمایشی": ["تجربی", "تجربی", "تجربی", "تجربی", "تجربی"],
            "جنسیت|gender": ["1", "1", "0", "1", "0"],
            "دانش آموز فارغ|graduation status": ["1", "1", "0", "1", "1"],
            "مرکز گلستان صدرا|center": ["0", "0", "0", "0", "0"],
            "مالی حکمت بنیاد|finance": ["0", "0", "0", "0", "0"],
            "کد مدرسه|school code": ["111", "0", "1081", "286", "663"],
            "remaining_capacity": [2, 1, 3, 2, 2],
            "allocations_new": [0, 0, 0, 0, 0],
            "occupancy_ratio": [0.0, 0.0, 0.0, 0.0, 0.0],
        }
    )


def test_bilingual_headers_reduce_false_no_match(
    _policy: PolicyConfig,
    _students_alias_join: pd.DataFrame,
    _candidate_pool_alias: pd.DataFrame,
) -> None:
    students_norm = _normalize_students(_students_alias_join, _policy)
    pool_norm = _normalize_pool(_candidate_pool_alias, _policy)

    for column in _policy.join_keys:
        assert str(students_norm[column].dtype) == "Int64"
        assert str(pool_norm[column].dtype) == "Int64"

    alloc1, pool1, logs1, trace1 = allocate_batch(
        _students_alias_join,
        _candidate_pool_alias,
        policy=_policy,
    )
    alloc2, pool2, logs2, trace2 = allocate_batch(
        _students_alias_join,
        _candidate_pool_alias,
        policy=_policy,
    )

    threshold = math.ceil(_students_alias_join.shape[0] * 0.8)
    assert alloc1.shape[0] >= threshold

    no_match_mask = logs1["error_type"] == "ELIGIBILITY_NO_MATCH"
    assert no_match_mask.sum() == 1
    assert logs1.loc[no_match_mask, "candidate_count"].iloc[0] == 0
    success_mask = logs1["allocation_status"] == "success"
    assert (logs1.loc[success_mask, "candidate_count"] > 0).all()

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


def test_empty_pool_emits_type_alert(
    _policy: PolicyConfig,
    _students_alias_join: pd.DataFrame,
    _candidate_pool_alias: pd.DataFrame,
) -> None:
    students = _students_alias_join.iloc[[0]].copy()
    empty_pool = _candidate_pool_alias.iloc[0:0].copy()

    _, _, logs, _ = allocate_batch(students, empty_pool, policy=_policy)

    record = logs.iloc[0]
    assert record["error_type"] == "ELIGIBILITY_NO_MATCH"
    alerts = record["alerts"]
    assert isinstance(alerts, list) and alerts
    first_alert = alerts[0]
    assert first_alert["stage"] == "type"
    assert first_alert["code"] == "ELIGIBILITY_NO_MATCH"
    context = first_alert.get("context") or {}
    expected_value = context.get("expected_value")
    assert str(expected_value).strip() == str(students.iloc[0]["کدرشته"])


def test_zero_capacity_emits_capacity_alert(
    _policy: PolicyConfig,
    _students_alias_join: pd.DataFrame,
    _candidate_pool_alias: pd.DataFrame,
) -> None:
    students = _students_alias_join.iloc[[0]].copy()
    zero_capacity_pool = _candidate_pool_alias.copy()
    zero_capacity_pool["remaining_capacity"] = 0

    _, _, logs, _ = allocate_batch(students, zero_capacity_pool, policy=_policy)

    record = logs.iloc[0]
    assert record["error_type"] == "CAPACITY_FULL"
    alerts = record["alerts"]
    assert isinstance(alerts, list) and alerts
    first_alert = alerts[0]
    assert first_alert["stage"] == "capacity_gate"
    assert first_alert["code"] == "CAPACITY_FULL"
    context = first_alert.get("context") or {}
    assert context.get("column")
