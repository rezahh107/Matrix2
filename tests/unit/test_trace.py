from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.trace import build_allocation_trace, build_trace_plan
from app.core.policy_loader import PolicyConfig


@pytest.fixture()
def policy_config() -> PolicyConfig:
    return PolicyConfig(
        version="1.0.3",
        normal_statuses=[1, 0],
        school_statuses=[1],
        join_keys=[
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        ranking=[
            "min_occupancy_ratio",
            "min_allocations_new",
            "min_mentor_id",
        ],
    )


def _sample_student() -> dict[str, object]:
    return {
        "student_id": "STD-1",
        "کدرشته": 1201,
        "گروه_آزمایشی": "تجربی",
        "جنسیت": 1,
        "دانش_آموز_فارغ": 0,
        "مرکز_گلستان_صدرا": 1,
        "مالی_حکمت_بنیاد": 0,
        "کد_مدرسه": 3581,
    }


def _sample_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": ["زهرا", "علی", "زهرا"],
            "کدرشته": [1201, 1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی", "ریاضی"],
            "جنسیت": [1, 1, 1],
            "دانش آموز فارغ": [0, 0, 0],
            "مرکز گلستان صدرا": [1, 2, 1],
            "مالی حکمت بنیاد": [0, 0, 1],
            "کد مدرسه": [3581, 3581, 3581],
            "remaining_capacity": [2, 0, 3],
        }
    )


def test_build_trace_plan_prefers_policy_columns(policy_config: PolicyConfig) -> None:
    plan = build_trace_plan(policy_config)
    stages = [item.stage for item in plan]
    columns = [item.column for item in plan]

    assert stages[-1] == "capacity_gate"
    assert columns[:3] == ["کدرشته", "گروه آزمایشی", "جنسیت"]


def test_build_allocation_trace_counts_down(policy_config: PolicyConfig) -> None:
    student = _sample_student()
    pool = _sample_pool()

    trace = build_allocation_trace(student, pool, policy=policy_config)

    assert [step["stage"] for step in trace] == [
        "type",
        "group",
        "gender",
        "graduation_status",
        "center",
        "finance",
        "school",
        "capacity_gate",
    ]
    counts = [step["total_after"] for step in trace]
    assert counts == [3, 2, 2, 2, 1, 1, 1, 1]
    assert trace[-1]["matched"] is True


def test_capacity_gate_handles_no_capacity(policy_config: PolicyConfig) -> None:
    student = _sample_student()
    pool = _sample_pool()
    pool["remaining_capacity"] = [0, 0, 0]

    trace = build_allocation_trace(student, pool, policy=policy_config)

    assert trace[-1]["total_after"] == 0
    assert trace[-1]["matched"] is False
