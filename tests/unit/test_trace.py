from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.trace import build_allocation_trace, build_trace_plan
from app.core.policy_loader import (
    ExcelOptions,
    PolicyAliasRule,
    PolicyColumns,
    PolicyConfig,
    RankingRule,
    TraceStageDefinition,
    parse_policy_dict,
)


def _policy_payload() -> dict[str, object]:
    return {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "postal_valid_range": [1000, 9999],
        "finance_variants": [0, 1, 3],
        "center_map": {"شهدخت کشاورز": 1, "آیناز هوشمند": 2, "*": 0},
        "school_code_empty_as_zero": True,
        "alias_rule": {"normal": "postal_or_fallback_mentor_id", "school": "mentor_id"},
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "columns": {
            "postal_code": "کدپستی",
            "school_count": "تعداد مدارس تحت پوشش",
            "school_code": "کد مدرسه",
            "capacity_current": "تعداد داوطلبان تحت پوشش",
            "capacity_special": "تعداد تحت پوشش خاص",
            "remaining_capacity": "remaining_capacity",
        },
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio", "ascending": True},
            {"name": "min_allocations_new", "column": "allocations_new", "ascending": True},
            {"name": "min_mentor_id", "column": "mentor_sort_key", "ascending": True},
        ],
        "trace_stages": [
            {"stage": "type", "column": "کدرشته"},
            {"stage": "group", "column": "گروه آزمایشی"},
            {"stage": "gender", "column": "جنسیت"},
            {"stage": "graduation_status", "column": "دانش آموز فارغ"},
            {"stage": "center", "column": "مرکز گلستان صدرا"},
            {"stage": "finance", "column": "مالی حکمت بنیاد"},
            {"stage": "school", "column": "کد مدرسه"},
            {"stage": "capacity_gate", "column": "remaining_capacity"},
        ],
    }


@pytest.fixture()
def policy_config() -> PolicyConfig:
    return parse_policy_dict(_policy_payload())


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
    assert columns[-1] == "remaining_capacity"


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


def test_build_trace_plan_rejects_noncanonical_order() -> None:
    config = PolicyConfig(
        version="1.0.3",
        normal_statuses=[1, 0],
        school_statuses=[1],
        postal_valid_range=(1000, 9999),
        finance_variants=(0, 1, 3),
        center_map={"شهدخت کشاورز": 1, "آیناز هوشمند": 2, "*": 0},
        school_code_empty_as_zero=True,
        alias_rule=PolicyAliasRule(normal="postal_or_fallback_mentor_id", school="mentor_id"),
        columns=PolicyColumns(
            postal_code="کدپستی",
            school_count="تعداد مدارس تحت پوشش",
            school_code="کد مدرسه",
            capacity_current="تعداد داوطلبان تحت پوشش",
            capacity_special="تعداد تحت پوشش خاص",
            remaining_capacity="remaining_capacity",
        ),
        join_keys=[
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        ranking_rules=[
            RankingRule(name="min_occupancy_ratio", column="occupancy_ratio", ascending=True),
            RankingRule(name="min_allocations_new", column="allocations_new", ascending=True),
            RankingRule(name="min_mentor_id", column="mentor_sort_key", ascending=True),
        ],
        trace_stages=[
            TraceStageDefinition(stage="type", column="کدرشته"),
            TraceStageDefinition(stage="gender", column="جنسیت"),
            TraceStageDefinition(stage="group", column="گروه آزمایشی"),
            TraceStageDefinition(stage="graduation_status", column="دانش آموز فارغ"),
            TraceStageDefinition(stage="center", column="مرکز گلستان صدرا"),
            TraceStageDefinition(stage="finance", column="مالی حکمت بنیاد"),
            TraceStageDefinition(stage="school", column="کد مدرسه"),
            TraceStageDefinition(stage="capacity_gate", column="remaining_capacity"),
        ],
        column_aliases={},
        excel=ExcelOptions(rtl=True, font_name="Vazirmatn", header_mode="fa"),
    )

    with pytest.raises(ValueError, match="canonical 8-stage order"):
        build_trace_plan(config)
