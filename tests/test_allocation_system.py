from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.allocate_students import allocate_batch
from app.core.common.normalization import normalize_fa, resolve_group_code
from app.core.policy_adapter import policy as policy_adapter


def _make_group_map(mapping: dict[str, int]) -> dict[str, int]:
    return {normalize_fa(key): value for key, value in mapping.items()}


def test_resolve_group_code_prefers_major_when_mismatch(caplog):
    row = pd.Series(
        {
            "student_id": "STD-1",
            "کدرشته": "۱۲۳",
            "گروه آزمایشی": "ریاضی",
        }
    )
    stats: dict[str, int] = {}
    group_map = _make_group_map({"ریاضی": 456})
    logger = logging.getLogger("group-test")
    with caplog.at_level("WARNING"):
        resolved = resolve_group_code(
            row,
            group_map,
            major_column="کدرشته",
            group_column="گروه آزمایشی",
            prefer_major_code=True,
            stats=stats,
            logger=logger,
        )
    assert resolved == 123
    assert stats.get("resolved_by_major_code") == 1
    assert "mismatch" in caplog.text


def test_resolve_group_code_uses_crosswalk_when_major_absent():
    row = pd.Series({"کدرشته": "", "گروه آزمایشی": "تجربی"})
    group_map = _make_group_map({"تجربی": 789})
    stats: dict[str, int] = {}
    resolved = resolve_group_code(
        row,
        group_map,
        major_column="کدرشته",
        group_column="گروه آزمایشی",
        prefer_major_code=True,
        stats=stats,
        logger=logging.getLogger("group-test"),
    )
    assert resolved == 789
    assert stats.get("resolved_by_crosswalk") == 1


def test_resolve_group_code_handles_persian_digits():
    row = pd.Series({"کدرشته": "۳", "گروه آزمایشی": None})
    resolved = resolve_group_code(
        row,
        {},
        major_column="کدرشته",
        group_column="گروه آزمایشی",
        prefer_major_code=True,
        stats={},
        logger=logging.getLogger("group-test"),
    )
    assert resolved == 3


def test_resolve_group_code_returns_none_when_no_data():
    row = pd.Series({"کدرشته": None, "گروه آزمایشی": ""})
    stats: dict[str, int] = {}
    resolved = resolve_group_code(
        row,
        {},
        major_column="کدرشته",
        group_column="گروه آزمایشی",
        prefer_major_code=True,
        stats=stats,
        logger=logging.getLogger("group-test"),
    )
    assert resolved is None
    assert stats.get("unresolved_group_code") == 1


def test_allocate_batch_uses_policy_capacity_column():
    policy_config = policy_adapter.config
    capacity_column = policy_adapter.stage_column("capacity_gate")
    assert capacity_column is not None

    students = pd.DataFrame(
        [
            {
                "student_id": "STD-777",
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

    candidate_pool = pd.DataFrame(
        [
            {
                "پشتیبان": "Mentor A",
                "کد کارمندی پشتیبان": "EMP-1",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                capacity_column: 1,
                "occupancy_ratio": 0.1,
                "allocations_new": 0,
                "mentor_sort_key": 1,
            }
        ]
    )

    allocations, updated_pool, logs, trace = allocate_batch(
        students,
        candidate_pool,
        policy=policy_config,
    )

    assert allocations.shape[0] == 1
    assert updated_pool.loc[0, capacity_column] == 0
    assert logs.iloc[0]["allocation_status"] == "success"
    assert trace.iloc[0]["stage"] == policy_config.trace_stages[0].stage
