from __future__ import annotations

import pandas as pd

from app.core.common.ranking import apply_ranking_policy
from app.core.policy_loader import load_policy


def _base_candidate_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "کد کارمندی پشتیبان": ["MENTOR-5", "MENTOR-1"],
            "کدرشته": [1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی"],
            "جنسیت": [1, 1],
            "دانش آموز فارغ": [0, 0],
            "مرکز گلستان صدرا": [0, 0],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [1010, 1010],
        }
    )


def test_capacity_ranking_prefers_larger_remaining_when_occupancy_equal() -> None:
    policy = load_policy()
    candidate_pool = _base_candidate_pool()
    state = {
        "MENTOR-5": {"initial": 5, "remaining": 5, "alloc_new": 0},
        "MENTOR-1": {"initial": 1, "remaining": 1, "alloc_new": 0},
    }
    ranked = apply_ranking_policy(candidate_pool, state=state, policy=policy)
    assert list(ranked["کد کارمندی پشتیبان"]) == ["MENTOR-5", "MENTOR-1"]


def test_occupancy_still_has_priority_over_absolute_capacity() -> None:
    policy = load_policy()
    candidate_pool = _base_candidate_pool()
    state = {
        "MENTOR-5": {"initial": 5, "remaining": 1, "alloc_new": 4},
        "MENTOR-1": {"initial": 1, "remaining": 1, "alloc_new": 0},
    }
    ranked = apply_ranking_policy(candidate_pool, state=state, policy=policy)
    assert list(ranked["کد کارمندی پشتیبان"]) == ["MENTOR-1", "MENTOR-5"]
