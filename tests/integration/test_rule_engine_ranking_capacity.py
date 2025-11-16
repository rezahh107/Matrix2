from __future__ import annotations

import pandas as pd

from app.core.policy_loader import load_policy
from app.core.rule_engine import rank_rule_engine_candidates


def test_rule_engine_ranking_prefers_larger_capacity_at_equal_occupancy() -> None:
    policy = load_policy()
    pool = pd.DataFrame(
        [
            {
                "کد کارمندی پشتیبان": "EMP-LARGE",
                "remaining_capacity": 4,
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

    ranked = rank_rule_engine_candidates(pool, policy=policy)

    assert list(ranked["کد کارمندی پشتیبان"]) == ["EMP-LARGE", "EMP-SMALL"]
    assert int(ranked.loc[0, "remaining_capacity"]) == 4
