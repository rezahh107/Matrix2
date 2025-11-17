from __future__ import annotations

import pandas as pd

from app.core.common.ranking import apply_ranking_policy  # noqa: E402


def test_apply_ranking_policy_is_deterministic_and_natural() -> None:
    df = pd.DataFrame(
        {
            "کد کارمندی پشتیبان": ["EMP-2", "EMP-10", "EMP-1"],
            "occupancy_ratio": [0.5, 0.5, 0.5],
            "allocations_new": [1, 1, 1],
        }
    )

    ranked_once = apply_ranking_policy(df)
    ranked_twice = apply_ranking_policy(df)

    expected = ["EMP-1", "EMP-2", "EMP-10"]
    assert ranked_once["کد کارمندی پشتیبان"].tolist() == expected
    assert ranked_twice["کد کارمندی پشتیبان"].tolist() == expected
