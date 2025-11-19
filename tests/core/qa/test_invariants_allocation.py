from __future__ import annotations

import pandas as pd

from app.core.policy_loader import load_policy
from app.core.qa.invariants import check_ALLOC_01


def test_allocation_capacity_happy_path() -> None:
    policy = load_policy()
    allocation = pd.DataFrame({"mentor_id": [1, 1, 2]})
    summary = pd.DataFrame(
        {
            "mentor_id": [1, 2],
            policy.columns.remaining_capacity: [1, 2],
            "allocations_new": [2, 1],
            "occupancy_ratio": [2 / 3, 1 / 3],
        }
    )

    result = check_ALLOC_01(
        allocation=allocation, allocation_summary=summary, policy=policy
    )

    assert result.passed


def test_allocation_capacity_violation() -> None:
    policy = load_policy()
    allocation = pd.DataFrame({"mentor_id": [1, 1, 1]})
    summary = pd.DataFrame(
        {
            "mentor_id": [1],
            policy.columns.remaining_capacity: [0],
            "allocations_new": [2],
            "occupancy_ratio": [0.1],
        }
    )

    result = check_ALLOC_01(
        allocation=allocation, allocation_summary=summary, policy=policy
    )

    assert not result.passed
    assert result.violations[0].rule_id == "QA_RULE_ALLOC_01"

