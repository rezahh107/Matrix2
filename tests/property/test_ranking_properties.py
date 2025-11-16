from __future__ import annotations

import pandas as pd
import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings  # type: ignore  # noqa: E402
from hypothesis import strategies as st  # type: ignore  # noqa: E402

from app.core.common.ranking import apply_ranking_policy, natural_key


@settings(max_examples=50)
@given(st.lists(st.text(min_size=1), min_size=3, max_size=6))
def test_natural_key_monotonic(ids: list[str]) -> None:
    sorted_ids = sorted(ids, key=natural_key)
    for earlier, later in zip(sorted_ids, sorted_ids[1:]):
        assert natural_key(earlier) <= natural_key(later)


@settings(max_examples=30)
@given(
    st.integers(min_value=1, max_value=50),
    st.integers(min_value=1, max_value=50),
    st.integers(min_value=0, max_value=10),
    st.integers(min_value=0, max_value=10),
)
def test_apply_ranking_policy_orders_by_policy(
    initial_a: int,
    initial_b: int,
    alloc_a: int,
    alloc_b: int,
) -> None:
    state = {
        "MENTOR-1": {"initial": initial_a, "remaining": max(initial_a - alloc_a, 0), "alloc_new": alloc_a},
        "MENTOR-2": {"initial": initial_b, "remaining": max(initial_b - alloc_b, 0), "alloc_new": alloc_b},
    }

    df = pd.DataFrame(
        {
            "mentor_id": ["MENTOR-1", "MENTOR-2"],
            "remaining_capacity": [state["MENTOR-1"]["remaining"], state["MENTOR-2"]["remaining"]],
        }
    )

    ranked = apply_ranking_policy(df, state=state)
    occupancy = ranked["occupancy_ratio"].tolist()
    remaining_capacity = ranked["remaining_capacity"].tolist()
    allocations = ranked["allocations_new"].tolist()
    sort_keys = ranked["mentor_sort_key"].tolist()

    # سورت باید مطابق Policy باشد: occupancy → ظرفیت مطلق → تخصیص جدید → کلید طبیعی
    assert occupancy == sorted(occupancy)
    if occupancy[0] == occupancy[1]:
        assert remaining_capacity == sorted(remaining_capacity, reverse=True)
        if remaining_capacity[0] == remaining_capacity[1]:
            assert allocations == sorted(allocations)
            if allocations[0] == allocations[1]:
                assert sort_keys == sorted(sort_keys)
