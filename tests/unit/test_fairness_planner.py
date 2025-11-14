from dataclasses import replace
from hashlib import blake2b

import pandas as pd

from app.core.common.ranking import apply_ranking_policy
from app.core.counter import stable_counter_hash, validate_counter
from app.core.policy_loader import load_policy


def _tie_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mentor_id": ["EMP-101", "EMP-102", "EMP-103"],
            "occupancy_ratio": [0.1, 0.1, 0.1],
            "allocations_new": [0, 0, 0],
            "mentor_sort_key": [(), (), ()],
            "remaining_capacity": [5, 5, 5],
            "counter": ["543570001", "543570005", "543570003"],
        }
    )


def test_deterministic_jitter_orders_by_counter_hash() -> None:
    policy = replace(load_policy(), fairness_strategy="deterministic_jitter")
    pool = _tie_pool()
    ranked = apply_ranking_policy(pool, policy=policy)
    expected = sorted(
        pool["counter"].astype(str).tolist(),
        key=lambda value: stable_counter_hash(validate_counter(value)),
    )
    assert ranked["counter"].astype(str).tolist() == expected


def test_round_robin_uses_hashed_mentor_id() -> None:
    policy = replace(load_policy(), fairness_strategy="round_robin")
    ranked = apply_ranking_policy(_tie_pool(), policy=policy)
    hashed = ranked["mentor_id_en"].astype(str).map(
        lambda value: int.from_bytes(blake2b(value.encode("utf-8"), digest_size=8).digest(), "big")
    )
    assert hashed.is_monotonic_increasing
