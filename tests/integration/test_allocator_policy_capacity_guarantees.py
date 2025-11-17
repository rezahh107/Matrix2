from __future__ import annotations

import pandas as pd

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.common.trace import FinalStatus, find_allocation_policy_violations
from app.core.policy_loader import load_policy


def _basic_frames(policy):
    students = pd.DataFrame(
        [
            {
                "student_id": 1,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 0,
                policy.stage_column("school"): 0,
            }
        ]
    )
    pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10],
            policy.stage_column("gender"): [0],
            policy.stage_column("graduation_status"): [0],
            policy.stage_column("center"): [1],
            policy.stage_column("finance"): [0],
            policy.stage_column("school"): [0],
            policy.columns.remaining_capacity: [1],
            "mentor_id": ["EMP-1"],
            "پشتیبان": ["Mentor"],
        }
    )
    return students, pool


def test_policy_violation_detector_is_empty_for_basic_run() -> None:
    policy = load_policy()
    students, pool = _basic_frames(policy)

    allocations, updated_pool, _, trace = allocate_batch(students, pool, policy=policy)
    summary_df = trace.attrs["summary_df"]
    assert allocations.shape[0] == 1
    violations = find_allocation_policy_violations(summary_df, updated_pool, policy=policy)
    assert violations.empty


def test_policy_violation_detector_flags_positive_capacity() -> None:
    policy = load_policy()
    summary_df = pd.DataFrame(
        [
            {
                "student_id": 1,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 0,
                policy.stage_column("school"): 0,
                "final_status": FinalStatus.NO_ELIGIBLE_MENTOR.value,
            }
        ]
    )
    pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10],
            policy.stage_column("gender"): [0],
            policy.stage_column("graduation_status"): [0],
            policy.stage_column("center"): [1],
            policy.stage_column("finance"): [0],
            policy.stage_column("school"): [0],
            policy.columns.remaining_capacity: [2],
        }
    )

    violations = find_allocation_policy_violations(summary_df, pool, policy=policy)
    assert violations.shape[0] == 1


def test_rule_excluded_rows_are_not_flagged() -> None:
    policy = load_policy()
    summary_df = pd.DataFrame(
        [
            {
                "student_id": 2,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 0,
                policy.stage_column("school"): 0,
                "final_status": FinalStatus.RULE_EXCLUDED.value,
            }
        ]
    )
    pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10],
            policy.stage_column("gender"): [0],
            policy.stage_column("graduation_status"): [0],
            policy.stage_column("center"): [1],
            policy.stage_column("finance"): [0],
            policy.stage_column("school"): [0],
            policy.columns.remaining_capacity: [3],
        }
    )

    violations = find_allocation_policy_violations(summary_df, pool, policy=policy)
    assert violations.empty
