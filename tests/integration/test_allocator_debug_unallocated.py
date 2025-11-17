"""Integration-style checks for allocation trace diagnostics."""

from __future__ import annotations

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.allocate_students import build_selection_reason_rows
from app.core.common.trace import FinalStatus, build_unallocated_summary
from app.core.policy_loader import load_policy


def _build_basic_frames(policy):
    students = pd.DataFrame(
        [
            {
                "student_id": 1,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 1,
                policy.stage_column("school"): 1112,
                "student_national_code": "001",
                "student_educational_status": 1,
                "student_registration_status": 3,
                "first_name": "Ali",
                "family_name": "Karimi",
            },
            {
                "student_id": 2,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 0,
                policy.stage_column("school"): 0,
                "student_national_code": "002",
                "student_educational_status": 0,
                "student_registration_status": 1,
            },
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
            "mentor_id": ["EMP-1"],
            policy.columns.remaining_capacity: [1],
            "پشتیبان": ["Mentor"],
        }
    )
    return students, pool


def test_trace_summary_includes_unallocated_and_allocated() -> None:
    policy = load_policy()
    students, pool = _build_basic_frames(policy)

    allocations, updated_pool, logs, trace = allocate_batch(students, pool, policy=policy)

    assert allocations.shape[0] == 1
    assert int(updated_pool[policy.columns.remaining_capacity].iloc[0]) == 0

    summary_df = trace.attrs.get("summary_df")
    assert summary_df is not None
    assert summary_df.shape[0] == students.shape[0]
    assert set(summary_df["final_status"].unique()) <= {status.value for status in FinalStatus}
    assert {"candidate_count", "has_candidates", "capacity_candidate_count"}.issubset(
        summary_df.columns
    )
    assert {"student_educational_status", "student_registration_status", "student_national_code"}.issubset(
        summary_df.columns
    )
    assert {"student_first_name", "student_last_name"}.issubset(summary_df.columns)

    school_row = summary_df.loc[summary_df[policy.stage_column("school")] == 1112].iloc[0]
    assert school_row["final_status"] != FinalStatus.ALLOCATED.value
    assert bool(school_row["passed_finance"]) is False

    unallocated_summary = build_unallocated_summary(summary_df)
    assert not unallocated_summary.empty
    assert {"candidate_count", "has_candidates", "capacity_candidate_count"}.issubset(
        unallocated_summary.columns
    )
    assert policy.join_keys[0] in unallocated_summary.columns
    assert trace.attrs.get("policy_violations") is not None
    assert trace.attrs.get("final_status_counts") is not None


def test_selection_reason_contains_join_keys() -> None:
    policy = load_policy()
    students, pool = _build_basic_frames(policy)
    allocations, _, logs, trace = allocate_batch(students, pool, policy=policy)

    reasons = build_selection_reason_rows(
        allocations,
        students,
        pool,
        policy=policy,
        logs=logs,
        trace=trace,
    )

    assert "student_id" in reasons.columns
    assert reasons["student_id"].notna().all()
    assert reasons["کدملی"].notna().all()
