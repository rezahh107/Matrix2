"""Unit tests for trace outcome summarization."""

from __future__ import annotations

import pandas as pd

from app.core.common.trace import (
    FinalStatus,
    build_allocation_trace,
    classify_final_status,
    summarize_trace_outcome,
)
from app.core.policy_loader import load_policy


def test_summarize_trace_marks_failure_stage() -> None:
    policy = load_policy()
    student = {
        "student_id": 1,
        policy.stage_column("group"): 10,
        policy.stage_column("gender"): 0,
        policy.stage_column("graduation_status"): 0,
        policy.stage_column("center"): 1,
        policy.stage_column("finance"): 0,
        policy.stage_column("school"): 1112,
    }
    candidate_pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10, 10],
            policy.stage_column("gender"): [0, 0],
            policy.stage_column("graduation_status"): [0, 0],
            policy.stage_column("center"): [1, 1],
            policy.stage_column("finance"): [0, 0],
            policy.stage_column("school"): [2222, 2222],
            policy.columns.remaining_capacity: [1, 1],
        }
    )
    trace_records = build_allocation_trace(student, candidate_pool, policy=policy)
    log = {
        "allocation_status": "failed",
        "candidate_count": 0,
        "stage_candidate_counts": {
            "type": 2,
            "group": 2,
            "gender": 2,
            "graduation_status": 2,
            "center": 2,
            "finance": 2,
            "school": 0,
        },
    }

    outcome = summarize_trace_outcome(student, trace_records, log, policy=policy)

    assert outcome.failure_stage == "school"
    assert outcome.final_status == FinalStatus.RULE_EXCLUDED.value
    assert outcome.stage_flags["school"] is False


def test_summarize_trace_marks_capacity_failure() -> None:
    policy = load_policy()
    student = {
        "student_id": 2,
        policy.stage_column("group"): 10,
        policy.stage_column("gender"): 0,
        policy.stage_column("graduation_status"): 0,
        policy.stage_column("center"): 1,
        policy.stage_column("finance"): 0,
        policy.stage_column("school"): 0,
    }
    candidate_pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10],
            policy.stage_column("gender"): [0],
            policy.stage_column("graduation_status"): [0],
            policy.stage_column("center"): [1],
            policy.stage_column("finance"): [0],
            policy.stage_column("school"): [0],
            policy.columns.remaining_capacity: [0],
        }
    )
    trace_records = build_allocation_trace(student, candidate_pool, policy=policy)
    log = {"allocation_status": "failed", "candidate_count": 1}

    outcome = summarize_trace_outcome(student, trace_records, log, policy=policy)

    assert outcome.failure_stage == "capacity_gate"
    assert outcome.final_status == FinalStatus.NO_CAPACITY.value
    assert outcome.stage_flags["capacity_gate"] is False


def test_classify_final_status_matrix() -> None:
    assert (
        classify_final_status(
            allocated=True,
            has_candidates=True,
            passed_capacity=True,
            passed_school=True,
            data_ok=True,
        )
        == FinalStatus.ALLOCATED
    )
    assert (
        classify_final_status(
            allocated=False,
            has_candidates=True,
            passed_capacity=False,
            passed_school=True,
            data_ok=True,
        )
        == FinalStatus.NO_CAPACITY
    )
    assert (
        classify_final_status(
            allocated=False,
            has_candidates=False,
            passed_capacity=False,
            passed_school=False,
            data_ok=True,
            rule_reason_code="RULE",
        )
        == FinalStatus.RULE_EXCLUDED
    )
    assert (
        classify_final_status(
            allocated=False,
            has_candidates=False,
            passed_capacity=False,
            passed_school=False,
            data_ok=True,
        )
        == FinalStatus.RULE_EXCLUDED
    )
    assert (
        classify_final_status(
            allocated=False,
            has_candidates=False,
            passed_capacity=False,
            passed_school=False,
            data_ok=False,
        )
        == FinalStatus.DATA_ERROR
    )
