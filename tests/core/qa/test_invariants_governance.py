import json

import pandas as pd

from app.core.qa import invariants
from app.core.policy_loader import MentorStatus, parse_policy_dict


def _policy_with_disabled(mentor_id: int):
    with open("config/policy.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["mentor_pool_governance"] = {
        "default_status": "active",
        "allowed_statuses": ["active", "inactive"],
        "mentors": [{"mentor_id": mentor_id, "status": "inactive"}],
    }
    return parse_policy_dict(payload)


def test_governance_violation_for_disabled_mentor():
    policy = _policy_with_disabled(501)
    allocation = pd.DataFrame({"student_id": [1], "mentor_id": [501]})

    result = invariants.check_GOV_01(
        allocation=allocation,
        allocation_summary=None,
        policy=policy,
        overrides=None,
    )

    assert not result.passed
    assert result.violations[0].details["mentor_id"] == 501


def test_governance_override_enables_disabled_mentor():
    policy = _policy_with_disabled(777)
    allocation = pd.DataFrame({"student_id": [2], "mentor_id": [777]})

    report = invariants.run_all_invariants(
        policy=policy,
        allocation=allocation,
        allocation_summary=None,
        governance_overrides={777: True},
    )

    assert report.passed


def test_default_inactive_blocks_unknown():
    with open("config/policy.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["mentor_pool_governance"] = {
        "default_status": "inactive",
        "allowed_statuses": ["active", "inactive"],
        "mentors": [],
    }
    policy = parse_policy_dict(payload)
    allocation = pd.DataFrame({"student_id": [3], "mentor_id": [900]})

    result = invariants.check_GOV_01(
        allocation=allocation,
        allocation_summary=None,
        policy=policy,
        overrides=None,
    )

    assert not result.passed
    assert result.violations[0].details["status"] == "inactive"


def test_gov_01_matches_compute_effective_status():
    with open("config/policy.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["mentor_pool_governance"] = {
        "default_status": "active",
        "allowed_statuses": ["active", "inactive"],
        "mentors": [
            {"mentor_id": 1, "status": "inactive"},
            {"mentor_id": 2, "status": "inactive"},
        ],
    }
    policy = parse_policy_dict(payload)
    allocation = pd.DataFrame({"student_id": [1, 2, 3], "mentor_id": [1, 2, 3]})

    overrides = {2: True}
    report = invariants.check_GOV_01(
        allocation=allocation,
        allocation_summary=None,
        policy=policy,
        overrides=overrides,
    )

    effective_status = invariants.compute_effective_status(
        pd.DataFrame({"mentor_id": [1, 2, 3]}), policy.mentor_pool_governance, overrides
    )
    inactive_ids = [
        mid for mid, status in zip((1, 2, 3), effective_status) if status != MentorStatus.ACTIVE
    ]

    violation_ids = {violation.details["mentor_id"] for violation in report.violations}

    assert not report.passed
    assert violation_ids == {1}
    assert set(inactive_ids) == violation_ids
