import copy
import json

import pandas as pd

from app.core.allocation.mentor_pool import (
    compute_effective_status,
    filter_active_mentors,
)
from app.core.policy_loader import MentorStatus, parse_policy_dict


def _policy_with_governance(base_payload: dict, governance: dict):
    payload = copy.deepcopy(base_payload)
    payload["mentor_pool_governance"] = governance
    return parse_policy_dict(payload)


def _base_policy_payload() -> dict:
    with open("config/policy.json", "r", encoding="utf-8") as handle:
        return json.load(handle)


def test_filter_retains_all_when_policy_active():
    policy = _policy_with_governance(
        _base_policy_payload(),
        {
            "default_status": "active",
            "allowed_statuses": ["active", "inactive"],
            "mentors": [],
        },
    )
    mentors = pd.DataFrame(
        {
            "mentor_id": [11, 12],
            "نام": ["الف", "ب"],
            "has_school_constraint": [False, True],
        }
    )
    baseline = mentors.copy(deep=True)

    filtered = filter_active_mentors(mentors, policy.mentor_pool_governance)

    assert len(filtered) == 2
    assert filtered["has_school_constraint"].tolist() == [False, True]
    pd.testing.assert_frame_equal(mentors, baseline)


def test_policy_disables_specific_mentor():
    policy = _policy_with_governance(
        _base_policy_payload(),
        {
            "default_status": "active",
            "allowed_statuses": ["active", "inactive"],
            "mentors": [
                {"mentor_id": 20, "status": "inactive"},
            ],
        },
    )
    mentors = pd.DataFrame({"mentor_id": [10, 20], "نام": ["الف", "ب"]})

    statuses = compute_effective_status(mentors, policy.mentor_pool_governance)
    assert statuses.tolist() == [MentorStatus.ACTIVE, MentorStatus.INACTIVE]

    filtered = filter_active_mentors(mentors, policy.mentor_pool_governance)
    assert filtered["mentor_id"].tolist() == [10]


def test_override_enables_disabled_and_attaches_status():
    governance_payload = {
        "default_status": "active",
        "allowed_statuses": ["active", "inactive"],
        "mentors": [
            {"mentor_id": 21, "status": "inactive"},
        ],
    }
    policy = _policy_with_governance(_base_policy_payload(), governance_payload)
    mentors = pd.DataFrame({"mentor_id": [21, 22], "نام": ["ج", "د"]})

    filtered = filter_active_mentors(
        mentors,
        policy.mentor_pool_governance,
        overrides={21: True},
        attach_status=True,
    )

    assert filtered["mentor_id"].tolist() == [21, 22]
    assert filtered["mentor_status"].tolist() == ["active", "active"]


def test_override_disables_active_idempotent():
    policy = _policy_with_governance(
        _base_policy_payload(),
        {
            "default_status": "active",
            "allowed_statuses": ["active", "inactive"],
            "mentors": [],
        },
    )
    mentors = pd.DataFrame({"mentor_id": [31, 32, 33]})

    first = filter_active_mentors(
        mentors, policy.mentor_pool_governance, overrides={32: False}
    )
    second = filter_active_mentors(
        mentors, policy.mentor_pool_governance, overrides={32: False}
    )

    assert first.equals(second)
    assert first["mentor_id"].tolist() == [31, 33]


def test_default_inactive_requires_override():
    policy = _policy_with_governance(
        _base_policy_payload(),
        {
            "default_status": "inactive",
            "allowed_statuses": ["active", "inactive"],
            "mentors": [],
        },
    )
    mentors = pd.DataFrame({"mentor_id": [40, 41]})

    filtered = filter_active_mentors(mentors, policy.mentor_pool_governance)
    assert filtered.empty

    overridden = filter_active_mentors(
        mentors, policy.mentor_pool_governance, overrides={41: True}
    )
    assert overridden["mentor_id"].tolist() == [41]
