from __future__ import annotations

import pandas as pd

from app.core.policy_loader import load_policy
from app.core.qa.invariants import check_JOIN_01, check_SCHOOL_01


def _sample_matrix(policy) -> pd.DataFrame:
    data = {
        key: [1201] for key in policy.join_keys
    }
    data["has_school_constraint"] = [False]
    data[policy.columns.school_code] = [1010]
    data["mentor_id"] = [1]
    return pd.DataFrame(data)


def test_join_keys_pass() -> None:
    policy = load_policy()
    matrix = _sample_matrix(policy)
    result = check_JOIN_01(matrix=matrix, policy=policy)

    assert result.passed


def test_join_keys_missing_or_wrong_type() -> None:
    policy = load_policy()
    matrix = _sample_matrix(policy)
    matrix.loc[0, policy.join_keys[0]] = None
    matrix[policy.join_keys[1]] = matrix[policy.join_keys[1]].astype(str)

    result = check_JOIN_01(matrix=matrix, policy=policy)

    assert not result.passed
    assert any(v.details and "null_rows" in v.details for v in result.violations)
    assert any("dtype" in (v.details or {}) for v in result.violations)


def test_school_rule_blocks_unrestricted_in_invalid_list() -> None:
    policy = load_policy()
    matrix = _sample_matrix(policy)
    invalid = pd.DataFrame({"mentor_id": [1]})

    result = check_SCHOOL_01(matrix=matrix, invalid_mentors=invalid, policy=policy)

    assert not result.passed
    assert result.violations[0].rule_id == "QA_RULE_SCHOOL_01"


def test_school_rule_requires_school_for_restricted() -> None:
    policy = load_policy()
    matrix = _sample_matrix(policy)
    matrix.loc[0, "has_school_constraint"] = True
    matrix.loc[0, policy.columns.school_code] = 0

    result = check_SCHOOL_01(matrix=matrix, invalid_mentors=None, policy=policy)

    assert not result.passed
    assert result.violations[0].rule_id == "QA_RULE_SCHOOL_01"

