import argparse
import argparse
import json

import pandas as pd

from app.core.allocation.mentor_pool import (
    MentorPoolGovernanceConfig,
    apply_manager_mentor_governance,
    apply_mentor_pool_governance,
)
from app.core.policy_loader import MentorStatus, get_policy
from app.infra import cli


def test_apply_mentor_pool_governance_filters_overrides() -> None:
    pool = pd.DataFrame({"mentor_id": ["1", "2"], "mentor_status": ["ACTIVE", "ACTIVE"]})
    config = MentorPoolGovernanceConfig(
        default_status=MentorStatus.ACTIVE,
        mentor_status_map={},
        allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
    )

    filtered = apply_mentor_pool_governance(pool, config, overrides={"1": False})

    assert list(filtered["mentor_id"]) == ["2"]
    assert filtered.attrs["mentor_pool_governance"]["removed"] == 1


def test_resolve_overrides_merges_ui_and_cli(monkeypatch):
    args = argparse.Namespace(
        mentor_overrides=json.dumps({"A": False}),
        _ui_overrides={"mentor_pool_overrides": {"B": True}},
    )
    merged = cli._resolve_mentor_pool_overrides(args)
    assert merged == {"B": True, "A": False}


def test_apply_overrides_in_run_uses_policy_defaults(tmp_path, monkeypatch):
    args = argparse.Namespace(mentor_overrides=None, _ui_overrides={"mentor_pool_overrides": {"9": False}})
    pool = pd.DataFrame({"mentor_id": ["9", "10"], "mentor_status": ["ACTIVE", "ACTIVE"]})
    policy = get_policy()
    result = cli._apply_mentor_pool_overrides(pool, policy, args)
    assert list(result["mentor_id"]) == ["10"]


def test_apply_manager_overrides_filters_rows_before_matrix_build():
    df = pd.DataFrame(
        {"mentor_id": ["1", "2"], "manager": ["A", "B"], "mentor_status": ["ACTIVE", "ACTIVE"]}
    )
    cfg = MentorPoolGovernanceConfig(
        default_status=MentorStatus.ACTIVE,
        mentor_status_map={},
        allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
    )

    filtered = apply_manager_mentor_governance(
        df,
        cfg,
        mentor_overrides={"2": True},
        manager_overrides={"B": False},
    )

    assert list(filtered["mentor_id"]) == ["1"]
    meta = filtered.attrs["mentor_pool_governance"]
    assert meta["manager_removed"] == 1


def test_resolve_manager_overrides_merges_sources():
    args = argparse.Namespace(
        manager_overrides=json.dumps({"X": False}), _ui_overrides={"mentor_pool_manager_overrides": {"Y": True}}
    )
    merged = cli._resolve_manager_overrides(args)
    assert merged == {"Y": True, "X": False}
