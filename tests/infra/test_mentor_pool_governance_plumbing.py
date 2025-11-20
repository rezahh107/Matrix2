import argparse
import json

import pandas as pd

from app.core.allocation.mentor_pool import MentorPoolGovernanceConfig, apply_mentor_pool_governance
from app.core.policy_loader import get_policy
from app.infra import cli


def test_apply_mentor_pool_governance_filters_overrides() -> None:
    pool = pd.DataFrame({"mentor_id": ["1", "2"], "mentor_status": ["ACTIVE", "ACTIVE"]})
    config = MentorPoolGovernanceConfig(enabled=True)

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
