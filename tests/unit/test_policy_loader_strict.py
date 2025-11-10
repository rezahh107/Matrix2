from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.policy_loader import load_policy  # noqa: E402


def _write_policy(tmp_path: Path, payload: dict) -> Path:
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return policy_path


def test_policy_missing_required_keys_raises(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, {"version": "1.0.3"})
    with pytest.raises(ValueError):
        load_policy(policy_path)


def test_policy_join_keys_must_have_six_entries(tmp_path: Path) -> None:
    bad_policy = {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "join_keys": ["a", "b", "c", "d", "e"],
        "ranking": ["min_occupancy_ratio", "min_allocations_new", "min_mentor_id"],
    }
    policy_path = _write_policy(tmp_path, bad_policy)
    with pytest.raises(ValueError):
        load_policy(policy_path)
