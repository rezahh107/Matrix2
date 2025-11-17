from __future__ import annotations

import json
from pathlib import Path

import pytest

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
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio", "ascending": True},
            {
                "name": "max_remaining_capacity",
                "column": "remaining_capacity_desc",
                "ascending": True,
            },
            {"name": "min_allocations_new", "column": "allocations_new", "ascending": True},
            {"name": "min_mentor_id", "column": "mentor_sort_key", "ascending": True},
        ],
        "trace_stages": [
            {"stage": "type", "column": "کدرشته"},
            {"stage": "group", "column": "گروه آزمایشی"},
            {"stage": "gender", "column": "جنسیت"},
            {"stage": "graduation_status", "column": "دانش آموز فارغ"},
            {"stage": "center", "column": "مرکز گلستان صدرا"},
            {"stage": "finance", "column": "مالی حکمت بنیاد"},
            {"stage": "school", "column": "کد مدرسه"},
            {"stage": "capacity_gate", "column": "remaining_capacity"},
        ],
    }
    policy_path = _write_policy(tmp_path, bad_policy)
    with pytest.raises(ValueError):
        load_policy(policy_path)
