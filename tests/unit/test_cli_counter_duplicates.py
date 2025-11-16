import types
from pathlib import Path

import pandas as pd

from app.infra import cli
from app.core.policy_loader import load_policy


def _load_policy() -> object:
    return load_policy(Path("config/policy.json"))


def _build_args(**overrides):
    payload = {
        "prior_roster": None,
        "current_roster": None,
        "academic_year": 1404,
        "counter_duplicate_strategy": "prompt",
        "_ui_overrides": {},
    }
    payload.update(overrides)
    return types.SimpleNamespace(**payload)


def test_inject_student_ids_drop_strategy_removes_extra_rows():
    students = pd.DataFrame(
        {
            "national_id": ["0000000001", "0000000001", "0000000002"],
            "gender": [1, 1, 0],
        }
    )
    args = _build_args(counter_duplicate_strategy="drop")
    policy = _load_policy()

    counters, _, updated_students = cli._inject_student_ids(students, args, policy)

    assert len(counters) == 2
    assert len(updated_students) == 2
    assert counters.is_unique
    assert "0000000002" in updated_students["national_id"].astype(str).tolist()


def test_inject_student_ids_assign_new_strategy_keeps_all_rows():
    students = pd.DataFrame(
        {
            "national_id": ["0000000003", "0000000003"],
            "gender": [0, 0],
        }
    )
    args = _build_args(counter_duplicate_strategy="assign-new")
    policy = _load_policy()

    counters, summary, updated_students = cli._inject_student_ids(students, args, policy)

    assert len(counters) == 2
    assert counters.is_unique
    assert summary.get("duplicate_resolution_mode") == "assign-new"
    assert summary.get("duplicate_resolution_count") == 1
    assert len(updated_students) == 2
