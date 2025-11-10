from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infra import cli


@pytest.fixture()
def policy_file(tmp_path: Path) -> Path:
    payload = {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio", "ascending": True},
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
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return policy_path


def test_build_matrix_command_uses_progress(policy_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    called: dict[str, str] = {}

    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        called["inspactor"] = args.inspactor
        called["policy_version"] = policy.version
        progress(5, "start")
        progress(100, "done")
        return 0

    exit_code = cli.main(
        [
            "build-matrix",
            "--inspactor",
            "insp.xlsx",
            "--schools",
            "schools.xlsx",
            "--crosswalk",
            "cross.xlsx",
            "--output",
            "out.xlsx",
            "--policy",
            str(policy_file),
        ],
        build_runner=fake_runner,
    )

    captured = capsys.readouterr()
    assert "  5% | start" in captured.out
    assert "100% | done" in captured.out
    assert exit_code == 0
    assert called["policy_version"] == "1.0.3"


def test_allocate_command_passes_through(policy_file: Path) -> None:
    observed: dict[str, str] = {}

    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        observed["students"] = args.students
        observed["pool"] = args.pool
        observed["capacity"] = args.capacity_column
        progress(10, "alloc")
        return 0

    exit_code = cli.main(
        [
            "allocate",
            "--students",
            "students.csv",
            "--pool",
            "pool.csv",
            "--output",
            "alloc.xlsx",
            "--capacity-column",
            "remaining_capacity",
            "--policy",
            str(policy_file),
        ],
        allocate_runner=fake_runner,
    )

    assert exit_code == 0
    assert observed == {
        "students": "students.csv",
        "pool": "pool.csv",
        "capacity": "remaining_capacity",
    }
