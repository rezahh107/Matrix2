from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from app.infra import cli
from app.infra.io_utils import ALT_CODE_COLUMN


_HAS_OPENPYXL = importlib.util.find_spec("openpyxl") is not None


@pytest.fixture()
def policy_file(tmp_path: Path) -> Path:
    payload = {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "postal_valid_range": [1000, 9999],
        "finance_variants": [0, 1, 3],
        "center_map": {"شهدخت کشاورز": 1, "آیناز هوشمند": 2, "*": 0},
        "school_code_empty_as_zero": True,
        "prefer_major_code": True,
        "alias_rule": {"normal": "postal_or_fallback_mentor_id", "school": "mentor_id"},
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "columns": {
            "postal_code": "کدپستی",
            "school_count": "تعداد مدارس تحت پوشش",
            "school_code": "کد مدرسه",
            "capacity_current": "تعداد داوطلبان تحت پوشش",
            "capacity_special": "تعداد تحت پوشش خاص",
            "remaining_capacity": "remaining_capacity",
        },
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
        "virtual_alias_ranges": [[7000, 7999]],
        "virtual_name_patterns": ["در\\s+انتظار\\s+تخصیص"],
        "excel": {
            "rtl": True,
            "font_name": "Vazirmatn",
            "header_mode_internal": "en",
            "header_mode_write": "fa_en",
        },
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


@pytest.mark.parametrize(
    ("file_format", "writer"),
    [
        ("xlsx", lambda df, p: df.to_excel(p, index=False)),
        ("csv", lambda df, p: df.to_csv(p, index=False)),
    ],
    ids=["excel", "csv"],
)
def test_detect_reader_coerces_alt_code(
    tmp_path: Path, file_format: str, writer: Callable[[pd.DataFrame, Path], None]
) -> None:
    if file_format == "xlsx" and not _HAS_OPENPYXL:
        pytest.skip("openpyxl لازم است برای خواندن .xlsx")

    sample = tmp_path / f"students.{file_format}"
    df = pd.DataFrame({ALT_CODE_COLUMN: [987654], "name": ["x"]})
    writer(df, sample)

    reader = cli._detect_reader(sample)
    loaded = reader(sample)

    assert loaded[ALT_CODE_COLUMN].dtype == object
    assert loaded.loc[0, ALT_CODE_COLUMN] == "987654"
