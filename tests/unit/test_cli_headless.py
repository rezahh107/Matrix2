from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from app.core.policy_loader import load_policy
from app.infra import cli
from app.infra.io_utils import ALT_CODE_COLUMN
from tests.conftest import JOIN_KEYS_6

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
        "coverage_threshold": 0.95,
        "dedup_removed_ratio_threshold": 0.05,
        "school_lookup_mismatch_threshold": 0.0,
        "join_key_duplicate_threshold": 0,
        "alias_rule": {"normal": "postal_or_fallback_mentor_id", "school": "mentor_id"},
        "join_keys": JOIN_KEYS_6,
        "gender_codes": {
            "male": {"value": 1, "counter_code": "357"},
            "female": {"value": 0, "counter_code": "373"},
        },
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
        "virtual_alias_ranges": [[7000, 7999]],
        "virtual_name_patterns": ["در\\s+انتظار\\s+تخصیص"],
        "excel": {
            "rtl": True,
            "font_name": "Tahoma",
            "font_size": 8,
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


def test_cli_reports_coverage_threshold_error(policy_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        error = ValueError("نسبت پوشش پایین است")
        setattr(error, "is_coverage_threshold_error", True)
        raise error

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
    assert exit_code == 2
    assert "نسبت پوشش" in captured.err
    assert captured.out == ""


def test_cli_reports_dedup_threshold_error(policy_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        error = ValueError("حذف رکوردهای تکراری زیاد است")
        setattr(error, "is_dedup_removed_threshold_error", True)
        raise error

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
    assert exit_code == 2
    assert "حذف رکوردهای تکراری" in captured.err
    assert captured.out == ""


def test_cli_reports_duplicate_threshold_error(
    policy_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        error = ValueError("کلید تکراری")
        setattr(error, "is_join_key_duplicate_threshold_error", True)
        raise error

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
    assert exit_code == 2
    assert "کلید" in captured.err
    assert captured.out == ""


def test_cli_reports_school_lookup_threshold_error(
    policy_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        error = ValueError("کد/نام مدرسه ناشناخته")
        setattr(error, "is_school_lookup_threshold_error", True)
        raise error

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
    assert exit_code == 2
    assert "مدرسه" in captured.err
    assert captured.out == ""


def test_cli_propagates_coverage_error_for_ui(policy_file: Path) -> None:
    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        error = ValueError("fail")
        setattr(error, "is_coverage_threshold_error", True)
        raise error

    with pytest.raises(ValueError):
        cli.main(
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
            ui_overrides={},
        )


def test_run_build_matrix_raises_on_duplicate_threshold_exceeded(
    tmp_path: Path,
    policy_file: Path,
    mentor_pool_with_duplicates: pd.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = load_policy(policy_file)
    schools_stub = pd.DataFrame({"کد مدرسه": pd.Series([101], dtype="Int64")})
    crosswalk_stub = pd.DataFrame(
        {
            "کد مدرسه": pd.Series([101], dtype="Int64"),
            ALT_CODE_COLUMN: pd.Series([101], dtype="Int64"),
        }
    )
    insp = tmp_path / "insp.xlsx"
    schools = tmp_path / "schools.xlsx"
    crosswalk = tmp_path / "cross.xlsx"
    output = tmp_path / "out.xlsx"
    for path in (insp, schools, crosswalk):
        path.write_text("placeholder", encoding="utf-8")

    args = argparse.Namespace(
        inspactor=str(insp),
        schools=str(schools),
        crosswalk=str(crosswalk),
        output=str(output),
        min_coverage=None,
        local_db_path=str(tmp_path / "local.db"),
    )

    monkeypatch.setattr(
        cli,
        "import_mentor_pool_from_excel",
        lambda *_args, **_kwargs: mentor_pool_with_duplicates,
    )
    monkeypatch.setattr(cli, "import_school_report_from_excel", lambda *_args, **_kwargs: schools_stub)
    monkeypatch.setattr(
        cli,
        "import_school_crosswalk_from_excel",
        lambda *_args, **_kwargs: (crosswalk_stub, pd.DataFrame()),
    )
    monkeypatch.setattr(
        cli,
        "get_school_reference_frames",
        lambda *_args, **_kwargs: (schools_stub, crosswalk_stub, pd.DataFrame()),
    )

    def fake_build_matrix(
        insp_df: pd.DataFrame,
        schools_df: pd.DataFrame,
        crosswalk_groups_df: pd.DataFrame,
        *,
        crosswalk_synonyms_df: pd.DataFrame,
        cfg,
        progress,
    ):  # type: ignore[no-untyped-def]
        assert_frame_equal(insp_df.reset_index(drop=True), mentor_pool_with_duplicates.reset_index(drop=True))
        assert_frame_equal(schools_df.reset_index(drop=True), schools_stub.reset_index(drop=True))
        assert_frame_equal(crosswalk_groups_df.reset_index(drop=True), crosswalk_stub.reset_index(drop=True))
        validation = pd.DataFrame(
            [
                {
                    "join_key_duplicate_rows": len(insp_df),
                    "join_key_duplicate_threshold": 0,
                    "warning_type": "duplicate",
                    "warning_message": "join-key duplicates detected",
                    "warning_payload": pd.NA,
                }
            ]
        )
        duplicates = insp_df.iloc[:2].copy()
        empties = pd.DataFrame()
        return (empties, validation, empties, empties, empties, empties, duplicates, empties)

    monkeypatch.setattr(cli, "build_matrix", fake_build_matrix)
    monkeypatch.setattr(cli, "write_xlsx_atomic", lambda *_, **__: None)

    with pytest.raises(ValueError) as excinfo:
        cli._run_build_matrix(args, policy, lambda *_args: None)

    assert getattr(excinfo.value, "is_join_key_duplicate_threshold_error", False)
    message = str(excinfo.value)
    assert "آستانهٔ مجاز" in message
    assert "(" in message and ")" in message


def test_run_build_matrix_verifies_policy_version(
    tmp_path: Path,
    policy_file: Path,
    mentor_pool_empty: pd.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = load_policy(policy_file)
    schools_stub = pd.DataFrame({"کد مدرسه": pd.Series([], dtype="Int64")})
    crosswalk_stub = pd.DataFrame(
        {
            "کد مدرسه": pd.Series([], dtype="Int64"),
            ALT_CODE_COLUMN: pd.Series([], dtype="Int64"),
        }
    )
    insp = tmp_path / "insp.xlsx"
    schools = tmp_path / "schools.xlsx"
    crosswalk = tmp_path / "cross.xlsx"
    output = tmp_path / "out.xlsx"
    for path in (insp, schools, crosswalk):
        path.write_text("placeholder", encoding="utf-8")

    args = argparse.Namespace(
        inspactor=str(insp),
        schools=str(schools),
        crosswalk=str(crosswalk),
        output=str(output),
        min_coverage=None,
        policy_version="sha256:deadbeef",
        local_db_path=str(tmp_path / "local.db"),
    )

    monkeypatch.setattr(cli, "import_mentor_pool_from_excel", lambda *_args, **_kwargs: mentor_pool_empty)
    monkeypatch.setattr(cli, "import_school_report_from_excel", lambda *_args, **_kwargs: schools_stub)
    monkeypatch.setattr(
        cli,
        "import_school_crosswalk_from_excel",
        lambda *_args, **_kwargs: (crosswalk_stub, pd.DataFrame()),
    )
    monkeypatch.setattr(
        cli,
        "get_school_reference_frames",
        lambda *_args, **_kwargs: (schools_stub, crosswalk_stub, pd.DataFrame()),
    )
    called: dict[str, bool] = {"build": False}

    def _build_matrix_guard(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        called["build"] = True
        raise AssertionError("build_matrix should not be invoked on version mismatch")

    monkeypatch.setattr(cli, "build_matrix", _build_matrix_guard)

    with pytest.raises(ValueError, match="policy version mismatch"):
        cli._run_build_matrix(args, policy, lambda *_args: None)

    assert not called["build"]


def test_allocate_command_passes_through(policy_file: Path) -> None:
    observed: dict[str, object] = {}

    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        observed["students"] = args.students
        observed["pool"] = args.pool
        observed["capacity"] = args.capacity_column
        observed["academic_year"] = args.academic_year
        progress(5, "alloc")
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
            "--academic-year",
            "1404",
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
        "academic_year": 1404,
    }


def test_rule_engine_command_passes_through(policy_file: Path) -> None:
    observed: dict[str, object] = {}

    def fake_runner(args, policy, progress):  # type: ignore[no-untyped-def]
        observed["matrix"] = args.matrix
        observed["students"] = args.students
        observed["capacity"] = args.capacity_column
        observed["academic_year"] = args.academic_year
        progress(5, "rule")
        return 0

    exit_code = cli.main(
        [
            "rule-engine",
            "--matrix",
            "matrix.xlsx",
            "--students",
            "students.xlsx",
            "--output",
            "rule.xlsx",
            "--capacity-column",
            "remaining_capacity",
            "--academic-year",
            "1403",
            "--policy",
            str(policy_file),
        ],
        rule_engine_runner=fake_runner,
    )

    assert exit_code == 0
    assert observed == {
        "matrix": "matrix.xlsx",
        "students": "students.xlsx",
        "capacity": "remaining_capacity",
        "academic_year": 1403,
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


def test_inject_student_ids_ui_mode_disables_prompt(
    monkeypatch: pytest.MonkeyPatch, policy_file: Path
) -> None:
    policy = load_policy(policy_file)
    students_df = pd.DataFrame(
        {
            "national_id": ["1", "2"],
            "gender": [1, 1],
        }
    )
    args = argparse.Namespace(
        counter_duplicate_strategy=None,
        academic_year=1402,
        prior_roster=None,
        current_roster=None,
        _ui_overrides={"academic_year": 1402},
        _ui_mode=True,
    )

    def _clone(df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
        return df.copy()

    base_counters = pd.Series(["STD-1", "STD-1"], index=students_df.index, dtype="string")
    base_counters.attrs["counter_summary"] = {"reused_count": 0}

    def fake_assign(*_args, **_kwargs):
        return base_counters.copy()

    observed: dict[str, object] = {}

    def fake_apply(**kwargs):
        observed["interactive"] = kwargs["interactive"]
        observed["strategy"] = kwargs["strategy"]
        resolved = pd.Series(["STD-1", "STD-2"], index=kwargs["counters"].index, dtype="string")
        resolved.attrs["counter_summary"] = {"reused_count": 0}
        return resolved, False, tuple()

    class _DummyStdin:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(cli, "canonicalize_headers", _clone)
    monkeypatch.setattr(cli, "enrich_school_columns_en", lambda df, empty_as_zero: df)
    monkeypatch.setattr(cli, "assign_counters", fake_assign)
    monkeypatch.setattr(cli, "find_duplicate_student_id_groups", lambda counters: {"STD-1": list(counters.index)})
    monkeypatch.setattr(cli, "_apply_counter_duplicate_strategy", fake_apply)
    monkeypatch.setattr(cli, "assert_unique_student_ids", lambda *_: None)
    monkeypatch.setattr(cli.sys, "stdin", _DummyStdin())

    student_ids, summary, _ = cli._inject_student_ids(students_df.copy(), args, policy)

    assert isinstance(student_ids, pd.Series)
    assert summary["reused_count"] == 0
    assert observed["interactive"] is False
    assert observed["strategy"] == "assign-new"


def test_build_duplicate_row_report_handles_numeric_national_id() -> None:
    students_df = pd.DataFrame({"student_id": ["S-1", "S-1"]})
    students_df.index = [0, 1]
    students_en = pd.DataFrame(
        {
            "student_id": ["S-1", "S-1"],
            "national_id": [np.int64(1234567890), " 0042 "],
            "full_name": ["الف", "ب"],
        },
        index=students_df.index,
    )
    duplicate_groups = {"S-1": [0, 1]}

    report = cli._build_duplicate_row_report(students_df, students_en, duplicate_groups)

    assert report[0]["rows"][0]["national_id"] == "1234567890"
    assert report[0]["rows"][1]["national_id"] == "0042"


def test_load_matrix_candidate_pool_filters_virtual(
    tmp_path: Path, policy_file: Path
) -> None:
    engine = None
    for candidate in ("openpyxl", "xlsxwriter"):
        if importlib.util.find_spec(candidate) is not None:
            engine = candidate
            break
    if engine is None:
        pytest.skip("اکسل‌نویس در دسترس نیست")

    matrix_path = tmp_path / "matrix.xlsx"
    df = pd.DataFrame(
        {
            "mentor_name": ["مجازی", "علی"],
            "alias": [7505, 102],
            "remaining_capacity": [0, 5],
            "allocations_new": [0, 0],
            "mentor_id": [1, 2],
            "کدرشته": [1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی"],
            "جنسیت": [1, 1],
            "دانش آموز فارغ": [0, 0],
            "مرکز گلستان صدرا": [0, 0],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [1010, 1010],
        }
    )
    with pd.ExcelWriter(matrix_path, engine=engine) as writer:
        df.to_excel(writer, sheet_name="matrix", index=False)

    policy = load_policy(policy_file)
    pool_raw = cli._load_matrix_candidate_pool(matrix_path, policy)
    assert sorted(pool_raw["mentor_id"].astype(int)) == [1, 2]

    students = pd.DataFrame(
        {
            "student_id": ["STD-1"],
            "کدرشته": [1201],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [1010],
        }
    )
    _, pool_canon = cli._prepare_allocation_frames(
        students,
        pool_raw,
        policy=policy,
        sanitize_pool=True,
        pool_source="matrix",
    )

    assert list(pool_canon["mentor_id"].astype(int)) == [2]
    assert "allocations_new" in pool_canon.columns
