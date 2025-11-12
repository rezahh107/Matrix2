from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas.testing as pd_testing
import pytest

from app.core.allocate_students import allocate_batch, build_selection_reason_rows
from app.core.policy_loader import load_policy
from app.infra.cli import _sanitize_pool_for_allocation
from app.infra.excel.exporter import write_selection_reasons_sheet
from app.infra.io_utils import write_xlsx_atomic


def _ensure_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    for engine in ("openpyxl", "xlsxwriter"):
        try:
            __import__(engine)
        except Exception:
            continue
        monkeypatch.setenv("EXCEL_ENGINE", engine)
        return
    pytest.skip("هیچ engine اکسل در محیط تست نصب نیست")


def test_reason_sheet_schema_and_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ensure_engine(monkeypatch)
    policy = load_policy()
    expected_columns = list(policy.emission.selection_reasons.columns)

    students = pd.DataFrame(
        [
            {
                "student_id": "543570001",
                "کدملی": "0012345678",
                "نام": "زهرا",
                "نام خانوادگی": "محمدی",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.female.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
                "after_school": "true",
            },
            {
                "student_id": "543570002",
                "کدملی": "0098765432",
                "نام": "علی",
                "نام خانوادگی": "کاظمی",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.male.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
                "after_school": "false",
            },
            {
                "student_id": "543570003",
                "کدملی": "0076543210",
                "نام": "مریم",
                "نام خانوادگی": "حیدری",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.female.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
                "after_school": "false",
            },
        ]
    )

    pool_raw = pd.DataFrame(
        [
            {
                "mentor_name": "منتور الف",
                "alias": 101,
                "remaining_capacity": 2,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.female.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
                "کد کارمندی پشتیبان": 101,
            },
            {
                "mentor_name": "منتور ب",
                "alias": 102,
                "remaining_capacity": 3,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.male.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
                "کد کارمندی پشتیبان": 102,
            },
        ]
    )

    sanitized_pool = _sanitize_pool_for_allocation(pool_raw, policy=policy)
    allocations, _, logs, trace = allocate_batch(students, sanitized_pool, policy=policy)

    reasons = build_selection_reason_rows(
        allocations,
        students,
        sanitized_pool,
        policy=policy,
        logs=logs,
        trace=trace,
    )
    assert list(reasons.columns) == expected_columns
    assert reasons["شمارنده"].tolist() == list(range(1, len(reasons) + 1))
    for column in expected_columns:
        if column == "شمارنده":
            continue
        assert reasons[column].dtype.name.startswith("string")

    sheet_name, sanitized = write_selection_reasons_sheet(reasons, writer=None, policy=policy)
    assert list(sanitized.columns) == expected_columns
    assert sanitized["شمارنده"].tolist() == list(range(1, len(sanitized) + 1))

    output = tmp_path / "reasons.xlsx"
    write_xlsx_atomic({sheet_name: sanitized}, output)
    assert output.exists()

    with pd.ExcelFile(output) as workbook:
        assert sheet_name in workbook.sheet_names
        parsed = workbook.parse(sheet_name, dtype=str)

    snapshot_path = Path("tests/snapshots/reason_sheet_expected.csv")
    expected = pd.read_csv(snapshot_path, dtype=str)
    pd_testing.assert_frame_equal(
        parsed.head(len(expected)).loc[:, expected_columns],
        expected.loc[:, expected_columns],
        check_dtype=False,
    )
