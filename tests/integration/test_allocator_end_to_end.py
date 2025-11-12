from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pandas.testing as pd_testing
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy
from app.core.common.columns import canonicalize_headers
from app.infra.audit_allocations import audit_allocations
from app.infra.cli import _sanitize_pool_for_allocation
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


def test_allocator_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _ensure_engine(monkeypatch)
    policy = load_policy()

    students = pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
            {
                "student_id": "STD-2",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
            {
                "student_id": "STD-3",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
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
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 101,
            },
            {
                "mentor_name": "منتور ب",
                "alias": 102,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 102,
            },
            {
                "mentor_name": "در انتظار تخصیص",
                "alias": 7501,
                "remaining_capacity": 5,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 7501,
            },
        ]
    )

    sanitized_pool = _sanitize_pool_for_allocation(pool_raw, policy=policy)
    assert "mentor_name" in sanitized_pool.columns

    allocations, updated_pool, logs, trace = allocate_batch(
        students,
        sanitized_pool,
        policy=policy,
    )

    header_internal = policy.excel.header_mode_internal
    sheets = {
        "allocations": canonicalize_headers(allocations, header_mode=header_internal),
        "updated_pool": canonicalize_headers(updated_pool, header_mode=header_internal),
        "logs": canonicalize_headers(logs, header_mode=header_internal),
        "trace": canonicalize_headers(trace, header_mode=header_internal),
    }

    output = tmp_path / "alloc-end-to-end.xlsx"
    write_xlsx_atomic(
        sheets,
        output,
        rtl=policy.excel.rtl,
        font_name=policy.excel.font_name,
        header_mode=policy.excel.header_mode_write,
    )

    assert output.exists()

    report = audit_allocations(output)
    assert report["VirtualMentorHits"]["count"] == 0
    assert report["CapacityStuck"]["count"] == 0
    assert report["TraceMismatch"]["count"] == 0

    with pd.ExcelFile(output) as workbook:
        updated_columns = workbook.parse("updated_pool").columns.tolist()
    assert any(" | " in str(col) for col in updated_columns)


def test_allocator_determinism() -> None:
    policy = load_policy()

    students = pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
            {
                "student_id": "STD-2",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
        ]
    )

    pool_raw = pd.DataFrame(
        [
            {
                "mentor_name": "منتور الف",
                "alias": 101,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 101,
            },
            {
                "mentor_name": "منتور ب",
                "alias": 102,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 102,
            },
        ]
    )

    pool = _sanitize_pool_for_allocation(pool_raw, policy=policy)

    first = allocate_batch(students.copy(deep=True), pool.copy(deep=True), policy=policy)
    second = allocate_batch(students.copy(deep=True), pool.copy(deep=True), policy=policy)

    header_internal = policy.excel.header_mode_internal

    for lhs, rhs in zip(first, second):
        left = canonicalize_headers(lhs, header_mode=header_internal).reset_index(drop=True)
        right = canonicalize_headers(rhs, header_mode=header_internal).reset_index(drop=True)
        pd_testing.assert_frame_equal(left, right)
