from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.infra import cli


def _write_excel(df: pd.DataFrame, path: Path, *, sheet_name: str) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)


def test_cli_allocate_accepts_counter_args(tmp_path: Path) -> None:
    students = pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "national_id": "0000000001",
                "gender": 1,
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
                "national_id": "0000000002",
                "gender": 0,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
            },
        ]
    )
    students_path = tmp_path / "students.xlsx"
    _write_excel(students, students_path, sheet_name="Students")

    pool = pd.DataFrame(
        [
            {
                "mentor_name": "منتور الف",
                "mentor_id": 101,
                "alias": 101,
                "remaining_capacity": 1,
                "allocations_new": 0,
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
                "mentor_id": 102,
                "alias": 102,
                "remaining_capacity": 1,
                "allocations_new": 0,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 0,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 102,
            },
        ]
    )
    pool_path = tmp_path / "pool.xlsx"
    _write_excel(pool, pool_path, sheet_name="Pool")

    prior = pd.DataFrame(
        {
            "national_id": ["0000000001"],
            "student_id": ["533570123"],
        }
    )
    prior_path = tmp_path / "prior.xlsx"
    _write_excel(prior, prior_path, sheet_name="شمارنده")

    current = pd.DataFrame({"student_id": ["543570099", "543730050"]})
    current_path = tmp_path / "current.xlsx"
    _write_excel(current, current_path, sheet_name="StudentReport")

    output_path = tmp_path / "allocations.xlsx"

    argv = [
        "allocate",
        "--students",
        str(students_path),
        "--pool",
        str(pool_path),
        "--output",
        str(output_path),
        "--capacity-column",
        "remaining_capacity",
        "--policy",
        "config/policy.json",
        "--academic-year",
        "1404",
        "--prior-roster",
        str(prior_path),
        "--current-roster",
        str(current_path),
    ]

    exit_code = cli.main(argv)
    assert exit_code == 0

    allocations = pd.read_excel(output_path, sheet_name="allocations")
    allocations_en = canonicalize_headers(allocations, header_mode="en")
    assert "student_id" in allocations_en.columns
    assert allocations_en["student_id"].notna().all()
