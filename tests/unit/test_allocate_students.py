from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.allocate_students import allocate_batch


@pytest.fixture()
def _base_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": ["زهرا", "علی"],
            "کد کارمندی پشتیبان": ["EMP-001", "EMP-002"],
            "کدرشته": [1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی"],
            "جنسیت": [1, 1],
            "دانش آموز فارغ": [0, 0],
            "مرکز گلستان صدرا": [1, 1],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [3581, 3581],
            "remaining_capacity": [2, 2],
            "occupancy_ratio": [0.1, 0.2],
            "allocations_new": [0, 0],
        }
    )


def _single_student(**overrides: object) -> pd.DataFrame:
    base = {
        "student_id": "STD-001",
        "کدرشته": 1201,
        "گروه_آزمایشی": "تجربی",
        "جنسیت": 1,
        "دانش_آموز_فارغ": 0,
        "مرکز_گلستان_صدرا": 1,
        "مالی_حکمت_بنیاد": 0,
        "کد_مدرسه": 3581,
    }
    base.update(overrides)
    return pd.DataFrame([base])


def test_allocate_batch_no_match_sets_error(_base_pool: pd.DataFrame) -> None:
    students = _single_student(**{"کد_مدرسه": 9999})

    allocations, updated_pool, logs, _ = allocate_batch(students, _base_pool)

    assert allocations.empty
    assert updated_pool.equals(_base_pool)
    assert logs.iloc[0]["error_type"] == "ELIGIBILITY_NO_MATCH"
    assert logs.iloc[0]["detailed_reason"] == "No candidates matched join keys"


def test_allocate_batch_capacity_full_sets_error(_base_pool: pd.DataFrame) -> None:
    students = _single_student()
    pool = _base_pool.assign(remaining_capacity=[0, 0])

    allocations, updated_pool, logs, _ = allocate_batch(students, pool)

    assert allocations.empty
    assert (updated_pool["remaining_capacity"] == 0).all()
    assert logs.iloc[0]["error_type"] == "CAPACITY_FULL"
    assert logs.iloc[0]["candidate_count"] == 2
    assert logs.iloc[0]["detailed_reason"] == "No capacity among matched candidates"


def test_allocate_batch_progress_reports_start_and_end(_base_pool: pd.DataFrame) -> None:
    students = pd.concat([_single_student(), _single_student(student_id="STD-002")], ignore_index=True)
    progress_calls: List[Tuple[int, str]] = []

    def _progress(pct: int, msg: str) -> None:
        progress_calls.append((pct, msg))

    allocate_batch(students, _base_pool, progress=_progress)

    assert progress_calls[0][0] == 0
    assert progress_calls[0][1] == "start"
    assert any(pct == 100 for pct, _ in progress_calls)
    assert progress_calls[-1][1] == "done"


@pytest.mark.skipif(importlib.util.find_spec("openpyxl") is None, reason="openpyxl لازم است")
def test_allocation_outputs_excel_openable(tmp_path: Path, _base_pool: pd.DataFrame) -> None:
    from openpyxl import load_workbook

    from app.infra.io_utils import write_xlsx_atomic

    students = pd.concat(
        [_single_student(), _single_student(student_id="STD-002")],
        ignore_index=True,
    )

    allocations, updated_pool, logs, trace = allocate_batch(students, _base_pool)

    out_path = tmp_path / "allocation_bundle.xlsx"
    write_xlsx_atomic(
        {
            "allocations": allocations,
            "pool": updated_pool,
            "logs": logs,
            "trace": trace,
        },
        out_path,
    )

    workbook = load_workbook(out_path)
    assert set(workbook.sheetnames) == {"allocations", "pool", "logs", "trace"}
