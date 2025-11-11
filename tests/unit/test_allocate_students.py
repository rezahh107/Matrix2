from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.allocate_students import allocate_batch
from app.core.common.types import JoinKeyValues
from app.core.policy_loader import parse_policy_dict


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


def test_join_key_values_validates_length() -> None:
    with pytest.raises(ValueError):
        JoinKeyValues({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})


def test_join_key_values_rejects_non_int() -> None:
    with pytest.raises(TypeError):
        JoinKeyValues(
            {
                "a": 1,
                "b": 2,
                "c": "oops",
                "d": 4,
                "e": 5,
                "f": 6,
            }
        )


def test_allocate_batch_join_keys_are_typed(_base_pool: pd.DataFrame) -> None:
    students = _single_student()

    _, _, logs, _ = allocate_batch(students, _base_pool)

    join_values = logs.iloc[0]["join_keys"]
    assert isinstance(join_values, JoinKeyValues)
    assert list(join_values.keys()) == [
        "کدرشته",
        "جنسیت",
        "دانش_آموز_فارغ",
        "مرکز_گلستان_صدرا",
        "مالی_حکمت_بنیاد",
        "کد_مدرسه",
    ]


def test_allocate_batch_logs_capacity_transition(_base_pool: pd.DataFrame) -> None:
    students = _single_student()

    _, updated_pool, logs, _ = allocate_batch(students, _base_pool)

    assert logs.iloc[0]["capacity_before"] == 2
    assert logs.iloc[0]["capacity_after"] == 1
    assert int(updated_pool.loc[0, "remaining_capacity"]) == 1


def test_allocate_batch_invalid_join_value_raises(_base_pool: pd.DataFrame) -> None:
    students = _single_student(**{"کدرشته": ""})

    with pytest.raises(ValueError):
        allocate_batch(students, _base_pool)


def test_policy_required_fields_enforced_from_config(
    _base_pool: pd.DataFrame,
) -> None:
    payload = json.loads(Path("config/policy.json").read_text(encoding="utf-8"))
    payload["required_student_fields"] = payload["join_keys"] + ["exam_group"]
    policy = parse_policy_dict(payload)

    students = _single_student()
    allocate_batch(students, _base_pool, policy=policy)

    missing_group = students.drop(columns=["گروه_آزمایشی"]).copy()
    with pytest.raises(ValueError, match="Missing columns"):
        allocate_batch(missing_group, _base_pool, policy=policy)


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


@pytest.mark.skipif(
    importlib.util.find_spec("openpyxl") is None
    and importlib.util.find_spec("xlsxwriter") is None,
    reason="نیاز به یکی از موتورهای Excel (openpyxl/xlsxwriter)",
)
def test_cli_capacity_column_default_from_policy(tmp_path: Path) -> None:
    from app.infra import cli

    policy_path = tmp_path / "policy.json"
    payload = json.loads(Path("config/policy.json").read_text(encoding="utf-8"))
    payload["columns"]["remaining_capacity"] = "ظرفیت"
    with policy_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)

    students_path = tmp_path / "students.xlsx"
    pool_path = tmp_path / "pool.xlsx"
    output_path = tmp_path / "out.xlsx"

    pd.DataFrame(
        [
            {
                "student_id": "S1",
                "کدرشته": 1201,
                "گروه_آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش_آموز_فارغ": 0,
                "مرکز_گلستان_صدرا": 1,
                "مالی_حکمت_بنیاد": 0,
                "کد_مدرسه": 100,
            }
        ]
    ).to_excel(students_path, index=False)

    pd.DataFrame(
        [
            {
                "پشتیبان": "زهرا",
                "کد کارمندی پشتیبان": "EMP-1",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 100,
                "ظرفیت": 1,
                "occupancy_ratio": 0.2,
                "allocations_new": 0,
            }
        ]
    ).to_excel(pool_path, index=False)

    rc = cli.main(
        [
            "allocate",
            "--students",
            str(students_path),
            "--pool",
            str(pool_path),
            "--output",
            str(output_path),
            "--policy",
            str(policy_path),
        ]
    )

    assert rc == 0
    assert output_path.exists()
