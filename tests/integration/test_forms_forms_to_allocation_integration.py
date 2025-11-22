from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy
from app.infra.cli import _import_students_from_forms_cache, _sanitize_pool_for_allocation
from app.infra.forms_repository import FormsRepository
from app.infra.local_database import LocalDatabase


class _FakeFormsClient:
    def __init__(self, entries: list[dict]):
        self.entries = entries

    def fetch_entries(self, *, since=None):  # type: ignore[no-untyped-def]
        return list(self.entries)


def _forms_entry():
    return {
        "id": "501",
        "form_id": "9",
        "date_created": "2024-03-01T09:00:00Z",
        "fields": {
            "student_id": "ST-F-1",
            "کدرشته": 1201,
            "گروه آزمایشی": "تجربی",
            "جنسیت": 1,
            "دانش آموز فارغ": 0,
            "مرکز گلستان صدرا": 0,
            "مالی حکمت بنیاد": 0,
            "کد مدرسه": 1010,
        },
    }


def _baseline_student_row():
    entry = _forms_entry()
    row = {"student_id": entry["fields"]["student_id"]}
    row.update(entry["fields"])
    return row


def _mentor_pool():
    return pd.DataFrame(
        [
            {
                "mentor_name": "منتور اصلی",
                "alias": 201,
                "remaining_capacity": 1,
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 1010,
                "کد کارمندی پشتیبان": 201,
            }
        ]
    )


def _run_allocation(students: pd.DataFrame, policy):
    pool = _sanitize_pool_for_allocation(_mentor_pool(), policy=policy)
    allocations, pool_after, logs, trace = allocate_batch(students, pool, policy=policy)
    return allocations, pool_after, logs, trace


def test_forms_to_allocation_matches_baseline(tmp_path: Path):
    policy = load_policy()
    db = LocalDatabase(tmp_path / "forms.sqlite")

    client = _FakeFormsClient([_forms_entry()])
    repo = FormsRepository(client=client, db=db)
    repo.sync_from_wordpress()

    students_from_forms = _import_students_from_forms_cache(db=db, policy=policy)
    forms_allocs, forms_pool, forms_logs, forms_trace = _run_allocation(students_from_forms, policy)

    baseline_students = pd.DataFrame([_baseline_student_row()])
    for key in policy.join_keys:
        baseline_students[key] = pd.to_numeric(baseline_students[key], errors="coerce").astype("Int64")
    baseline_allocs, baseline_pool, baseline_logs, baseline_trace = _run_allocation(
        baseline_students, policy
    )

    assert len(forms_allocs) == len(baseline_allocs) == 1
    assert len(forms_logs) == len(baseline_logs) == 1
    assert forms_allocs["mentor_id"].iloc[0] == baseline_allocs["mentor_id"].iloc[0]
    assert forms_logs["allocation_status"].iloc[0] == baseline_logs["allocation_status"].iloc[0]
