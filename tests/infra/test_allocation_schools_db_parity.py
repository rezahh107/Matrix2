from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from app.core.allocate_students import allocate_batch
from app.core.build_matrix import BuildConfig, build_matrix
from app.core.policy_loader import load_policy
from app.infra.local_database import LocalDatabase
from app.infra.reference_schools_repository import (
    get_school_reference_frames,
    import_school_crosswalk_from_excel,
    import_school_report_from_excel,
)


def _create_inspactor_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "نام پشتیبان": ["زهرا", "علی"],
            "نام مدیر": ["شهدخت کشاورز", "آینا هوشمند"],
            "کد کارمندی پشتیبان": ["EMP-1", "EMP-2"],
            "ردیف پشتیبان": [1, 2],
            "گروه آزمایشی": ["تجربی", "ریاضی"],
            "جنسیت": ["دختر", "پسر"],
            "دانش آموز فارغ": [0, 1],
            "کدپستی": ["1234", "5678"],
            "تعداد داوطلبان تحت پوشش": [5, 3],
            "تعداد تحت پوشش خاص": [10, 4],
            "نام مدرسه 1": ["", "مدرسه نمونه 1"],
            "تعداد مدارس تحت پوشش": [0, 1],
            "امکان جذب دانش آموز": ["بلی", "بلی"],
            "مالی حکمت بنیاد": [0, 0],
            "مرکز گلستان صدرا": [0, 0],
        }
    )


def _write_school_report(path: Path) -> None:
    df = pd.DataFrame({"کد مدرسه": ["5001"], "نام مدرسه 1": ["مدرسه نمونه 1"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def _write_crosswalk(path: Path) -> None:
    groups_df = pd.DataFrame(
        {
            "گروه آزمایشی": ["تجربی", "ریاضی"],
            "کد گروه": [1201, 2201],
            "مقطع تحصیلی": ["دهم", "دهم"],
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        groups_df.to_excel(writer, sheet_name="پایه تحصیلی (گروه آزمایشی)", index=False)


def _build_students() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": ["S1", "S2"],
            "کدرشته": [2201, 1201],
            "گروه آزمایشی": ["ریاضی", "تجربی"],
            "جنسیت": [1, 0],
            "دانش آموز فارغ": [1, 0],
            "مرکز گلستان صدرا": [0, 1],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [5001, 0],
        }
    )


def test_allocation_matches_excel_and_db(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "ref.db")
    db.initialize()

    school_report = tmp_path / "schools.xlsx"
    crosswalk = tmp_path / "crosswalk.xlsx"
    _write_school_report(school_report)
    _write_crosswalk(crosswalk)

    insp_df = _create_inspactor_frame()

    excel_schools = import_school_report_from_excel(school_report, db)
    excel_crosswalk_groups, excel_crosswalk_synonyms = import_school_crosswalk_from_excel(
        crosswalk, db
    )

    db_schools, db_crosswalk_groups, db_crosswalk_synonyms = get_school_reference_frames(db)

    cfg = BuildConfig()
    matrix_excel, *_ = build_matrix(
        insp_df,
        excel_schools,
        excel_crosswalk_groups,
        crosswalk_synonyms_df=excel_crosswalk_synonyms,
        cfg=cfg,
        progress=lambda *_: None,
    )
    matrix_db, *_ = build_matrix(
        insp_df,
        db_schools,
        db_crosswalk_groups,
        crosswalk_synonyms_df=db_crosswalk_synonyms,
        cfg=cfg,
        progress=lambda *_: None,
    )

    policy = load_policy(Path("config/policy.json"))
    students = _build_students()

    allocations_excel, _, _, _ = allocate_batch(
        students, matrix_excel, policy=policy, progress=lambda *_: None
    )
    allocations_db, _, _, _ = allocate_batch(students, matrix_db, policy=policy, progress=lambda *_: None)

    allocations_excel_sorted = allocations_excel.sort_values(by=["student_id"], kind="stable").reset_index(
        drop=True
    )
    allocations_db_sorted = allocations_db.sort_values(by=["student_id"], kind="stable").reset_index(drop=True)

    pdt.assert_frame_equal(allocations_excel_sorted, allocations_db_sorted, check_dtype=False)
