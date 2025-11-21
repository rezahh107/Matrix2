from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.infra.local_database import LocalDatabase
from app.infra.reference_schools_repository import (
    get_school_reference_frames,
    import_school_crosswalk_from_excel,
    import_school_report_from_excel,
)


def _write_school_report(path: Path) -> None:
    df = pd.DataFrame({"کد مدرسه": ["3001", "3002"], "نام مدرسه": ["الف", "ب"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def _write_crosswalk(path: Path) -> None:
    groups_df = pd.DataFrame(
        {"کد مدرسه": [3001, 3002], "کد جایگزین": [4001, 4002], "title": ["x", "y"]}
    )
    synonyms_df = pd.DataFrame({"کد مدرسه": [3001], "کد جایگزین": [4001], "alias": ["الف"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        groups_df.to_excel(writer, sheet_name="پایه تحصیلی (گروه آزمایشی)", index=False)
        synonyms_df.to_excel(writer, sheet_name="Synonyms", index=False)


def test_import_and_load_reference_frames(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "ref.db")
    db.initialize()

    school_report = tmp_path / "schools.xlsx"
    crosswalk = tmp_path / "crosswalk.xlsx"
    _write_school_report(school_report)
    _write_crosswalk(crosswalk)

    schools_df = import_school_report_from_excel(school_report, db)
    groups_df, synonyms_df = import_school_crosswalk_from_excel(crosswalk, db)

    assert list(schools_df["کد مدرسه"]) == [3001, 3002]
    assert str(schools_df["کد مدرسه"].dtype) == "Int64"
    assert list(groups_df["کد جایگزین"]) == [4001, 4002]
    assert str(groups_df["کد مدرسه"].dtype) == "Int64"
    assert synonyms_df is not None

    schools_loaded, groups_loaded, synonyms_loaded = get_school_reference_frames(db)
    assert len(schools_loaded) == len(schools_df)
    assert list(groups_loaded["کد مدرسه"]) == [3001, 3002]
    assert synonyms_loaded is not None
