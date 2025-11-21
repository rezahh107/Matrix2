from __future__ import annotations

import pandas as pd

from app.infra.local_database import LocalDatabase
from app.infra.local_database import _table_exists


def test_initialize_creates_reference_tables(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "ref.db")
    db.initialize()

    with db.connect() as conn:
        assert _table_exists(conn, "schools")
        assert _table_exists(conn, "school_crosswalk_groups")
        assert _table_exists(conn, "school_crosswalk_synonyms")


def test_upsert_and_load_schools(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "ref.db")
    db.initialize()

    schools_df = pd.DataFrame({"کد مدرسه": ["101", 202], "نام مدرسه": ["الف", "ب"]})

    db.upsert_schools(schools_df)
    loaded = db.load_schools()

    assert list(loaded["کد مدرسه"]) == [101, 202]
    assert str(loaded["کد مدرسه"].dtype) == "Int64"
    assert list(loaded["نام مدرسه"]) == ["الف", "ب"]


def test_upsert_and_load_crosswalk(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "ref.db")
    db.initialize()

    groups_df = pd.DataFrame(
        {"کد مدرسه": [1, 2], "کد جایگزین": [11, 22], "title": ["الف", "ب"]}
    )
    synonyms_df = pd.DataFrame({"کد مدرسه": [1], "کد جایگزین": [11], "alias": ["الف"]})

    db.upsert_school_crosswalk(groups_df, synonyms_df=synonyms_df)
    loaded_groups, loaded_synonyms = db.load_school_crosswalk()

    assert list(loaded_groups["کد مدرسه"]) == [1, 2]
    assert str(loaded_groups["کد مدرسه"].dtype) == "Int64"
    assert loaded_synonyms is not None
    assert list(loaded_synonyms["کد جایگزین"]) == [11]
