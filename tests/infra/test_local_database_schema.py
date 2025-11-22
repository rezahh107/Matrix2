# file: tests/infra/test_local_database_schema.py
import sqlite3

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from app.infra.errors import DatabaseOperationError, SchemaVersionMismatchError
from app.infra.local_database import LocalDatabase, _SCHEMA_VERSION


def test_schema_meta_initialized(tmp_path):
    db = LocalDatabase(tmp_path / "schema.sqlite")
    db.initialize()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT schema_version, policy_version, ssot_version, created_at FROM schema_meta"
        ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert int(row[0]) == _SCHEMA_VERSION
    assert row[1] == "1.0.3"
    assert row[2] == "1.0.2"
    assert isinstance(row[3], str) and row[3]


def test_schema_version_mismatch_raises(tmp_path):
    db = LocalDatabase(tmp_path / "schema_mismatch.sqlite")
    db.initialize()
    with db.connect() as conn:
        conn.execute(
            "UPDATE schema_meta SET schema_version = schema_version - 1 WHERE id = 1"
        )
        conn.commit()
    with pytest.raises(SchemaVersionMismatchError) as excinfo:
        db.initialize()
    assert excinfo.value.actual_version == _SCHEMA_VERSION - 1
    assert excinfo.value.expected_version == _SCHEMA_VERSION


def test_atomic_schools_import_replaces_dataset(tmp_path):
    db = LocalDatabase(tmp_path / "atomic_replace.sqlite")
    initial = pd.DataFrame({"کد مدرسه": pd.Series([1], dtype="Int64"), "نام مدرسه": ["الف"]})
    db.upsert_schools(initial)

    updated = pd.DataFrame({"کد مدرسه": pd.Series([2], dtype="Int64"), "نام مدرسه": ["ب"]})
    db.upsert_schools(updated)

    restored = db.load_schools()
    assert_frame_equal(
        restored.sort_values(by="کد مدرسه").reset_index(drop=True),
        updated.reset_index(drop=True),
    )
    with db.connect() as conn:
        temp_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_schools_new'"
        ).fetchone()
    assert temp_exists is None


def test_atomic_schools_import_rolls_back_on_failure(tmp_path, monkeypatch):
    db = LocalDatabase(tmp_path / "atomic.sqlite")
    initial = pd.DataFrame({"کد مدرسه": pd.Series([1], dtype="Int64"), "نام مدرسه": ["الف"]})
    db.upsert_schools(initial)

    bad_df = pd.DataFrame({"کد مدرسه": [2], "نام مدرسه": ["ب"]})

    def bad_replace(conn, *, table_name, df, index_statements=None):
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(db, "_replace_table_atomic", bad_replace)
    with pytest.raises(DatabaseOperationError):
        db.upsert_schools(bad_df)

    restored = db.load_schools()
    assert_frame_equal(restored.sort_values(by="کد مدرسه").reset_index(drop=True), initial)
