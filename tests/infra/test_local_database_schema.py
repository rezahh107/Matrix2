# file: tests/infra/test_local_database_schema.py
import sqlite3
from pathlib import Path

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
            "UPDATE schema_meta SET schema_version = schema_version + 1 WHERE id = 1"
        )
        conn.commit()
    with pytest.raises(SchemaVersionMismatchError) as excinfo:
        db.initialize()
    assert excinfo.value.actual_version == _SCHEMA_VERSION + 1
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


def test_schema_contains_student_and_mentor_cache_tables(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "schema_cache.sqlite")
    db.initialize()

    with db.connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "students_cache" in tables
        assert "mentor_pool_cache" in tables

        student_info = conn.execute("PRAGMA table_info(students_cache)").fetchall()
        student_cols = [row[1] for row in student_info]
        for col in [
            "student_id",
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ]:
            assert col in student_cols

        mentor_info = conn.execute("PRAGMA table_info(mentor_pool_cache)").fetchall()
        mentor_cols = [row[1] for row in mentor_info]
        for col in [
            "mentor_id",
            "کد کارمندی پشتیبان",
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ]:
            assert col in mentor_cols

        idx_student = conn.execute(
            "PRAGMA index_list('students_cache')"
        ).fetchall()
        assert any("student_id" in str(row[1]) for row in idx_student)

        idx_mentor = conn.execute(
            "PRAGMA index_list('mentor_pool_cache')"
        ).fetchall()
        assert any("mentor_id" in str(row[1]) for row in idx_mentor)


def test_migrate_from_v5_creates_managers_reference(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "schema_v5.sqlite")
    with db.connect() as conn:
        LocalDatabase._ensure_schema_meta_table(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, 5, '1.0.3', '1.0.2', '2024-01-01T00:00:00Z')
            """
        )
        conn.commit()

    db.initialize()

    with db.connect() as conn:
        version = conn.execute(
            "SELECT schema_version FROM schema_meta WHERE id = 1"
        ).fetchone()[0]
        manager_info = conn.execute("PRAGMA table_info(managers_reference)").fetchall()
        manager_indexes = {row[1] for row in conn.execute("PRAGMA index_list('managers_reference')").fetchall()}

    assert int(version) == _SCHEMA_VERSION
    manager_cols = [row[1] for row in manager_info]
    assert "نام مدیر" in manager_cols
    assert "مرکز گلستان صدرا" in manager_cols
    assert any("center_manager" in name for name in manager_indexes)
    assert any("manager" in name for name in manager_indexes)


def test_initialize_raises_on_newer_schema(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "schema_newer.sqlite")
    with db.connect() as conn:
        LocalDatabase._ensure_schema_meta_table(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, ?, '1.0.3', '1.0.2', '2024-01-01T00:00:00Z')
            """,
            (_SCHEMA_VERSION + 1,),
        )
        conn.commit()

    with pytest.raises(SchemaVersionMismatchError):
        db.initialize()
