# file: tests/infra/test_local_database_errors.py
import sqlite3

import pandas as pd
import pytest

from app.infra.errors import (
    DatabaseOperationError,
    ReferenceDataMissingError,
    SchemaVersionMismatchError,
)
from app.infra.local_database import LocalDatabase, RunMetricRow


def test_load_schools_missing_table(tmp_path):
    db = LocalDatabase(tmp_path / "missing.sqlite")
    with pytest.raises(ReferenceDataMissingError):
        db.load_schools()


def test_reference_crosswalk_missing(tmp_path):
    db = LocalDatabase(tmp_path / "crosswalk_missing.sqlite")
    with pytest.raises(ReferenceDataMissingError):
        db.load_school_crosswalk()


def test_database_operation_error_when_table_removed(tmp_path):
    db = LocalDatabase(tmp_path / "ops.sqlite")
    db.initialize()
    with db.connect() as conn:
        conn.execute("DROP TABLE run_metrics")
        conn.commit()
    metric_row = RunMetricRow(run_id=1, metric_key="demo", metric_value=1.0)
    with pytest.raises(DatabaseOperationError):
        db.insert_run_metrics([metric_row])


def test_schema_version_error_mapping(tmp_path):
    db = LocalDatabase(tmp_path / "schema_error.sqlite")
    db.initialize()
    with db.connect() as conn:
        conn.execute("UPDATE schema_meta SET schema_version = schema_version + 5 WHERE id = 1")
        conn.commit()
    with pytest.raises(SchemaVersionMismatchError):
        db.initialize()


def test_generic_sqlite_error_wrapped(tmp_path, monkeypatch):
    db = LocalDatabase(tmp_path / "generic.sqlite")
    db.initialize()
    df = pd.DataFrame({"کد مدرسه": [1], "نام مدرسه": ["الف"]})

    def boom(*_args, **_kwargs):
        raise sqlite3.OperationalError("failure")

    monkeypatch.setattr(db, "_replace_table_atomic", boom)
    with pytest.raises(DatabaseOperationError):
        db.upsert_schools(df)
