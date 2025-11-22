from datetime import datetime
from pathlib import Path
import sqlite3

import pandas as pd
from pandas.api.types import is_datetime64tz_dtype

from app.infra.errors import SchemaVersionMismatchError
from app.infra.forms_repository import FormsRepository
from app.infra.local_database import LocalDatabase


class _FakeFormsClient:
    def __init__(self, entries: list[dict]):
        self.entries = entries
        self.requested_since: datetime | None = None

    def fetch_entries(self, *, since: datetime | None = None):
        self.requested_since = since
        return list(self.entries)


def _sample_entries():
    return [
        {
            "id": "11",
            "form_id": "1",
            "date_created": "2024-01-01T00:00:00Z",
            "fields": {"student_id": "S1", "کدرشته": 1201, "جنسیت": 1},
        },
        {
            "id": "12",
            "form_id": "1",
            "created_at": "2024-01-02T00:00:00Z",
            "fields": {"student_id": "S2", "کدرشته": 1202, "جنسیت": 0},
        },
    ]


def test_forms_repository_sync_and_load(tmp_path: Path):
    db = LocalDatabase(tmp_path / "forms.sqlite")
    client = _FakeFormsClient(_sample_entries())
    repo = FormsRepository(client=client, db=db)

    result = repo.sync_from_wordpress(since=datetime(2023, 12, 31))

    assert result.fetched_count == 2
    assert result.persisted_count == 2
    assert client.requested_since == datetime(2023, 12, 31)

    cached = repo.load_entries()
    assert list(cached["entry_id"]) == ["11", "12"]
    assert isinstance(cached["received_at"].dtype, pd.DatetimeTZDtype)

    meta = db.fetch_reference_meta("forms_entries")
    assert meta is not None
    assert meta[2] == 2


def test_forms_repository_idempotent_sync(tmp_path: Path):
    db = LocalDatabase(tmp_path / "forms.sqlite")
    client = _FakeFormsClient(_sample_entries())
    repo = FormsRepository(client=client, db=db)

    repo.sync_from_wordpress()
    repo.sync_from_wordpress()

    cached = repo.load_entries()
    assert len(cached) == 2
    assert list(cached["entry_id"]) == ["11", "12"]


def test_forms_repository_timestamp_roundtrip_and_privacy_hook(tmp_path: Path):
    calls: list[pd.DataFrame] = []

    def _privacy(df: pd.DataFrame) -> pd.DataFrame:
        calls.append(df)
        return df.drop(columns=["form_id"], errors="ignore")

    db = LocalDatabase(tmp_path / "forms.sqlite")
    client = _FakeFormsClient(_sample_entries())
    repo = FormsRepository(client=client, db=db, privacy_hook=_privacy)

    repo.sync_from_wordpress()
    cached = repo.load_entries()

    assert len(calls) == 1
    assert "form_id" not in cached.columns
    assert cached["received_at"].dt.tz is not None

    with sqlite3.connect(db.path) as conn:
        raw = conn.execute("SELECT received_at FROM forms_entries ORDER BY entry_id").fetchall()
    assert raw[0][0].endswith("Z")


def test_schema_migration_paths(tmp_path: Path):
    fresh_db = LocalDatabase(tmp_path / "fresh.sqlite")
    fresh_db.initialize()
    with fresh_db.connect() as conn:
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE id=1").fetchone()[0]
    assert version == 5

    upgrade_path = tmp_path / "upgrade.sqlite"
    with sqlite3.connect(upgrade_path) as conn:
        conn.execute(
            "CREATE TABLE schema_meta (id INTEGER PRIMARY KEY CHECK (id=1), schema_version INTEGER, policy_version TEXT, ssot_version TEXT, created_at TEXT)"
        )
        conn.execute(
            "INSERT INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at) VALUES (1, 3, '1.0.3', '1.0.2', '2024-01-01T00:00:00Z')"
        )
        conn.commit()
    upgrade_db = LocalDatabase(upgrade_path)
    upgrade_db.initialize()
    with upgrade_db.connect() as conn:
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE id=1").fetchone()[0]
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='forms_entries'"
        ).fetchall()
    assert version == 5
    assert tables

    legacy_path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(legacy_path) as conn:
        conn.execute(
            "CREATE TABLE schema_meta (id INTEGER PRIMARY KEY CHECK (id=1), schema_version INTEGER, policy_version TEXT, ssot_version TEXT, created_at TEXT)"
        )
        conn.execute(
            "INSERT INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at) VALUES (1, 1, '1.0.3', '1.0.2', '2023-01-01T00:00:00Z')"
        )
        conn.commit()
    legacy_db = LocalDatabase(legacy_path)
    try:
        legacy_db.initialize()
    except SchemaVersionMismatchError as exc:
        assert exc.actual_version == 1
    else:  # pragma: no cover
        raise AssertionError("expected legacy schema to fail")
