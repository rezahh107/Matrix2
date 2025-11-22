from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Iterable

from app.infra.local_database import LocalDatabase, _SCHEMA_VERSION


def _list_user_tables(conn: sqlite3.Connection) -> set[str]:
    """استخراج فهرست جدول‌های کاربری بدون جدول‌های داخلی SQLite."""

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row[0] for row in rows}


def _bootstrap_legacy_db(path: Path, version: int, *, tables: Iterable[str] = ()) -> None:
    """ساخت پایگاه‌دادهٔ آزمایشی با نسخهٔ قدیمی schema_meta و چند جدول ساده."""

    with sqlite3.connect(path) as conn:
        LocalDatabase._ensure_schema_meta_table(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, ?, '1.0.3', '1.0.2', ?)
            """,
            (version, datetime.utcnow().isoformat() + "Z"),
        )
        for table in tables:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        conn.commit()


def test_initialize_migrates_all_supported_versions(tmp_path: Path) -> None:
    canonical = LocalDatabase(tmp_path / "canonical.sqlite")
    canonical.initialize()
    with canonical.connect() as conn:
        expected_tables = _list_user_tables(conn)

    legacy_versions = (2, 3, 5)
    for legacy_version in legacy_versions:
        legacy_path = tmp_path / f"legacy_v{legacy_version}.sqlite"
        _bootstrap_legacy_db(legacy_path, legacy_version, tables=("runs",))
        db = LocalDatabase(legacy_path)
        db.initialize()

        with db.connect() as conn:
            version = conn.execute(
                "SELECT schema_version FROM schema_meta WHERE id = 1"
            ).fetchone()[0]
            migrated_tables = _list_user_tables(conn)

        assert int(version) == _SCHEMA_VERSION
        assert migrated_tables == expected_tables


def test_initialize_sets_expected_tables_for_new_database(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "fresh.sqlite")
    db.initialize()

    with db.connect() as conn:
        migrated_tables = _list_user_tables(conn)
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1").fetchone()[0]

    assert int(version) == _SCHEMA_VERSION
    assert "runs" in migrated_tables
    assert "forms_entries" in migrated_tables
    assert "qa_summary" in migrated_tables
    assert "trace_snapshots" in migrated_tables
