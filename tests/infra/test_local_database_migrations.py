from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3

from app.infra.local_database import LocalDatabase, _SCHEMA_VERSION


def _list_user_tables(conn: sqlite3.Connection) -> set[str]:
    """استخراج فهرست جدول‌های کاربری بدون جدول‌های داخلی SQLite."""

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row[0] for row in rows}


def _bootstrap_legacy_db(path: Path, version: int) -> None:
    """ایجاد پایگاه دادهٔ ساده با نسخهٔ قدیمی برای آزمایش مهاجرت."""

    with sqlite3.connect(path) as conn:
        LocalDatabase._ensure_schema_meta_table(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, ?, '1.0.3', '1.0.2', ?)
            """,
            (version, datetime.utcnow().isoformat() + "Z"),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_uuid TEXT NOT NULL UNIQUE,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                ssot_version TEXT NOT NULL,
                entrypoint TEXT NOT NULL,
                cli_args TEXT,
                db_path TEXT,
                input_files_json TEXT,
                input_hashes_json TEXT,
                total_students INTEGER,
                total_allocated INTEGER,
                total_unallocated INTEGER,
                history_metrics_json TEXT,
                qa_summary_json TEXT,
                status TEXT NOT NULL,
                message TEXT
            );
            """
        )
        conn.commit()


def test_initialize_migrates_all_supported_versions(tmp_path: Path) -> None:
    canonical = LocalDatabase(tmp_path / "canonical.sqlite")
    canonical.initialize()
    with canonical.connect() as conn:
        expected_tables = _list_user_tables(conn)

    for legacy_version in (2, 3, 5):
        legacy_path = tmp_path / f"legacy_v{legacy_version}.sqlite"
        _bootstrap_legacy_db(legacy_path, legacy_version)
        db = LocalDatabase(legacy_path)
        db.initialize()

        with db.connect() as conn:
            version = conn.execute(
                "SELECT schema_version FROM schema_meta WHERE id = 1"
            ).fetchone()[0]
            migrated_tables = _list_user_tables(conn)

        assert int(version) == _SCHEMA_VERSION
        assert expected_tables == migrated_tables
        assert {"managers_reference", "forms_entries"}.issubset(migrated_tables)
