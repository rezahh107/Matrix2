import sqlite3

from app.infra.sqlite_config import configure_connection


def test_configure_connection_applies_pragmas(tmp_path):
    db_path = tmp_path / "configured.sqlite"
    conn = configure_connection(sqlite3.connect(db_path))
    try:
        assert conn.row_factory is sqlite3.Row
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1
        journal_mode = str(conn.execute("PRAGMA journal_mode;").fetchone()[0]).lower()
        assert journal_mode == "wal"
        synchronous = conn.execute("PRAGMA synchronous;").fetchone()[0]
        assert str(synchronous).lower() in {"1", "normal"}
    finally:
        conn.close()
