from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.infra.exporter_archive_repository import (
    ExporterArchiveConfig,
    ExporterArchiveRepository,
)
from app.infra.local_database import LocalDatabase


def test_archive_snapshot_roundtrip(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "local.db")
    db.initialize()
    repo = ExporterArchiveRepository(db=db)
    df = pd.DataFrame({"b": ["x", "y"], "a": [2, 1]})

    snapshot_id = repo.archive_snapshot(
        rows_df=df,
        exporter_version="1.0",
        run_uuid="run-1",
        metadata={"note": "demo"},
        config=ExporterArchiveConfig(enabled=True, row_limit=10),
    )

    rows = repo.list_snapshots()
    assert len(rows) == 1
    assert rows[0]["row_count"] == 2
    assert rows[0]["is_truncated"] == 0

    row, restored = db.fetch_exporter_snapshot(snapshot_id)
    assert row is not None
    assert restored is not None
    assert json.loads(row["columns_json"]) == ["a", "b"]
    assert repo._hash_payload(  # type: ignore[attr-defined]
        {"columns": ["a", "b"], "rows": restored.to_dict(orient="records")}
    ) == row["row_hash"]
    # column order should be normalized
    assert list(restored.columns) == ["a", "b"]
    assert list(restored.to_dict(orient="records")) == [
        {"a": 1, "b": "y"},
        {"a": 2, "b": "x"},
    ]


def test_compare_snapshots_order_independent(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "local.db")
    db.initialize()
    repo = ExporterArchiveRepository(db=db)
    df_a = pd.DataFrame({"col": [2, 1], "other": ["b", "a"]})
    df_b = pd.DataFrame({"other": ["a", "c", "b"], "col": [1, 3, 2]})

    snap_a = repo.archive_snapshot(
        rows_df=df_a, exporter_version="1", run_uuid="a", config=ExporterArchiveConfig(enabled=True)
    )
    snap_b = repo.archive_snapshot(
        rows_df=df_b, exporter_version="1", run_uuid="b", config=ExporterArchiveConfig(enabled=True)
    )

    result = repo.compare_snapshots(snap_a, snap_b)
    assert result.row_count_delta == 1
    assert result.row_hash_equal is False
    assert result.added == [{"col": 3, "other": "c"}]
    assert result.removed == []


def test_row_limit_truncation(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "local.db")
    db.initialize()
    repo = ExporterArchiveRepository(db=db)
    df = pd.DataFrame({"col": list(range(5))})

    snapshot_id = repo.archive_snapshot(
        rows_df=df,
        exporter_version="1",
        run_uuid="a",
        config=ExporterArchiveConfig(enabled=True, row_limit=2),
    )

    row, restored = db.fetch_exporter_snapshot(snapshot_id)
    assert row is not None
    assert row["is_truncated"] == 1
    assert restored is None

    df_small = pd.DataFrame({"col": [1, 2]})
    snap_small = repo.archive_snapshot(
        rows_df=df_small,
        exporter_version="1",
        run_uuid="b",
        config=ExporterArchiveConfig(enabled=True, row_limit=10),
    )

    result = repo.compare_snapshots(snap_small, snap_small)
    assert result.row_hash_equal is True
    try:
        repo.compare_snapshots(snapshot_id, snap_small)
    except ValueError as exc:
        assert "truncated" in str(exc)
    else:  # pragma: no cover - safety
        assert False


def test_migration_from_v6_to_v7(tmp_path: Path) -> None:
    db_path = tmp_path / "local.db"
    db = LocalDatabase(db_path)
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                schema_version INTEGER NOT NULL,
                policy_version TEXT NOT NULL,
                ssot_version TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta (id, schema_version, policy_version, ssot_version, created_at) VALUES (1, 6, '1.0.3', '1.0.2', '2020-01-01T00:00:00Z')"
        )
        conn.commit()

    db.initialize()
    with db.connect() as conn:
        info = conn.execute("PRAGMA table_info(exporter_snapshots)").fetchall()
        columns = {row[1] for row in info}
        assert "row_limit" in columns
        assert "is_truncated" in columns
        # existing tables still operable
        conn.execute("INSERT INTO exporter_snapshots (exporter_name, created_at, row_count, row_hash, columns_json, row_limit, is_truncated) VALUES ('x', '2020-01-01T00:00:00Z', 0, 'h', '[]', -1, 0)")
