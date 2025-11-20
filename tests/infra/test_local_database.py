import sqlite3
from datetime import datetime, timezone

from app.infra.local_database import LocalDatabase, QaSummaryRow, RunMetricRow, RunRecord


def _sample_run(now: datetime) -> RunRecord:
    """ساخت نمونهٔ RunRecord برای استفاده در تست‌ها."""

    return RunRecord(
        run_uuid="abc123",
        started_at=now,
        finished_at=now,
        policy_version="1.0.3",
        ssot_version="1.0.2",
        entrypoint="allocate",
        cli_args="allocate --students s.xlsx",
        db_path="/tmp/db.sqlite3",
        input_files_json="{}",
        input_hashes_json="{}",
        total_students=10,
        total_allocated=8,
        total_unallocated=2,
        history_metrics_json=None,
        qa_summary_json=None,
        status="success",
        message="ok",
    )


def test_initialize_and_insert_run(tmp_path) -> None:
    db_path = tmp_path / "local.db"
    db = LocalDatabase(db_path)
    db.initialize()

    now = datetime.now(timezone.utc)
    run_id = db.insert_run(_sample_run(now))

    rows = db.fetch_runs()
    assert len(rows) == 1
    row = rows[0]
    assert int(row["id"]) == run_id
    assert row["run_uuid"] == "abc123"
    assert row["policy_version"] == "1.0.3"
    assert int(row["total_allocated"]) == 8
    # ISO 8601 formatting should be preserved with Z suffix
    assert row["started_at"].endswith("Z")

    # schema must remain after multiple initializes
    db.initialize()
    rows_after = db.fetch_runs()
    assert len(rows_after) == 1


def test_insert_metrics_and_qa(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "metrics.db")
    db.initialize()
    now = datetime.now(timezone.utc)
    run_id = db.insert_run(_sample_run(now))

    metric_row = RunMetricRow(run_id=run_id, metric_key="SCHOOL.students_total", metric_value=5)
    db.insert_run_metrics([metric_row])
    db.insert_qa_summary(
        [QaSummaryRow(run_id=run_id, violation_code="TOTAL", severity="info", count=0)]
    )

    metrics = db.fetch_metrics_for_run(run_id)
    assert len(metrics) == 1
    assert metrics[0]["metric_key"] == "SCHOOL.students_total"
    assert float(metrics[0]["metric_value"]) == 5

    qa_rows = db.fetch_qa_summary(run_id)
    assert len(qa_rows) == 1
    assert qa_rows[0]["violation_code"] == "TOTAL"
    assert int(qa_rows[0]["count"]) == 0


def test_schema_created_idempotently(tmp_path) -> None:
    db_path = tmp_path / "schema.db"
    db = LocalDatabase(db_path)
    db.initialize()
    db.initialize()

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        columns = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {"runs", "run_metrics", "qa_summary"}.issubset(tables)
    assert "run_uuid" in columns.get("runs", "")
