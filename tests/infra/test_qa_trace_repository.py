from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from pandas.testing import assert_frame_equal

from app.infra.local_database import LocalDatabase, RunRecord, _SCHEMA_VERSION


def _sample_run_record(start: datetime) -> RunRecord:
    return RunRecord(
        run_uuid="run-test",
        started_at=start,
        finished_at=start,
        policy_version="1.0.3",
        ssot_version="1.0.2",
        entrypoint="allocate",
        cli_args=None,
        db_path=None,
        input_files_json="{}",
        input_hashes_json="{}",
        total_students=2,
        total_allocated=1,
        total_unallocated=1,
        history_metrics_json=None,
        qa_summary_json=None,
        status="success",
        message=None,
    )


def test_trace_snapshot_round_trip(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "snap.db")
    db.initialize()
    now = datetime.now(timezone.utc)
    run_id = db.insert_run(_sample_run_record(now))

    trace_df = pd.DataFrame(
        {"student_id": [1, 2], "step": ["type", "group"], "candidates": [5, 3]}
    )
    summary_df = pd.DataFrame({"allocation_channel": ["SCHOOL"], "students_total": [2]})
    history_info_df = pd.DataFrame({"student_id": [1, 2], "history_status": [1, 0]})
    trace_df.attrs["summary_df"] = summary_df
    trace_df.attrs["history_info_df"] = history_info_df

    db.insert_trace_snapshot(
        run_id=run_id,
        trace_df=trace_df,
        summary_df=summary_df,
        history_info_df=history_info_df,
    )

    restored, restored_summary, restored_history = db.fetch_trace_snapshot(run_id)
    assert restored is not None
    assert_frame_equal(restored.reset_index(drop=True), trace_df.reset_index(drop=True))
    assert_frame_equal(
        restored_summary.reset_index(drop=True), summary_df.reset_index(drop=True)
    )
    assert_frame_equal(
        restored_history.reset_index(drop=True), history_info_df.reset_index(drop=True)
    )

    with db.connect() as conn:
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1").fetchone()[0]
    assert int(version) == _SCHEMA_VERSION


def test_qa_snapshot_round_trip(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "snap.db")
    db.initialize()
    now = datetime.now(timezone.utc)
    run_id = db.insert_run(_sample_run_record(now))

    qa_summary_df = pd.DataFrame(
        [
            {"rule_id": "QA_RULE_STU_01", "status": "PASS", "violations_count": 0},
            {"rule_id": "QA_RULE_ALLOC_01", "status": "FAIL", "violations_count": 2},
        ]
    )
    qa_details_df = pd.DataFrame(
        [
            {
                "rule_id": "QA_RULE_ALLOC_01",
                "level": "error",
                "message": "over capacity",
                "student_id": 10,
            },
            {
                "rule_id": "QA_RULE_ALLOC_01",
                "level": "error",
                "message": "over capacity",
                "student_id": 11,
            },
        ]
    )

    db.insert_qa_snapshot(
        run_id=run_id, qa_summary_df=qa_summary_df, qa_details_df=qa_details_df
    )

    restored_summary, restored_details = db.fetch_qa_snapshot(run_id)
    assert restored_summary is not None and restored_details is not None
    assert_frame_equal(restored_summary.reset_index(drop=True), qa_summary_df.reset_index(drop=True))
    assert_frame_equal(restored_details.reset_index(drop=True), qa_details_df.reset_index(drop=True))


def test_fetch_returns_none_for_missing_rows(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "snap.db")
    db.initialize()
    assert db.fetch_trace_snapshot(999) == (None, None, None)
    assert db.fetch_qa_snapshot(999) == (None, None)


def test_deserialize_error_is_handled(tmp_path, caplog) -> None:
    db = LocalDatabase(tmp_path / "snap.db")
    db.initialize()
    now = datetime.now(timezone.utc)
    run_id = db.insert_run(_sample_run_record(now))

    db.insert_trace_snapshot(run_id=run_id, trace_df=pd.DataFrame({"a": [1]}))
    with db.connect() as conn:
        conn.execute("UPDATE trace_snapshots SET trace_json = 'invalid-json' WHERE run_id = ?", (run_id,))

    caplog.set_level("ERROR")
    trace_df, summary_df, history_df = db.fetch_trace_snapshot(run_id)
    assert trace_df is None and summary_df is None and history_df is None
    assert any(
        "Failed to deserialize DataFrame payload" in record.getMessage()
        for record in caplog.records
    )
