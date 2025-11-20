import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.infra.history_store import build_run_context, log_allocation_run, summarize_qa
from app.infra.local_database import LocalDatabase


def _write_file(path: Path) -> Path:
    """نوشتن محتوای کوچک برای تولید هش پایدار در تست‌ها."""

    path.write_text("data", encoding="utf-8")
    return path


def test_history_store_persists_run_and_metrics(tmp_path) -> None:
    db = LocalDatabase(tmp_path / "history.db")
    students_path = _write_file(tmp_path / "students.xlsx")
    pool_path = _write_file(tmp_path / "pool.xlsx")
    policy_path = _write_file(tmp_path / "policy.json")
    output_path = tmp_path / "output.xlsx"

    start = datetime.now(timezone.utc)
    end = start
    ctx = build_run_context(
        command="allocate",
        cli_args="allocate --students students.xlsx --pool pool.xlsx",
        policy_version="1.0.3",
        ssot_version="1.0.2",
        started_at=start,
        completed_at=end,
        success=True,
        message="done",
        input_students=students_path,
        input_pool=pool_path,
        output=output_path,
        policy_path=policy_path,
        total_students=3,
        allocated_students=2,
        unallocated_students=1,
    )

    metrics_df = pd.DataFrame(
        [
            {
                "allocation_channel": "SCHOOL",
                "students_total": 3,
                "history_already_allocated": 1,
                "history_no_history_match": 1,
                "history_missing_or_invalid": 1,
                "same_history_mentor_true": 1,
                "same_history_mentor_ratio": 0.5,
            }
        ]
    )
    qa_report = SimpleNamespace(passed=True, violations=[])

    log_allocation_run(
        run_uuid="run-001",
        ctx=ctx,
        history_metrics=metrics_df,
        qa_outcome=summarize_qa(qa_report),
        db=db,
    )

    runs = db.fetch_runs()
    assert len(runs) == 1
    run_row = runs[0]
    assert run_row["run_uuid"] == "run-001"
    assert run_row["entrypoint"] == "allocate"
    assert run_row["status"] == "success"
    assert json.loads(run_row["input_hashes_json"]).get("policy") is not None
    assert int(run_row["total_allocated"]) == 2

    run_id = int(run_row["id"])
    metrics = db.fetch_metrics_for_run(run_id)
    assert len(metrics) >= 1
    assert any(m["metric_key"] == "SCHOOL.same_history_mentor_ratio" for m in metrics)

    qa_rows = db.fetch_qa_summary(run_id)
    assert len(qa_rows) == 1
    assert qa_rows[0]["violation_code"] == "TOTAL"
    assert int(qa_rows[0]["count"]) == 0


def test_history_store_handles_db_error(tmp_path, caplog) -> None:
    class FailingDb:
        """آداپتور ساده که در initialize خطا می‌دهد تا مسیر خطا تست شود."""

        def initialize(self) -> None:
            raise RuntimeError("boom")

    db = FailingDb()
    start = datetime.now(timezone.utc)
    ctx = build_run_context(
        command="allocate",
        cli_args=None,
        policy_version="1.0.3",
        ssot_version="1.0.2",
        started_at=start,
        completed_at=start,
        success=True,
        message="ok",
        input_students=None,
        input_pool=None,
        output=None,
        policy_path=None,
        total_students=None,
        allocated_students=None,
        unallocated_students=None,
    )

    metrics_df = pd.DataFrame()
    qa_report = SimpleNamespace(passed=True, violations=[])

    caplog.set_level("INFO")
    log_allocation_run(
        run_uuid="run-error",
        ctx=ctx,
        history_metrics=metrics_df,
        qa_outcome=summarize_qa(qa_report),
        db=db,  # type: ignore[arg-type]
    )

    assert any("Failed to log allocation run" in msg for msg in caplog.text.splitlines())
