import pandas as pd
import pytest

try:
    from PySide6.QtWidgets import QApplication
except ImportError as exc:  # pragma: no cover - محیط فاقد وابستگی Qt
    pytest.skip(f"PySide6 unavailable: {exc}", allow_module_level=True)

from app.infra.local_database import LocalDatabase, RunMetricRow, RunRecord
from app.ui.history_dialog import HistoryDialog


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _insert_run_with_snapshots(db: LocalDatabase) -> None:
    record = RunRecord(
        run_uuid="ui-run",
        started_at=pd.Timestamp.utcnow().to_pydatetime(),
        finished_at=pd.Timestamp.utcnow().to_pydatetime(),
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
    run_id = db.insert_run(record)

    trace_df = pd.DataFrame({"student_id": [1], "step": ["type"], "candidates": [3]})
    summary_df = pd.DataFrame({"allocation_channel": ["SCHOOL"], "students_total": [2]})
    trace_df.attrs["summary_df"] = summary_df
    db.insert_trace_snapshot(
        run_id=run_id, trace_df=trace_df, summary_df=summary_df, history_info_df=None
    )

    qa_summary_df = pd.DataFrame(
        [{"rule_id": "QA_RULE_ALLOC_01", "status": "FAIL", "violations_count": 1}]
    )
    qa_details_df = pd.DataFrame(
        [
            {
                "rule_id": "QA_RULE_ALLOC_01",
                "level": "error",
                "message": "over capacity",
                "student_id": 5,
            }
        ]
    )
    db.insert_qa_snapshot(
        run_id=run_id, qa_summary_df=qa_summary_df, qa_details_df=qa_details_df
    )

    db.insert_run_metrics(
        [RunMetricRow(run_id=run_id, metric_key="SCHOOL.students_total", metric_value=2.0)]
    )


def test_history_dialog_loads_snapshots(qapp: QApplication, tmp_path) -> None:
    db = LocalDatabase(tmp_path / "ui.db")
    db.initialize()
    _insert_run_with_snapshots(db)

    dialog = HistoryDialog(db)
    dialog._on_run_selected(dialog._run_list.currentRow())

    assert dialog.trace_model.rowCount() == 1
    assert dialog.qa_summary_model.rowCount() == 1
    assert dialog.qa_details_model.rowCount() == 1
    assert dialog.metrics_model.rowCount() == 1
