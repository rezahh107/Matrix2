from __future__ import annotations

import pandas as pd
import pytest

try:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication
except ImportError as exc:  # pragma: no cover - محیط فاقد وابستگی Qt
    pytest.skip(f"PySide6 unavailable: {exc}", allow_module_level=True)

from app.core.allocation.dedupe import HistoryStatus
from app.core.allocation.history_metrics import METRIC_COLUMNS, compute_history_metrics
from app.ui.history_metrics import HistoryMetricsDialog, HistoryMetricsModel, HistoryMetricsPanel


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _sample_metrics_df() -> pd.DataFrame:
    summary = pd.DataFrame(
        {
            "student_id": [1, 2, 3, 4],
            "allocation_channel": ["A", "A", "B", "B"],
            "history_status": [
                HistoryStatus.ALREADY_ALLOCATED.value,
                HistoryStatus.NO_HISTORY_MATCH.value,
                HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value,
                HistoryStatus.NO_HISTORY_MATCH.value,
            ],
            "same_history_mentor": [True, False, False, False],
        }
    )
    return compute_history_metrics(summary)


def test_history_metrics_model_counts_and_values(qapp: QApplication) -> None:
    metrics_df = _sample_metrics_df()
    model = HistoryMetricsModel(metrics_df)

    assert model.rowCount() == 2
    assert model.columnCount() == len(METRIC_COLUMNS)

    first_channel = model.data(model.index(0, 0), Qt.DisplayRole)
    ratio_value = model.data(model.index(0, METRIC_COLUMNS.index("same_history_mentor_ratio")), Qt.DisplayRole)

    assert first_channel == "A"
    assert ratio_value == "0.500"


def test_history_metrics_model_handles_empty(qapp: QApplication) -> None:
    model = HistoryMetricsModel(pd.DataFrame())

    assert model.rowCount() == 0
    assert model.columnCount() == len(METRIC_COLUMNS)


def test_history_metrics_panel_updates_and_dialog(qapp: QApplication) -> None:
    metrics_df = _sample_metrics_df()
    panel = HistoryMetricsPanel()
    panel.set_metrics(metrics_df)

    assert panel.model.rowCount() == len(metrics_df.index)
    assert panel.model.data(panel.model.index(1, 0), Qt.DisplayRole) == "B"

    dialog = HistoryMetricsDialog(metrics_df)
    dialog.update_metrics(metrics_df.iloc[:1])
    assert dialog._panel.model.rowCount() == 1
