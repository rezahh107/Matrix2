"""دیالوگ مشاهدهٔ KPI تاریخچهٔ ذخیره‌شده."""

from __future__ import annotations

import pandas as pd
from PySide6.QtWidgets import QDialog, QHBoxLayout, QListWidget, QWidget

from app.core.allocation.history_metrics import METRIC_COLUMNS
from app.infra.local_database import LocalDatabase
from app.ui.history_metrics import HistoryMetricsPanel


def _metrics_rows_to_frame(rows: list[object]) -> pd.DataFrame:
    """تبدیل ردیف‌های run_metrics به دیتافریم استاندارد."""

    channels: dict[str, dict[str, float]] = {}
    for row in rows:
        metric_key = str(row["metric_key"])
        if "." not in metric_key:
            continue
        channel, metric = metric_key.split(".", 1)
        channels.setdefault(channel, {})[metric] = float(row["metric_value"])
    payload = []
    for channel, metrics in channels.items():
        row = {"allocation_channel": channel}
        row.update(metrics)
        payload.append(row)
    if not payload:
        return pd.DataFrame(columns=METRIC_COLUMNS)
    frame = pd.DataFrame(payload)
    missing = [col for col in METRIC_COLUMNS if col not in frame.columns]
    for col in missing:
        frame[col] = pd.NA
    return frame.loc[:, METRIC_COLUMNS]


class HistoryMetricsDialog(QDialog):
    """دیالوگ مرور KPI های ذخیره‌شده برای اجراهای گذشته."""

    def __init__(self, db: LocalDatabase, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._db = db
        self._runs: list[object] = []
        self.setWindowTitle("Stored History Metrics")

        self._run_list = QListWidget(self)
        self._run_list.currentRowChanged.connect(self._on_run_selected)

        self._panel = HistoryMetricsPanel(self)

        layout = QHBoxLayout(self)
        layout.addWidget(self._run_list, 1)
        layout.addWidget(self._panel, 3)
        self.resize(720, 420)
        self._load_runs()

    def _load_runs(self) -> None:
        self._runs = self._db.fetch_runs()
        self._run_list.clear()
        for row in self._runs:
            self._run_list.addItem(f"#{row['id']} | {row['run_uuid']}")
        if self._runs:
            self._run_list.setCurrentRow(len(self._runs) - 1)
        else:
            self._panel.set_metrics(pd.DataFrame(columns=METRIC_COLUMNS))

    def _on_run_selected(self, row: int) -> None:
        if row < 0 or row >= len(self._runs):
            self._panel.set_metrics(pd.DataFrame(columns=METRIC_COLUMNS))
            return
        run_id = int(self._runs[row]["id"])
        metric_rows = self._db.fetch_metrics_for_run(run_id)
        metrics_df = _metrics_rows_to_frame(metric_rows)
        self._panel.set_metrics(metrics_df)

    @property
    def panel(self) -> HistoryMetricsPanel:
        return self._panel

