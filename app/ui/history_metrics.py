from __future__ import annotations

"""ویجت و مدل نمایش KPI تاریخچه تخصیص در UI."""

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QTableView, QVBoxLayout, QWidget

from app.core.allocation.history_metrics import METRIC_COLUMNS


class HistoryMetricsModel(QAbstractTableModel):
    """مدل فقط‌خواندنی برای نمایش خلاصه KPI تاریخچه.

    Parameters
    ----------
    metrics_df:
        دیتافریم خروجی :func:`compute_history_metrics`.
    """

    def __init__(self, metrics_df: pd.DataFrame | None = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._metrics_df = self._normalize(metrics_df)

    @staticmethod
    def _normalize(metrics_df: pd.DataFrame | None) -> pd.DataFrame:
        if metrics_df is None:
            return pd.DataFrame(columns=METRIC_COLUMNS)
        missing = [col for col in METRIC_COLUMNS if col not in metrics_df.columns]
        if missing:
            normalized = metrics_df.copy()
            for col in missing:
                normalized[col] = pd.NA
            normalized = normalized[METRIC_COLUMNS]
            return normalized
        return metrics_df.copy()

    def update_metrics(self, metrics_df: pd.DataFrame | None) -> None:
        """به‌روزرسانی محتوا و اطلاع‌رسانی به View."""

        self.beginResetModel()
        self._metrics_df = self._normalize(metrics_df)
        self.endResetModel()

    # Qt model overrides -------------------------------------------------
    def rowCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        if parent and parent.isValid():
            return 0
        return len(self._metrics_df.index)

    def columnCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        if parent and parent.isValid():
            return 0
        return len(METRIC_COLUMNS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # type: ignore[override]
        if not index.isValid() or role not in {Qt.DisplayRole, Qt.EditRole}:
            return None
        value = self._metrics_df.iloc[index.row(), index.column()]
        if pd.isna(value):
            return ""
        if METRIC_COLUMNS[index.column()] == "same_history_mentor_ratio":
            try:
                return f"{float(value):.3f}"
            except (TypeError, ValueError):
                return str(value)
        return str(value)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):  # type: ignore[override]
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(METRIC_COLUMNS):
                return METRIC_COLUMNS[section]
        else:
            return str(section + 1)
        return None


class HistoryMetricsPanel(QWidget):
    """پانل ساده برای نمایش جدول KPI تاریخچه."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._model = HistoryMetricsModel()
        self._table = QTableView(self)
        self._table.setModel(self._model)
        self._table.setSelectionMode(QTableView.NoSelection)
        self._table.setEditTriggers(QTableView.NoEditTriggers)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.verticalHeader().setVisible(False)

        self._empty_label = QLabel("No history metrics available", self)
        self._empty_label.setAlignment(Qt.AlignCenter)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)
        layout.addWidget(self._table)
        layout.addWidget(self._empty_label)
        self._update_visibility()

    def set_metrics(self, metrics_df: pd.DataFrame | None) -> None:
        """بارگذاری دیتافریم KPI در مدل و به‌روزرسانی نما."""

        self._model.update_metrics(metrics_df)
        self._update_visibility()

    def _update_visibility(self) -> None:
        is_empty = self._model.rowCount() == 0
        self._table.setVisible(not is_empty)
        self._empty_label.setVisible(is_empty)

    @property
    def model(self) -> HistoryMetricsModel:
        return self._model


class HistoryMetricsDialog(QDialog):
    """دیالوگ جمع‌وجور برای نمایش KPI تاریخچه."""

    def __init__(self, metrics_df: pd.DataFrame | None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("History Metrics")
        layout = QHBoxLayout(self)
        self._panel = HistoryMetricsPanel(self)
        self._panel.set_metrics(metrics_df)
        layout.addWidget(self._panel)
        self.resize(720, 320)

    def update_metrics(self, metrics_df: pd.DataFrame | None) -> None:
        self._panel.set_metrics(metrics_df)

