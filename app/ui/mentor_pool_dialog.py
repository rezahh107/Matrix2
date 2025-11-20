from __future__ import annotations

"""دیالوگ حاکمیت استخر منتورها با جدول قابل ویرایش."""

from typing import Iterable, Mapping

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableView,
    QVBoxLayout,
)

from app.ui.models import MentorPoolEntry


class MentorPoolTableModel(QAbstractTableModel):
    """مدل ساده برای نمایش لیست منتورها با ستون فعال/غیرفعال."""

    _COLUMNS = (
        ("enabled", "فعال"),
        ("mentor_id", "شناسه"),
        ("mentor_name", "نام منتور"),
        ("manager", "مدیر"),
        ("center", "مرکز"),
        ("school", "مدرسه"),
        ("capacity", "ظرفیت"),
    )

    def __init__(self, entries: Iterable[MentorPoolEntry] | None = None, parent=None) -> None:
        super().__init__(parent)
        self._entries: list[MentorPoolEntry] = list(entries or [])

    def set_entries(self, entries: Iterable[MentorPoolEntry]) -> None:
        self.beginResetModel()
        self._entries = list(entries)
        self.endResetModel()

    # Qt model API -----------------------------------------------------
    def rowCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        if parent and parent.isValid():
            return 0
        return len(self._entries)

    def columnCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        if parent and parent.isValid():
            return 0
        return len(self._COLUMNS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # type: ignore[override]
        if not index.isValid():
            return None
        entry = self._entries[index.row()]
        column_key, _ = self._COLUMNS[index.column()]

        if role == Qt.DisplayRole:
            if column_key == "enabled":
                return "✅" if entry.enabled else "⏸"
            return str(getattr(entry, column_key, "") or "")
        if role == Qt.CheckStateRole and column_key == "enabled":
            return Qt.Checked if entry.enabled else Qt.Unchecked
        return None

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole):  # type: ignore[override]
        if not index.isValid():
            return False
        column_key, _ = self._COLUMNS[index.column()]
        if column_key != "enabled" or role not in {Qt.EditRole, Qt.CheckStateRole}:
            return False
        current = self._entries[index.row()]
        toggled = bool(value == Qt.Checked or value is True)
        if current.enabled == toggled:
            return False
        self._entries[index.row()] = MentorPoolEntry(
            mentor_id=current.mentor_id,
            mentor_name=current.mentor_name,
            manager=current.manager,
            center=current.center,
            school=current.school,
            capacity=current.capacity,
            enabled=toggled,
        )
        self.dataChanged.emit(index, index, [Qt.CheckStateRole, Qt.DisplayRole])
        return True

    def flags(self, index: QModelIndex):  # type: ignore[override]
        base = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if index.isValid() and self._COLUMNS[index.column()][0] == "enabled":
            return base | Qt.ItemIsUserCheckable
        return base

    def headerData(self, section: int, orientation, role=Qt.DisplayRole):  # type: ignore[override]
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self._COLUMNS):
            return self._COLUMNS[section][1]
        if orientation == Qt.Vertical:
            return str(section + 1)
        return None

    def overrides(self) -> dict[str, bool]:
        return {entry.mentor_id: entry.enabled for entry in self._entries}


class MentorPoolDialog(QDialog):
    """دیالوگ حاکمیت استخر منتورها (غیربلاک‌کننده)."""

    def __init__(self, entries: Iterable[MentorPoolEntry], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("حاکمیت استخر منتورها")
        self._model = MentorPoolTableModel(entries, self)
        self._table = QTableView(self)
        self._table.setModel(self._model)
        self._table.setSelectionMode(QTableView.NoSelection)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.verticalHeader().setVisible(False)

        hint = QLabel("منتورها را برای اجرای جاری فعال/غیرفعال کنید.", self)
        hint.setWordWrap(True)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(hint)
        layout.addWidget(self._table)

        footer = QHBoxLayout()
        footer.addStretch(1)
        refresh_button = QPushButton("بازنشانی", self)
        refresh_button.clicked.connect(self._reset_all)
        footer.addWidget(refresh_button)
        footer.addWidget(buttons)
        layout.addLayout(footer)
        self.resize(800, 420)

    def set_entries(self, entries: Iterable[MentorPoolEntry]) -> None:
        self._model.set_entries(entries)

    def _reset_all(self) -> None:
        entries = [
            MentorPoolEntry(
                mentor_id=item.mentor_id,
                mentor_name=item.mentor_name,
                manager=item.manager,
                center=item.center,
                school=item.school,
                capacity=item.capacity,
                enabled=True,
            )
            for item in self._model._entries
        ]
        self._model.set_entries(entries)

    def get_overrides(self) -> Mapping[str, bool]:
        return self._model.overrides()

    @property
    def model(self) -> MentorPoolTableModel:
        return self._model

