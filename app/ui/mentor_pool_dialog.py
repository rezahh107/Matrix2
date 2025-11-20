from __future__ import annotations

"""دیالوگ حاکمیت استخر منتورها با گروه‌بندی مدیر→منتور."""

from typing import Iterable, Mapping

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTreeView,
    QVBoxLayout,
)

from app.ui.mentor_pool_model import ManagerMentorFilterProxy, ManagerMentorModel
from app.ui.models import MentorPoolEntry


class MentorPoolDialog(QDialog):
    """دیالوگ حاکمیت استخر منتورها (غیربلاک‌کننده).

    قابلیت‌ها:
        - گروه‌بندی منتورها زیر مدیر مربوطه.
        - تیک گروهی مدیر برای فعال/غیرفعال کردن همهٔ منتورها.
        - فیلتر ساده بر اساس نام/شناسه.
    """

    def __init__(self, entries: Iterable[MentorPoolEntry], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("حاکمیت استخر منتورها")
        self._model = ManagerMentorModel(entries, self)
        self._proxy = ManagerMentorFilterProxy()
        self._proxy.setSourceModel(self._model)

        self._tree = QTreeView(self)
        self._tree.setModel(self._proxy)
        self._tree.setRootIsDecorated(True)
        self._tree.setUniformRowHeights(True)
        self._tree.setHeaderHidden(False)
        self._tree.setColumnWidth(0, 80)
        self._tree.setAllColumnsShowFocus(True)
        self._tree.expandAll()

        self._search = QLineEdit(self)
        self._search.setPlaceholderText("جستجو بر اساس مدیر یا منتور…")
        self._search.textChanged.connect(self._proxy.set_query)

        hint = QLabel("منتورها را برای اجرای جاری فعال/غیرفعال کنید.", self)
        hint.setWordWrap(True)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(hint)
        layout.addWidget(self._search)
        layout.addWidget(self._tree)

        footer = QHBoxLayout()
        footer.addStretch(1)
        select_all = QPushButton("✔️ فعال‌سازی همه", self)
        select_all.clicked.connect(lambda: self._model.set_all(True))
        select_none = QPushButton("⏸ غیرفعالسازی همه", self)
        select_none.clicked.connect(lambda: self._model.set_all(False))
        refresh_button = QPushButton("بازنشانی", self)
        refresh_button.clicked.connect(self._reset_all)
        footer.addWidget(select_all)
        footer.addWidget(select_none)
        footer.addWidget(refresh_button)
        footer.addWidget(buttons)
        layout.addLayout(footer)
        self.resize(920, 540)

    def set_entries(self, entries: Iterable[MentorPoolEntry]) -> None:
        self._model.set_entries(entries)
        self._tree.expandAll()

    def _reset_all(self) -> None:
        self._model.set_all(True)
        self._tree.expandAll()

    def get_overrides(self) -> Mapping[str, bool]:
        return self._model.overrides()

    def get_manager_overrides(self) -> Mapping[str, bool]:
        return self._model.manager_overrides()

    @property
    def model(self) -> ManagerMentorModel:
        return self._model

