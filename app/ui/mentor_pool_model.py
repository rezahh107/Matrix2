"""مدل درختی مدیر→منتور برای دیالوگ حاکمیت استخر.

این ماژول وابستگی به Qt دارد و صرفاً در لایهٔ UI استفاده می‌شود.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from PySide6.QtCore import QSortFilterProxyModel, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

from app.ui.models import MentorPoolEntry

_ROLE_KIND = Qt.UserRole + 1
_ROLE_ID = Qt.UserRole + 2
_ROLE_SEARCH = Qt.UserRole + 3


@dataclass
class _ManagerGroup:
    name: str
    mentors: list[MentorPoolEntry]


class ManagerMentorFilterProxy(QSortFilterProxyModel):
    """فیلتر ساده برای جستجوی مدیر/منتور در مدل درختی."""

    def __init__(self) -> None:
        super().__init__()
        self._query = ""

    def set_query(self, text: str) -> None:
        self._query = (text or "").strip().lower()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent):  # type: ignore[override]
        if not self._query:
            return True
        model = self.sourceModel()
        index = model.index(source_row, 0, source_parent)
        kind = model.data(index, _ROLE_KIND)
        search_text = str(model.data(index, _ROLE_SEARCH) or "").lower()
        if self._query in search_text:
            return True
        if kind == "manager":
            child_count = model.rowCount(index)
            return any(self.filterAcceptsRow(i, index) for i in range(child_count))
        return False


class ManagerMentorModel(QStandardItemModel):
    """مدل درختی با ریشهٔ مدیر و فرزندان منتور همراه با وضعیت تیک‌خورده.

    این مدل وضعیت انتخاب مدیر را بر اساس فرزندان به‌روزرسانی می‌کند و هنگام تغییر
    مدیر، همهٔ فرزندان را به همان وضعیت می‌برد. خروجی `overrides()` یک نگاشت
    دترمینیستیک «mentor_id→enabled» است و `manager_overrides()` وضعیت مدیران را
    برمی‌گرداند.
    """

    HEADERS = ["فعال", "نام", "شناسه", "مدیر", "مرکز", "مدرسه", "ظرفیت"]

    def __init__(self, entries: Iterable[MentorPoolEntry], parent=None) -> None:
        super().__init__(parent)
        self._suppress = False
        self.setHorizontalHeaderLabels(self.HEADERS)
        self._populate(entries)
        self.itemChanged.connect(self._handle_item_changed)

    def _group_entries(self, entries: Iterable[MentorPoolEntry]) -> list[_ManagerGroup]:
        buckets: dict[str, list[MentorPoolEntry]] = {}
        for entry in entries:
            manager = (entry.manager or "(بدون مدیر)").strip() or "(بدون مدیر)"
            buckets.setdefault(manager, []).append(entry)
        groups: list[_ManagerGroup] = []
        for manager in sorted(buckets.keys(), key=str.lower):
            groups.append(_ManagerGroup(name=manager, mentors=buckets[manager]))
        return groups

    def _populate(self, entries: Iterable[MentorPoolEntry]) -> None:
        self.clear()
        self.setHorizontalHeaderLabels(self.HEADERS)
        for group in self._group_entries(entries):
            manager_item = QStandardItem(group.name)
            manager_item.setCheckable(True)
            manager_item.setCheckState(Qt.Checked)
            manager_item.setData("manager", _ROLE_KIND)
            manager_item.setData(group.name, _ROLE_ID)
            manager_item.setData(group.name, _ROLE_SEARCH)

            name_item = QStandardItem(group.name)
            id_item = QStandardItem("—")
            center_item = QStandardItem("")
            school_item = QStandardItem("")
            capacity_item = QStandardItem("")
            for extra in (name_item, id_item, center_item, school_item, capacity_item):
                extra.setEditable(False)

            self.appendRow([manager_item, name_item, id_item, QStandardItem(group.name), center_item, school_item, capacity_item])

            for mentor in group.mentors:
                child_enabled = Qt.Checked if mentor.enabled else Qt.Unchecked
                child_active = QStandardItem()
                child_active.setCheckable(True)
                child_active.setCheckState(child_enabled)
                child_active.setData("mentor", _ROLE_KIND)
                child_active.setData(mentor.mentor_id, _ROLE_ID)
                child_active.setData(
                    f"{mentor.mentor_name} {mentor.mentor_id} {group.name}", _ROLE_SEARCH
                )

                child_name = QStandardItem(mentor.mentor_name)
                child_id = QStandardItem(str(mentor.mentor_id))
                child_manager = QStandardItem(group.name)
                child_center = QStandardItem(str(mentor.center or ""))
                child_school = QStandardItem(str(mentor.school or ""))
                child_capacity = QStandardItem(str(mentor.capacity or ""))
                for extra in (
                    child_name,
                    child_id,
                    child_manager,
                    child_center,
                    child_school,
                    child_capacity,
                ):
                    extra.setEditable(False)

                manager_item.appendRow(
                    [
                        child_active,
                        child_name,
                        child_id,
                        child_manager,
                        child_center,
                        child_school,
                        child_capacity,
                    ]
                )
            self._refresh_manager_state(manager_item)

    def _handle_item_changed(self, item: QStandardItem) -> None:
        if self._suppress:
            return
        kind = item.data(_ROLE_KIND)
        if kind == "manager":
            self._apply_manager_to_children(item)
        elif kind == "mentor":
            parent = item.parent()
            if parent:
                self._refresh_manager_state(parent)

    def _apply_manager_to_children(self, manager_item: QStandardItem) -> None:
        self._suppress = True
        try:
            state = manager_item.checkState()
            for row in range(manager_item.rowCount()):
                child = manager_item.child(row, 0)
                child.setCheckState(state)
        finally:
            self._suppress = False

    def _refresh_manager_state(self, manager_item: QStandardItem) -> None:
        if manager_item.rowCount() == 0:
            manager_item.setCheckState(Qt.Unchecked)
            return
        states = {manager_item.child(row, 0).checkState() for row in range(manager_item.rowCount())}
        if states == {Qt.Checked}:
            manager_item.setCheckState(Qt.Checked)
        elif states == {Qt.Unchecked}:
            manager_item.setCheckState(Qt.Unchecked)
        else:
            manager_item.setCheckState(Qt.PartiallyChecked)

    def set_entries(self, entries: Iterable[MentorPoolEntry]) -> None:
        self._suppress = True
        try:
            self._populate(entries)
        finally:
            self._suppress = False

    def overrides(self) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for r in range(self.rowCount()):
            manager_item = self.item(r, 0)
            for c in range(manager_item.rowCount()):
                mentor_item = manager_item.child(c, 0)
                mentor_id = str(mentor_item.data(_ROLE_ID))
                result[mentor_id] = mentor_item.checkState() == Qt.Checked
        return result

    def manager_overrides(self) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for r in range(self.rowCount()):
            manager_item = self.item(r, 0)
            manager_id = str(manager_item.data(_ROLE_ID))
            result[manager_id] = manager_item.checkState() == Qt.Checked
        return result

    def set_all(self, enabled: bool) -> None:
        state = Qt.Checked if enabled else Qt.Unchecked
        self._suppress = True
        try:
            for r in range(self.rowCount()):
                manager_item = self.item(r, 0)
                manager_item.setCheckState(state)
                for c in range(manager_item.rowCount()):
                    manager_item.child(c, 0).setCheckState(state)
        finally:
            self._suppress = False
        for r in range(self.rowCount()):
            self._refresh_manager_state(self.item(r, 0))
