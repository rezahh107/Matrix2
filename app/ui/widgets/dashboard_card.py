"""ویجت کارت داشبورد با هدر و بدنه انعطاف‌پذیر."""

from __future__ import annotations

from typing import Iterable

from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget, QFrame

__all__ = ["DashboardCard"]


class DashboardCard(QFrame):
    """کارت داشبورد با عنوان، توضیح و محتوای سفارشی."""

    def __init__(self, title: str, description: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("dashboardCard")
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(20, 16, 20, 16)
        self._layout.setSpacing(12)

        header = QVBoxLayout()
        header.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("dashboardCardTitle")
        description_label = QLabel(description)
        description_label.setObjectName("dashboardCardDescription")
        description_label.setWordWrap(True)

        header.addWidget(title_label)
        header.addWidget(description_label)
        self._layout.addLayout(header)

        self._body = QVBoxLayout()
        self._body.setSpacing(8)
        self._layout.addLayout(self._body)

    def body_layout(self) -> QVBoxLayout:
        """دسترسی به لایهٔ محتوای کارت."""

        return self._body

    def add_widgets(self, widgets: Iterable[QWidget]) -> None:
        """افزودن مجموعه‌ای از ویجت‌ها به بدنه کارت."""

        for widget in widgets:
            self._body.addWidget(widget)
