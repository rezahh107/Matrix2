"""ویجت کارت داشبورد با هدر و بدنه انعطاف‌پذیر."""

from __future__ import annotations

from typing import Iterable

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
    QFrame,
)

__all__ = ["DashboardCard"]


class DashboardCard(QFrame):
    """کارت داشبورد با عنوان، توضیح و محتوای سفارشی."""

    def __init__(
        self,
        title: str,
        description: str,
        parent: QWidget | None = None,
        *,
        max_height: int | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("dashboardCard")
        policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        if max_height is not None:
            policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.setMaximumHeight(max_height)
        self.setSizePolicy(policy)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(14, 12, 14, 12)
        self._layout.setSpacing(8)

        header = QVBoxLayout()
        header.setSpacing(2)

        title_label = QLabel(title)
        title_label.setObjectName("dashboardCardTitle")
        description_label = QLabel(description)
        description_label.setObjectName("dashboardCardDescription")
        description_label.setWordWrap(True)

        header.addWidget(title_label)
        header.addWidget(description_label)
        self._layout.addLayout(header)

        self._body_container = QScrollArea(self)
        self._body_container.setWidgetResizable(True)
        self._body_container.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._body_container.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._body_container.setFrameShape(QFrame.NoFrame)
        self._body_widget = QWidget(self._body_container)
        self._body = QVBoxLayout(self._body_widget)
        self._body.setContentsMargins(0, 0, 0, 0)
        self._body.setSpacing(4)
        self._body_container.setWidget(self._body_widget)
        self._layout.addWidget(self._body_container)

    def body_layout(self) -> QVBoxLayout:
        """دسترسی به لایهٔ محتوای کارت."""

        return self._body

    def add_widgets(self, widgets: Iterable[QWidget]) -> None:
        """افزودن مجموعه‌ای از ویجت‌ها به بدنه کارت."""

        for widget in widgets:
            self._body.addWidget(widget)
