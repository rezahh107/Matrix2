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

from app.ui.theme import Theme

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
        theme: Theme | None = None,
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

        self._title_label = QLabel(title)
        self._title_label.setObjectName("dashboardCardTitle")
        self._description_label = QLabel(description)
        self._description_label.setObjectName("dashboardCardDescription")
        self._description_label.setWordWrap(True)

        header.addWidget(self._title_label)
        header.addWidget(self._description_label)
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

        if theme is not None:
            self.apply_theme(theme)

    def body_layout(self) -> QVBoxLayout:
        """دسترسی به لایهٔ محتوای کارت."""

        return self._body

    def add_widgets(self, widgets: Iterable[QWidget]) -> None:
        """افزودن مجموعه‌ای از ویجت‌ها به بدنه کارت."""

        for widget in widgets:
            self._body.addWidget(widget)

    def clear_body(self) -> None:
        """پاک‌سازی محتوای بدنه برای بازسازی ترجمه یا محتوا."""

        while self._body.count():
            item = self._body.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)

    def set_header(self, title: str, description: str) -> None:
        """به‌روزرسانی عنوان و توضیح کارت."""

        self._title_label.setText(title)
        self._description_label.setText(description)

    def apply_theme(self, theme: Theme) -> None:
        """اعمال تم روی کارت و متن‌های آن."""

        self.setStyleSheet(
            f"#dashboardCard{{"
            f"background:{theme.card.name()};"
            f"border:1px solid {theme.border.name()};"
            f"border-radius:{theme.radius}px;}}"
            f"#dashboardCardTitle{{color:{theme.text_primary.name()};font-weight:600;}}"
            f"#dashboardCardDescription, #dashboardChecklistItem{{color:{theme.text_muted.name()};}}"
        )
