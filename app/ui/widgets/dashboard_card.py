"""ویجت کارت داشبورد با هدر و بدنه انعطاف‌پذیر."""

from __future__ import annotations

from typing import Iterable

from PySide6.QtCore import QEasingCurve, QParallelAnimationGroup, QPropertyAnimation, Qt
from PySide6.QtWidgets import (
    QLabel,
    QGraphicsDropShadowEffect,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
    QFrame,
)

from app.ui.theme import Theme, ShadowSpec

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
        self._theme = theme
        policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        if max_height is not None:
            policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.setMaximumHeight(max_height)
        self.setSizePolicy(policy)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(14, 12, 14, 12)
        self._layout.setSpacing(8)
        self._shadow_effect = QGraphicsDropShadowEffect(self)
        self.setGraphicsEffect(self._shadow_effect)
        self._hover_animation: QParallelAnimationGroup | None = None

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

    def enterEvent(self, event) -> None:  # type: ignore[override]
        self._lift(True)
        super().enterEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self._lift(False)
        super().leaveEvent(event)

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

        self._theme = theme
        self._layout.setContentsMargins(
            theme.spacing_md + theme.spacing_xs,
            theme.spacing_md,
            theme.spacing_md + theme.spacing_xs,
            theme.spacing_md,
        )
        self._layout.setSpacing(theme.spacing_sm)
        self._body.setSpacing(theme.spacing_xs + 2)
        self._apply_shadow(theme.shadow_ambient)
        self.setStyleSheet(
            f"#dashboardCard{{"
            f"background:{theme.card.name()};"
            f"border:1px solid {theme.border.name()};"
            f"border-radius:{theme.radius}px;"
            f"transition: background 120ms ease, border-color 120ms ease;}}"
            f"#dashboardCard:hover{{background:{theme.surface_alt.name()};border-color:{theme.accent_soft.name()};}}"
            f"#dashboardCardTitle{{color:{theme.text_primary.name()};font-weight:700;}}"
            f"#dashboardCardDescription, #dashboardChecklistItem{{color:{theme.text_muted.name()};}}"
        )

    def _lift(self, hovered: bool) -> None:
        """انیمیشن لیفت کارت هنگام Hover."""

        if self._theme is None:
            return
        target = self._theme.shadow_hover if hovered else self._theme.shadow_ambient
        self._apply_shadow(target, animated=True)
        self.setProperty("hovered", hovered)
        self.style().unpolish(self)
        self.style().polish(self)

    def _apply_shadow(self, spec: Theme | ShadowSpec, animated: bool = False) -> None:
        """اعمال یا انیمیشن سایه نرم برای ایجاد عمق کارت."""

        target_spec = spec.shadow_ambient if isinstance(spec, Theme) else spec
        if not animated:
            self._shadow_effect.setBlurRadius(target_spec.blur_radius)
            self._shadow_effect.setOffset(target_spec.x_offset, target_spec.y_offset)
            self._shadow_effect.setColor(target_spec.color)
            return

        blur_anim = QPropertyAnimation(self._shadow_effect, b"blurRadius", self)
        blur_anim.setDuration(180)
        blur_anim.setStartValue(self._shadow_effect.blurRadius())
        blur_anim.setEndValue(target_spec.blur_radius)
        blur_anim.setEasingCurve(QEasingCurve.InOutQuad)

        y_anim = QPropertyAnimation(self._shadow_effect, b"yOffset", self)
        y_anim.setDuration(180)
        y_anim.setStartValue(self._shadow_effect.yOffset())
        y_anim.setEndValue(target_spec.y_offset)
        y_anim.setEasingCurve(QEasingCurve.InOutQuad)

        group = QParallelAnimationGroup(self)
        group.addAnimation(blur_anim)
        group.addAnimation(y_anim)
        self._hover_animation = group
        group.start(QPropertyAnimation.DeleteWhenStopped)
