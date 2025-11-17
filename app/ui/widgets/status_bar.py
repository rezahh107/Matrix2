from __future__ import annotations

from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QLabel, QStatusBar, QWidget

from app.ui.fonts import get_app_font
from app.ui.theme import Theme

__all__ = ["ThemedStatusBar"]


class ThemedStatusBar(QStatusBar):
    """نوار وضعیت با اعمال رنگ و فونت هماهنگ با تم فعال."""

    def __init__(self, theme: Theme, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._theme = theme
        self.setSizeGripEnabled(False)
        self.apply_theme(theme)
        self.setFont(get_app_font())

    def apply_theme(self, theme: Theme) -> None:
        """به‌روزرسانی رنگ پس‌زمینه و برچسب‌ها بر اساس تم."""

        self._theme = theme
        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Window, theme.card)
        palette.setColor(QPalette.ColorRole.Base, theme.card)
        palette.setColor(QPalette.ColorRole.Text, theme.text_primary)
        palette.setColor(QPalette.ColorRole.WindowText, theme.text_primary)
        self.setPalette(palette)
        self.setAutoFillBackground(True)

        self.setStyleSheet(
            f"QStatusBar {{"
            f"background: {theme.colors.card};"
            f"border-top: 1px solid {theme.colors.border};"
            f"padding: {theme.spacing_xs}px {theme.spacing_md}px;"
            f"}}"
            f"QLabel#languagePill, QLabel#statusPill {{"
            f"background: {theme.colors.surface_alt};"
            f"border: 1px solid {theme.colors.border};"
            f"border-radius: {theme.radius_sm}px;"
            f"padding: {theme.spacing_xs}px {theme.spacing_md}px;"
            f"font-weight: 700;"
            f"}}"
        )

    def refresh_fonts(self) -> None:
        """بازنشانی فونت برای هماهنگی با فونت سراسری."""

        self.setFont(get_app_font())
        for label in self.findChildren(QLabel):
            label.setFont(get_app_font())
