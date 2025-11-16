"""مدیر تم سبک برای پیاده‌سازی سبک Fluent/Windows 11.

این ماژول یک تم مرکزی را تعریف می‌کند و روی QApplication اعمال می‌کند تا
یکدستی رنگ و استایل در تمام ویجت‌های UI حفظ شود.
"""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

__all__ = ["Theme", "build_system_light_theme", "apply_theme", "relative_luminance"]


@dataclass(frozen=True)
class Theme:
    """تعریف نقش‌های رنگی و ابعادی تم UI.

    تمام رنگ‌ها باید معتبر و روشن برای تم روشن باشند تا خوانایی تضمین شود.
    """

    window: QColor
    surface: QColor
    surface_alt: QColor
    card: QColor
    accent: QColor
    accent_soft: QColor
    border: QColor
    text_primary: QColor
    text_muted: QColor
    success: QColor
    warning: QColor
    error: QColor
    log_bg: QColor
    log_border: QColor
    radius: int = 8
    spacing: int = 8

    @property
    def window_bg(self) -> QColor:
        """هم‌معنای window برای شفافیت نام نقش."""

        return self.window

    @property
    def card_bg(self) -> QColor:
        """هم‌معنای card برای نام‌گذاری روشن."""

        return self.card


def _from_palette(palette: QPalette, role: QPalette.ColorRole, fallback: str) -> QColor:
    color = palette.color(role)
    if not color.isValid():
        return QColor(fallback)
    return color


def _blend(base: QColor, overlay: QColor, alpha: float) -> QColor:
    """ترکیب دو رنگ با ضریب آلفا برای تولید سطح نرم."""

    r = int(base.red() * (1 - alpha) + overlay.red() * alpha)
    g = int(base.green() * (1 - alpha) + overlay.green() * alpha)
    b = int(base.blue() * (1 - alpha) + overlay.blue() * alpha)
    return QColor(r, g, b)


def relative_luminance(color: QColor) -> float:
    """محاسبهٔ روشنایی نسبی رنگ برای سنجش کنتراست."""

    return (0.2126 * color.red() + 0.7152 * color.green() + 0.0722 * color.blue()) / 255


def build_system_light_theme() -> Theme:
    """ساخت تم روشن نزدیک به Windows 11 بر پایه پالت سیستم."""

    palette = QApplication.instance().palette() if QApplication.instance() else QPalette()
    window = _from_palette(palette, QPalette.ColorRole.Window, "#f7f7f8")
    base = _from_palette(palette, QPalette.ColorRole.Base, "#ffffff")
    text = _from_palette(palette, QPalette.ColorRole.Text, "#111827")
    highlight = _from_palette(palette, QPalette.ColorRole.Highlight, "#2563eb")

    card = _blend(base, window, 0.08)
    border = _blend(text, window, 0.15)
    accent = highlight if highlight.isValid() else QColor("#2563eb")
    accent_soft = _blend(accent, window, 0.8)
    muted = _blend(text, window, 0.55)

    return Theme(
        window=window,
        surface=base,
        surface_alt=_blend(window, base, 0.12),
        card=card,
        accent=accent,
        accent_soft=accent_soft,
        border=border,
        text_primary=text,
        text_muted=muted,
        success=QColor("#16a34a"),
        warning=QColor("#d97706"),
        error=QColor("#dc2626"),
        log_bg=_blend(base, window, 0.06),
        log_border=_blend(text, window, 0.2),
    )


def _stylesheet(theme: Theme) -> str:
    """تولید StyleSheet یکپارچه برای کنترل‌های کلیدی."""

    return f"""
        QWidget {{
            color: {theme.text_primary.name()};
            background-color: {theme.window.name()};
            font-size: 10pt;
        }}
        QMainWindow, QDialog {{
            background-color: {theme.window.name()};
        }}
        QGroupBox {{
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius}px;
            margin-top: 6px;
            background: {theme.surface.name()};
            padding: 6px;
        }}
        QGroupBox::title {{
            subcontrol-origin: margin;
            left: 8px;
            padding: 0 4px 2px 4px;
            color: {theme.text_primary.name()};
        }}
        QTabWidget::pane {{
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius}px;
            background: {theme.surface.name()};
        }}
        QTabBar::tab {{
            background: {theme.surface_alt.name()};
            color: {theme.text_muted.name()};
            padding: 6px 10px;
            border: 1px solid {theme.border.name()};
            border-bottom: none;
            border-top-left-radius: {theme.radius}px;
            border-top-right-radius: {theme.radius}px;
            margin-right: 2px;
        }}
        QTabBar::tab:selected {{
            background: {theme.surface.name()};
            color: {theme.text_primary.name()};
        }}
        QToolButton, QPushButton {{
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius - 2}px;
            padding: 6px 10px;
            background: {theme.surface_alt.name()};
        }}
        QToolButton:hover, QPushButton:hover {{
            background: {theme.surface.name()};
        }}
        QPushButton#primaryButton {{
            background: {theme.accent.name()};
            color: #ffffff;
            border: 1px solid {theme.accent.darker(110).name()};
        }}
        QPushButton#primaryButton:hover {{
            background: {theme.accent.darker(110).name()};
        }}
        QProgressBar {{
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius - 2}px;
            background: {theme.surface_alt.name()};
            text-align: center;
        }}
        QProgressBar::chunk {{
            background: {theme.accent.name()};
            border-radius: {theme.radius - 3}px;
        }}
        QTextEdit {{
            background: {theme.surface.name()};
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius}px;
        }}
        #dashboardCard {{
            background: {theme.card.name()};
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius}px;
        }}
        #dashboardCardTitle {{
            color: {theme.text_primary.name()};
            font-weight: 600;
        }}
        #dashboardCardDescription, #dashboardChecklistItem {{
            color: {theme.text_muted.name()};
        }}
        #fileStatusRow {{
            border-bottom: 1px solid {theme.border.name()};
        }}
        #dashboardShortcut {{
            background: {theme.surface_alt.name()};
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius - 2}px;
            padding: 6px 10px;
        }}
        #dashboardShortcut:hover {{
            background: {theme.surface.name()};
        }}
        #logPanel {{
            background: {theme.log_bg.name()};
            border: 1px solid {theme.log_border.name()};
            border-radius: {theme.radius}px;
        }}
    """


def apply_theme(app: QApplication, theme: Theme) -> None:
    """اعمال تم بر روی QApplication و تنظیم StyleSheet کلی."""

    palette = app.palette()
    palette.setColor(QPalette.ColorRole.Window, theme.window)
    palette.setColor(QPalette.ColorRole.Base, theme.surface)
    palette.setColor(QPalette.ColorRole.Button, theme.surface_alt)
    palette.setColor(QPalette.ColorRole.Text, theme.text_primary)
    palette.setColor(QPalette.ColorRole.WindowText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.ButtonText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Highlight, theme.accent)
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
    palette.setColor(QPalette.ColorRole.Link, theme.accent)
    app.setPalette(palette)
    app.setStyleSheet(_stylesheet(theme))

