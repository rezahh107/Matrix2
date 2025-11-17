"""مدیر تم سبک برای پیاده‌سازی سبک Fluent/Windows 11.

این ماژول یک تم مرکزی را تعریف می‌کند و روی QApplication اعمال می‌کند تا
یکدستی رنگ و استایل در تمام ویجت‌های UI حفظ شود.
"""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

__all__ = [
    "Theme",
    "build_light_theme",
    "build_dark_theme",
    "build_theme",
    "apply_theme_mode",
    "apply_theme",
    "relative_luminance",
]


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


def _blend(base: QColor, overlay: QColor, alpha: float) -> QColor:
    """ترکیب دو رنگ با ضریب آلفا برای تولید سطح نرم."""

    r = int(base.red() * (1 - alpha) + overlay.red() * alpha)
    g = int(base.green() * (1 - alpha) + overlay.green() * alpha)
    b = int(base.blue() * (1 - alpha) + overlay.blue() * alpha)
    return QColor(r, g, b)


def relative_luminance(color: QColor) -> float:
    """محاسبهٔ روشنایی نسبی رنگ برای سنجش کنتراست."""

    return (0.2126 * color.red() + 0.7152 * color.green() + 0.0722 * color.blue()) / 255


def build_light_theme() -> Theme:
    """ساخت تم روشن ثابت با پس‌زمینهٔ خاکستری بسیار روشن."""

    window = QColor("#f5f6f8")
    base = QColor("#ffffff")
    surface_alt = QColor("#eef0f4")
    card = QColor("#ffffff")
    text = QColor("#0f172a")
    accent = QColor("#2563eb")
    complement = QColor("#f59e0b")

    border = _blend(text, window, 0.2)
    accent_soft = _blend(complement, window, 0.22)
    muted = _blend(text, window, 0.5)

    return Theme(
        window=window,
        surface=base,
        surface_alt=surface_alt,
        card=card,
        accent=accent,
        accent_soft=accent_soft,
        border=border,
        text_primary=text,
        text_muted=muted,
        success=QColor("#15803d"),
        warning=QColor("#d97706"),
        error=QColor("#b91c1c"),
        log_bg=QColor("#f9fafb"),
        log_border=_blend(text, window, 0.25),
    )


def build_dark_theme() -> Theme:
    """ساخت تم تیره با کنتراست بالا و رنگ مکمل برای هایلایت‌ها."""

    window = QColor("#0b1220")
    base = QColor("#111827")
    text = QColor("#e2e8f0")
    accent = QColor("#22d3ee")
    complement = QColor("#f59e0b")

    card = _blend(base, window, 0.3)
    border = _blend(text, window, 0.35)
    accent_soft = _blend(complement, window, 0.3)
    muted = _blend(text, window, 0.4)

    return Theme(
        window=window,
        surface=base,
        surface_alt=_blend(window, base, 0.25),
        card=card,
        accent=accent,
        accent_soft=accent_soft,
        border=border,
        text_primary=text,
        text_muted=muted,
        success=QColor("#34d399"),
        warning=QColor("#f59e0b"),
        error=QColor("#f87171"),
        log_bg=_blend(base, window, 0.18),
        log_border=_blend(text, window, 0.25),
    )


def build_theme(mode: str) -> Theme:
    """ساخت تم بر اساس نام حالت روشن/تیره."""

    normalized = (mode or "").strip().lower()
    if normalized == "dark":
        return build_dark_theme()
    return build_light_theme()


def _stylesheet(theme: Theme) -> str:
    """تولید StyleSheet یکپارچه برای کنترل‌های کلیدی."""

    return f"""
        QWidget {{
            color: {theme.text_primary.name()};
            background-color: {theme.window.name()};
            font-size: 11pt;
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
            border-radius: {theme.radius}px;
            padding: 6px 14px;
            background: {theme.surface_alt.name()};
        }}
        QToolButton:hover, QPushButton:hover {{
            background: {theme.surface.name()};
        }}
        QToolButton:checked {{
            background: {theme.accent_soft.name()};
            border-color: {theme.accent.name()};
        }}
        QPushButton#primaryButton {{
            background: {theme.accent.name()};
            color: #ffffff;
            border: 1px solid {theme.accent.darker(110).name()};
        }}
        QPushButton#primaryButton:hover {{
            background: {theme.accent.darker(110).name()};
        }}
        QComboBox {{
            border: 1px solid {theme.border.name()};
            border-radius: {theme.radius}px;
            padding: 6px 10px;
            background: {theme.surface_alt.name()};
            color: {theme.text_primary.name()};
        }}
        QComboBox::drop-down {{
            border: none;
        }}
        QComboBox QAbstractItemView {{
            background: {theme.surface.name()};
            color: {theme.text_primary.name()};
            selection-background-color: {theme.accent_soft.name()};
            selection-color: {theme.text_primary.name()};
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
    palette.setColor(QPalette.ColorRole.AlternateBase, theme.surface_alt)
    palette.setColor(QPalette.ColorRole.WindowText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.ButtonText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Highlight, theme.accent)
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
    palette.setColor(QPalette.ColorRole.Link, theme.accent)
    app.setPalette(palette)
    app.setStyleSheet(_stylesheet(theme))


def apply_theme_mode(app: QApplication, mode: str) -> Theme:
    """ساخت و اعمال تم بر اساس حالت داده شده."""

    theme = build_theme(mode)
    apply_theme(app, theme)
    return theme

