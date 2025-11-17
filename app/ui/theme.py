"""مدیر تم سبک برای پیاده‌سازی سبک Fluent/Windows 11."""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

OFFICE_FONT_FALLBACK = "'Segoe UI', 'Tahoma', 'Arial', 'Vazirmatn', 'Sans-Serif'"

__all__ = [
    "Typography",
    "ShadowSpec",
    "Theme",
    "build_light_theme",
    "build_dark_theme",
    "build_theme",
    "apply_theme_mode",
    "apply_theme",
    "relative_luminance",
]


@dataclass(frozen=True)
class Typography:
    """مقیاس تایپوگرافی هماهنگ با هر دو زبان فارسی و انگلیسی."""

    caption: int = 10
    body: int = 11
    body_strong: int = 12
    subtitle: int = 13
    title: int = 14
    headline: int = 16


@dataclass(frozen=True)
class ShadowSpec:
    """تعریف سایه برای کارت‌ها و کنترل‌های شناور."""

    color: QColor
    blur_radius: int = 24
    x_offset: int = 0
    y_offset: int = 8


@dataclass(frozen=True)
class Theme:
    """تعریف نقش‌های رنگی و ابعادی تم UI."""

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
    shadow_ambient: ShadowSpec
    shadow_hover: ShadowSpec
    radius_sm: int = 4
    radius: int = 8
    radius_lg: int = 12
    spacing_xs: int = 4
    spacing_sm: int = 8
    spacing_md: int = 12
    spacing_lg: int = 16
    spacing_xl: int = 24
    typography: Typography = Typography()
    font_family: str = OFFICE_FONT_FALLBACK

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

    window = QColor("#f6f8fb")
    base = QColor("#ffffff")
    surface_alt = QColor("#eef2f8")
    card = QColor("#ffffff")
    text = QColor("#0f172a")
    accent = QColor("#0078d4")
    complement = QColor("#00b7c3")

    border = _blend(text, window, 0.18)
    accent_soft = _blend(complement, window, 0.24)
    muted = _blend(text, window, 0.52)
    shadow_color = QColor(0, 0, 0, 35)

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
        shadow_ambient=ShadowSpec(color=shadow_color, blur_radius=32, y_offset=10),
        shadow_hover=ShadowSpec(color=shadow_color, blur_radius=36, y_offset=12),
    )


def build_dark_theme() -> Theme:
    """ساخت تم تیره با کنتراست بالا و رنگ مکمل برای هایلایت‌ها."""

    window = QColor("#0c1628")
    base = QColor("#111b2f")
    text = QColor("#e6edf5")
    accent = QColor("#4fb3ff")
    complement = QColor("#22d3ee")

    card = _blend(base, window, 0.28)
    border = _blend(text, window, 0.32)
    accent_soft = _blend(complement, window, 0.28)
    muted = _blend(text, window, 0.42)
    shadow_color = QColor(0, 0, 0, 120)

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
        shadow_ambient=ShadowSpec(color=shadow_color, blur_radius=32, y_offset=12),
        shadow_hover=ShadowSpec(color=shadow_color, blur_radius=40, y_offset=14),
    )


def build_theme(mode: str) -> Theme:
    """ساخت تم بر اساس نام حالت روشن/تیره."""

    normalized = (mode or "").strip().lower()
    if normalized == "dark":
        return build_dark_theme()
    return build_light_theme()


def _stylesheet(theme: Theme) -> str:
    """تولید StyleSheet یکپارچه برای کنترل‌های کلیدی."""

    colors = {
        "text": theme.text_primary.name(),
        "muted": theme.text_muted.name(),
        "window": theme.window.name(),
        "surface": theme.surface.name(),
        "surface_alt": theme.surface_alt.name(),
        "card": theme.card.name(),
        "accent": theme.accent.name(),
        "accent_soft": theme.accent_soft.name(),
        "border": theme.border.name(),
        "success": theme.success.name(),
        "warning": theme.warning.name(),
        "error": theme.error.name(),
        "log_bg": theme.log_bg.name(),
        "log_border": theme.log_border.name(),
    }

    return f"""
        QWidget {{
            color: {colors['text']};
            background-color: {colors['window']};
            font-size: {theme.typography.body}pt;
            font-family: {theme.font_family};
            letter-spacing: 0.1px;
        }}
        QMainWindow, QDialog {{
            background-color: {colors['window']};
        }}
        QPushButton, QToolButton, QComboBox {{
            transition: all 0.2s ease-in-out;
        }}
        QGroupBox {{
            border: 1px solid {colors['border']};
            border-top: 2px solid {colors['accent_soft']};
            border-radius: {theme.radius}px;
            margin-top: 8px;
            background: {colors['surface']};
            padding: {theme.spacing_sm}px {theme.spacing_md}px;
        }}
        QGroupBox::title {{
            subcontrol-origin: margin;
            left: {theme.spacing_sm}px;
            padding: 0 {theme.spacing_sm}px {theme.spacing_xs}px {theme.spacing_sm}px;
            color: {colors['text']};
            font-weight: 600;
            font-size: {theme.typography.body_strong}pt;
        }}
        QTabWidget::pane {{
            border: 1px solid {colors['border']};
            border-radius: {theme.radius_lg}px;
            background: {colors['surface']};
            padding: {theme.spacing_sm}px;
        }}
        QTabBar::tab {{
            background: {colors['surface_alt']};
            color: {colors['muted']};
            padding: 10px 20px;
            margin-right: 4px;
            border: 1px solid transparent;
            border-top-left-radius: {theme.radius_lg}px;
            border-top-right-radius: {theme.radius_lg}px;
            font-weight: 600;
        }}
        QTabBar::tab:selected {{
            background: {colors['surface']};
            color: {colors['text']};
            border: 1px solid {colors['border']};
            border-bottom: 1px solid {colors['surface']};
        }}
        QTabBar::tab:hover {{
            background: {colors['surface']};
            color: {colors['text']};
        }}
        QTabWidget[busy="true"]::pane {{
            border-color: {colors['accent']};
        }}
        QToolButton, QPushButton {{
            border: none;
            border-radius: {theme.radius_sm}px;
            padding: {theme.spacing_sm + 2}px {theme.spacing_lg + 2}px;
            min-height: 34px;
            background: {colors['accent']};
            color: #ffffff;
            font-weight: 600;
            font-size: {theme.typography.body_strong}pt;
        }}
        QPushButton:hover, QToolButton:hover {{
            background: {theme.accent.darker(110).name()};
        }}
        QPushButton:pressed, QToolButton:pressed {{
            background: {theme.accent.darker(120).name()};
            transform: scale(0.98);
        }}
        QPushButton:focus-visible, QToolButton:focus-visible, QComboBox:focus-visible, QLineEdit:focus-visible {{
            outline: 2px solid #0078D4;
            outline-offset: 2px;
            border-radius: 4px;
        }}
        *:focus {{
            outline: 2px solid #0078D4;
            outline-offset: 2px;
            border-radius: 4px;
        }}
        QPushButton#secondaryButton, QPushButton[objectName="secondaryButton"], QToolButton#secondaryButton {{
            background: transparent;
            border: 1px solid {colors['border']};
            color: {colors['text']};
        }}
        QPushButton#secondaryButton:hover, QPushButton[objectName="secondaryButton"]:hover, QToolButton#secondaryButton:hover {{
            background: {colors['surface_alt']};
        }}
        QPushButton:disabled, QToolButton:disabled, QComboBox:disabled {{
            opacity: 0.4;
            background: {colors['surface_alt']};
            color: {colors['muted']};
            border-color: {colors['border']};
        }}
        QComboBox {{
            border: 1px solid {colors['border']};
            border-radius: {theme.radius_sm}px;
            padding: {theme.spacing_sm}px {theme.spacing_md}px;
            background: {colors['surface_alt']};
            color: {colors['text']};
        }}
        QComboBox::drop-down {{
            border: none;
        }}
        QComboBox QAbstractItemView {{
            background: {colors['surface']};
            color: {colors['text']};
            selection-background-color: {colors['accent_soft']};
            selection-color: {colors['text']};
        }}
        QLineEdit, QTextEdit {{
            border: 1px solid {colors['border']};
            border-radius: {theme.radius_sm}px;
            padding: 8px 12px;
            background: {colors['surface']};
            transition: all 0.2s ease-in-out;
        }}
        QLineEdit:focus, QTextEdit:focus {{
            border: 2px solid {colors['accent']};
            background: {colors['surface_alt']};
        }}
        QTextEdit[placeholderText]:empty::before {{
            content: attr(placeholderText);
            color: {colors['muted']};
            text-align: center;
        }}
        QProgressBar {{
            border: 1px solid {colors['border']};
            border-radius: 6px;
            background: {colors['surface_alt']};
            text-align: center;
            height: 8px;
        }}
        QProgressBar::chunk {{
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #0078D4, stop:1 #00BCF2);
            border-radius: 5px;
        }}
        QProgressBar[busy="true"]::chunk {{
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 rgba(0,120,212,0.35), stop:1 rgba(0,188,242,0.75));
        }}
        FilePicker QLineEdit {{
            border: 1px solid {colors['border']};
            border-radius: 4px;
            padding: 8px 12px;
        }}
        FilePicker QLineEdit:focus {{
            border: 2px solid #0078D4;
            outline: none;
            background: {colors['surface_alt']};
        }}
        #fileIconLabel {{
            color: {colors['muted']};
        }}
        #dashboardCard {{
            background: {colors['card']};
            border: 1px solid {colors['border']};
            border-radius: {theme.radius}px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        DashboardCard:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
        }}
        #dashboardCardTitle {{
            color: {colors['text']};
            font-weight: 700;
            font-size: {theme.typography.subtitle}pt;
        }}
        #dashboardCardDescription, #dashboardChecklistItem {{
            color: {colors['muted']};
            font-size: {theme.typography.body}pt;
        }}
        #fileStatusRow {{
            border-bottom: 1px solid {colors['border']};
        }}
        #dashboardShortcut {{
            background: {colors['surface_alt']};
            border: 1px solid {colors['border']};
            border-radius: {theme.radius - 2}px;
            padding: 6px 10px;
        }}
        #dashboardShortcut:hover {{
            background: {colors['surface']};
        }}
        #logPanel {{
            background: {colors['log_bg']};
            border: 1px solid {colors['log_border']};
            border-radius: {theme.radius}px;
        }}
        QToolBar {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 {colors['surface']}, stop:1 {colors['surface_alt']});
            border: 1px solid {colors['border']};
            border-radius: {theme.radius}px;
            padding: {theme.spacing_sm}px {theme.spacing_md}px;
            spacing: {theme.spacing_md}px;
            min-height: 42px;
        }}
        QToolBar::separator {{
            background: {colors['border']};
            width: 1px;
            margin: 0 6px;
        }}
        QStatusBar {{
            background: {colors['surface']};
            border-top: 1px solid {colors['border']};
            padding: {theme.spacing_xs}px {theme.spacing_md}px;
        }}
        QStatusBar::item {{
            border-right: 1px solid {colors['border']};
            padding: 0 8px;
        }}
        QMessageBox {{
            background: {colors['surface']};
            border: 1px solid {colors['border']};
            border-radius: {theme.radius}px;
        }}
        QMessageBox QLabel {{
            color: {colors['text']};
        }}
        QMessageBox QPushButton {{
            min-width: 88px;
        }}
        QSplitter::handle {{
            background: {colors['border']};
            width: 5px;
            margin: 2px 0;
        }}
        QSplitter::handle:hover {{
            background: {colors['accent']};
        }}
        QScrollArea {{
            background: transparent;
            border: none;
        }}
        QLabel#labelStageBadge {{
            font-weight: 700;
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
