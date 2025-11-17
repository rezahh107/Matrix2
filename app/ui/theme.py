"""مدیریت تم سبک با توکن‌های مرکزی برای UI PySide6.

این ماژول یک تم سبک و مینیمال را برای رابط فارسی‌محور تعریف می‌کند،
رنگ‌ها، تایپوگرافی و فواصل را در یک نقطه متمرکز می‌سازد و توابع کمکی
برای اعمال فونت، سایهٔ کارت و انیمیشن سبک Hover دکمه‌ها فراهم می‌کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict

from PySide6.QtCore import QEasingCurve, QObject, QPropertyAnimation
from PySide6.QtGui import QColor, QFont, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QGraphicsDropShadowEffect,
    QPushButton,
    QWidget,
)

__all__ = [
    "ThemeColors",
    "ThemeTypography",
    "Theme",
    "apply_global_font",
    "apply_palette",
    "load_stylesheet",
    "apply_theme",
    "apply_card_shadow",
    "setup_button_hover_animation",
    "build_theme",
    "apply_theme_mode",
]


@dataclass(frozen=True)
class ThemeColors:
    """تعریف توکن‌های رنگی اصلی تم.

    مثال:
        >>> colors = ThemeColors()
        >>> colors.primary
        '#2563eb'
    """

    background: str = "#f5f6fa"
    card: str = "#ffffff"
    text: str = "#111827"
    text_muted: str = "#6b7280"
    primary: str = "#2563eb"
    success: str = "#16a34a"
    warning: str = "#f59e0b"
    error: str = "#dc2626"
    log_background: str = "#020617"
    border: str = "#e5e7eb"
    surface_alt: str = "#eef2f7"


@dataclass(frozen=True)
class ThemeTypography:
    """تعریف مقیاس تایپوگرافی فارسی و انگلیسی.

    مثال:
        >>> typo = ThemeTypography()
        >>> typo.font_fa_stack.split(',')[0]
        'Vazirmatn'
    """

    font_fa_stack: str = "Vazirmatn, IRANSansX, Tahoma, sans-serif"
    font_en_stack: str = "Segoe UI, system-ui, sans-serif"
    title_size: int = 16
    card_title_size: int = 14
    body_size: int = 12


@dataclass(frozen=True)
class Theme:
    """بستهٔ توکن‌های تم شامل رنگ، تایپوگرافی و فاصله.

    مثال:
        >>> theme = Theme()
        >>> theme.spacing_sm
        8
    """

    colors: ThemeColors = ThemeColors()
    typography: ThemeTypography = ThemeTypography()
    spacing_base: int = 8
    radius_sm: int = 6
    radius_md: int = 10
    radius_lg: int = 14

    @property
    def spacing_xs(self) -> int:
        return max(2, self.spacing_base // 2)

    @property
    def spacing_sm(self) -> int:
        return self.spacing_base

    @property
    def spacing_md(self) -> int:
        return int(self.spacing_base * 1.5)

    @property
    def spacing_lg(self) -> int:
        return self.spacing_base * 2

    @property
    def spacing_xl(self) -> int:
        return int(self.spacing_base * 3)

    @property
    def accent_soft(self) -> str:
        base = QColor(self.colors.primary)
        soft = QColor(base)
        soft.setAlphaF(0.1)
        return soft.name(QColor.HexArgb)

    # Backward-friendly names for legacy call sites
    @property
    def window(self) -> QColor:
        return QColor(self.colors.background)

    @property
    def surface(self) -> QColor:
        return QColor(self.colors.card)

    @property
    def surface_alt(self) -> QColor:
        return QColor(self.colors.surface_alt)

    @property
    def card(self) -> QColor:
        return QColor(self.colors.card)

    @property
    def accent(self) -> QColor:
        return QColor(self.colors.primary)

    @property
    def border(self) -> QColor:
        return QColor(self.colors.border)

    @property
    def text_primary(self) -> QColor:
        return QColor(self.colors.text)

    @property
    def text_muted(self) -> QColor:
        return QColor(self.colors.text_muted)

    @property
    def success(self) -> QColor:
        return QColor(self.colors.success)

    @property
    def warning(self) -> QColor:
        return QColor(self.colors.warning)

    @property
    def error(self) -> QColor:
        return QColor(self.colors.error)

    @property
    def log_bg(self) -> QColor:
        return QColor(self.colors.log_background)

    @property
    def log_border(self) -> QColor:
        return QColor("#0f172a")


def apply_global_font(app: QApplication, typography: ThemeTypography = ThemeTypography()) -> None:
    """اعمال فونت پیش‌فرض برنامه بر اساس پشتهٔ فارسی.

    پارامترها:
        app: نمونهٔ QApplication فعال.
        typography: تنظیمات تایپوگرافی شامل پشتهٔ فونت و اندازه.

    مثال:
        >>> app = QApplication.instance() or QApplication([])
        >>> apply_global_font(app)
    """

    font = QFont()
    font.setFamily(typography.font_fa_stack)
    font.setPointSize(typography.body_size)
    app.setFont(font)


def apply_palette(app: QApplication, theme: Theme) -> None:
    """تنظیم پالت روشن هماهنگ با توکن‌های تم."""

    palette = app.palette()
    palette.setColor(QPalette.ColorRole.Window, QColor(theme.colors.background))
    palette.setColor(QPalette.ColorRole.Base, QColor(theme.colors.card))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor(theme.colors.surface_alt))
    palette.setColor(QPalette.ColorRole.Text, QColor(theme.colors.text))
    palette.setColor(QPalette.ColorRole.Button, QColor(theme.colors.card))
    palette.setColor(QPalette.ColorRole.ButtonText, QColor(theme.colors.text))
    palette.setColor(QPalette.ColorRole.WindowText, QColor(theme.colors.text))
    palette.setColor(QPalette.ColorRole.Highlight, QColor(theme.colors.primary))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
    app.setPalette(palette)


def _token_mapping(theme: Theme) -> Dict[str, str]:
    return {
        "background": theme.colors.background,
        "card": theme.colors.card,
        "surface_alt": theme.colors.surface_alt,
        "text": theme.colors.text,
        "text_muted": theme.colors.text_muted,
        "primary": theme.colors.primary,
        "primary_soft": theme.accent_soft,
        "success": theme.colors.success,
        "warning": theme.colors.warning,
        "error": theme.colors.error,
        "log_background": theme.colors.log_background,
        "border": theme.colors.border,
        "font_fa": theme.typography.font_fa_stack,
        "font_en": theme.typography.font_en_stack,
        "title_size": str(theme.typography.title_size),
        "card_title_size": str(theme.typography.card_title_size),
        "body_size": str(theme.typography.body_size),
        "spacing_xs": str(theme.spacing_xs),
        "spacing_sm": str(theme.spacing_sm),
        "spacing_md": str(theme.spacing_md),
        "spacing_lg": str(theme.spacing_lg),
        "radius_sm": str(theme.radius_sm),
        "radius_md": str(theme.radius_md),
        "radius_lg": str(theme.radius_lg),
    }


def load_stylesheet(theme: Theme) -> str:
    """خواندن QSS و جایگذاری توکن‌های تم."""

    qss_path = Path(__file__).with_name("styles.qss")
    qss = qss_path.read_text(encoding="utf-8")
    return _inject_tokens(qss, _token_mapping(theme))


def _inject_tokens(qss: str, tokens: Dict[str, str]) -> str:
    """جایگذاری امن توکن‌ها بدون درگیر شدن با آکولادهای QSS.

    این تابع به‌جای ``str.format`` از جایگزینی ساده استفاده می‌کند تا آکولادهای
    بلاکی QSS (``{`` و ``}``) به‌اشتباه به‌عنوان placeholder تفسیر نشوند.

    پارامترها:
        qss: متن خام QSS.
        tokens: نگاشت کلید به مقدار برای جایگذاری.

    مثال:
        >>> sample = "QWidget { background: {background}; }"
        >>> _inject_tokens(sample, {"background": "#fff"})
        'QWidget { background: #fff; }'
    """

    for key, value in tokens.items():
        qss = qss.replace(f"{{{key}}}", value)

    unresolved = {
        match
        for match in re.findall(r"{([A-Za-z0-9_]+)}", qss)
        if match not in tokens
    }
    if unresolved:
        missing = ", ".join(sorted(unresolved))
        raise KeyError(f"unresolved stylesheet tokens: {missing}")

    return qss


def apply_theme(app: QApplication, theme: Theme | None = None) -> Theme:
    """اعمال تم پیش‌فرض روی برنامه و بارگذاری QSS.

    پارامترها:
        app: برنامه Qt فعال.
        theme: تم انتخابی؛ اگر None باشد تم پیش‌فرض روشن اعمال می‌شود.

    مثال:
        >>> app = QApplication.instance() or QApplication([])
        >>> apply_theme(app)
    """

    theme = theme or Theme()
    apply_global_font(app, theme.typography)
    apply_palette(app, theme)
    app.setStyleSheet(load_stylesheet(theme))
    return theme


def apply_card_shadow(widget: QWidget) -> None:
    """افزودن سایهٔ نرم به کارت‌ها با Qt.

    پارامترها:
        widget: ویجتی که باید سایه بگیرد.
    """

    shadow = QGraphicsDropShadowEffect(widget)
    shadow.setBlurRadius(24)
    shadow.setOffset(0, 10)
    shadow.setColor(QColor(0, 0, 0, 35))
    widget.setGraphicsEffect(shadow)


class _HoverAnimationFilter(QObject):
    """فیلتر ساده برای انیمیشن Hover دکمه."""

    def __init__(self, button: QPushButton, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._button = button
        self._animation = QPropertyAnimation(button, b"windowOpacity", self)
        self._animation.setEasingCurve(QEasingCurve.InOutQuad)
        self._animation.setDuration(120)

    def eventFilter(self, obj: QObject, event) -> bool:  # type: ignore[override]
        from PySide6.QtCore import QEvent

        if obj is self._button:
            if event.type() == QEvent.Enter:
                self._fade_to(0.94)
            elif event.type() == QEvent.Leave:
                self._fade_to(1.0)
        return super().eventFilter(obj, event)

    def _fade_to(self, value: float) -> None:
        self._animation.stop()
        self._animation.setStartValue(self._button.windowOpacity())
        self._animation.setEndValue(value)
        self._animation.start()


def setup_button_hover_animation(button: QPushButton) -> None:
    """نصب انیمیشن Hover سبک برای دکمه‌ها.

    پارامترها:
        button: دکمه هدف.
    """

    filter_ = _HoverAnimationFilter(button, button)
    button.installEventFilter(filter_)
    button.setProperty("_hover_filter", filter_)


def build_theme(mode: str | None = None) -> Theme:
    """ساخت تم بر اساس حالت مورد نظر (فعلاً فقط روشن)."""

    return Theme()


def apply_theme_mode(app: QApplication, mode: str | None = None) -> Theme:
    """اعمال تم بر اساس حالت درخواستی."""

    return apply_theme(app, build_theme(mode or "light"))
