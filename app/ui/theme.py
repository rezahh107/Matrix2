"""مدیریت تم سبک با توکن‌های مرکزی برای UI PySide6.

این ماژول یک تم روشن و مینیمال را برای رابط انگلیسی‌محور تعریف می‌کند،
رنگ‌ها، تایپوگرافی و فواصل را در یک نقطه متمرکز می‌سازد و توابع کمکی
برای اعمال فونت، سایهٔ کارت و انیمیشن سبک Hover دکمه‌ها فراهم می‌کند.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging

from PySide6.QtCore import QEasingCurve, QObject, QPropertyAnimation, Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication, QPushButton, QWidget

from app.ui.fonts import create_app_font
from app.ui.i18n import Language
from app.ui.effects import SafeDropShadowEffect

__all__ = [
    "BASE_FONT_PT",
    "ThemeColors",
    "ThemeTypography",
    "Theme",
    "apply_layout_direction",
    "apply_global_font",
    "apply_palette",
    "apply_theme",
    "apply_card_shadow",
    "setup_button_hover_animation",
    "build_theme",
    "apply_theme_mode",
]


# اندازهٔ پایهٔ فونت برنامه (بدنه): ۹ پوینت.
BASE_FONT_PT = 9


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThemeColors:
    """تعریف توکن‌های رنگی اصلی تم.

    مثال:
        >>> colors = ThemeColors()
        >>> colors.primary
        '#2563eb'
    """

    background: str = "#f5f5f7"
    card: str = "#ffffff"
    text: str = "#111827"
    text_muted: str = "#6b7280"
    primary: str = "#2563eb"
    success: str = "#16a34a"
    warning: str = "#f59e0b"
    error: str = "#dc2626"
    log_background: str = "#f6f7fb"
    log_foreground: str = "#1f2937"
    log_border: str = "#d5d9e3"
    log_success: str = "#15803d"
    log_warning: str = "#b45309"
    log_error: str = "#b91c1c"
    border: str = "#d1d5db"
    surface_alt: str = "#f3f4f6"


@dataclass(frozen=True)
class ThemeTypography:
    """تعریف مقیاس تایپوگرافی انگلیسی با fallback فارسی.

    مثال:
        >>> typo = ThemeTypography()
        >>> typo.font_fa_stack.split(',')[0]
        'Vazirmatn'
    """

    font_fa_stack: str = "Vazirmatn, Vazir, IRANSansX, Tahoma, sans-serif"
    font_en_stack: str = "Segoe UI, system-ui, sans-serif"
    title_size: int = 13
    card_title_size: int = 11
    body_size: int = BASE_FONT_PT


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
    mode: str = "light"

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
    def success_soft(self) -> QColor:
        """نسخهٔ ملایم رنگ موفقیت برای هایلایت لاگ."""

        base = QColor(self.colors.success).darker(110)
        base.setAlpha(90)
        return base

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
        return QColor(self.colors.log_border)

    @property
    def log_text(self) -> QColor:
        return QColor(self.colors.log_foreground)


def apply_global_font(app: QApplication) -> None:
    """اعمال فونت پیش‌فرض برنامه بر اساس وزیر یا تاهوما."""

    app.setFont(create_app_font(point_size=BASE_FONT_PT))


def apply_palette(app: QApplication, theme: Theme) -> None:
    """تنظیم پالت روشن هماهنگ با توکن‌های تم."""

    palette = _create_palette_from_theme(theme)
    app.setPalette(palette)


def _create_palette_from_theme(theme: Theme) -> QPalette:
    """ساخت پالت هماهنگ با توکن‌های تم (روشن یا تیره)."""

    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window, theme.window)
    palette.setColor(QPalette.ColorRole.Base, theme.card)
    palette.setColor(QPalette.ColorRole.AlternateBase, theme.surface_alt)
    palette.setColor(QPalette.ColorRole.ToolTipBase, theme.card)
    palette.setColor(QPalette.ColorRole.ToolTipText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Text, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Button, theme.card)
    palette.setColor(QPalette.ColorRole.ButtonText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.WindowText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Highlight, theme.accent)
    palette.setColor(QPalette.ColorRole.HighlightedText, theme.text_primary)
    palette.setColor(QPalette.ColorRole.Link, theme.accent)
    palette.setColor(QPalette.ColorRole.BrightText, theme.error)
    return palette


def apply_theme(app: QApplication, theme: Theme | str | None = None) -> Theme:
    """اعمال تم روشن/تیره صرفاً با QPalette و سبک Fusion.

    این تابع استایل شیت سراسری را پاک کرده و بر اساس حالت درخواستی
    پالت مناسب را روی برنامه اعمال می‌کند. ورودی می‌تواند نمونهٔ ``Theme``
    یا نام تم (``"light"``/``"dark"``) باشد و امضای عمومی تابع حفظ شده است.
    """

    app.setStyle("Fusion")
    apply_global_font(app)
    app.setStyleSheet("")

    if isinstance(theme, Theme):
        resolved_theme = theme
    elif isinstance(theme, str):
        resolved_theme = build_theme(theme)
    else:
        resolved_theme = build_theme("light")

    palette = _create_palette_from_theme(resolved_theme)

    app.setPalette(palette)
    return resolved_theme


def apply_layout_direction(app: QApplication, language: Language | str) -> None:
    """تنظیم جهت چیدمان اپلیکیشن بر اساس زبان."""

    lang_enum = language if isinstance(language, Language) else Language.from_code(language)
    if lang_enum is Language.FA:
        app.setLayoutDirection(Qt.RightToLeft)
    else:
        app.setLayoutDirection(Qt.LeftToRight)


def apply_card_shadow(widget: QWidget) -> None:
    """افزودن سایهٔ نرم به کارت‌ها با Qt.

    پارامترها:
        widget: ویجتی که باید سایه بگیرد.
    """

    shadow = SafeDropShadowEffect(
        f"card_shadow[{widget.objectName() or widget.__class__.__name__}]",
        widget,
    )
    shadow.setBlurRadius(24)
    shadow.setOffset(0, 10)
    shadow.setColor(QColor(0, 0, 0, 35))
    widget.setGraphicsEffect(shadow)
    LOGGER.debug(
        "card_shadow installed | widget=%s effect=%s blur=%s offset=%s",
        widget,
        hex(id(shadow)),
        shadow.blurRadius(),
        shadow.offset(),
    )


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
    """ساخت تم بر پایهٔ حالت روشن یا تیره با توکن‌های هماهنگ."""

    normalized = "dark" if (mode or "").lower() == "dark" else "light"
    if normalized == "dark":
        colors = ThemeColors(
            background="#202020",
            card="#2a2a2a",
            text="#f2f2f2",
            text_muted="#c4c4c4",
            primary="#0078d7",
            success="#16a34a",
            warning="#f59e0b",
            error="#dc2626",
            log_background="#171717",
            log_foreground="#e5e7eb",
            log_border="#0f172a",
            log_success="#22c55e",
            log_warning="#f59e0b",
            log_error="#f87171",
            border="#3a3a3a",
            surface_alt="#353535",
        )
    else:
        colors = ThemeColors()

    return Theme(colors=colors, typography=ThemeTypography(), mode=normalized)


def apply_theme_mode(app: QApplication, mode: str | None = None) -> Theme:
    """اعمال تم بر اساس حالت درخواستی."""

    return apply_theme(app, build_theme(mode or "light"))
