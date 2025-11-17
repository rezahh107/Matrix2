"""مدیریت تم سبک با توکن‌های مرکزی برای UI PySide6.

این ماژول یک تم روشن و مینیمال را برای رابط انگلیسی‌محور تعریف می‌کند،
رنگ‌ها، تایپوگرافی و فواصل را در یک نقطه متمرکز می‌سازد و توابع کمکی
برای اعمال فونت، سایهٔ کارت و انیمیشن سبک Hover دکمه‌ها فراهم می‌کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict

from PySide6.QtCore import QEasingCurve, QObject, QPropertyAnimation, Qt
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
    "apply_layout_direction",
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
    """تعریف مقیاس تایپوگرافی انگلیسی با fallback فارسی.

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
    app.setFont(_create_app_font())


def _create_app_font() -> QFont:
    """ایجاد فونت سراسری برنامه بر پایهٔ Tahoma 8pt.

    این فونت کوچک و خوانا بوده و در زبان‌های فارسی و انگلیسی رفتار باثباتی دارد.
    """

    font = QFont("Tahoma", 8)
    font.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)
    return font


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
    """خواندن QSS و جایگذاری ایمن توکن‌های تم.

    - فقط الگوهای {TOKEN} با حروف/عدد/underline جایگزین می‌شوند.
    - براکت‌های ساختاری CSS (مثلاً ``QWidget {``) دست‌نخورده می‌مانند.
    - در صورت وجود توکن ناشناخته، خطای واضح ایجاد می‌شود.
    """

    # NOTE: اعمال QSS در apply_theme فعلاً غیرفعال است؛ این تابع برای استفاده‌های
    # محلی/آتی نگه داشته شده است.
    qss_path = Path(__file__).with_name("styles.qss")
    qss = qss_path.read_text(encoding="utf-8")
    return _render_stylesheet(qss, _token_mapping(theme))


_TOKEN_PATTERN = re.compile(r"\{([A-Za-z0-9_]+)\}")


def _render_stylesheet(qss: str, mapping: Dict[str, str]) -> str:
    """جایگزینی امن توکن‌های `{TOKEN}` در QSS.

    مثال:
        >>> _render_stylesheet("QWidget { background: {background}; }", {"background": "#fff"})
        'QWidget { background: #fff; }'
    """

    placeholders = {match.group(1) for match in _TOKEN_PATTERN.finditer(qss)}
    missing = sorted(token for token in placeholders if token not in mapping)
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Unknown stylesheet tokens: {missing_str}")

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(mapping[key])

    return _TOKEN_PATTERN.sub(_replace, qss)


def _create_dark_palette() -> QPalette:
    """ساخت پالت تیره بر پایهٔ سبک Fusion."""

    palette = QPalette()
    window = QColor(32, 32, 32)
    base = QColor(24, 24, 24)
    alternate = QColor(40, 40, 40)
    accent = QColor(0, 120, 215)

    palette.setColor(QPalette.ColorRole.Window, window)
    palette.setColor(QPalette.ColorRole.Base, base)
    palette.setColor(QPalette.ColorRole.AlternateBase, alternate)
    palette.setColor(QPalette.ColorRole.ToolTipBase, base)
    palette.setColor(QPalette.ColorRole.ToolTipText, Qt.white)
    palette.setColor(QPalette.ColorRole.Text, Qt.white)
    palette.setColor(QPalette.ColorRole.Button, alternate)
    palette.setColor(QPalette.ColorRole.ButtonText, Qt.white)
    palette.setColor(QPalette.ColorRole.WindowText, Qt.white)
    palette.setColor(QPalette.ColorRole.Highlight, accent)
    palette.setColor(QPalette.ColorRole.HighlightedText, Qt.white)
    palette.setColor(QPalette.ColorRole.Link, accent)
    palette.setColor(QPalette.ColorRole.BrightText, Qt.red)
    return palette


def _is_dark_theme_candidate(theme: Theme | str | None) -> bool:
    """تشخیص تیره بودن تم از روی نام یا مشخصه‌های آن."""

    if isinstance(theme, Theme):
        return theme.mode.lower() == "dark" or "dark" in theme.colors.background.lower()
    if isinstance(theme, str):
        return "dark" in theme.lower()
    return False


def apply_theme(app: QApplication, theme: Theme | str | None = None) -> Theme:
    """اعمال تم روشن/تیره صرفاً با QPalette و سبک Fusion.

    این تابع استایل شیت سراسری را پاک کرده و بر اساس حالت درخواستی
    پالت مناسب را روی برنامه اعمال می‌کند. ورودی می‌تواند نمونهٔ ``Theme``
    یا نام تم (``"light"``/``"dark"``) باشد و امضای عمومی تابع حفظ شده است.
    """

    app.setStyle("Fusion")
    app.setFont(_create_app_font())
    app.setStyleSheet("")

    if isinstance(theme, Theme):
        resolved_theme = theme
    elif isinstance(theme, str):
        resolved_theme = build_theme(theme)
    else:
        resolved_theme = build_theme("light")

    is_dark = _is_dark_theme_candidate(resolved_theme)

    if is_dark:
        palette = _create_dark_palette()
    else:
        palette = app.style().standardPalette()

    app.setPalette(palette)
    return resolved_theme


def apply_layout_direction(app: QApplication, language_code: str) -> None:
    """تنظیم جهت چیدمان اپلیکیشن بر اساس زبان."""

    normalized = (language_code or "").lower()
    if normalized.startswith("fa"):
        app.setLayoutDirection(Qt.RightToLeft)
    else:
        app.setLayoutDirection(Qt.LeftToRight)


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
            border="#3a3a3a",
            surface_alt="#353535",
        )
    else:
        colors = ThemeColors()

    return Theme(colors=colors, typography=ThemeTypography(), mode=normalized)


def apply_theme_mode(app: QApplication, mode: str | None = None) -> Theme:
    """اعمال تم بر اساس حالت درخواستی."""

    return apply_theme(app, build_theme(mode or "light"))
