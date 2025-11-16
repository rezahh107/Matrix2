"""مدیریت فونت‌های رابط کاربری با پشتیبانی از فونت تاهوما و بستهٔ وزیر."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Iterable, List, Tuple, TYPE_CHECKING

from app.core.policy_loader import get_policy
from app.ui.assets.font_data_vazirmatn import VAZIRMATN_REGULAR_TTF_BASE64

if TYPE_CHECKING:  # pragma: no cover
    from PySide6.QtGui import QFont
    from PySide6.QtWidgets import QApplication

__all__ = [
    "prepare_default_font",
    "apply_default_font",
]

DEFAULT_UI_POINT_SIZE = 11

LOGGER = logging.getLogger(__name__)

_BUNDLED_FONT_PAYLOADS: dict[str, str] = {
    "vazir": VAZIRMATN_REGULAR_TTF_BASE64,
    "vazirmatn": VAZIRMATN_REGULAR_TTF_BASE64,
}
_SYSTEM_FALLBACKS: Tuple[str, ...] = (
    "Tahoma",
    "Segoe UI",
    "Vazirmatn",
    "Vazir",
    "Arial",
    "Verdana",
    "Microsoft Sans Serif",
)


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    """حذف مقادیر تکراری با حفظ ترتیب اولیه.

    مثال::

        >>> _dedupe_preserve_order(["a", "B", "a", ""])  # doctest: +SKIP
        ['a', 'B']
    """

    seen: set[str] = set()
    result: List[str] = []
    for item in items:
        text = (item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _iter_bundled_font_payloads(preferred_name: str | None) -> List[str]:
    """بازگرداندن لیست داده‌های base64 فونت‌های باندل‌شده."""

    ordered: List[str] = []
    if preferred_name:
        payload = _BUNDLED_FONT_PAYLOADS.get(preferred_name.casefold())
        if payload:
            ordered.append(payload)
    for payload in _BUNDLED_FONT_PAYLOADS.values():
        if payload not in ordered:
            ordered.append(payload)
    return ordered


def _install_bundled_fonts(preferred_name: str | None) -> List[str]:
    """نصب فونت‌های موجود در بسته و بازگرداندن خانواده‌های ثبت‌شده."""

    from PySide6.QtGui import QFontDatabase

    families: List[str] = []
    from PySide6.QtCore import QByteArray

    for payload in _iter_bundled_font_payloads(preferred_name):
        byte_array = QByteArray.fromBase64(payload.encode("ascii"))
        if byte_array.isEmpty():
            LOGGER.warning("دادهٔ base64 فونت خالی است و نادیده گرفته شد.")
            continue
        font_id = QFontDatabase.addApplicationFontFromData(byte_array)
        if font_id == -1:
            LOGGER.warning("بارگذاری فونت داخلی با شکست روبه‌رو شد (base64).")
            continue
        families.extend(QFontDatabase.applicationFontFamilies(font_id))
    return _dedupe_preserve_order(families)


@lru_cache(maxsize=1)
def _policy_font_name() -> str:
    """خواندن نام فونت از Policy با تضمین خطای کنترل‌شده."""

    try:
        name = (get_policy().excel.font_name or "Vazirmatn").strip()
    except Exception as exc:  # pragma: no cover - خطاهای محیطی Policy
        LOGGER.warning("خواندن فونت از Policy شکست خورد؛ استفاده از پیش‌فرض.", exc_info=exc)
        return "Vazirmatn"
    return name or "Vazirmatn"


def prepare_default_font(*, point_size: int = DEFAULT_UI_POINT_SIZE) -> "QFont":
    """ساخت شیء فونت پیش‌فرض با نصب فونت‌های لازم (وزیرمتن/تاهوما).

    مثال::

        >>> font = prepare_default_font(point_size=11)  # doctest: +SKIP
        >>> font.family()  # doctest: +SKIP
        'Vazirmatn'
    """

    from PySide6.QtGui import QFont, QFontDatabase

    policy_font = _policy_font_name()
    installed_families = _install_bundled_fonts(policy_font)
    candidates = _dedupe_preserve_order(
        [policy_font, *installed_families, *_SYSTEM_FALLBACKS]
    )

    database = QFontDatabase()
    for family in candidates:
        if database.hasFamily(family):
            font = QFont(family)
            font.setPointSize(point_size)
            font.setStyleHint(QFont.StyleHint.AnyStyle)
            font.setStyleStrategy(QFont.StyleStrategy.PreferDefault)
            LOGGER.info("فونت رابط کاربری انتخاب شد: %s", family)
            return font

    fallback = QFont()
    fallback.setPointSize(point_size)
    LOGGER.warning("هیچ فونت مناسبی یافت نشد؛ استفاده از پیش‌فرض سیستم.")
    return fallback


def apply_default_font(
    app: "QApplication", *, point_size: int = DEFAULT_UI_POINT_SIZE
) -> "QFont":
    """نصب و اعمال فونت پیش‌فرض (تاهوما یا وزیر) بر روی QApplication.

    مثال::

        >>> app = QApplication([])  # doctest: +SKIP
        >>> font = apply_default_font(app, point_size=11)  # doctest: +SKIP
    """

    font = prepare_default_font(point_size=point_size)
    app.setFont(font)
    return font
