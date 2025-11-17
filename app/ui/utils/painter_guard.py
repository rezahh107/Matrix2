"""بررسی سادهٔ فعال بودن نقاش برای جلوگیری از هشدارهای Qt.

این نگهبان سبک به شما کمک می‌کند استفادهٔ نادرست از ``QPainter`` در
``paintEvent`` یا ``QGraphicsEffect.draw`` را زود تشخیص دهید. به‌صورت پیش‌فرض
در حالت لاگ اخطار عمل می‌کند و در صورت نیاز می‌توان آن را با متغیر محیطی
``MATRIX_DEBUG_PAINTER`` یا تنظیم مستقیم ``painter_guard_enabled`` سخت‌گیرانه
کرد تا AssertionError بدهد.
"""

from __future__ import annotations

import logging
import os
from typing import Final

from PySide6.QtGui import QPainter

LOGGER = logging.getLogger(__name__)

_painter_guard_env = os.getenv("MATRIX_DEBUG_PAINTER", "").lower() in {"1", "true", "yes"}
painter_guard_enabled: bool = _painter_guard_env
"""فعال‌سازی حالت سخت‌گیر برای assert؛ با متغیر محیطی قابل تنظیم است."""


def assert_painter_active(painter: QPainter, context: str, *, strict: bool | None = None) -> bool:
    """بررسی فعال بودن نقاش و لاگ/Assert در صورت نیاز.

    پارامترها:
        painter: نمونهٔ نقاش که باید فعال باشد.
        context: توضیح کوتاه برای پیام خطا.
        strict: اگر مقدار‌دهی شود، بر رفتار پیش‌فرض ``painter_guard_enabled``
            اولویت دارد. در صورت ``True``، AssertionError پرتاب می‌شود.

    بازگشت:
        ``True`` اگر نقاش فعال باشد، در غیر این‌صورت ``False`` و یک پیام
        اخطار یا استثناء ایجاد می‌شود.
    """

    active = painter.isActive()
    if active:
        return True

    message = f"QPainter inactive during {context}"
    enforce_strict = painter_guard_enabled if strict is None else strict
    if enforce_strict:
        raise AssertionError(message)

    LOGGER.warning(message)
    return False


__all__: Final = ["assert_painter_active", "painter_guard_enabled"]
