"""توابع کمکی سبک برای نرمال‌سازی فارسی و رشته‌های عددنما.

این ماژول در Core استفاده می‌شود و هیچ وابستگی خارجی ندارد. تمرکز اصلی:

* یکسان‌سازی کاراکترهای فارسی/عربی و فاصله‌ها برای مقایسهٔ پایدار.
* تبدیل رشته‌های عددی (با صفر پیشرو یا ارقام فارسی) به فرم قابل sort.

مثال ساده::

    >>> normalize_fa("كريم\u200cیاسر ١٢۳")
    'کریم یاسر 123'
    >>> to_numlike_str("٠٠۷")
    '7'
"""
from __future__ import annotations

import re
from typing import Any

__all__ = ["normalize_fa", "to_numlike_str"]

_DIGITS_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
_SPACE_RE = re.compile(r"\s+")


def normalize_fa(value: Any) -> str:
    """نرمال‌سازی سبک فارسی؛ فاصله‌ها، ارقام و حروف عربی را یکدست می‌کند.

    Args:
        value: مقدار ورودی (هر نوع قابل رشته‌سازی).

    Returns:
        str: رشتهٔ نرمال‌شده. اگر مقدار تهی یا نامعتبر باشد، رشتهٔ تهی برمی‌گردد.
    """

    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.translate(_DIGITS_MAP)
    text = text.replace("\u200c", " ")  # نیم‌فاصله → فاصله
    text = text.replace("ي", "ی").replace("ك", "ک")
    text = _SPACE_RE.sub(" ", text)
    return text


def to_numlike_str(value: Any) -> str:
    """تبدیل رشته‌های عددنما به فرم بدون صفر پیشرو برای sort.

    اگر مقدار شامل حروف باشد، همان مقدار نرمال‌شده (trim شده) بازگردانده می‌شود.
    در غیر این صورت، صفرهای پیشرو حذف می‌شود تا مقایسهٔ عددی ساده شود.
    """

    text = normalize_fa(value)
    if not text:
        return ""
    if not text.isdigit():
        return text
    return str(int(text))
