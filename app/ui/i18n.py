"""کمک‌یار زبان رابط کاربری با enum شفاف و توابع کمکی.

این ماژول یک منبع واحد برای تعیین زبان (فارسی/انگلیسی) ارائه می‌دهد
تا مدیریت جهت و ترجمه در UI هماهنگ بماند.
"""

from __future__ import annotations

from enum import Enum

__all__ = ["Language"]


class Language(str, Enum):
    """زبان رابط کاربری به‌صورت enum خوانا."""

    EN = "en"
    FA = "fa"

    @classmethod
    def from_code(cls, code: str | None) -> "Language":
        """تبدیل کد زبان (fa/en) به enum با پیش‌فرض امن."""

        normalized = (code or "en").strip().lower()
        if normalized.startswith("fa"):
            return cls.FA
        return cls.EN

    @property
    def code(self) -> str:
        """نمایش رشته‌ای استاندارد برای ذخیره‌سازی/ترجمه."""

        return self.value
