"""لایه ترجمهٔ سبک برای متن‌های UI.

این ماژول نگاشت کلید/متن را از فایل JSON بارگذاری می‌کند و با
fallback درون‌برنامه‌ای، متن مناسب زبان را بازمی‌گرداند.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict

from app.utils.path_utils import resource_path

__all__ = ["UiTranslator", "SUPPORTED_LANGUAGES", "DEFAULT_LANGUAGE"]

SUPPORTED_LANGUAGES = {"fa", "en"}
DEFAULT_LANGUAGE = "fa"


@dataclass(frozen=True)
class UiTranslator:
    """مترجم ساده برای متن‌های UI بر پایه فایل JSON.

    مثال::
        >>> t = UiTranslator("en")
        >>> t.text("status.ready", "آماده")
        'Ready'
    """

    language: str

    def __post_init__(self) -> None:
        normalized = self.language if self.language in SUPPORTED_LANGUAGES else DEFAULT_LANGUAGE
        object.__setattr__(self, "_lang", normalized)
        object.__setattr__(self, "_messages", self._load_messages())

    def _load_messages(self) -> Dict[str, str]:
        """بارگذاری دیکشنری ترجمه از فایل JSON با fallback به مقدار پیش‌فرض."""

        payload_path = resource_path("resources", "translations", "ui_texts.json")
        default: Dict[str, Dict[str, str]] = {lang: {} for lang in SUPPORTED_LANGUAGES}
        if payload_path.exists():
            try:
                loaded = json.loads(payload_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    return loaded.get(self._lang, default.get(self._lang, {}))
            except (OSError, json.JSONDecodeError):
                return default.get(self._lang, {})
        return default.get(self._lang, {})

    def text(self, key: str, fallback: str = "") -> str:
        """برگرداندن متن ترجمه‌شده با fallback امن.

        Args:
            key: کلید ترجمه.
            fallback: متن پیش‌فرض در صورت نبود ترجمه.

        Returns:
            str: متن ترجمه‌شده یا fallback در صورت نبود.
        """

        return self._messages.get(key, fallback)
