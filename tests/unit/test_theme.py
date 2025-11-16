from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 GUI stack نیاز به libGL دارد")
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication

from app.ui.theme import build_system_light_theme, relative_luminance
from app.ui.texts import UiTranslator


@pytest.fixture(scope="module")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_build_system_light_theme_has_contrast(qapp: QApplication) -> None:
    theme = build_system_light_theme()
    diff = abs(relative_luminance(theme.text_primary) - relative_luminance(theme.surface))
    assert diff > 0.05
    assert theme.accent.isValid()


def test_translator_fallback_returns_default() -> None:
    translator = UiTranslator("en")
    assert translator.text("nonexistent.key", "fallback") == "fallback"


def test_theme_roles_initialized(qapp: QApplication) -> None:
    theme = build_system_light_theme()
    roles = [
        theme.window,
        theme.surface,
        theme.surface_alt,
        theme.card,
        theme.accent,
        theme.accent_soft,
        theme.border,
        theme.text_primary,
        theme.text_muted,
        theme.success,
        theme.warning,
        theme.error,
        theme.log_bg,
        theme.log_border,
    ]
    assert all(color.isValid() for color in roles)


def test_translation_fallbacks_for_languages() -> None:
    fa = UiTranslator("fa")
    en = UiTranslator("en")
    assert fa.text("dashboard.title", "عنوان") != ""
    assert en.text("dashboard.title", "Title") != ""
    assert en.text("missing.key", "Default") == "Default"
    fallback_lang = UiTranslator("de")
    assert fallback_lang.text("dashboard.title", "عنوان") != ""
