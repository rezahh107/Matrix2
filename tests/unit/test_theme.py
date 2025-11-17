from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 GUI stack نیاز به libGL دارد")
from PySide6.QtWidgets import QApplication

from app.ui.theme import (
    apply_theme_mode,
    build_dark_theme,
    build_light_theme,
    relative_luminance,
)
from app.ui.texts import UiTranslator


@pytest.fixture(scope="module")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_light_theme_background_is_bright(qapp: QApplication) -> None:
    theme = build_light_theme()
    assert relative_luminance(theme.window) > 0.8
    assert relative_luminance(theme.card) > 0.85
    assert relative_luminance(theme.log_bg) > 0.85
    assert theme.accent.isValid()


def test_translator_fallback_returns_default() -> None:
    translator = UiTranslator("en")
    assert translator.text("nonexistent.key", "fallback") == "fallback"


def test_theme_roles_initialized(qapp: QApplication) -> None:
    theme = build_light_theme()
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


def test_dark_vs_light_theme_contrast() -> None:
    light = build_light_theme()
    dark = build_dark_theme()
    assert relative_luminance(light.window) - relative_luminance(dark.window) > 0.4
    assert relative_luminance(light.card) - relative_luminance(dark.card) > 0.3


def test_theme_switch_mode_round_trip(qapp: QApplication) -> None:
    first_light = apply_theme_mode(qapp, "light")
    dark = apply_theme_mode(qapp, "dark")
    second_light = apply_theme_mode(qapp, "light")

    assert relative_luminance(dark.window) < relative_luminance(first_light.window)
    assert second_light.window == first_light.window
    assert second_light.accent == first_light.accent
