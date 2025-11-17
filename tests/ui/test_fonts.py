"""تست‌های مقاوم برای ماژول فونت."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from app.ui import fonts


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_create_app_font_does_not_raise_without_full_hinting_preference(monkeypatch: pytest.MonkeyPatch) -> None:
    if hasattr(QFont, "HintingPreference"):
        monkeypatch.delattr(QFont.HintingPreference, "PreferFullHinting", raising=False)
    font = fonts.create_app_font(point_size=9)
    assert isinstance(font, QFont)


def test_create_app_font_sets_antialias_and_quality_flags() -> None:
    font = fonts.create_app_font(point_size=10)

    strategy = font.styleStrategy()
    assert strategy & QFont.StyleStrategy.PreferAntialias
    assert strategy & QFont.StyleStrategy.PreferQuality

    if hasattr(QFont, "HintingPreference") and hasattr(QFont.HintingPreference, "PreferFullHinting"):
        assert font.hintingPreference() == QFont.HintingPreference.PreferFullHinting


def test_create_app_font_uses_vazir_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    vazir = QFont("Vazir", 10)
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: vazir)

    font = fonts.create_app_font(point_size=10)

    assert font.family().lower().startswith("vazir")
    assert font.pointSize() == 10


def test_create_app_font_falls_back_when_vazir_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)

    called_with: list[str | None] = []

    def _fake_select(preferred: str | None) -> str:
        called_with.append(preferred)
        return "Tahoma"

    monkeypatch.setattr(fonts, "_select_fallback_family", _fake_select)

    font = fonts.create_app_font(point_size=9, fallback_family="Tahoma")

    assert called_with == ["Tahoma"]
    assert isinstance(font, QFont)
    assert font.pointSize() == 9


def test_apply_default_font_on_dummy_app(qapp: QApplication, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)

    font = fonts.apply_default_font(qapp, point_size=8, family_override="Tahoma")

    assert isinstance(font, QFont)
    assert qapp.font().family() == font.family()
