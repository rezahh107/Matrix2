"""تست یکپارچه برای تم، جهت چیدمان و فونت وزیر."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from app.ui import fonts, theme
from app.ui.i18n import Language
from app.ui.fonts import create_app_font


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_create_app_font_uses_fallback_when_vazir_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)
    font = create_app_font()
    assert font.family().lower().startswith(fonts.FALLBACK_FAMILY.lower())


def test_create_app_font_prefers_vazir(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = QFont("Vazir", 11)
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: fake)
    font = create_app_font()
    assert font.family().lower().startswith("vazir")


def test_layout_direction_for_languages(qapp: QApplication) -> None:
    theme.apply_layout_direction(qapp, Language.FA)
    assert qapp.layoutDirection() == Qt.RightToLeft

    theme.apply_layout_direction(qapp, Language.EN)
    assert qapp.layoutDirection() == Qt.LeftToRight
