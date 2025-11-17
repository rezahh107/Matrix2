"""تست‌های واحد برای helper فونت وزیر."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "PySide6.QtWidgets", reason="PySide6 GUI stack نیاز به libGL دارد"
)
from PySide6.QtWidgets import QApplication

from app.ui import fonts
from app.ui.fonts import create_app_font


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_ensure_vazir_creates_folder(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fonts, "FONTS_DIR", tmp_path)
    fonts.ensure_vazir_local_fonts()
    assert fonts.FONTS_DIR.exists()


def test_create_app_font_returns_font(monkeypatch: pytest.MonkeyPatch, qapp: QApplication) -> None:
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)
    font = create_app_font()
    assert font.family().lower().startswith(fonts.FALLBACK_FAMILY.lower())
