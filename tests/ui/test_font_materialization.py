"""تست استخراج فونت تعبیه‌شدهٔ وزیرمتن بدون وابستگی به PySide."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtGui", reason="PySide6 not available in test environment")

from app.ui import fonts
from PySide6.QtGui import QFont


def test_embedded_vazirmatn_materialized(tmp_path, monkeypatch):
    fonts_dir = tmp_path / "fonts"
    monkeypatch.setattr(fonts, "FONTS_DIR", fonts_dir)
    monkeypatch.setattr(fonts, "_windows_candidates", lambda: [])

    fonts.ensure_vazir_local_fonts()

    files = list(fonts_dir.glob("*.ttf"))
    assert files, "فونت تعبیه‌شده باید تولید شود"
    assert any(path.name.startswith("Vazirmatn") for path in files)
    assert all(path.stat().st_size > 0 for path in files)

    # فراخوانی مجدد باید idempotent باشد و فایل جدیدی اضافه نکند
    fonts.ensure_vazir_local_fonts()
    files_again = list(fonts_dir.glob("*.ttf"))
    assert files_again == files


def test_create_app_font_defaults_to_antialias_and_size(monkeypatch):
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)

    font = fonts.create_app_font()

    assert font.pointSize() == fonts.DEFAULT_POINT_SIZE
    assert font.styleStrategy() & QFont.StyleStrategy.PreferAntialias
    assert font.styleStrategy() & QFont.StyleStrategy.PreferFullHinting
    assert font.styleStrategy() & QFont.StyleStrategy.PreferQuality
