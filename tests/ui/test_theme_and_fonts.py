"""تست یکپارچه برای تم، جهت چیدمان و فونت وزیر."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QColor
from PySide6.QtWidgets import QApplication, QLabel

from app.ui import fonts, theme
from app.ui.i18n import Language
from app.ui.fonts import create_app_font, resolve_vazir_family_name


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


def test_create_app_font_sets_bold_weight_for_vazir(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = QFont("Vazir", 11)
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: fake)

    font = create_app_font()

    assert font.weight() == QFont.Weight.Bold


def test_create_app_font_sets_bold_weight_for_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fonts, "load_vazir_font", lambda point_size=None: None)

    font = create_app_font()

    assert font.weight() == QFont.Weight.Bold


def test_resolve_vazir_family_prefers_vazirmatn(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeDB:
        def __init__(self, families: list[str]):
            self._families = families

        def families(self) -> list[str]:
            return self._families

    db = _FakeDB(["Tahoma", "Vazir", "Vazirmatn", "Vazir Code"])
    family = resolve_vazir_family_name(db)
    assert family == "Vazirmatn"


def test_apply_global_font_sets_qapplication_font(qapp: QApplication, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    fonts_dir = tmp_path / "fonts"
    monkeypatch.setattr(fonts, "FONTS_DIR", fonts_dir)
    monkeypatch.setattr(fonts, "_windows_candidates", lambda: [])

    # نصب فونت و اعمال تم روی اپلیکیشن
    fonts.ensure_vazir_local_fonts()
    fonts._install_fonts_from_directory(fonts_dir)
    theme.apply_global_font(qapp)

    app_font = qapp.font()
    assert app_font.family().casefold().startswith(("vazir", "vazirmatn"))
    assert app_font.pointSize() == fonts.DEFAULT_POINT_SIZE
    assert app_font.styleStrategy() & QFont.StyleStrategy.PreferAntialias
    assert app_font.styleStrategy() & QFont.StyleStrategy.PreferQuality
    assert app_font.styleHint() == QFont.StyleHint.SansSerif
    assert app_font.hintingPreference() == QFont.HintingPreference.PreferFullHinting
    assert app_font.kerning()


def test_widgets_inherit_global_font(qapp: QApplication, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    fonts_dir = tmp_path / "fonts"
    monkeypatch.setattr(fonts, "FONTS_DIR", fonts_dir)
    monkeypatch.setattr(fonts, "_windows_candidates", lambda: [])

    fonts.ensure_vazir_local_fonts()
    fonts._install_fonts_from_directory(fonts_dir)
    theme.apply_global_font(qapp)

    label = QLabel("sample")
    assert label.font().family() == qapp.font().family()
    assert label.font().pointSize() == fonts.DEFAULT_POINT_SIZE


def test_layout_direction_for_languages(qapp: QApplication) -> None:
    theme.apply_layout_direction(qapp, Language.FA)
    assert qapp.layoutDirection() == Qt.RightToLeft

    theme.apply_layout_direction(qapp, Language.EN)
    assert qapp.layoutDirection() == Qt.LeftToRight


def test_light_theme_log_background_is_light() -> None:
    light_theme = theme.build_theme("light")
    dark_theme = theme.build_theme("dark")

    light_luminance = QColor(light_theme.colors.log_background).lightness()
    dark_luminance = QColor(dark_theme.colors.log_background).lightness()

    assert light_luminance > 200
    assert dark_luminance < light_luminance
