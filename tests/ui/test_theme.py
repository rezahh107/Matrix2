"""تست‌های مربوط به تم و پالت برنامه."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QApplication

from app.ui import theme


def test_apply_theme_switches_between_light_and_dark_palettes() -> None:
    app = QApplication.instance() or QApplication([])

    light_theme = theme.apply_theme(app, "light")
    light_window = app.palette().color(QPalette.ColorRole.Window)
    light_base = app.palette().color(QPalette.ColorRole.Base)
    light_highlight = app.palette().color(QPalette.ColorRole.Highlight)

    dark_theme = theme.apply_theme(app, "dark")
    dark_window = app.palette().color(QPalette.ColorRole.Window)
    dark_base = app.palette().color(QPalette.ColorRole.Base)
    dark_highlight = app.palette().color(QPalette.ColorRole.Highlight)

    assert light_theme.mode == "light"
    assert dark_theme.mode == "dark"
    assert dark_window != light_window
    assert dark_window.value() < light_window.value()
    assert app.styleSheet() == ""

    assert dark_window == dark_theme.window
    assert dark_base == dark_theme.card
    assert dark_highlight == dark_theme.accent

    assert light_window == light_theme.window
    assert light_base == light_theme.card
    assert light_highlight == light_theme.accent


def test_apply_theme_is_idempotent_and_resets_stylesheet() -> None:
    app = QApplication.instance() or QApplication([])
    app.setStyleSheet("QWidget { background: red; }")

    first_palette_theme = theme.apply_theme(app, "dark")
    first_window_color = app.palette().color(QPalette.ColorRole.Window)

    second_palette_theme = theme.apply_theme(app, "dark")
    second_window_color = app.palette().color(QPalette.ColorRole.Window)

    assert first_palette_theme.mode == "dark"
    assert second_palette_theme.mode == "dark"
    assert first_window_color == second_window_color
    assert app.styleSheet() == ""
