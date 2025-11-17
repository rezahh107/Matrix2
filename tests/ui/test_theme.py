"""تست‌های رندر ایمن استایل‌شیت تم."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtWidgets import QApplication

from app.ui import theme


def test_load_stylesheet_replaces_all_tokens() -> None:
    qss = theme.load_stylesheet(theme.Theme())
    assert re.search(r"\{[A-Za-z0-9_]+\}", qss) is None


def test_render_stylesheet_handles_structural_braces() -> None:
    qss = "QWidget {\n    background-color: {background};\n}"
    rendered = theme._render_stylesheet(qss, {"background": "#ffffff"})
    assert "{background}" not in rendered
    assert "#ffffff" in rendered


def test_render_stylesheet_raises_on_unknown_placeholder() -> None:
    qss = "QWidget { color: {text}; border: 1px solid {UNKNOWN}; }"
    with pytest.raises(ValueError, match="UNKNOWN"):
        theme._render_stylesheet(qss, {"text": "#111111"})


def test_apply_theme_resets_global_stylesheet() -> None:
    app = QApplication.instance() or QApplication([])
    app.setStyleSheet("QWidget { background: red; }")

    applied_theme = theme.apply_theme(app)

    assert applied_theme is not None
    assert app.styleSheet() == ""
    assert app.palette() == app.style().standardPalette()
