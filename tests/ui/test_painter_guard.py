"""تست سبک برای نگهبان نقاش و افکت‌های ایمن شده."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtCore import Qt
from PySide6.QtGui import QImage, QPainter
from PySide6.QtWidgets import QApplication, QLabel, QWidget

from app.ui.effects import SafeDropShadowEffect, SafeOpacityEffect
from app.ui.utils import assert_painter_active
from app.ui.utils import painter_guard as painter_guard_module


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_safe_drop_shadow_renders_offscreen(monkeypatch: pytest.MonkeyPatch, qapp: QApplication) -> None:
    monkeypatch.setattr(painter_guard_module, "painter_guard_enabled", True)
    widget = QWidget()
    widget.resize(160, 120)
    widget.setGraphicsEffect(SafeDropShadowEffect("test_shadow", widget))

    image = QImage(widget.size(), QImage.Format_ARGB32_Premultiplied)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    assert assert_painter_active(painter, "test_safe_drop_shadow_renders_offscreen", strict=True)
    widget.render(painter)
    painter.end()


def test_safe_opacity_renders_offscreen(monkeypatch: pytest.MonkeyPatch, qapp: QApplication) -> None:
    monkeypatch.setattr(painter_guard_module, "painter_guard_enabled", True)
    widget = QWidget()
    widget.resize(140, 90)

    label = QLabel("fade", widget)
    label.resize(80, 40)
    label.setGraphicsEffect(SafeOpacityEffect("test_opacity", label))

    image = QImage(widget.size(), QImage.Format_ARGB32_Premultiplied)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    assert assert_painter_active(painter, "test_safe_opacity_renders_offscreen", strict=True)
    widget.render(painter)
    painter.end()
