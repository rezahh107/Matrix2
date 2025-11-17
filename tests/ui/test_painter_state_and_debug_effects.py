"""تست‌های چرخهٔ نقاش برای افکت‌های امن و کارت داشبورد."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="PySide6 not available in test environment")
from PySide6.QtCore import Qt
from PySide6.QtGui import QImage, QPainter, QPen, QTransform
from PySide6.QtWidgets import QApplication, QWidget

from app.ui.effects import SafeDropShadowEffect, SafeOpacityEffect
from app.ui.utils import painter_state
from app.ui.widgets import DashboardCard


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_painter_state_restores_properties(qapp: QApplication) -> None:
    image = QImage(32, 24, QImage.Format_ARGB32_Premultiplied)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    assert painter.isActive()

    original_transform = QTransform(painter.transform())
    original_pen = QPen(painter.pen())

    with painter_state(painter):
        painter.translate(5, 3)
        custom_pen = QPen(original_pen)
        custom_pen.setWidth(custom_pen.width() + 2)
        painter.setPen(custom_pen)
        painter.drawRect(0, 0, 10, 10)

    assert painter.transform() == original_transform
    assert painter.pen() == original_pen
    painter.end()


def test_safe_effects_render_without_state_leak(qapp: QApplication) -> None:
    widget = QWidget()
    widget.resize(160, 120)
    widget.setGraphicsEffect(SafeDropShadowEffect("shadow_test", widget))
    inner = QWidget(widget)
    inner.setGraphicsEffect(SafeOpacityEffect("opacity_test", inner))
    inner.resize(80, 60)

    image = QImage(widget.size(), QImage.Format_ARGB32_Premultiplied)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    identity_before = QTransform(painter.transform())
    widget.render(painter)
    assert painter.isActive()
    assert painter.transform() == identity_before
    painter.end()


def test_dashboard_card_render_offscreen(qapp: QApplication) -> None:
    card = DashboardCard("title", "desc")
    card.resize(200, 140)

    image = QImage(card.size(), QImage.Format_ARGB32_Premultiplied)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    card.render(painter)
    assert painter.isActive()
    painter.end()
