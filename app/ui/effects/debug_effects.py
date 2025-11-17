"""افکت‌های ایمن‌شده برای سایه و شفافیت با چرخهٔ نقاش ساده و محلی.

این پیاده‌سازی‌ها از الگوی رسمی Qt برای `draw()` پیروی می‌کنند تا از
استفادهٔ هم‌زمان چند نقاش روی یک دستگاه جلوگیری شود و هشدارهای
«Painter not active» حذف شود. ثبت لاگ اختیاری و پشت فلگ
`DEBUG_PAINT_EFFECTS` فعال می‌شود.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from PySide6.QtCore import QPoint, QPointF, QSize, Qt
from PySide6.QtGui import QColor, QImage, QPainter, QPixmap, QTransform
from PySide6.QtWidgets import (
    QGraphicsBlurEffect,
    QGraphicsDropShadowEffect,
    QGraphicsEffect,
    QGraphicsOpacityEffect,
)

from app.ui.utils import assert_painter_active

LOGGER = logging.getLogger(__name__)
DEBUG_PAINT_EFFECTS = False


@dataclass(frozen=True)
class _EffectMeta:
    """شناسهٔ انسانی برای لاگ‌گیری."""

    name: str


class _LoggingMixin:
    _effect_meta: _EffectMeta

    def _log(self, message: str, painter: QPainter) -> None:
        if not DEBUG_PAINT_EFFECTS:
            return
        device = painter.device() if painter.isActive() else None
        LOGGER.debug(
            "%s | %s | active=%s device=%s id=%s source=%s effect=%s",  # noqa: TRY003
            self._effect_meta.name,
            message,
            painter.isActive(),
            device.__class__.__name__ if device else "<none>",
            hex(id(device)) if device else "<none>",
            self.source() if isinstance(self, QGraphicsEffect) else "<none>",
            hex(id(self)),
        )


class SafeOpacityEffect(_LoggingMixin, QGraphicsOpacityEffect):
    """افکت شفافیت با نقاش محلی و بدون شروع/پایان دستی."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        object.__setattr__(self, "_effect_meta", _EffectMeta(effect_name))

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        if not assert_painter_active(painter, f"{self._effect_meta.name}.draw"):
            return

        pixmap, offset = self.sourcePixmap(
            Qt.DeviceCoordinates, mode=QGraphicsEffect.PadToEffectiveBoundingRect
        )
        if pixmap.isNull():
            self._log("draw.skip(null_pixmap)", painter)
            return

        self._log("draw.begin", painter)
        painter.save()
        try:
            painter.setWorldTransform(QTransform())
            painter.setOpacity(self.opacity())
            painter.drawPixmap(offset, pixmap)
        finally:
            painter.restore()
            self._log("draw.end", painter)


class SafeDropShadowEffect(_LoggingMixin, QGraphicsDropShadowEffect):
    """افکت سایه با چرخهٔ نقاش محلی و محاسبهٔ بلور روی QImage."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        object.__setattr__(self, "_effect_meta", _EffectMeta(effect_name))

    def _build_shadow_image(
        self, pixmap: QPixmap, radius: float, offset: QPointF, color: QColor
    ) -> QImage:
        margin = int(radius * 2)
        image_size = pixmap.size() + QSize(margin * 2, margin * 2)
        shadow_source = QImage(image_size, QImage.Format_ARGB32_Premultiplied)
        shadow_source.fill(Qt.transparent)

        painter = QPainter(shadow_source)
        if not assert_painter_active(painter, f"{self._effect_meta.name}.shadow_image"):
            return shadow_source
        painter.setCompositionMode(QPainter.CompositionMode_Source)
        painter.drawPixmap(margin + int(offset.x()), margin + int(offset.y()), pixmap)
        painter.setCompositionMode(QPainter.CompositionMode_SourceIn)
        painter.fillRect(shadow_source.rect(), color)
        painter.end()

        blurred = QImage(image_size, QImage.Format_ARGB32_Premultiplied)
        blurred.fill(Qt.transparent)
        QGraphicsBlurEffect.blurImage(blurred, shadow_source, radius, True)
        return blurred

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        if not assert_painter_active(painter, f"{self._effect_meta.name}.draw"):
            return

        pixmap, offset = self.sourcePixmap(
            Qt.DeviceCoordinates, mode=QGraphicsEffect.PadToEffectiveBoundingRect
        )
        if pixmap.isNull():
            self._log("draw.skip(null_pixmap)", painter)
            return

        radius = self.blurRadius()
        shadow_offset = self.offset()
        color = self.color()
        margin = int(radius * 2)

        self._log("draw.begin", painter)
        shadow_image = self._build_shadow_image(pixmap, radius, shadow_offset, color)
        offset_point = QPointF(offset)

        painter.save()
        try:
            painter.setWorldTransform(QTransform())
            painter.drawImage(offset_point.toPoint() - QPoint(margin, margin), shadow_image)
            painter.drawPixmap(offset_point, pixmap)
        finally:
            painter.restore()
            self._log("draw.end", painter)
