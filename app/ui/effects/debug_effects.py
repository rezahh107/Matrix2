"""افکت‌های ایمن‌شده برای سایه و شفافیت با چرخهٔ نقاش ساده و محلی.

این پیاده‌سازی‌ها از الگوی رسمی Qt برای `draw()` پیروی می‌کنند تا از
استفادهٔ هم‌زمان چند نقاش روی یک دستگاه جلوگیری شود و هشدارهای
«Painter not active» حذف شود. ثبت لاگ اختیاری و پشت فلگ
`DEBUG_PAINT_EFFECTS` فعال می‌شود.
"""

from __future__ import annotations

import logging
from typing import Any

from PySide6.QtCore import QPoint, Qt
from PySide6.QtGui import QPainter, QPixmap, QTransform
from PySide6.QtWidgets import QGraphicsDropShadowEffect, QGraphicsEffect, QGraphicsOpacityEffect

from app.ui.utils import painter_state

LOGGER = logging.getLogger(__name__)
DEBUG_PAINT_EFFECTS = False


class _LoggingMixin:
    _effect_name: str | None = None
    _logged_inactive: bool = False

    def _init_effect_name(self, effect_name: str) -> None:
        """ثبت نام افکت با fallback به Qt property در صورت محدودیت __setattr__."""

        try:
            self._effect_name = effect_name
        except TypeError:
            # برخی آبجکت‌های Qt اجازهٔ __setattr__ ندارند؛ از setProperty استفاده می‌کنیم.
            self.setProperty("_effect_name", effect_name)
        try:
            self._logged_inactive = False
        except TypeError:
            self.setProperty("_logged_inactive", False)

    def _get_effect_name(self) -> str:
        name = getattr(self, "_effect_name", None)
        if isinstance(name, str):
            return name
        prop_name = self.property("_effect_name") if hasattr(self, "property") else None
        if isinstance(prop_name, str):
            return prop_name
        return self.__class__.__name__

    def _mark_inactive_once(self) -> None:
        already_logged = getattr(self, "_logged_inactive", False)
        if already_logged:
            return
        LOGGER.warning("%s | painter inactive on entry", self._get_effect_name())
        try:
            self._logged_inactive = True
        except TypeError:
            self.setProperty("_logged_inactive", True)

    def _log(self, message: str, painter: QPainter) -> None:
        if not DEBUG_PAINT_EFFECTS:
            return
        device = painter.device() if painter.isActive() else None
        LOGGER.debug(
            "%s | %s | active=%s device=%s id=%s source=%s effect=%s",  # noqa: TRY003
            self._get_effect_name(),
            message,
            painter.isActive(),
            device.__class__.__name__ if device else "<none>",
            hex(id(device)) if device else "<none>",
            self.source() if isinstance(self, QGraphicsEffect) else "<none>",
            hex(id(self)),
        )

    def _source_with_offset(
        self,
        coordinates: Qt.CoordinateSystem,
        pad_mode: QGraphicsEffect.PixmapPadMode,
    ) -> tuple[QPixmap, QPoint]:
        """Normalize PySide/PyQt sourcePixmap return shapes."""

        offset = QPoint()
        source = self.sourcePixmap(coordinates, offset, pad_mode)
        if isinstance(source, tuple):
            pixmap, returned_offset = source
            if isinstance(returned_offset, QPoint):
                offset = returned_offset
        else:
            pixmap = source

        return pixmap, offset


class SafeOpacityEffect(_LoggingMixin, QGraphicsOpacityEffect):
    """افکت شفافیت با نقاش محلی و بدون شروع/پایان دستی."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        self._init_effect_name(effect_name)

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        if not painter.isActive():
            self._mark_inactive_once()
            return

        offset = QPoint()
        pixmap = self.sourcePixmap(
            Qt.LogicalCoordinates, offset, QGraphicsEffect.PadToEffectiveBoundingRect
        )
        if pixmap.isNull():
            self._log("draw.skip(null_pixmap)", painter)
            self.drawSource(painter)
            return

        self._log("draw.begin", painter)
        try:
            with painter_state(painter):
                painter.setOpacity(self.opacity())
                painter.drawPixmap(offset, pixmap)
        finally:
            self._log("draw.end", painter)


class SafeDropShadowEffect(_LoggingMixin, QGraphicsDropShadowEffect):
    """افکت سایه با چرخهٔ نقاش محلی و تکیه بر پیاده‌سازی پیش‌فرض Qt."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        self._init_effect_name(effect_name)

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        if not painter.isActive():
            self._mark_inactive_once()
            return

        offset = QPoint()
        pixmap = self.sourcePixmap(
            Qt.LogicalCoordinates, offset, QGraphicsEffect.PadToEffectiveBoundingRect
        )
        if pixmap.isNull():
            self._log("draw.skip(null_pixmap)", painter)
            self.drawSource(painter)
            return

        self._log("draw.begin", painter)
        try:
            with painter_state(painter):
                painter.setWorldTransform(QTransform())
                super().draw(painter)
        finally:
            self._log("draw.end", painter)
