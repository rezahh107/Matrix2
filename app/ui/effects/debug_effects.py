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
from PySide6.QtGui import QPainter, QPixmap
from PySide6.QtWidgets import QGraphicsDropShadowEffect, QGraphicsEffect, QGraphicsOpacityEffect

LOGGER = logging.getLogger(__name__)
DEBUG_PAINT_EFFECTS = False


class _LoggingMixin:
    _effect_name_key = "_effect_name"
    _logged_inactive_key = "_logged_inactive"

    def _init_effect_name(self, effect_name: str) -> None:
        """ثبت نام افکت با تکیه بر Qt dynamic property تا از TypeError جلوگیری شود."""

        # برخی کلاس‌های Qt از __setattr__ پشتیبانی نمی‌کنند؛ setProperty همیشه قابل‌اتکا است.
        self.setProperty(self._effect_name_key, effect_name)
        self.setProperty(self._logged_inactive_key, False)

    def _get_effect_name(self) -> str:
        if hasattr(self, "property"):
            prop_name = self.property(self._effect_name_key)
            if isinstance(prop_name, str) and prop_name:
                return prop_name
        name = getattr(self, "_effect_name", None)
        if isinstance(name, str) and name:
            return name
        return self.__class__.__name__

    def _mark_inactive_once(self) -> None:
        already_logged = False
        if hasattr(self, "property"):
            prop_flag = self.property(self._logged_inactive_key)
            already_logged = bool(prop_flag)
        else:
            already_logged = getattr(self, "_logged_inactive", False)

        if already_logged:
            return

        LOGGER.warning("%s | painter inactive on entry", self._get_effect_name())
        if hasattr(self, "setProperty"):
            self.setProperty(self._logged_inactive_key, True)
        else:
            try:
                self._logged_inactive = True
            except TypeError:
                pass

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
        painter.save()
        try:
            painter.setOpacity(self.opacity())
            painter.drawPixmap(offset, pixmap)
        finally:
            painter.restore()
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
        painter.save()
        try:
            super().draw(painter)
        finally:
            painter.restore()
            self._log("draw.end", painter)
