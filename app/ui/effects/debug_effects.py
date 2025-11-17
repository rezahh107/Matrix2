"""Debug-friendly graphics effect wrappers.

These helpers instrument QGraphicsEffect drawing paths to surface painter
lifecycle issues (e.g., inactive painters or nested painters on the same
device). Use only for temporary diagnostics.
"""

from __future__ import annotations

import logging
from typing import Any

from PySide6.QtGui import QPainter
from PySide6.QtWidgets import QGraphicsDropShadowEffect, QGraphicsOpacityEffect

LOGGER = logging.getLogger(__name__)


def log_painter_state(prefix: str, painter: QPainter) -> None:
    """Log painter activity and device info for diagnostics."""

    try:
        active = painter.isActive()
    except RuntimeError:  # painter might be deleted by Qt
        LOGGER.warning("%s | painter state unavailable (deleted)", prefix)
        return
    device = painter.device() if active else None
    LOGGER.debug(
        "%s | active=%s device=%s id=%s",  # noqa: TRY003
        prefix,
        active,
        device.__class__.__name__ if device else "<none>",
        hex(id(device)) if device else "<none>",
    )


class _DebugEffectMixin:
    """Mixin to emit detailed logs during effect drawing."""

    _effect_name: str

    def _log(self, message: str, painter: QPainter) -> None:
        device = painter.device() if painter.isActive() else None
        LOGGER.debug(
            "%s | %s | active=%s device=%s id=%s source=%s effect=%s",  # noqa: TRY003
            self._effect_name,
            message,
            painter.isActive(),
            device.__class__.__name__ if device else "<none>",
            hex(id(device)) if device else "<none>",
            self.source(),
            hex(id(self)),
        )


class DebugOpacityEffect(_DebugEffectMixin, QGraphicsOpacityEffect):
    """Opacity effect with painter state logging."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        self._effect_name = effect_name
        LOGGER.debug("%s | init opacity effect id=%s", self._effect_name, hex(id(self)))

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        self._log("draw.begin", painter)
        try:
            super().draw(painter)
        finally:
            self._log("draw.end", painter)


class DebugDropShadowEffect(_DebugEffectMixin, QGraphicsDropShadowEffect):
    """Drop shadow effect with painter state logging."""

    def __init__(self, effect_name: str, parent: Any | None = None) -> None:
        super().__init__(parent)
        self._effect_name = effect_name
        LOGGER.debug("%s | init drop shadow id=%s", self._effect_name, hex(id(self)))

    def draw(self, painter: QPainter) -> None:  # type: ignore[override]
        self._log("draw.begin", painter)
        try:
            super().draw(painter)
        finally:
            self._log("draw.end", painter)
