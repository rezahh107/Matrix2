"""Instrumentation helpers for graphics effects.

Temporary debug utilities to trace QPainter usage in graphics effects.
"""

from .debug_effects import DebugDropShadowEffect, DebugOpacityEffect, log_painter_state

__all__ = ["DebugDropShadowEffect", "DebugOpacityEffect", "log_painter_state"]
