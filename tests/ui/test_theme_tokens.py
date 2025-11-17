"""اطمینان از وجود توکن‌های تم و پشته فونت."""

from __future__ import annotations

import pytest

pytest.importorskip("PySide6.QtWidgets", reason="محیط CI بدون libGL")

from app.ui import theme


def test_theme_tokens_present() -> None:
    t = theme.Theme()
    assert t.colors.background.startswith("#")
    assert t.colors.card.startswith("#")
    assert t.colors.text.startswith("#")
    assert t.colors.text_muted.startswith("#")
    assert t.colors.primary.startswith("#")
    assert t.colors.log_background.startswith("#")
    assert t.typography.font_fa_stack
    assert t.typography.font_en_stack
    assert t.typography.title_size > 0
    assert t.typography.body_size > 0
