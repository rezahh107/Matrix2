import logging

import pytest

from app.infra.excel import exporter


@pytest.fixture(autouse=True)
def _reset_warning_flag() -> None:
    exporter._reset_font_warning_flag()
    yield
    exporter._reset_font_warning_flag()


def test_font_warning_emitted_once(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="app.infra.excel.exporter")

    exporter._warn_fonts_not_embedded()
    exporter._warn_fonts_not_embedded()

    warnings = [rec for rec in caplog.records if "فونت‌های سفارشی" in rec.message]
    assert len(warnings) == 1
    assert "PDF" in warnings[0].message
