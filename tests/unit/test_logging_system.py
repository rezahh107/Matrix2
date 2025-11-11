"""تست‌های واحد برای سیستم لاگ زیرساختی."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from textwrap import dedent

from app.infra.logging import configure_logging, install_exception_hook


def _create_logging_config(tmp_path: Path) -> Path:
    """ساخت فایل پیکربندی موقت برای آزمون‌ها."""

    log_path = tmp_path / "logs" / "test.log"
    config_path = tmp_path / "logging.yaml"
    config_path.write_text(
        dedent(
            f"""
            version: 1
            disable_existing_loggers: false
            formatters:
              detailed:
                format: "[%(asctime)s] %(levelname)s %(name)s | session=%(session_id)s user=%(user)s error=%(error_id)s report=%(report_path)s | %(message)s"
                datefmt: "%Y-%m-%d %H:%M:%S"
            handlers:
              file:
                class: logging.FileHandler
                level: DEBUG
                formatter: detailed
                filename: "{log_path}"
                encoding: utf-8
            loggers:
              test.logger:
                level: DEBUG
                handlers: [file]
                propagate: false
            root:
              level: WARNING
              handlers: [file]
            """
        ).strip()
    )
    return config_path


def test_configure_logging_enriches_records(tmp_path: Path) -> None:
    """اطمینان از این‌که کانتکست لاگ در خروجی فایل درج می‌شود."""

    config_path = _create_logging_config(tmp_path)
    context = configure_logging(
        app_name="TestApp",
        app_version="0.1",
        logger_name="test.logger",
        config_path=config_path,
        log_dir=tmp_path / "logs",
    )

    logger = logging.getLogger("test.logger")
    logger.info("hello world")
    logger.error("boom")
    logging.shutdown()

    log_file = tmp_path / "logs" / "test.log"
    content = log_file.read_text(encoding="utf-8")
    assert context.session_id in content
    assert "error=" in content


def test_install_exception_hook_creates_error_report(tmp_path: Path) -> None:
    """بررسی ایجاد فایل گزارش خطای مستقل هنگام بروز استثنا."""

    config_path = _create_logging_config(tmp_path)
    context = configure_logging(
        app_name="TestApp",
        app_version="0.2",
        logger_name="test.logger",
        config_path=config_path,
        log_dir=tmp_path / "logs",
    )

    logger = logging.getLogger("test.logger")
    restore = install_exception_hook(logger, context)
    try:
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            exc_type, exc_value, exc_tb = sys.exc_info()
            assert exc_tb is not None
            sys.excepthook(exc_type, exc_value, exc_tb)
    finally:
        restore()
        logging.shutdown()

    reports = sorted(context.error_dir.glob("*.log"))
    assert reports, "گزارش خطا ایجاد نشده است"
    content = reports[-1].read_text(encoding="utf-8")
    assert "RuntimeError" in content
    assert "boom" in content
