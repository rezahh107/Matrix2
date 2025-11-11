"""زیرساخت راه‌اندازی لاگ و مدیریت خطا برای لایهٔ زیرساخت."""
from __future__ import annotations

import getpass
import logging
import logging.config
import os
import sys
import threading
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

import yaml

DEFAULT_LOGGING_CONFIG = Path("config/logging.yaml")


@dataclass(slots=True, frozen=True)
class LoggingContext:
    """نگهدارندهٔ اطلاعات کانتکست لاگ با ابزار ساخت گزارش خطا.

    مثال::

        >>> from pathlib import Path
        >>> ctx = LoggingContext(
        ...     application="TestApp",
        ...     version="0.1",
        ...     session_id="abc",
        ...     user="tester",
        ...     pid=123,
        ...     log_dir=Path("logs"),
        ...     error_dir=Path("logs/errors"),
        ... )
        >>> _ = ctx.new_error_id()
        >>> # تولید گزارش خطا در فایل موقتی (نمونه آزمایشی)
    """

    application: str
    version: str
    session_id: str
    user: str
    pid: int
    log_dir: Path
    error_dir: Path

    def new_error_id(self) -> str:
        """ساخت شناسهٔ خطای یکتا برای نشست جاری."""

        return f"{self.session_id}-{uuid.uuid4().hex[:8]}"

    def write_error_report(self, *, error_id: str, message: str, traceback_text: str) -> Path:
        """نوشتن گزارش خطا روی دیسک با متادیتای کامل.

        Args:
            error_id: شناسهٔ یکتای خطا.
            message: خلاصهٔ خطا برای خواندن سریع.
            traceback_text: استک‌تریس کامل.

        Returns:
            Path: مسیر فایل گزارش ایجاد شده.
        """

        timestamp = datetime.now(timezone.utc)
        filename = f"{error_id}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}.log"
        self.error_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.error_dir / filename
        header = [
            f"application={self.application}",
            f"version={self.version}",
            f"session_id={self.session_id}",
            f"error_id={error_id}",
            f"user={self.user}",
            f"pid={self.pid}",
            f"timestamp={timestamp.isoformat().replace('+00:00', 'Z')}",
            "",
            message.strip(),
            "",
            traceback_text.strip(),
            "",
        ]
        report_path.write_text("\n".join(header), encoding="utf-8")
        return report_path


class SessionContextFilter(logging.Filter):
    """افزودن اطلاعات نشست به همهٔ رکوردهای لاگ."""

    def __init__(self, context: LoggingContext) -> None:
        super().__init__(name="")
        self._context = context

    def filter(self, record: logging.LogRecord) -> bool:
        record.session_id = getattr(record, "session_id", self._context.session_id)
        record.user = getattr(record, "user", self._context.user)
        record.application = getattr(record, "application", self._context.application)
        record.app_version = getattr(record, "app_version", self._context.version)
        record.error_id = getattr(record, "error_id", "")
        record.report_path = getattr(record, "report_path", "")
        return True


def _attach_filter(target: logging.Logger, filter_obj: logging.Filter) -> None:
    """افزودن فیلتر به logger و handlerهای وابسته بدون تکرار."""

    if not any(isinstance(existing, SessionContextFilter) for existing in target.filters):
        target.addFilter(filter_obj)
    for handler in target.handlers:
        if not any(isinstance(existing, SessionContextFilter) for existing in handler.filters):
            handler.addFilter(filter_obj)


def _apply_log_dir_override(config: dict[str, Any], log_directory: Path) -> None:
    """به‌روزرسانی مسیر handlerهای فایل بر اساس log_dir سفارشی."""

    handlers = config.get("handlers", {})
    if not isinstance(handlers, dict):
        return

    for handler_cfg in handlers.values():
        if not isinstance(handler_cfg, dict):
            continue
        filename = handler_cfg.get("filename")
        if not filename:
            continue
        filename_path = Path(str(filename))
        if filename_path.is_absolute():
            handler_cfg["filename"] = str(filename_path)
            continue
        handler_cfg["filename"] = str((log_directory / filename_path.name).resolve())


def setup_logging(
    config_path: str | Path = DEFAULT_LOGGING_CONFIG,
    log_dir: str | Path | None = None,
) -> None:
    """بارگذاری پیکربندی logging از فایل YAML و اعمال آن.

    مثال::

        >>> setup_logging()  # doctest: +SKIP

    Args:
        config_path: مسیر فایل پیکربندی YAML.
        log_dir: مسیر دلخواه برای نگهداری فایل‌های لاگ.

    Raises:
        FileNotFoundError: اگر فایل پیکربندی وجود نداشته باشد.
        ValueError: اگر ساختار YAML معتبر نباشد.
    """

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"logging config not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data: Any = yaml.safe_load(handle)

    if not isinstance(data, dict):
        raise ValueError("logging config must be a mapping")

    log_directory = Path(log_dir).expanduser().resolve() if log_dir else None
    if log_directory:
        _apply_log_dir_override(data, log_directory)

    handlers = data.get("handlers", {})
    if isinstance(handlers, dict):
        for handler_cfg in handlers.values():
            if not isinstance(handler_cfg, dict):
                continue
            cls_name = str(handler_cfg.get("class", ""))
            if "FileHandler" in cls_name:
                filename = handler_cfg.get("filename")
                if filename:
                    file_path = Path(str(filename)).expanduser()
                    if not file_path.is_absolute():
                        file_path = file_path.resolve()
                    handler_cfg["filename"] = str(file_path)
                    file_path.parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(data)


def configure_logging(
    *,
    app_name: str,
    app_version: str,
    logger_name: str,
    config_path: str | Path = DEFAULT_LOGGING_CONFIG,
    log_dir: str | Path | None = None,
) -> LoggingContext:
    """پیکربندی logging با افزودن فیلتر کانتکست و بازگرداندن آن.

    Args:
        app_name: نام برنامه برای درج در گزارش‌ها.
        app_version: نسخهٔ برنامه.
        logger_name: نام logger اصلی برنامه.
        config_path: مسیر پیکربندی YAML.
        log_dir: مسیر دلخواه برای نگهداری فایل‌های لاگ.

    Returns:
        LoggingContext: کانتکست نشست فعلی برای تولید گزارش خطا.
    """

    log_directory = Path(log_dir).expanduser().resolve() if log_dir else Path("logs").resolve()
    log_directory.mkdir(parents=True, exist_ok=True)
    setup_logging(config_path, log_directory)
    error_directory = log_directory / "errors"
    error_directory.mkdir(parents=True, exist_ok=True)

    context = LoggingContext(
        application=app_name,
        version=app_version,
        session_id=uuid.uuid4().hex,
        user=getpass.getuser(),
        pid=os.getpid(),
        log_dir=log_directory,
        error_dir=error_directory,
    )

    filter_obj = SessionContextFilter(context)
    _attach_filter(logging.getLogger(), filter_obj)
    _attach_filter(logging.getLogger(logger_name), filter_obj)
    logging.captureWarnings(True)

    return context


def install_exception_hook(logger: logging.Logger, context: LoggingContext) -> Callable[[], None]:
    """نصب هندلر خطای سراسری برای ثبت و ذخیرهٔ گزارش تفصیلی.

    Args:
        logger: logger اصلی برای ثبت خطا.
        context: کانتکست نشست برای تولید شناسه و مسیر گزارش.

    Returns:
        Callable[[], None]: تابعی برای بازگردانی هندلرهای قبلی.
    """

    previous_sys_hook = sys.excepthook
    has_thread_hook = hasattr(threading, "excepthook")
    previous_thread_hook = threading.excepthook if has_thread_hook else None

    def _log_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
        source: str,
    ) -> None:
        error_id = context.new_error_id()
        traceback_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        report_path = context.write_error_report(
            error_id=error_id,
            message=f"{source}: {exc_value}",
            traceback_text=traceback_text,
        )
        logger.critical(
            "Unhandled exception from %s",
            source,
            exc_info=(exc_type, exc_value, exc_tb),
            extra={"error_id": error_id, "report_path": str(report_path)},
        )

    def handle_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            previous_sys_hook(exc_type, exc_value, exc_tb)
            return
        _log_exception(exc_type, exc_value, exc_tb, source="main-thread")
        previous_sys_hook(exc_type, exc_value, exc_tb)

    def handle_thread_exception(args: threading.ExceptHookArgs) -> None:
        thread_name = getattr(getattr(args, "thread", None), "name", "thread")
        _log_exception(args.exc_type, args.exc_value, args.exc_traceback, source=f"{thread_name}")
        if previous_thread_hook:
            previous_thread_hook(args)

    sys.excepthook = handle_exception
    if has_thread_hook:
        threading.excepthook = handle_thread_exception  # type: ignore[assignment]

    def restore() -> None:
        sys.excepthook = previous_sys_hook
        if has_thread_hook and previous_thread_hook is not None:
            threading.excepthook = previous_thread_hook  # type: ignore[assignment]

    return restore


__all__ = [
    "LoggingContext",
    "SessionContextFilter",
    "configure_logging",
    "install_exception_hook",
    "setup_logging",
]
