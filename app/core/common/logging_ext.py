"""ابزارک‌های Logging مرحله‌ای برای لایهٔ Infra."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Iterator
from logging import Logger

__all__ = ["log_step", "StepLogger"]


@contextmanager
def log_step(logger: Logger, step: str) -> Iterator[None]:
    """Context manager ساده برای ثبت شروع/پایان مراحل پرهزینه."""

    start = perf_counter()
    logger.info("شروع مرحلهٔ %s", step)
    try:
        yield
    except Exception:
        logger.exception("مرحلهٔ %s با خطا پایان یافت", step)
        raise
    else:
        elapsed = perf_counter() - start
        logger.info("مرحلهٔ %s تکمیل شد (%.2fs)", step, elapsed)


@dataclass(slots=True)
class StepLogger:
    """رپر شیٔ‌گرا برای استفادهٔ تکراری از :func:`log_step`."""

    logger: Logger

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        with log_step(self.logger, name):
            yield
