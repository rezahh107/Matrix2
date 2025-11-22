"""مدل خطای لایهٔ Infra برای عملیات پایگاه داده."""
from __future__ import annotations

from dataclasses import dataclass


class InfraError(RuntimeError):
    """پایهٔ همهٔ خطاهای لایهٔ زیرساخت."""

    def __str__(self) -> str:  # pragma: no cover - ساده
        return super().__str__()


@dataclass(eq=True)
class DatabaseDisabledError(InfraError):
    """وقتی دیتابیس محلی بنا به تنظیمات غیرفعال شده باشد."""

    reason: str = "پایگاه دادهٔ محلی غیرفعال است."

    def __str__(self) -> str:
        return self.reason


@dataclass(eq=True)
class ReferenceDataMissingError(InfraError):
    """نبود جداول مرجع ضروری مانند مدارس یا Crosswalk."""

    table: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(eq=True)
class SchemaVersionMismatchError(InfraError):
    """عدم تطابق نسخهٔ Schema پایگاه داده با نسخهٔ مورد انتظار."""

    expected_version: int
    actual_version: int
    message: str

    def __str__(self) -> str:
        return f"{self.message} (expected={self.expected_version}, actual={self.actual_version})"


@dataclass(eq=True)
class DatabaseOperationError(InfraError):
    """خطای کلی عملیات SQLite با پیام خوانا."""

    message: str

    def __str__(self) -> str:
        return self.message


__all__ = [
    "InfraError",
    "DatabaseDisabledError",
    "ReferenceDataMissingError",
    "SchemaVersionMismatchError",
    "DatabaseOperationError",
]
