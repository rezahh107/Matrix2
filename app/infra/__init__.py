"""لایهٔ زیرساختی برای عملیات I/O و پل‌های سیستم Eligibility Matrix."""

from app.infra.errors import (
    DatabaseDisabledError,
    DatabaseOperationError,
    InfraError,
    ReferenceDataMissingError,
    SchemaVersionMismatchError,
)
from app.infra.sqlite_config import configure_connection

__all__ = [
    "DatabaseDisabledError",
    "DatabaseOperationError",
    "InfraError",
    "ReferenceDataMissingError",
    "SchemaVersionMismatchError",
    "configure_connection",
]
