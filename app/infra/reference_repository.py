"""الگوی مشترک مخازن مرجع SQLite برای کش‌های مدارس، دانش‌آموزان و منتورها."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Protocol, runtime_checkable

import pandas as pd

from app.infra.errors import ReferenceDataMissingError
from app.infra.local_database import LocalDatabase, _build_index_statements, _table_exists
from app.infra.sqlite_types import coerce_int_columns


@dataclass(frozen=True)
class ReferenceRefreshMeta:
    """متادیتای آخرین تازه‌سازی کش مرجع."""

    table_name: str
    refreshed_at: datetime
    source: str
    row_count: int


@runtime_checkable
class ReferenceRepository(Protocol):
    """قرارداد مشترک مخازن مرجع SQLite."""

    def upsert_frame(self, df: pd.DataFrame, *, source: str) -> None:
        """ذخیرهٔ دیتافریم مرجع با ایندکس‌های استاندارد و ثبت منبع."""

    def load_frame(self) -> pd.DataFrame:
        """بارگذاری دیتافریم مرجع با انواع عددی نرمال‌شده."""

    def last_refresh_meta(self) -> ReferenceRefreshMeta | None:
        """دریافت متادیتای آخرین تازه‌سازی (زمان، منبع، شمارش)."""


class SQLiteReferenceRepository(ReferenceRepository):
    """پیاده‌سازی عمومی مخزن مرجع بر پایهٔ :class:`LocalDatabase`.

    این کلاس برای کش‌های مرجع تکرارشونده (مدارس، دانش‌آموزان، منتورها) سه
    رفتار اصلی را یکسان می‌کند: ذخیرهٔ دترمینیستیک DataFrame، بارگذاری با
    انواع Int64 پایدار، و ثبت متادیتای تازه‌سازی.
    """

    def __init__(
        self,
        *,
        db: LocalDatabase,
        table_name: str,
        int_columns: Iterable[str] = (),
        join_keys: Iterable[str] = (),
        unique_columns: Iterable[str] = (),
    ) -> None:
        self._db = db
        self._table_name = table_name
        self._int_columns = tuple(int_columns)
        self._join_keys = tuple(join_keys)
        self._unique_columns = tuple(unique_columns)

    def upsert_frame(self, df: pd.DataFrame, *, source: str) -> None:
        if df is None:
            raise ValueError("دیتافریم مرجع تهی است؛ ورودی معتبر بدهید.")
        int_like_columns = set(self._int_columns) | set(self._join_keys)
        normalized = coerce_int_columns(df, int_like_columns)
        self._db.initialize()
        index_statements = _build_index_statements(
            table_name=self._table_name,
            df=normalized,
            unique_candidates=self._unique_columns,
            join_keys=self._join_keys,
        )
        with self._db.connect() as conn:
            LocalDatabase._replace_table_atomic(
                conn,
                table_name=self._table_name,
                df=normalized,
                index_statements=index_statements,
            )
            self._db.record_reference_meta(
                table_name=self._table_name,
                source=source,
                row_count=int(normalized.shape[0]),
                conn=conn,
            )

    def load_frame(self) -> pd.DataFrame:
        with self._db.connect() as conn:
            if not _table_exists(conn, self._table_name):
                raise ReferenceDataMissingError(
                    table=self._table_name,
                    message=(
                        f"جدول {self._table_name} در پایگاه داده یافت نشد؛ ابتدا import مربوطه را اجرا کنید."
                    ),
                )
            df = pd.read_sql_query(f"SELECT * FROM {self._table_name}", conn)
        int_like_columns = set(self._int_columns) | set(self._join_keys)
        return coerce_int_columns(df, int_like_columns)

    def last_refresh_meta(self) -> ReferenceRefreshMeta | None:
        raw = self._db.fetch_reference_meta(self._table_name)
        if raw is None:
            return None
        refreshed_at, source, row_count = raw
        dt = datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
        return ReferenceRefreshMeta(
            table_name=self._table_name,
            refreshed_at=dt,
            source=source or "",
            row_count=int(row_count) if row_count is not None else 0,
        )


__all__ = [
    "ReferenceRefreshMeta",
    "ReferenceRepository",
    "SQLiteReferenceRepository",
]
