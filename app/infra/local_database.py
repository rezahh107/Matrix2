# file: app/infra/local_database.py
"""پایگاه دادهٔ محلی SQLite برای نگهداشت تاریخچه و مراجع.

این ماژول یک لایهٔ نازک روی :mod:`sqlite3` است تا بدون وابستگی خارجی
لاگ اجرای تخصیص و داده‌های مرجع (مدارس و Crosswalk) را ذخیره کند.
Schema به‌صورت دترمینیستیک ساخته می‌شود و نسخهٔ آن در ``schema_meta``
ثبت و در هر بار مقداردهی اولیه اعتبارسنجی می‌شود.

نمونهٔ استفادهٔ سریع:

>>> db = LocalDatabase(Path("smart_alloc.db"))
>>> db.initialize()
>>> run_id = db.insert_run(sample_run_record)
>>> db.insert_run_metrics([RunMetricRow(run_id, "SCHOOL.students_total", 10.0)])
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

from app.infra.errors import (
    DatabaseOperationError,
    ReferenceDataMissingError,
    SchemaVersionMismatchError,
)
from app.infra.sqlite_config import configure_connection

_SCHEMA_VERSION = 1
_POLICY_VERSION = "1.0.3"
_SSOT_VERSION = "1.0.2"
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunRecord:
    """نمایندهٔ ردیف جدول ``runs`` برای یک اجرای تخصیص."""

    run_uuid: str
    started_at: datetime
    finished_at: datetime
    policy_version: str
    ssot_version: str
    entrypoint: str
    cli_args: str | None
    db_path: str | None
    input_files_json: str
    input_hashes_json: str
    total_students: int | None
    total_allocated: int | None
    total_unallocated: int | None
    history_metrics_json: str | None
    qa_summary_json: str | None
    status: str
    message: str | None


@dataclass(frozen=True)
class RunMetricRow:
    """ردیف جدول ``run_metrics`` به‌صورت کلید/مقدار."""

    run_id: int
    metric_key: str
    metric_value: float


@dataclass(frozen=True)
class QaSummaryRow:
    """خلاصهٔ QA برای یک اجرای تخصیص (یک ردیف در ``qa_summary``)."""

    run_id: int
    violation_code: str
    severity: str
    count: int


class LocalDatabase:
    """کلاس مدیریت اتصال و Schema پایگاه دادهٔ محلی.

    این کلاس رفتار را تغییر نمی‌دهد و فقط یک API ساده برای ایجاد
    جداول و درج داده در اختیار Infra قرار می‌دهد.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def _open_connection(self) -> sqlite3.Connection:
        """ایجاد اتصال پیکربندی‌شده با PRAGMA های یکسان."""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        return configure_connection(sqlite3.connect(self.path))

    def connect(self) -> sqlite3.Connection:
        """برگشت اتصال SQLite با تنظیمات استاندارد."""

        return self._open_connection()

    def initialize(self) -> None:
        """ایجاد Schema و اعتبارسنجی نسخه به‌صورت idempotent."""

        with self._open_connection() as conn:
            try:
                self._ensure_schema(conn)
                self._ensure_schema_meta_table(conn)
                self._ensure_schema_meta_row(conn)
                self._validate_schema_version(conn)
                conn.commit()
            except SchemaVersionMismatchError:
                raise
            except sqlite3.Error as exc:  # pragma: no cover - خطاهای غیرمنتظره
                raise DatabaseOperationError("خطا در آماده‌سازی پایگاه داده.") from exc
        logger.debug("Local DB schema ensured at %s", self.path)

    def insert_run(self, record: RunRecord) -> int:
        """درج ردیف جدید در جدول ``runs`` و بازگرداندن شناسه."""

        try:
            with self._open_connection() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO runs (
                        run_uuid, started_at, finished_at, policy_version, ssot_version,
                        entrypoint, cli_args, db_path, input_files_json, input_hashes_json,
                        total_students, total_allocated, total_unallocated,
                        history_metrics_json, qa_summary_json, status, message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.run_uuid,
                        _to_iso(record.started_at),
                        _to_iso(record.finished_at),
                        record.policy_version,
                        record.ssot_version,
                        record.entrypoint,
                        record.cli_args,
                        record.db_path,
                        record.input_files_json,
                        record.input_hashes_json,
                        record.total_students,
                        record.total_allocated,
                        record.total_unallocated,
                        record.history_metrics_json,
                        record.qa_summary_json,
                        record.status,
                        record.message,
                    ),
                )
                conn.commit()
                return int(cursor.lastrowid)
        except sqlite3.Error as exc:
            raise DatabaseOperationError("ثبت اجرای جدید در SQLite ناکام ماند.") from exc

    def insert_run_metrics(self, rows: Iterable[RunMetricRow]) -> None:
        """درج چندین ردیف KPI تاریخچه برای یک اجرا."""

        payload = [
            (
                row.run_id,
                row.metric_key,
                row.metric_value,
            )
            for row in rows
        ]
        if not payload:
            logger.debug("No metric rows to insert for run_metrics")
            return
        try:
            with self._open_connection() as conn:
                conn.executemany(
                    """
                    INSERT INTO run_metrics (
                        run_id, metric_key, metric_value
                    ) VALUES (?, ?, ?)
                    """,
                    payload,
                )
                conn.commit()
        except sqlite3.Error as exc:
            raise DatabaseOperationError("ثبت KPI تاریخچه با خطا روبه‌رو شد.") from exc

    def insert_qa_summary(self, rows: Iterable[QaSummaryRow]) -> None:
        """ثبت خلاصهٔ QA برای یک اجرا."""

        payload = [
            (row.run_id, row.violation_code, row.severity, row.count) for row in rows
        ]
        if not payload:
            logger.debug("No QA rows to insert for qa_summary")
            return
        try:
            with self._open_connection() as conn:
                conn.executemany(
                    """
                    INSERT INTO qa_summary (run_id, violation_code, severity, count)
                    VALUES (?, ?, ?, ?)
                    """,
                    payload,
                )
                conn.commit()
        except sqlite3.Error as exc:
            raise DatabaseOperationError("ثبت خلاصهٔ QA با خطا روبه‌رو شد.") from exc

    def fetch_runs(self) -> List[sqlite3.Row]:
        """بازیابی همهٔ اجراها (برای تست/دیباگ)."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM runs ORDER BY started_at ASC, id ASC"
            )
            return cursor.fetchall()

    def fetch_metrics_for_run(self, run_id: int) -> List[sqlite3.Row]:
        """بازیابی KPI تاریخچه برای یک شناسه اجرا."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM run_metrics WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            )
            return cursor.fetchall()

    def fetch_qa_summary(self, run_id: int) -> List[sqlite3.Row]:
        """بازیابی خلاصهٔ QA برای یک اجرا."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM qa_summary WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            )
            return cursor.fetchall()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        """ساخت جدول‌های runs/run_metrics/qa_summary و مراجع به‌صورت idempotent."""

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_uuid TEXT NOT NULL UNIQUE,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                ssot_version TEXT NOT NULL,
                entrypoint TEXT NOT NULL,
                cli_args TEXT,
                db_path TEXT,
                input_files_json TEXT,
                input_hashes_json TEXT,
                total_students INTEGER,
                total_allocated INTEGER,
                total_unallocated INTEGER,
                history_metrics_json TEXT,
                qa_summary_json TEXT,
                status TEXT NOT NULL,
                message TEXT
            );

            CREATE TABLE IF NOT EXISTS run_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                metric_key TEXT NOT NULL,
                metric_value REAL NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS qa_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                violation_code TEXT NOT NULL,
                severity TEXT NOT NULL,
                count INTEGER NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS schools (
                "کد مدرسه" INTEGER,
                "نام مدرسه" TEXT
            );

            CREATE TABLE IF NOT EXISTS school_crosswalk_groups (
                "کد مدرسه" INTEGER,
                "کد جایگزین" TEXT,
                title TEXT
            );

            CREATE TABLE IF NOT EXISTS school_crosswalk_synonyms (
                "کد مدرسه" INTEGER,
                "کد جایگزین" TEXT,
                alias TEXT
            );
            """
        )

    @staticmethod
    def _ensure_schema_meta_table(conn: sqlite3.Connection) -> None:
        """ایجاد جدول متادیتای نسخه در صورت نبود."""

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                schema_version INTEGER NOT NULL,
                policy_version TEXT NOT NULL,
                ssot_version TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

    @staticmethod
    def _ensure_schema_meta_row(conn: sqlite3.Connection) -> None:
        """تضمین وجود رکورد نسخهٔ Schema با درج اولیه در صورت نبود."""

        cursor = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1")
        rows = cursor.fetchall()
        if not rows:
            conn.execute(
                """
                INSERT INTO schema_meta (id, schema_version, policy_version, ssot_version, created_at)
                VALUES (1, ?, ?, ?, ?)
                """,
                (_SCHEMA_VERSION, _POLICY_VERSION, _SSOT_VERSION, _to_iso(datetime.utcnow())),
            )
            return
        if len(rows) > 1:
            conn.execute("DELETE FROM schema_meta WHERE id != 1")

    @staticmethod
    def _validate_schema_version(conn: sqlite3.Connection) -> None:
        """اعتبارسنجی تطابق نسخهٔ Schema پایگاه داده."""

        cursor = conn.execute(
            "SELECT schema_version FROM schema_meta WHERE id = 1"
        )
        row = cursor.fetchone()
        if row is None:
            raise SchemaVersionMismatchError(
                expected_version=_SCHEMA_VERSION,
                actual_version=-1,
                message="رکورد نسخهٔ Schema یافت نشد.",
            )
        actual = int(row[0])
        if actual != _SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                expected_version=_SCHEMA_VERSION,
                actual_version=actual,
                message="نسخهٔ Schema پایگاه داده با نسخهٔ برنامه هم‌خوان نیست.",
            )

    # ------------------------------------------------------------------
    # جدول‌های مرجع مدارس / Crosswalk
    # ------------------------------------------------------------------
    def upsert_schools(self, df: pd.DataFrame) -> None:
        """افزودن/جایگزینی جدول مدارس از DataFrame.

        این تابع دیتافریم ورودی را بدون index در جدول ``schools`` ذخیره
        می‌کند و در صورت وجود ستون «کد مدرسه»، ایندکس یکتا می‌سازد تا
        جست‌وجوی مبتنی‌بر کلید اتصال سریع و پایدار بماند.
        """

        if df is None:
            raise ValueError("DataFrame مدارس تهی است؛ ورودی معتبر بدهید.")
        self.initialize()
        try:
            with self._open_connection() as conn:
                self._replace_table_atomic(
                    conn,
                    table_name="schools",
                    df=df,
                    index_statements=(
                        ['CREATE UNIQUE INDEX IF NOT EXISTS idx_schools_code ON schools("کد مدرسه")']
                        if "کد مدرسه" in df.columns
                        else []
                    ),
                )
        except sqlite3.Error as exc:
            raise DatabaseOperationError("ذخیرهٔ مدارس در SQLite ناکام ماند.") from exc

    def upsert_school_crosswalk(
        self, groups_df: pd.DataFrame, *, synonyms_df: pd.DataFrame | None = None
    ) -> None:
        """ذخیرهٔ Crosswalk مدارس (شیت گروه‌ها و Synonyms).

        - ``groups_df`` در جدول ``school_crosswalk_groups`` ذخیره می‌شود.
        - اگر ``synonyms_df`` موجود باشد، در ``school_crosswalk_synonyms``
          ذخیره می‌شود؛ در غیر این صورت جدول Synonyms حذف نمی‌شود تا دادهٔ
          قبلی باقی بماند.
        """

        if groups_df is None:
            raise ValueError("DataFrame گروه مدارس تهی است؛ ورودی معتبر بدهید.")
        self.initialize()
        try:
            with self._open_connection() as conn:
                self._replace_table_atomic(
                    conn,
                    table_name="school_crosswalk_groups",
                    df=groups_df,
                )
                if synonyms_df is not None:
                    self._replace_table_atomic(
                        conn,
                        table_name="school_crosswalk_synonyms",
                        df=synonyms_df,
                    )
        except sqlite3.Error as exc:
            raise DatabaseOperationError("ذخیرهٔ Crosswalk مدارس ناکام ماند.") from exc

    def load_schools(self) -> pd.DataFrame:
        """بارگذاری جدول مدارس از SQLite با حفظ نوع عددی کلید اتصال."""

        try:
            with self._open_connection() as conn:
                if not _table_exists(conn, "schools"):
                    raise ReferenceDataMissingError(
                        table="schools",
                        message="جدول مدارس در پایگاه داده یافت نشد؛ ابتدا import-schools را اجرا کنید.",
                    )
                df = pd.read_sql_query("SELECT * FROM schools", conn)
            return _coerce_int_columns(df, ["کد مدرسه"])
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise ReferenceDataMissingError(
                    table="schools",
                    message="جدول مدارس در پایگاه داده یافت نشد؛ ابتدا import-schools را اجرا کنید.",
                ) from exc
            raise DatabaseOperationError("خواندن جدول مدارس با خطا مواجه شد.") from exc
        except sqlite3.Error as exc:
            raise DatabaseOperationError("خواندن جدول مدارس با خطا مواجه شد.") from exc

    def load_school_crosswalk(self) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """بارگذاری Crosswalk مدارس از SQLite."""

        try:
            with self._open_connection() as conn:
                if not _table_exists(conn, "school_crosswalk_groups"):
                    raise ReferenceDataMissingError(
                        table="school_crosswalk_groups",
                        message="جدول Crosswalk مدارس یافت نشد؛ ابتدا import-crosswalk را اجرا کنید.",
                    )
                groups_df = pd.read_sql_query("SELECT * FROM school_crosswalk_groups", conn)
                synonyms_df = None
                if _table_exists(conn, "school_crosswalk_synonyms"):
                    synonyms_df = pd.read_sql_query(
                        "SELECT * FROM school_crosswalk_synonyms", conn
                    )
            return _coerce_int_columns(groups_df, ["کد مدرسه", "کد جایگزین"]), synonyms_df
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise ReferenceDataMissingError(
                    table="school_crosswalk_groups",
                    message="جدول Crosswalk مدارس یافت نشد؛ ابتدا import-crosswalk را اجرا کنید.",
                ) from exc
            raise DatabaseOperationError("خواندن Crosswalk مدارس با خطا مواجه شد.") from exc
        except sqlite3.Error as exc:
            raise DatabaseOperationError("خواندن Crosswalk مدارس با خطا مواجه شد.") from exc

    @staticmethod
    def _replace_table_atomic(
        conn: sqlite3.Connection,
        *,
        table_name: str,
        df: pd.DataFrame,
        index_statements: Sequence[str] | None = None,
    ) -> None:
        """جایگزینی اتمیک یک جدول با الگوی temp→swap در یک تراکنش."""

        temp_table = f"_{table_name}_new"
        try:
            conn.execute("BEGIN IMMEDIATE")
            df.to_sql(temp_table, conn, if_exists="replace", index=False)
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
            for stmt in index_statements or []:
                conn.execute(stmt)
        except sqlite3.Error as exc:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            raise DatabaseOperationError("جایگزینی جدول به‌صورت اتمیک با خطا مواجه شد.") from exc


def _to_iso(dt: datetime) -> str:
    """تبدیل datetime به رشتهٔ ISO8601 با پسوند Z."""

    return dt.strftime(_ISO_FORMAT)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """بررسی وجود جدول به‌صورت امن و دترمینیستیک."""

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cursor.fetchone() is not None


def _coerce_int_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """تبدیل ستون‌های اعلام‌شده به نوع Int64 بدون تغییر سایر ستون‌ها."""

    if df is None:
        return pd.DataFrame()
    coerced = df.copy()
    for col in columns:
        if col in coerced.columns:
            coerced[col] = coerced[col].map(_coerce_int_like).astype("Int64")
    return coerced


def _coerce_int_like(value: object) -> int | None:
    """تبدیل مقدار به int در صورت امکان؛ در غیر این صورت None."""

    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None
