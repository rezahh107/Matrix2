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
import io
from typing import Iterable, List, Sequence

import pandas as pd
from pandas.api.types import is_integer_dtype

from app.infra.errors import (
    DatabaseOperationError,
    ReferenceDataMissingError,
    SchemaVersionMismatchError,
)
from app.infra.sqlite_config import configure_connection
from app.infra.sqlite_types import coerce_int_columns as _sqlite_coerce_int_columns
from app.infra.sqlite_types import coerce_int_like as _sqlite_coerce_int_like

_SCHEMA_VERSION = 6
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
                self._ensure_schema_meta_table(conn)
                existing_version = self._get_schema_version(conn)
                if existing_version is None:
                    self._ensure_schema(conn)
                    self._ensure_schema_meta_row(conn, version=_SCHEMA_VERSION)
                elif existing_version < 2:
                    raise SchemaVersionMismatchError(
                        expected_version=_SCHEMA_VERSION,
                        actual_version=existing_version,
                        message="نسخهٔ Schema بسیار قدیمی است و پشتیبانی نمی‌شود؛ پایگاه داده را بازسازی کنید.",
                    )
                elif existing_version < _SCHEMA_VERSION:
                    self._migrate_schema(conn, from_version=existing_version)
                elif existing_version > _SCHEMA_VERSION:
                    raise SchemaVersionMismatchError(
                        expected_version=_SCHEMA_VERSION,
                        actual_version=existing_version,
                        message="نسخهٔ Schema پایگاه داده از نسخهٔ برنامه جدیدتر است.",
                    )
                self._ensure_schema(conn)
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

    # ------------------------------------------------------------------
    # Snapshot های QA/Trace
    # ------------------------------------------------------------------
    def insert_trace_snapshot(
        self,
        *,
        run_id: int,
        trace_df: pd.DataFrame,
        summary_df: pd.DataFrame | None = None,
        history_info_df: pd.DataFrame | None = None,
    ) -> None:
        """ذخیرهٔ Snapshot تریس تخصیص برای یک اجرا.

        داده‌ها به‌صورت JSON دترمینیستیک ذخیره می‌شوند تا رفتار قابل‌آزمایش
        باشد و در مرحلهٔ بازیابی بدون تغییر semantics بازسازی شوند.
        """

        if trace_df is None:
            raise ValueError("دیتافریم تریس تهی است؛ ورودی معتبر بدهید.")
        payload = _serialize_dataframe(trace_df)
        summary_json = _serialize_dataframe(summary_df) if summary_df is not None else None
        history_json = (
            _serialize_dataframe(history_info_df) if history_info_df is not None else None
        )
        try:
            with self._open_connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO trace_snapshots (
                        run_id, trace_json, summary_json, history_info_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (run_id, payload, summary_json, history_json),
                )
                conn.commit()
        except sqlite3.Error as exc:  # pragma: no cover - مسیر غیرمنتظره
            raise DatabaseOperationError("ثبت Snapshot تریس با خطا روبه‌رو شد.") from exc

    def fetch_trace_snapshot(
        self, run_id: int
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]:
        """بازیابی Snapshot تریس برای یک اجرای مشخص."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                """
                SELECT trace_json, summary_json, history_info_json
                FROM trace_snapshots WHERE run_id = ?
                """,
                (run_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None, None, None
        trace_df = _safe_deserialize_dataframe(row["trace_json"], label="trace_json")
        summary_df = _safe_deserialize_dataframe(row["summary_json"], label="summary_json")
        history_df = _safe_deserialize_dataframe(
            row["history_info_json"], label="history_info_json"
        )
        return trace_df, summary_df, history_df

    def insert_qa_snapshot(
        self,
        *,
        run_id: int,
        qa_summary_df: pd.DataFrame | None,
        qa_details_df: pd.DataFrame | None,
    ) -> None:
        """ثبت Snapshot QA شامل خلاصه و جزئیات قوانین."""

        summary_json = _serialize_dataframe(qa_summary_df) if qa_summary_df is not None else None
        details_json = _serialize_dataframe(qa_details_df) if qa_details_df is not None else None
        if summary_json is None and details_json is None:
            logger.debug("Skipping QA snapshot insert; both payloads are empty")
            return
        try:
            with self._open_connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO qa_snapshots (
                        run_id, qa_summary_json, qa_details_json
                    ) VALUES (?, ?, ?)
                    """,
                    (run_id, summary_json, details_json),
                )
                conn.commit()
        except sqlite3.Error as exc:  # pragma: no cover - مسیر غیرمنتظره
            raise DatabaseOperationError("ثبت Snapshot QA با خطا مواجه شد.") from exc

    def fetch_qa_snapshot(
        self, run_id: int
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """بازیابی Snapshot QA برای یک اجرا."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                """
                SELECT qa_summary_json, qa_details_json
                FROM qa_snapshots WHERE run_id = ?
                """,
                (run_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None, None
        summary_df = _safe_deserialize_dataframe(
            row["qa_summary_json"], label="qa_summary_json"
        )
        details_df = _safe_deserialize_dataframe(
            row["qa_details_json"], label="qa_details_json"
        )
        return summary_df, details_df

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

            CREATE TABLE IF NOT EXISTS trace_snapshots (
                run_id INTEGER PRIMARY KEY,
                trace_json TEXT NOT NULL,
                summary_json TEXT,
                history_info_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS qa_snapshots (
                run_id INTEGER PRIMARY KEY,
                qa_summary_json TEXT,
                qa_details_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reference_meta (
                table_name TEXT PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                source TEXT,
                row_count INTEGER
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

            CREATE TABLE IF NOT EXISTS managers_reference (
                "نام مدیر" TEXT,
                "مرکز گلستان صدرا" INTEGER
            );

            CREATE TABLE IF NOT EXISTS students_cache (
                student_id TEXT,
                "کد ملی" TEXT,
                "کدرشته" INTEGER,
                "گروه آزمایشی" TEXT,
                "جنسیت" INTEGER,
                "دانش آموز فارغ" INTEGER,
                "مرکز گلستان صدرا" INTEGER,
                "مالی حکمت بنیاد" INTEGER,
                "کد مدرسه" INTEGER,
                school_code_raw TEXT,
                school_code_norm INTEGER,
                school_status_resolved INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_students_cache_student_id
            ON students_cache(student_id);

            CREATE INDEX IF NOT EXISTS idx_students_cache_join_keys
            ON students_cache("کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", "مالی حکمت بنیاد", "کد مدرسه");

            CREATE TABLE IF NOT EXISTS mentor_pool_cache (
                mentor_id TEXT,
                "کد کارمندی پشتیبان" TEXT,
                "کدرشته" INTEGER,
                "گروه آزمایشی" TEXT,
                "جنسیت" INTEGER,
                "دانش آموز فارغ" INTEGER,
                "مرکز گلستان صدرا" INTEGER,
                "مالی حکمت بنیاد" INTEGER,
                "کد مدرسه" INTEGER,
                remaining_capacity REAL,
                allocations_new INTEGER,
                occupancy_ratio REAL
            );

            CREATE TABLE IF NOT EXISTS forms_entries (
                entry_id TEXT,
                form_id TEXT,
                received_at TEXT,
                normalized_at TEXT,
                PRIMARY KEY(entry_id)
            );

            CREATE INDEX IF NOT EXISTS idx_forms_entries_form_id
            ON forms_entries(form_id);

            CREATE INDEX IF NOT EXISTS idx_mentor_pool_cache_mentor_id
            ON mentor_pool_cache(mentor_id);

            CREATE INDEX IF NOT EXISTS idx_mentor_pool_cache_join_keys
            ON mentor_pool_cache("کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", "مالی حکمت بنیاد", "کد مدرسه");
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
    def _ensure_schema_meta_row(conn: sqlite3.Connection, *, version: int) -> None:
        """تضمین وجود رکورد نسخهٔ Schema با درج اولیه در صورت نبود."""

        conn.execute(
            """
            INSERT OR IGNORE INTO schema_meta (id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, ?, ?, ?, ?)
            """,
            (version, _POLICY_VERSION, _SSOT_VERSION, _to_iso(datetime.utcnow())),
        )

    @staticmethod
    def _get_schema_version(conn: sqlite3.Connection) -> int | None:
        cursor = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1")
        row = cursor.fetchone()
        return int(row[0]) if row is not None else None

    @staticmethod
    def _validate_schema_version(conn: sqlite3.Connection) -> None:
        """اعتبارسنجی تطابق نسخهٔ Schema پایگاه داده."""

        actual = LocalDatabase._get_schema_version(conn)
        if actual is None:
            raise SchemaVersionMismatchError(
                expected_version=_SCHEMA_VERSION,
                actual_version=-1,
                message="رکورد نسخهٔ Schema یافت نشد.",
            )
        if actual != _SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                expected_version=_SCHEMA_VERSION,
                actual_version=actual,
                message="نسخهٔ Schema پایگاه داده با نسخهٔ برنامه هم‌خوان نیست.",
            )

    def _migrate_schema(self, conn: sqlite3.Connection, *, from_version: int) -> None:
        """مهاجرت نسخهٔ Schema به نسخهٔ جاری."""

        version = from_version
        while version < _SCHEMA_VERSION:
            if version == 2:
                self._migrate_v2_to_v3(conn)
                version = 3
                continue
            if version == 3:
                self._migrate_v3_to_v4(conn)
                version = 4
                continue
            if version == 4:
                self._migrate_v4_to_v5(conn)
                version = 5
                continue
            if version == 5:
                self._migrate_v5_to_v6(conn)
                version = 6
                continue
            raise SchemaVersionMismatchError(
                expected_version=_SCHEMA_VERSION,
                actual_version=version,
                message="نسخهٔ Schema پشتیبانی نمی‌شود.",
            )

    def _migrate_v2_to_v3(self, conn: sqlite3.Connection) -> None:
        """افزودن جداول Snapshot برای نسخهٔ ۳."""

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trace_snapshots (
                run_id INTEGER PRIMARY KEY,
                trace_json TEXT NOT NULL,
                summary_json TEXT,
                history_info_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS qa_snapshots (
                run_id INTEGER PRIMARY KEY,
                qa_summary_json TEXT,
                qa_details_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute(
            "UPDATE schema_meta SET schema_version = ? WHERE id = 1", (3,)
        )

    def _migrate_v3_to_v4(self, conn: sqlite3.Connection) -> None:
        """افزودن جدول متادیتای کش مراجع برای نسخهٔ ۴."""

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS reference_meta (
                table_name TEXT PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                source TEXT,
                row_count INTEGER
            );
            """
        )
        conn.execute(
            "UPDATE schema_meta SET schema_version = ? WHERE id = 1", (4,)
        )

    def _migrate_v4_to_v5(self, conn: sqlite3.Connection) -> None:
        """افزودن جدول کش ورودی‌های فرم برای نسخهٔ ۵."""

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS forms_entries (
                entry_id TEXT,
                form_id TEXT,
                received_at TEXT,
                normalized_at TEXT,
                PRIMARY KEY(entry_id)
            );

            CREATE INDEX IF NOT EXISTS idx_forms_entries_form_id
            ON forms_entries(form_id);
            """
        )
        conn.execute(
            "UPDATE schema_meta SET schema_version = ? WHERE id = 1", (5,),
        )

    def _migrate_v5_to_v6(self, conn: sqlite3.Connection) -> None:
        """افزودن جدول مرجع مدیران برای نسخهٔ ۶."""

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS managers_reference (
                "نام مدیر" TEXT,
                "مرکز گلستان صدرا" INTEGER
            );
            """
        )
        conn.execute(
            "UPDATE schema_meta SET schema_version = ? WHERE id = 1", (6,),
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
                self.record_reference_meta(
                    table_name="schools",
                    source=None,
                    row_count=int(df.shape[0]),
                    conn=conn,
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

    # ------------------------------------------------------------------
    # کش گزارش دانش‌آموز و استخر منتورها
    # ------------------------------------------------------------------
    def upsert_students_cache(
        self, df: pd.DataFrame, *, join_keys: Sequence[str]
    ) -> None:
        """جایگزینی دیتافریم دانش‌آموزان در جدول ``students_cache``.

        دیتافریم ورودی باید پیش‌تر بر اساس Policy نرمال شده باشد؛ این تابع تنها
        ذخیره‌سازی اتمیک و ساخت ایندکس روی شناسه و کلیدهای اتصال را بر عهده دارد.
        """

        if df is None:
            raise ValueError("DataFrame دانش‌آموزان تهی است؛ ورودی معتبر بدهید.")
        self.initialize()
        _validate_join_keys(df, join_keys)
        index_statements = _build_index_statements(
            table_name="students_cache",
            df=df,
            unique_candidates=("student_id",),
            join_keys=join_keys,
        )
        try:
            with self._open_connection() as conn:
                self._replace_table_atomic(
                    conn,
                    table_name="students_cache",
                    df=df,
                    index_statements=index_statements,
                )
        except sqlite3.Error as exc:
            raise DatabaseOperationError(
                "ذخیرهٔ کش دانش‌آموزان در SQLite ناکام ماند."
            ) from exc

    def load_students_cache(self, *, join_keys: Sequence[str]) -> pd.DataFrame:
        """خواندن دیتافریم دانش‌آموزان از کش SQLite با حفظ نوع کلیدها."""

        try:
            with self._open_connection() as conn:
                if not _table_exists(conn, "students_cache"):
                    raise ReferenceDataMissingError(
                        table="students_cache",
                        message="کش دانش‌آموز یافت نشد؛ ابتدا import-students را اجرا کنید.",
                    )
                df = pd.read_sql_query("SELECT * FROM students_cache", conn)
                if df.empty:
                    raise ReferenceDataMissingError(
                        table="students_cache",
                        message="کش دانش‌آموز خالی است؛ ابتدا import-students را اجرا کنید.",
                    )
            return _coerce_int_columns(df, join_keys)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise ReferenceDataMissingError(
                    table="students_cache",
                    message="کش دانش‌آموز یافت نشد؛ ابتدا import-students را اجرا کنید.",
                ) from exc
            raise DatabaseOperationError("خواندن کش دانش‌آموزان با خطا مواجه شد.") from exc
        except sqlite3.Error as exc:
            raise DatabaseOperationError("خواندن کش دانش‌آموزان با خطا مواجه شد.") from exc

    def upsert_mentor_pool_cache(
        self, df: pd.DataFrame, *, join_keys: Sequence[str]
    ) -> None:
        """جایگزینی دیتافریم استخر منتورها در جدول ``mentor_pool_cache``."""

        if df is None:
            raise ValueError("DataFrame استخر منتورها تهی است؛ ورودی معتبر بدهید.")
        self.initialize()
        _validate_join_keys(df, join_keys)
        index_statements = _build_index_statements(
            table_name="mentor_pool_cache",
            df=df,
            unique_candidates=("mentor_id", "کد کارمندی پشتیبان"),
            join_keys=join_keys,
        )
        try:
            with self._open_connection() as conn:
                self._replace_table_atomic(
                    conn,
                    table_name="mentor_pool_cache",
                    df=df,
                    index_statements=index_statements,
                )
        except sqlite3.Error as exc:
            raise DatabaseOperationError(
                "ذخیرهٔ کش استخر منتورها در SQLite ناکام ماند."
            ) from exc

    def load_mentor_pool_cache(self, *, join_keys: Sequence[str]) -> pd.DataFrame:
        """خواندن دیتافریم استخر منتورها از کش SQLite با حفظ نوع کلیدها."""

        try:
            with self._open_connection() as conn:
                if not _table_exists(conn, "mentor_pool_cache"):
                    raise ReferenceDataMissingError(
                        table="mentor_pool_cache",
                        message="کش استخر منتورها یافت نشد؛ ابتدا import-mentors را اجرا کنید.",
                    )
                df = pd.read_sql_query("SELECT * FROM mentor_pool_cache", conn)
                if df.empty:
                    raise ReferenceDataMissingError(
                        table="mentor_pool_cache",
                        message="کش استخر منتورها خالی است؛ ابتدا import-mentors را اجرا کنید.",
                    )
            return _coerce_int_columns(df, join_keys)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise ReferenceDataMissingError(
                    table="mentor_pool_cache",
                    message="کش استخر منتورها یافت نشد؛ ابتدا import-mentors را اجرا کنید.",
                ) from exc
            raise DatabaseOperationError("خواندن کش استخر منتورها با خطا مواجه شد.") from exc
        except sqlite3.Error as exc:
            raise DatabaseOperationError("خواندن کش استخر منتورها با خطا مواجه شد.") from exc

    # ------------------------------------------------------------------
    # ورودی‌های فرم وردپرس / Gravity Forms
    # ------------------------------------------------------------------
    def upsert_forms_entries(
        self,
        df: pd.DataFrame,
        *,
        source: str | None = None,
    ) -> None:
        """ذخیرهٔ دیتافریم نرمال‌شدهٔ ورودی‌های فرم در جدول ``forms_entries``.

        ورودی باید شامل ستون ``entry_id`` باشد. ستون‌های زمان (received_at و
        normalized_at) به‌صورت ISO8601 ذخیره می‌شوند تا بازسازی دترمینیستیک
        آسان شود.
        """

        if df is None:
            raise ValueError("DataFrame ورودی‌های فرم تهی است؛ ورودی معتبر بدهید.")
        if "entry_id" not in df.columns:
            raise ValueError("ستون entry_id برای ذخیرهٔ کش فرم ضروری است.")

        normalized = _normalize_forms_timestamps(df)
        normalized = normalized.dropna(subset=["entry_id"])\
            .drop_duplicates(subset=["entry_id"], keep="last")\
            .sort_values(by=["received_at", "entry_id"], kind="stable")\
            .reset_index(drop=True)

        self.initialize()
        index_statements = _build_index_statements(
            table_name="forms_entries",
            df=normalized,
            unique_candidates=("entry_id",),
            join_keys=(),
        )
        try:
            with self._open_connection() as conn:
                self._replace_table_atomic(
                    conn,
                    table_name="forms_entries",
                    df=normalized,
                    index_statements=index_statements,
                )
                self.record_reference_meta(
                    table_name="forms_entries",
                    source=source,
                    row_count=int(normalized.shape[0]),
                    conn=conn,
                )
        except sqlite3.Error as exc:  # pragma: no cover - مسیر غیرمنتظره
            raise DatabaseOperationError("ثبت کش ورودی‌های فرم با خطا مواجه شد.") from exc

    def load_forms_entries(self) -> pd.DataFrame:
        """بازیابی کش ورودی‌های فرم به‌صورت DataFrame."""

        with self._open_connection() as conn:
            if not _table_exists(conn, "forms_entries"):
                raise ReferenceDataMissingError(
                    table="forms_entries",
                    message="جدول forms_entries در پایگاه داده یافت نشد؛ ابتدا sync-forms را اجرا کنید.",
                )
            df = pd.read_sql_query(
                "SELECT * FROM forms_entries ORDER BY received_at ASC, entry_id ASC", conn
            )
        if df.empty:
            return df
        restored = _restore_timestamp_columns(
            df, columns=("received_at", "normalized_at")
        )
        return restored

    def record_reference_meta(
        self,
        *,
        table_name: str,
        source: str | None,
        row_count: int | None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        """ثبت زمان به‌روزرسانی کش مرجع برای مصرف مخازن اشتراکی."""

        needs_close = False
        target_conn = conn
        if target_conn is None:
            target_conn = self._open_connection()
            needs_close = True
        try:
            target_conn.execute(
                """
                INSERT OR REPLACE INTO reference_meta(table_name, refreshed_at, source, row_count)
                VALUES (?, ?, ?, ?)
                """,
                (table_name, _to_iso(datetime.utcnow()), source, row_count),
            )
            target_conn.commit()
        finally:
            if needs_close:
                target_conn.close()

    def fetch_reference_meta(self, table_name: str) -> tuple[str, str | None, int | None] | None:
        """بازیابی متادیتای کش مرجع (زمان، منبع، شمارش ردیف)."""

        with self._open_connection() as conn:
            cursor = conn.execute(
                "SELECT refreshed_at, source, row_count FROM reference_meta WHERE table_name = ?",
                (table_name,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return row[0], row[1], row[2]

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
        backup_table = f"_{table_name}_backup"
        try:
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            conn.execute(f"DROP TABLE IF EXISTS {backup_table}")
            df.to_sql(temp_table, conn, if_exists="replace", index=False)

            conn.execute("BEGIN IMMEDIATE")
            if _table_exists(conn, table_name):
                conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_table}")
            conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
            for stmt in index_statements or []:
                conn.execute(stmt)
            conn.execute(f"DROP TABLE IF EXISTS {backup_table}")
            conn.commit()
        except sqlite3.Error as exc:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                conn.execute(f"DROP TABLE IF EXISTS {backup_table}")
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

    return _sqlite_coerce_int_columns(df, list(columns))


def _normalize_timestamp_columns(
    df: pd.DataFrame, *, columns: Iterable[str]
) -> pd.DataFrame:
    """تبدیل ستون‌های زمانی به datetime و ذخیره به‌صورت ISO8601."""

    normalized = df.copy()
    for col in columns:
        if col in normalized.columns:
            series = pd.to_datetime(normalized[col], errors="coerce", utc=True)
            normalized[col] = series.dt.strftime(_ISO_FORMAT)
    return normalized


def _restore_timestamp_columns(
    df: pd.DataFrame, *, columns: Iterable[str]
) -> pd.DataFrame:
    """بازگردانی ستون‌های زمانی به نوع datetime با timezone آگاه."""

    restored = df.copy()
    for col in columns:
        if col in restored.columns:
            restored[col] = pd.to_datetime(restored[col], errors="coerce", utc=True)
    return restored


def _normalize_forms_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی ستون‌های زمانی forms_entries به یک گذر ثابت.

    دریافت دیتافریم شامل ``received_at`` و ``normalized_at`` (در صورت عدم وجود
    normalized_at در ورودی، مقدار UTC فعلی اضافه می‌شود)، تبدیل همهٔ مقادیر به
    datetime آگاه از timezone، و سپس سریال‌سازی ISO8601 با پسوند ``Z``.
    """

    normalized = df.copy()
    if "normalized_at" not in normalized.columns:
        normalized["normalized_at"] = datetime.utcnow()
    for col in ("received_at", "normalized_at"):
        if col in normalized.columns:
            normalized[col] = (
                pd.to_datetime(normalized[col], errors="coerce", utc=True)
                .dt.strftime(_ISO_FORMAT)
            )
    return normalized


def _serialize_dataframe(df: pd.DataFrame | None) -> str | None:
    """سریال‌سازی دترمینیستیک دیتافریم به JSON orient=split."""

    if df is None:
        return None
    normalized = df.copy()
    return normalized.to_json(
        orient="split", force_ascii=False, date_format="iso", double_precision=15
    )


def _safe_deserialize_dataframe(payload: str | bytes | None, *, label: str) -> pd.DataFrame | None:
    """بازسازی امن دیتافریم از JSON ذخیره‌شده به‌صورت split."""

    if payload in (None, b"", ""):
        return None
    try:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return pd.read_json(io.StringIO(payload), orient="split")
    except Exception:
        logger.exception("Failed to deserialize DataFrame payload for %s", label)
        return None


def _coerce_int_like(value: object) -> int | None:
    """تبدیل مقدار به int در صورت امکان؛ در غیر این صورت None."""

    return _sqlite_coerce_int_like(value)


def _normalize_index_name(name: str) -> str:
    safe = name.replace(" ", "_")
    return "".join(ch for ch in safe if ch.isalnum() or ch == "_")


def _build_index_statements(
    *,
    table_name: str,
    df: pd.DataFrame,
    unique_candidates: Sequence[str] = (),
    join_keys: Sequence[str] = (),
) -> list[str]:
    """تولید ایندکس‌های پایدار برای کلیدهای طبیعی و ۶ کلید اتصال Policy.

    این تابع در تمام کش‌های مرجع استفاده می‌شود تا یکتا بودن شناسه‌های
    طبیعی (student_id, mentor_id, کد مدرسه) و ایندکس‌گذاری join_keys بر اساس
    Policy/SSoT در یک مکان متمرکز باشد.
    """

    statements: list[str] = []
    seen: set[str] = set()
    for column in unique_candidates:
        if column in df.columns:
            idx = _normalize_index_name(f"idx_{table_name}_{column}_uniq")
            if idx not in seen:
                statements.append(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS {idx} ON {table_name}("{column}")'
                )
                seen.add(idx)
    for column in join_keys:
        if column in df.columns:
            idx = _normalize_index_name(f"idx_{table_name}_{column}")
            if idx not in seen:
                statements.append(
                    f'CREATE INDEX IF NOT EXISTS {idx} ON {table_name}("{column}")'
                )
                seen.add(idx)
    return statements


def _validate_join_keys(df: pd.DataFrame, join_keys: Sequence[str]) -> None:
    """تضمین می‌کند کلیدهای اتصال پیش از ذخیره از نوع عددی باشند."""

    missing = [col for col in join_keys if col not in df.columns]
    if missing:
        raise ValueError(f"ستون‌های کلید اتصال وجود ندارند: {missing}")
    for col in join_keys:
        series = df[col]
        if not is_integer_dtype(series):
            try:
                df[col] = series.astype("Int64")
            except Exception as exc:  # pragma: no cover - مسیر خطا
                raise ValueError(f"ستون {col} باید عددی باشد.") from exc
