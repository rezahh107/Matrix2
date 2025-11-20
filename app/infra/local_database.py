"""پایگاه دادهٔ محلی SQLite برای نگهداشت تاریخچهٔ اجرا.

این ماژول یک لایهٔ نازک روی :mod:`sqlite3` است تا بدون وابستگی خارجی
لاگ اجرای تخصیص را در حالت آفلاین ذخیره کند. Schema کاملاً ایستا و
دترمینیستیک است و در اولین استفاده ساخته می‌شود.

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
from typing import Iterable, List

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

    def connect(self) -> sqlite3.Connection:
        """ایجاد اتصال با فعال‌سازی کلید خارجی و Row factory.

        اتصال‌ها همیشه Row factory را روی ``sqlite3.Row`` تنظیم می‌کنند تا
        فراخوانی‌کننده بتواند به‌صورت نام‌دار به ستون‌ها دسترسی داشته باشد.
        """

        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def initialize(self) -> None:
        """ایجاد Schema در صورت نبود؛ عملیات idempotent است."""

        with self.connect() as conn:
            self._ensure_schema(conn)
            conn.commit()
        logger.debug("Local DB schema ensured at %s", self.path)

    def insert_run(self, record: RunRecord) -> int:
        """درج ردیف جدید در جدول ``runs`` و بازگرداندن شناسه."""

        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs (
                    run_uuid, started_at, finished_at,
                    policy_version, ssot_version,
                    entrypoint, cli_args, db_path,
                    input_files_json, input_hashes_json,
                    total_students, total_allocated, total_unallocated,
                    history_metrics_json, qa_summary_json,
                    status, message
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
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO run_metrics (
                    run_id, metric_key, metric_value
                ) VALUES (?, ?, ?)
                """,
                payload,
            )
            conn.commit()

    def insert_qa_summary(self, rows: Iterable[QaSummaryRow]) -> None:
        """ثبت خلاصهٔ QA برای یک اجرا."""

        payload = [
            (row.run_id, row.violation_code, row.severity, row.count) for row in rows
        ]
        if not payload:
            logger.debug("No QA rows to insert for qa_summary")
            return
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO qa_summary (run_id, violation_code, severity, count)
                VALUES (?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()

    def fetch_runs(self) -> List[sqlite3.Row]:
        """بازیابی همهٔ اجراها (برای تست/دیباگ)."""

        with self.connect() as conn:
            cursor = conn.execute(
                "SELECT * FROM runs ORDER BY started_at ASC, id ASC"
            )
            return cursor.fetchall()

    def fetch_metrics_for_run(self, run_id: int) -> List[sqlite3.Row]:
        """بازیابی KPI تاریخچه برای یک شناسه اجرا."""

        with self.connect() as conn:
            cursor = conn.execute(
                "SELECT * FROM run_metrics WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            )
            return cursor.fetchall()

    def fetch_qa_summary(self, run_id: int) -> List[sqlite3.Row]:
        """بازیابی خلاصهٔ QA برای یک اجرا."""

        with self.connect() as conn:
            cursor = conn.execute(
                "SELECT * FROM qa_summary WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            )
            return cursor.fetchall()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        """ساخت جدول‌های runs/run_metrics/qa_summary به‌صورت idempotent."""

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
            """
        )


def _to_iso(dt: datetime) -> str:
    """تبدیل datetime به رشتهٔ ISO8601 با پسوند Z."""

    return dt.strftime(_ISO_FORMAT)
