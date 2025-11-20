"""لایهٔ ثبت تاریخچهٔ اجرا با تکیه بر پایگاه دادهٔ محلی.

این ماژول محاسبات ورودی (مسیر فایل‌ها، هش محتوا، KPI) را به
ساختارهای `local_database` تبدیل و در SQLite ذخیره می‌کند. Core از این
ماژول بی‌خبر است و تنها Infra مسئول فراخوانی آن است. خطاهای DB صرفاً
لاگ می‌شوند تا تجربهٔ کاربر و جریان Excel/Core مختل نشود.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.infra.local_database import LocalDatabase, QaSummaryRow, RunMetricRow, RunRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunContext:
    """اطلاعات پایهٔ اجرای تخصیص برای ثبت در تاریخچه."""

    command: str
    cli_args: str | None
    policy_version: str
    ssot_version: str
    started_at: datetime
    completed_at: datetime
    success: bool
    message: str
    input_students_path: Path | None
    input_pool_path: Path | None
    output_path: Path | None
    policy_path: Path | None
    total_students: int | None
    allocated_students: int | None
    unallocated_students: int | None


@dataclass(frozen=True)
class QaOutcome:
    """نتیجهٔ خلاصهٔ QA."""

    passed: bool
    violation_count: int


def _hash_file(path: Path | None, *, chunk_size: int = 8192) -> str | None:
    """محاسبهٔ هش SHA256 فایل؛ در صورت نبود فایل، None."""

    if path is None or not path.exists() or not path.is_file():
        return None
    sha = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            sha.update(chunk)
    return sha.hexdigest()


def _build_run_record(
    run_uuid: str,
    ctx: RunContext,
    *,
    db_path: Path | None,
    history_metrics: pd.DataFrame | None,
    qa_outcome: "QaOutcome | None",
) -> RunRecord:
    """ساخت ``RunRecord`` بر پایهٔ زمینهٔ اجرا و داده‌های QA/متریک.

    این تابع JSON ورودی/هش‌ها را آماده می‌کند و مجموعهٔ حداقلی از
    متادیتا را برای درج در جدول ``runs`` فراهم می‌سازد.
    """
    input_files = {
        "students": str(ctx.input_students_path) if ctx.input_students_path else None,
        "pool": str(ctx.input_pool_path) if ctx.input_pool_path else None,
        "policy": str(ctx.policy_path) if ctx.policy_path else None,
        "output": str(ctx.output_path) if ctx.output_path else None,
    }
    input_hashes = {
        "students": _hash_file(ctx.input_students_path),
        "pool": _hash_file(ctx.input_pool_path),
        "policy": _hash_file(ctx.policy_path),
        "output": None,
    }
    history_payload = (
        history_metrics.to_dict(orient="records")
        if isinstance(history_metrics, pd.DataFrame) and not history_metrics.empty
        else None
    )
    qa_payload = (
        {"passed": qa_outcome.passed, "violation_count": qa_outcome.violation_count}
        if qa_outcome is not None
        else None
    )
    return RunRecord(
        run_uuid=run_uuid,
        started_at=ctx.started_at,
        finished_at=ctx.completed_at,
        policy_version=ctx.policy_version,
        ssot_version=ctx.ssot_version,
        entrypoint=ctx.command,
        cli_args=ctx.cli_args,
        db_path=str(db_path) if db_path is not None else None,
        input_files_json=json.dumps(input_files, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        total_students=ctx.total_students,
        total_allocated=ctx.allocated_students,
        total_unallocated=ctx.unallocated_students,
        history_metrics_json=(
            json.dumps(history_payload, ensure_ascii=False) if history_payload else None
        ),
        qa_summary_json=(json.dumps(qa_payload, ensure_ascii=False) if qa_payload else None),
        status="success" if ctx.success else "failed",
        message=ctx.message,
    )


def _build_metric_rows(run_id: int, history_metrics: pd.DataFrame | None) -> list[RunMetricRow]:
    """تبدیل DataFrame متریک به ردیف‌های ``run_metrics``."""
    if history_metrics is None or history_metrics.empty:
        return []
    rows: list[RunMetricRow] = []
    numeric_columns = [
        "students_total",
        "history_already_allocated",
        "history_no_history_match",
        "history_missing_or_invalid",
        "same_history_mentor_true",
        "same_history_mentor_ratio",
    ]
    for _, item in history_metrics.iterrows():
        channel = str(item.get("allocation_channel", ""))
        for col in numeric_columns:
            metric_value = item.get(col, 0)
            try:
                value = float(metric_value)
            except Exception:
                value = 0.0
            rows.append(
                RunMetricRow(
                    run_id=run_id,
                    metric_key=f"{channel}.{col}",
                    metric_value=value,
                )
            )
    return rows


def _build_qa_rows(run_id: int, qa_outcome: QaOutcome | None) -> list[QaSummaryRow]:
    """تبدیل خلاصهٔ QA به ردیف‌های ``qa_summary`` برای درج."""
    if qa_outcome is None:
        return []
    return [
        QaSummaryRow(
            run_id=run_id,
            violation_code="TOTAL",
            severity="info" if qa_outcome.passed else "warning",
            count=qa_outcome.violation_count,
        )
    ]


def log_allocation_run(
    *,
    run_uuid: str,
    ctx: RunContext,
    history_metrics: pd.DataFrame | None,
    qa_outcome: QaOutcome | None,
    db: LocalDatabase | None,
) -> None:
    """ثبت کامل اجرای تخصیص/RuleEngine در SQLite.

    اگر ``db`` تهی باشد یا خطایی در لایهٔ ذخیره رخ دهد، تنها لاگ
    ثبت می‌شود و جریان اصلی متوقف نمی‌شود تا تجربهٔ کاربر/GUI دچار
    اختلال نشود.
    """

    if db is None:
        logger.info("Local DB logging disabled; skipping run_uuid=%s", run_uuid)
        return

    try:
        db.initialize()
        run_record = _build_run_record(
            run_uuid,
            ctx,
            db_path=db.path,
            history_metrics=history_metrics,
            qa_outcome=qa_outcome,
        )
        run_id = db.insert_run(run_record)
        metric_rows = _build_metric_rows(run_id, history_metrics)
        if metric_rows:
            db.insert_run_metrics(metric_rows)
        qa_rows = _build_qa_rows(run_id, qa_outcome)
        if qa_rows:
            db.insert_qa_summary(qa_rows)
    except Exception:
        logger.exception(
            "Failed to log allocation run to local DB (run_uuid=%s)", run_uuid
        )


def build_run_context(
    *,
    command: str,
    cli_args: str | None,
    policy_version: str,
    ssot_version: str,
    started_at: datetime,
    completed_at: datetime,
    success: bool,
    message: str,
    input_students: Path | None,
    input_pool: Path | None,
    output: Path | None,
    policy_path: Path | None,
    total_students: int | None,
    allocated_students: int | None,
    unallocated_students: int | None,
) -> RunContext:
    """ساخت RunContext استاندارد برای ثبت در تاریخچه."""

    return RunContext(
        command=command,
        cli_args=cli_args,
        policy_version=policy_version,
        ssot_version=ssot_version,
        started_at=started_at.astimezone(timezone.utc),
        completed_at=completed_at.astimezone(timezone.utc),
        success=success,
        message=message,
        input_students_path=input_students,
        input_pool_path=input_pool,
        output_path=output,
        policy_path=policy_path,
        total_students=total_students,
        allocated_students=allocated_students,
        unallocated_students=unallocated_students,
    )


def summarize_qa(qa_report: object | None) -> QaOutcome | None:
    """استخراج خلاصهٔ QA از آبجکت invariant در Infra/QA."""

    if qa_report is None:
        return None
    passed = getattr(qa_report, "passed", None)
    violations = getattr(qa_report, "violations", None)
    if passed is None or violations is None:
        return None
    try:
        violation_count = len(violations)
    except Exception:
        violation_count = 0
    return QaOutcome(passed=bool(passed), violation_count=int(violation_count))
