"""خروجی Excel برای گزارش اعتبارسنجی QA (Policy-First)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import pandas as pd

from app.core.qa.invariants import QaReport
from app.infra.io_utils import write_xlsx_atomic

__all__ = ["QaValidationContext", "export_qa_validation"]

_RULE_DESCRIPTIONS: dict[str, str] = {
    "QA_RULE_STU_01": "تطابق شمار دانش‌آموز در ورودی/خروجی‌ها",
    "QA_RULE_STU_02": "شمار دانش‌آموز به ازای هر منتور مطابق Inspactor/Allocation",
    "QA_RULE_JOIN_01": "سلامت ستون‌های join ماتریس",
    "QA_RULE_SCHOOL_01": "تفکیک منتورهای آزاد و مقید به مدرسه",
    "QA_RULE_ALLOC_01": "کنترل ظرفیت و نسبت اشغال منتورها",
}


@dataclass(frozen=True)
class QaValidationContext:
    """ورودی‌های تکمیلی برای ساخت ورک‌بوک اعتبارسنجی QA."""

    matrix: pd.DataFrame | None = None
    allocation: pd.DataFrame | None = None
    allocation_summary: pd.DataFrame | None = None
    inspactor: pd.DataFrame | None = None
    invalid_mentors: pd.DataFrame | None = None
    meta: Mapping[str, object] | None = None


def _summary_sheet(report: QaReport) -> pd.DataFrame:
    summary = report.to_summary_frame(descriptions=_RULE_DESCRIPTIONS)
    if summary.empty:
        return summary
    summary = summary.sort_values(by=["rule_id"], kind="stable").reset_index(drop=True)
    return summary


def _students_per_mentor_sheet(report: QaReport) -> pd.DataFrame:
    details = report.to_details_frame("QA_RULE_STU_02")
    if details.empty:
        return pd.DataFrame(columns=["mentor_id", "expected", "assigned", "message", "level"])
    preferred_order = ["mentor_id", "expected", "assigned", "message", "level"]
    cols = [col for col in preferred_order if col in details.columns]
    remaining = [col for col in details.columns if col not in cols]
    ordered = details.loc[:, cols + remaining]
    return ordered


def _school_binding_sheet(report: QaReport) -> pd.DataFrame:
    details = report.to_details_frame("QA_RULE_SCHOOL_01")
    rows: list[dict[str, object]] = []
    for _, row in details.iterrows():
        mentor_ids = row.get("mentor_ids")
        if isinstance(mentor_ids, (list, tuple)) and mentor_ids:
            for mentor_id in mentor_ids:
                rows.append({
                    "mentor_id": mentor_id,
                    "issue": row.get("message"),
                    "level": row.get("level"),
                })
        else:
            rows.append({
                "mentor_id": row.get("mentor_id"),
                "issue": row.get("message"),
                "level": row.get("level"),
            })
    if not rows:
        return pd.DataFrame(columns=["mentor_id", "issue", "level"])
    frame = pd.DataFrame(rows)
    return frame.sort_values(by=["mentor_id", "issue"], kind="stable").reset_index(drop=True)


def _allocation_capacity_sheet(report: QaReport) -> pd.DataFrame:
    details = report.to_details_frame("QA_RULE_ALLOC_01")
    if details.empty:
        return pd.DataFrame(
            columns=["mentor_id", "assigned", "remaining", "allocations_new", "expected_ratio", "actual_ratio", "level"]
        )
    preferred = ["mentor_id", "assigned", "remaining", "allocations_new", "expected_ratio", "actual_ratio", "message", "level"]
    cols = [col for col in preferred if col in details.columns]
    remaining = [col for col in details.columns if col not in cols]
    ordered = details.loc[:, cols + remaining]
    return ordered


def _join_key_sheet(report: QaReport) -> pd.DataFrame:
    details = report.to_details_frame("QA_RULE_JOIN_01")
    if details.empty:
        return pd.DataFrame(columns=["message", "level"])
    return details


def _stu_count_sheet(report: QaReport) -> pd.DataFrame:
    details = report.to_details_frame("QA_RULE_STU_01")
    if details.empty:
        return pd.DataFrame(columns=["student_report", "matrix", "allocation", "message", "level"])
    preferred = ["student_report", "matrix", "allocation", "message", "level"]
    cols = [col for col in preferred if col in details.columns]
    remaining = [col for col in details.columns if col not in cols]
    return details.loc[:, cols + remaining]


def _meta_sheet(context: QaValidationContext, report: QaReport) -> pd.DataFrame:
    meta: dict[str, object] = {}
    if context.meta:
        meta.update(context.meta)
    meta.setdefault("generated_at", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    meta.setdefault("rules_total", len(report.results))
    meta.setdefault("rules_failed", sum(not r.passed for r in report.results))
    meta.setdefault("policy_version", meta.get("policy_version"))
    meta.setdefault("ssot_version", meta.get("ssot_version"))
    return pd.json_normalize([meta])


def export_qa_validation(
    report: QaReport,
    *,
    output: Path,
    context: QaValidationContext | None = None,
) -> None:
    """نوشتن ورک‌بوک اعتبارسنجی QA به‌صورت اتمیک و قابل تکرار."""

    ctx = context or QaValidationContext()
    sheets: dict[str, pd.DataFrame] = {
        "summary": _summary_sheet(report),
        "students_per_mentor": _students_per_mentor_sheet(report),
        "school_binding_issues": _school_binding_sheet(report),
        "allocation_capacity": _allocation_capacity_sheet(report),
        "join_keys": _join_key_sheet(report),
        "student_counts": _stu_count_sheet(report),
        "meta": _meta_sheet(ctx, report),
    }
    sheet_modes = {name: None for name in sheets}
    write_xlsx_atomic(sheets, output, header_mode=None, sheet_header_modes=sheet_modes)

