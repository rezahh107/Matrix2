"""خروجی دلایل انتخاب پشتیبان با تاکید بر مسیر ستون‌های دانش‌آموز.

این ماژول Behavior جدیدی اضافه نمی‌کند و تنها مسیر داده‌های حیاتی مثل
«کدملی» و «وضعیت تحصیلی» را مستند و برای دیباگ قابل‌دسترسی می‌کند.
"""

from __future__ import annotations

from typing import Mapping

import pandas as pd

from app.core.policy_loader import PolicyConfig
from app.core.reason.selection_reason import build_selection_reason_rows
from app.infra.excel.exporter import write_selection_reasons_sheet

__all__ = ["export_selection_reasons_with_sources"]


def export_selection_reasons_with_sources(
    allocations: pd.DataFrame,
    students: pd.DataFrame,
    mentors: pd.DataFrame,
    *,
    policy: PolicyConfig,
    logs: pd.DataFrame | None = None,
    trace: pd.DataFrame | None = None,
    writer: pd.ExcelWriter | None = None,
    sheet_name: str | None = None,
    extra_attrs: Mapping[str, object] | None = None,
    summary_df: pd.DataFrame | None = None,
) -> tuple[str, pd.DataFrame]:
    """ساخت دیتافریم دلایل و نوشتن آن (در صورت ارائه writer) همراه با متادیتا.

    مسیر ستون‌ها برای مقادیر حساس:
    - «کدملی»: ابتدا از ستون‌های دانش‌آموز (national_id, کدملی، کد ملی) و در
      صورت نبود از ستون ``student_national_code`` در allocations پر می‌شود.
    - نام/نام‌خانوادگی: از فیلدهای دانش‌آموز (first_name, family_name, نام، نام خانوادگی).

    این تابع تنها لایهٔ اتصال (adapter) است تا جایگاه تزریق داده‌های تکمیلی
    در دیباگ مشخص باشد و منطق اصلی در :func:`build_selection_reason_rows` باقی بماند.
    """

    enriched_students = students
    if summary_df is not None and "student_id" in summary_df.columns:
        summary_en = pd.DataFrame(summary_df).copy()
        enriched_students = pd.DataFrame(students).copy()
        try:
            summary_en = summary_en.drop_duplicates("student_id", keep="first")
            summary_en = summary_en.set_index("student_id", drop=False)
            enriched_students = enriched_students.set_index("student_id", drop=False)
            for column in (
                "student_national_code",
                "student_educational_status",
                "student_registration_status",
                "student_first_name",
                "student_last_name",
            ):
                if column in summary_en.columns:
                    aligned = summary_en[column].reindex(enriched_students.index)
                    base = (
                        enriched_students[column]
                        if column in enriched_students.columns
                        else pd.Series(pd.NA, index=enriched_students.index)
                    )
                    enriched_students[column] = base.where(base.notna(), aligned)
            enriched_students = enriched_students.reset_index(drop=True)
        except Exception:
            enriched_students = students

    reasons_df = build_selection_reason_rows(
        allocations,
        enriched_students,
        mentors,
        policy=policy,
        logs=logs,
        trace=trace,
    )
    if extra_attrs:
        reasons_df.attrs.update(dict(extra_attrs))

    target_sheet, sanitized_df = write_selection_reasons_sheet(
        reasons_df,
        writer=writer,
        policy=policy,
    )
    if sheet_name:
        target_sheet = sheet_name
    return target_sheet, sanitized_df
