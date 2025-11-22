"""مخزن مرجع برای کش StudentReport در SQLite.

این لایه مسئول خواندن StudentReport از Excel/CSV، نرمال‌سازی بر اساس Policy و
ذخیرهٔ نسخهٔ تمیز در جدول ``students_cache`` است. Core از تغییرات ذخیره‌سازی
بی‌خبر می‌ماند و همچنان DataFrame دریافت می‌کند.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.canonical_frames import canonicalize_students_frame
from app.core.policy_loader import PolicyConfig
from app.infra.io_utils import read_excel_first_sheet
from app.infra.local_database import LocalDatabase, _coerce_int_columns


def _read_student_source(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return read_excel_first_sheet(path)
    return pd.read_csv(path)


def import_student_report_from_excel(
    path: Path, *, db: LocalDatabase, policy: PolicyConfig
) -> pd.DataFrame:
    """وارد کردن StudentReport از دیسک و ذخیره در کش SQLite.

    دیتافریم خروجی بر اساس Policy نرمال شده و سپس در ``students_cache``
    ذخیره می‌شود تا اجرای بعدی بدون خواندن مجدد Excel انجام شود.
    """

    raw_df = _read_student_source(path)
    normalized = canonicalize_students_frame(raw_df, policy=policy)
    db.upsert_students_cache(normalized, join_keys=policy.join_keys)
    return normalized


def load_students_from_cache(*, db: LocalDatabase, policy: PolicyConfig) -> pd.DataFrame:
    """بازیابی دیتافریم نرمال‌شدهٔ دانش‌آموزان از SQLite."""

    cached = db.load_students_cache(join_keys=policy.join_keys)
    return _coerce_int_columns(cached, policy.join_keys)


__all__ = [
    "import_student_report_from_excel",
    "load_students_from_cache",
]
