"""مخزن مرجع مدیران مراکز با تکیه بر SQLite.

ManagerReport (نگاشت مدیر→مرکز) تنها یک‌بار از Excel خوانده می‌شود و پس از
نرمال‌سازی سرستون‌ها و نوع ستون مرکز، در جدول ``managers_reference`` ذخیره
می‌گردد. اجرای بعدی می‌تواند بدون تکرار خواندن Excel از کش SQLite استفاده
کند.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.infra.io_utils import read_excel_first_sheet
from app.infra.local_database import LocalDatabase
from app.infra.reference_repository import SQLiteReferenceRepository
from app.infra.sqlite_types import coerce_int_series


def _normalize_manager_frame(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی دیتافریم ManagerReport برای ذخیرهٔ پایدار.

    - سرستون‌ها به حالت فارسی استاندارد تبدیل می‌شوند.
    - ستون «مرکز گلستان صدرا» به نوع عددی ``Int64`` تبدیل می‌شود تا با ۶ کلید
      اتصال Policy/SSoT سازگار بماند.
    """

    canonical = canonicalize_headers(df, header_mode="fa")
    center_col = "مرکز گلستان صدرا"
    if center_col in canonical.columns:
        canonical = canonical.copy()
        canonical[center_col] = coerce_int_series(canonical[center_col])
    return canonical


def _managers_repository(db: LocalDatabase) -> SQLiteReferenceRepository:
    return SQLiteReferenceRepository(
        db=db,
        table_name="managers_reference",
        int_columns=("مرکز گلستان صدرا",),
        unique_columns=(),
    )


def import_managers_from_excel(path: Path, db: LocalDatabase) -> pd.DataFrame:
    """ورود ManagerReport از Excel و ذخیره در جدول ``managers_reference``."""

    raw_df = read_excel_first_sheet(path)
    normalized = _normalize_manager_frame(raw_df)
    _managers_repository(db).upsert_frame(normalized, source=str(path))
    return normalized


def load_managers_from_cache(db: LocalDatabase) -> pd.DataFrame:
    """بازیابی نگاشت مدیران از کش SQLite."""

    return _managers_repository(db).load_frame()


__all__ = [
    "import_managers_from_excel",
    "load_managers_from_cache",
]
