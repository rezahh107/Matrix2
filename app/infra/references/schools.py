"""مخزن مرجع مدارس و Crosswalk با تکیه بر SQLite."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.infra.io_utils import ALT_CODE_COLUMN, read_crosswalk_workbook, read_excel_first_sheet
from app.infra.local_database import LocalDatabase
from app.infra.reference_repository import SQLiteReferenceRepository
from app.infra.sqlite_types import coerce_int_columns, coerce_int_series


def _coerce_school_code(series: pd.Series, *, fill_value: int | None = None) -> pd.Series:
    """تبدیل ستون کد مدرسه به نوع Int64 با مقدار پیش‌فرض مشخص."""

    return coerce_int_series(series, fill_value=fill_value)


def _normalize_schools_frame(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی دیتافریم مدارس برای ذخیره‌سازی قابل‌اتکا در SQLite."""

    canonical = canonicalize_headers(df, header_mode="fa")
    code_col = "کد مدرسه"
    if code_col in canonical.columns:
        canonical = canonical.copy()
        canonical[code_col] = _coerce_school_code(canonical[code_col])
    return canonical


def _schools_repository(db: LocalDatabase) -> SQLiteReferenceRepository:
    return SQLiteReferenceRepository(
        db=db,
        table_name="schools",
        int_columns=("کد مدرسه",),
        unique_columns=("کد مدرسه",),
    )


def import_school_report_from_excel(path: Path, db: LocalDatabase) -> pd.DataFrame:
    """ورود SchoolReport از Excel و ذخیره در SQLite.

    این تابع فایل گزارش مدارس را یک‌بار می‌خواند، سرستون‌ها را به حالت
    استاندارد تبدیل می‌کند، ستون «کد مدرسه» را به نوع عددی Int64
    درمی‌آورد و سپس در جدول مرجع ``schools`` ذخیره می‌کند.
    """

    raw_df = read_excel_first_sheet(path)
    normalized = _normalize_schools_frame(raw_df)
    _schools_repository(db).upsert_frame(normalized, source=str(path))
    return normalized


def import_school_crosswalk_from_excel(
    path: Path, db: LocalDatabase
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """ورود Crosswalk مدارس (گروه‌ها و Synonyms) و ذخیره در SQLite."""

    groups_df, synonyms_df = read_crosswalk_workbook(path)
    groups_df = coerce_int_columns(groups_df, ["کد مدرسه", ALT_CODE_COLUMN])
    if synonyms_df is not None:
        synonyms_df = coerce_int_columns(synonyms_df, ["کد مدرسه", ALT_CODE_COLUMN])
    db.upsert_school_crosswalk(groups_df, synonyms_df=synonyms_df)
    return groups_df, synonyms_df


def get_school_reference_frames(
    db: LocalDatabase,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """بازیابی دیتافریم‌های مرجع مدارس و Crosswalk از SQLite."""

    schools_df = _schools_repository(db).load_frame()
    crosswalk_groups_df, crosswalk_synonyms_df = db.load_school_crosswalk()
    return schools_df, crosswalk_groups_df, crosswalk_synonyms_df


__all__ = [
    "import_school_report_from_excel",
    "import_school_crosswalk_from_excel",
    "get_school_reference_frames",
]
