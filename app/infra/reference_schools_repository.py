"""مخزن مرجع مدارس و Crosswalk با تکیه بر SQLite.

این ماژول ورود یک‌بارهٔ SchoolReport/Crosswalk از Excel و بارگذاری مجدد
از پایگاه دادهٔ محلی را مدیریت می‌کند تا اجرای عادی نیازمند آپلود مجدد
فایل‌ها نباشد. تمامی تبدیل‌های نوعی (به‌ویژه «کد مدرسه» به int) در همین
لایه انجام می‌شود و Core بدون وابستگی به I/O باقی می‌ماند.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.infra.io_utils import ALT_CODE_COLUMN, read_crosswalk_workbook, read_excel_first_sheet
from app.infra.local_database import LocalDatabase, _coerce_int_columns, _coerce_int_like


def _coerce_school_code(series: pd.Series, *, fill_value: int | None = None) -> pd.Series:
    """تبدیل ستون کد مدرسه به نوع Int64 با مقدار پیش‌فرض مشخص.

    Parameters
    ----------
    series: pd.Series
        ستون کد مدرسه (ممکن است رشته/float یا NA باشد).
    fill_value: int | None, optional
        مقدار جایگزین برای مقادیر تهی/نامعتبر؛ اگر ``None`` باشد جایگزینی
        انجام نمی‌شود و مقادیر خالی به‌صورت NA باقی می‌مانند.
    """

    coerced = series.map(_coerce_int_like)
    result = coerced.astype("Int64")
    if fill_value is not None:
        result = result.fillna(int(fill_value))
    return result


def _normalize_schools_frame(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی دیتافریم مدارس برای ذخیره‌سازی قابل‌اتکا در SQLite."""

    canonical = canonicalize_headers(df, header_mode="fa")
    code_col = "کد مدرسه"
    if code_col in canonical.columns:
        canonical = canonical.copy()
        canonical[code_col] = _coerce_school_code(canonical[code_col])
    return canonical


def import_school_report_from_excel(path: Path, db: LocalDatabase) -> pd.DataFrame:
    """ورود SchoolReport از Excel و ذخیره در SQLite.

    این تابع فایل گزارش مدارس را یک‌بار می‌خواند، سرستون‌ها را به حالت
    استاندارد تبدیل می‌کند، ستون «کد مدرسه» را به نوع عددی Int64
    درمی‌آورد و سپس در جدول مرجع ``schools`` ذخیره می‌کند.

    Parameters
    ----------
    path: Path
        مسیر فایل Excel ورودی.
    db: LocalDatabase
        دیتابیس محلی SQLite که جدول مرجع در آن ذخیره می‌شود.

    Returns
    -------
    pd.DataFrame
        دیتافریم نرمال‌شدهٔ مدارس برای استفاده در اجرای جاری.

    مثال
    ----
    >>> from pathlib import Path
    >>> from app.infra.local_database import LocalDatabase
    >>> db = LocalDatabase(Path("smart_alloc.db"))
    >>> df = import_school_report_from_excel(Path("SchoolReport.xlsx"), db)  # doctest: +SKIP
    >>> df["کد مدرسه"].dtype
    Int64
    """

    raw_df = read_excel_first_sheet(path)
    normalized = _normalize_schools_frame(raw_df)
    db.upsert_schools(normalized)
    return normalized


def import_school_crosswalk_from_excel(
    path: Path, db: LocalDatabase
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """ورود Crosswalk مدارس (گروه‌ها و Synonyms) و ذخیره در SQLite.

    این تابع شیت «پایه تحصیلی (گروه آزمایشی)» و (در صورت وجود) شیت
    «Synonyms» را می‌خواند، ستون‌های کد را به Int64 تبدیل می‌کند و هر دو
    جدول را در پایگاه داده ذخیره می‌کند.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame | None]
        دیتافریم گروه‌های Crosswalk و دیتافریم Synonyms (در صورت وجود).
    """

    groups_df, synonyms_df = read_crosswalk_workbook(path)
    groups_df = _coerce_int_columns(groups_df, ["کد مدرسه", ALT_CODE_COLUMN])
    if synonyms_df is not None:
        synonyms_df = _coerce_int_columns(synonyms_df, ["کد مدرسه", ALT_CODE_COLUMN])
    db.upsert_school_crosswalk(groups_df, synonyms_df=synonyms_df)
    return groups_df, synonyms_df


def get_school_reference_frames(
    db: LocalDatabase,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """بازیابی دیتافریم‌های مرجع مدارس و Crosswalk از SQLite.

    این تابع جداول ``schools``، ``school_crosswalk_groups`` و در صورت وجود
    ``school_crosswalk_synonyms`` را می‌خواند و با حفظ نوع Int64 برای کدها
    بازمی‌گرداند.
    """

    schools_df = db.load_schools()
    crosswalk_groups_df, crosswalk_synonyms_df = db.load_school_crosswalk()
    return schools_df, crosswalk_groups_df, crosswalk_synonyms_df


__all__ = [
    "import_school_report_from_excel",
    "import_school_crosswalk_from_excel",
    "get_school_reference_frames",
]
