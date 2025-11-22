# -*- coding: utf-8 -*-
"""مدیریت کش SQLite برای نگاشت مدیر → مرکز (ManagerReport).

ManagerReport (نگاشت مدیر→مرکز) تنها یک‌بار از Excel خوانده می‌شود و پس از
نرمال‌سازی سرستون‌ها، حذف ستون‌های حاوی PII، و اطمینان از یکتا بودن
ترکیب «نام مدیر» و «مرکز گلستان صدرا» در جدول ``managers_reference`` ذخیره
می‌گردد. اجرای بعدی می‌تواند بدون تکرار خواندن Excel از کش SQLite استفاده
کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.infra.io_utils import read_excel_first_sheet
from app.infra.local_database import LocalDatabase
from app.infra.reference_repository import SQLiteReferenceRepository
from app.infra.sqlite_types import coerce_int_series


_MANAGER_COLUMN = "نام مدیر"
_CENTER_COLUMN = "مرکز گلستان صدرا"
_PII_COLUMNS: tuple[str, ...] = (
    "phone",
    "email",
    "موبایل",
    "ایمیل",
    "شماره تماس",
    "شماره موبایل",
    "شماره همراه",
)


@dataclass(frozen=True)
class ManagerReferenceRow:
    """ردیف استاندارد کش مدیران بدون ستون‌های PII.

    Attributes
    ----------
    manager_name: str
        نام مدیر یا شناسهٔ متنی مدیر در ManagerReport.
    center_code: int | None
        کد مرکز گلستان صدرا که مدیر به آن مرتبط است.
    """

    manager_name: str
    center_code: int | None


def _drop_pii_columns(df: pd.DataFrame, *, pii_columns: Iterable[str]) -> pd.DataFrame:
    """حذف ستون‌های حاوی اطلاعات تماس/PII در صورت وجود."""

    if not pii_columns:
        return df
    lower_map = {col: str(col).strip().lower() for col in df.columns}
    pii_lookup = {c.lower() for c in pii_columns}
    drop = [col for col, lowered in lower_map.items() if lowered in pii_lookup]
    if not drop:
        return df
    return df.drop(columns=drop, errors="ignore")


def _normalize_manager_frame(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی دیتافریم ManagerReport برای ذخیرهٔ پایدار.

    - سرستون‌ها به حالت فارسی استاندارد تبدیل می‌شوند.
    - ستون «مرکز گلستان صدرا» به نوع عددی ``Int64`` تبدیل می‌شود تا با ۶ کلید
      اتصال Policy/SSoT سازگار بماند.
    - ستون‌های PII (تلفن/ایمیل) ذخیره نمی‌شوند.
    - ترکیب «نام مدیر»، «مرکز گلستان صدرا» یکتا می‌شود و در صورت تکرار خطا
      دادهٔ واضح ایجاد می‌شود.
    """

    canonical = canonicalize_headers(df, header_mode="fa")
    canonical = _drop_pii_columns(canonical, pii_columns=_PII_COLUMNS)
    missing = [_MANAGER_COLUMN, _CENTER_COLUMN]
    if not set(missing).issubset(set(canonical.columns)):
        raise ValueError("ستون‌های موردنیاز ManagerReport یافت نشد: نام مدیر و مرکز گلستان صدرا")

    normalized = canonical[[
        _MANAGER_COLUMN,
        _CENTER_COLUMN,
    ]].copy()
    normalized[_MANAGER_COLUMN] = normalized[_MANAGER_COLUMN].astype(str).str.strip()
    normalized[_CENTER_COLUMN] = coerce_int_series(normalized[_CENTER_COLUMN])
    normalized = normalized.dropna(subset=[_MANAGER_COLUMN, _CENTER_COLUMN], how="any")

    duplicates = (
        normalized.groupby([_MANAGER_COLUMN, _CENTER_COLUMN], dropna=False)
        .size()
        .reset_index(name="count")
        .query("count > 1")
    )
    if not duplicates.empty:
        sample = duplicates.iloc[0]
        raise ValueError(
            f"ترکیب نام مدیر و مرکز تکراری است: {sample[_MANAGER_COLUMN]} / {sample[_CENTER_COLUMN]}"
        )
    return normalized.reset_index(drop=True)


def _managers_repository(db: LocalDatabase) -> SQLiteReferenceRepository:
    return SQLiteReferenceRepository(
        db=db,
        table_name="managers_reference",
        int_columns=(_CENTER_COLUMN,),
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
    "ManagerReferenceRow",
    "import_managers_from_excel",
    "load_managers_from_cache",
]
