from __future__ import annotations

"""توابع کمکی خالص برای استخراج نام مدیران مراکز.

نمونه:
    >>> load_manager_names_from_pool(Path("pool.xlsx"))
"""

from pathlib import Path
from typing import Iterable, List

import pandas as pd

from app.core.common.columns import canonicalize_headers


def _validate_manager_column(columns: Iterable[str]) -> None:
    if "manager_name" not in columns:
        raise ValueError("ستون manager_name در فایل استخر وجود ندارد")


def extract_manager_names(dataframe: pd.DataFrame) -> List[str]:
    """استخراج نام‌های منحصربه‌فرد مدیران پس از نرمال‌سازی.

    پارامترها:
        dataframe: دیتافریم خام استخر.
    """

    canonical = canonicalize_headers(dataframe, header_mode="en")
    _validate_manager_column(canonical.columns)
    managers = canonical["manager_name"].dropna().astype(str).map(str.strip)
    cleaned = [name for name in managers.tolist() if name]
    unique = list(dict.fromkeys(cleaned))
    if not unique:
        raise ValueError("هیچ مدیری در ستون manager_name یافت نشد")
    return unique


def load_manager_names_from_pool(pool_path: Path) -> List[str]:
    """خواندن فایل استخر و استخراج لیست مدیران."""

    if not pool_path.exists():
        raise FileNotFoundError(pool_path)
    if pool_path.is_dir():
        raise IsADirectoryError(pool_path)
    suffix = pool_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(pool_path)
    else:
        df = pd.read_excel(pool_path)
    return extract_manager_names(df)
