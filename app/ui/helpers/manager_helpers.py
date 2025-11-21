from __future__ import annotations

"""توابع کمکی خالص برای استخراج نام مدیران مراکز.

نمونه:
    >>> load_manager_names_from_pool(Path("pool.xlsx"))
"""

from pathlib import Path
from typing import Iterable

import pandas as pd

from app.core.common.columns import canonicalize_headers, resolve_aliases


def _validate_manager_column(columns: Iterable[str]) -> None:
    if "manager_name" not in columns:
        raise ValueError("ستون manager_name/مدیر در فایل استخر یافت نشد؛ لطفاً ستون مدیر را اضافه کنید")


def extract_manager_names(dataframe: pd.DataFrame) -> list[str]:
    """استخراج نام‌های منحصربه‌فرد مدیران پس از نرمال‌سازی ستون‌ها.

    نقش:
        - هدرها را با سینونیم‌های مجاز (از جمله «مدیر») به `manager_name` نگاشت می‌کند.
        - مقدارهای تهی/فاصله‌دار را حذف و لیست یکتا (به‌ترتیب ورود) بازمی‌گرداند.

    پارامترها:
        dataframe: دیتافریم خام استخر.

    خروجی:
        لیست یکتای نام مدیران پس از trim و حذف تهی.

    مثال کوتاه:
        >>> df = pd.DataFrame({"مدیر": [" علی ", "زهرا", "علی"]})
        >>> extract_manager_names(df)
        ['علی', 'زهرا']
    """

    resolved = resolve_aliases(dataframe, source="matrix")
    canonical = canonicalize_headers(resolved, header_mode="en")
    _validate_manager_column(canonical.columns)
    managers = canonical["manager_name"].dropna().astype(str).map(str.strip)
    cleaned = [name for name in managers.tolist() if name]
    unique = list(dict.fromkeys(cleaned))
    if not unique:
        raise ValueError("هیچ مدیری در ستون manager_name یافت نشد")
    return unique


def load_manager_names_from_pool(pool_path: Path) -> list[str]:
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
