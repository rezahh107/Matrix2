"""مخزن کش استخر منتورها با پشتیبانی SQLite.

Excel Inspactor تنها یک‌بار خوانده و با قواعد Policy نرمال می‌شود؛ نسخهٔ تمیز
در جدول ``mentor_pool_cache`` نگه‌داری می‌شود تا اجرای بعدی از SQLite خوانده شود.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.canonical_frames import canonicalize_pool_frame
from app.core.policy_loader import PolicyConfig
from app.infra.io_utils import read_inspactor_workbook
from app.infra.local_database import LocalDatabase, _coerce_int_columns


def import_mentor_pool_from_excel(
    path: Path,
    *,
    db: LocalDatabase,
    policy: PolicyConfig,
    pool_source: str = "inspactor",
) -> pd.DataFrame:
    """وارد کردن استخر منتورها از Inspactor و ذخیره در کش."""

    raw_df = read_inspactor_workbook(path)
    normalized = canonicalize_pool_frame(
        raw_df,
        policy=policy,
        sanitize_pool=False,
        pool_source=pool_source,
    )
    db.upsert_mentor_pool_cache(normalized, join_keys=policy.join_keys)
    return normalized


def load_mentor_pool_from_cache(
    *, db: LocalDatabase, policy: PolicyConfig
) -> pd.DataFrame:
    """بازیابی استخر منتورها از کش SQLite."""

    cached = db.load_mentor_pool_cache(join_keys=policy.join_keys)
    return _coerce_int_columns(cached, policy.join_keys)


__all__ = [
    "import_mentor_pool_from_excel",
    "load_mentor_pool_from_cache",
]
