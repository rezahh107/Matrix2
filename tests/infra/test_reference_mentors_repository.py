from pathlib import Path

from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from app.core.policy_loader import load_policy
from app.infra.local_database import LocalDatabase
from app.infra.reference_mentors_repository import (
    import_mentor_pool_from_excel,
    load_mentor_pool_from_cache,
)


def _write_pool_excel(df: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def test_mentor_pool_cache_roundtrip(tmp_path: Path) -> None:
    policy = load_policy()
    db = LocalDatabase(tmp_path / "cache.sqlite")

    raw = pd.DataFrame(
        {
            "پشتیبان": ["الف", "ب"],
            "کد کارمندی پشتیبان": ["M1", "M2"],
            "کدرشته": [1201, 1201],
            "گروه آزمایشی": ["تجربی", "تجربی"],
            "جنسیت": [1, 0],
            "دانش آموز فارغ": [0, 0],
            "مرکز گلستان صدرا": [1, 1],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [3581, 3581],
            "remaining_capacity": [2, 3],
        }
    )
    excel_path = tmp_path / "pool.xlsx"
    _write_pool_excel(raw, excel_path)

    normalized = import_mentor_pool_from_excel(excel_path, db=db, policy=policy)
    loaded = load_mentor_pool_from_cache(db=db, policy=policy)

    assert list(loaded.dtypes[policy.join_keys]) == ["Int64"] * len(policy.join_keys)
    assert_frame_equal(
        loaded.sort_values(by="کد کارمندی پشتیبان").reset_index(drop=True),
        normalized.sort_values(by="کد کارمندی پشتیبان").reset_index(drop=True),
        check_dtype=False,
    )
