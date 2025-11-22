from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from app.infra.local_database import LocalDatabase
from app.infra.reference_managers_repository import (
    import_managers_from_excel,
    load_managers_from_cache,
)


def test_import_and_load_managers_reference(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "local.sqlite")
    source = pd.DataFrame({"manager name": ["Alpha", "Beta"], "center": [1, 2]})
    path = tmp_path / "managers.xlsx"
    source.to_excel(path, index=False)

    imported = import_managers_from_excel(path, db)
    loaded = load_managers_from_cache(db)

    expected = pd.DataFrame(
        {
            "نام مدیر": ["Alpha", "Beta"],
            "مرکز گلستان صدرا": pd.Series([1, 2], dtype="Int64"),
        }
    )

    assert_frame_equal(
        imported.sort_values(by=["مرکز گلستان صدرا", "نام مدیر"], kind="stable").reset_index(
            drop=True
        ),
        expected,
    )
    assert_frame_equal(
        loaded.sort_values(by=["مرکز گلستان صدرا", "نام مدیر"], kind="stable").reset_index(
            drop=True
        ),
        expected,
    )

    refreshed_at, source_path, row_count = db.fetch_reference_meta("managers_reference")
    assert refreshed_at
    assert source_path == str(path)
    assert int(row_count) == 2


def test_duplicate_manager_center_raises(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "dup.sqlite")
    df = pd.DataFrame({"manager name": ["Alpha", "Alpha"], "center": [1, 1]})
    path = tmp_path / "managers.xlsx"
    df.to_excel(path, index=False)

    with pytest.raises(ValueError):
        import_managers_from_excel(path, db)


def test_pii_columns_are_stripped_and_idempotent(tmp_path: Path) -> None:
    db = LocalDatabase(tmp_path / "pii.sqlite")
    df = pd.DataFrame(
        {
            "manager name": ["Mgr"],
            "center": [7],
            "phone": ["123"],
            "email": ["mgr@example.com"],
        }
    )
    path = tmp_path / "mgr.xlsx"
    df.to_excel(path, index=False)

    first = import_managers_from_excel(path, db)
    second = import_managers_from_excel(path, db)
    loaded = load_managers_from_cache(db)

    assert set(first.columns) == {"نام مدیر", "مرکز گلستان صدرا"}
    assert set(second.columns) == {"نام مدیر", "مرکز گلستان صدرا"}
    assert set(loaded.columns) == {"نام مدیر", "مرکز گلستان صدرا"}
    assert_frame_equal(first, second)
    assert_frame_equal(
        loaded.sort_values(by=["مرکز گلستان صدرا", "نام مدیر"], kind="stable").reset_index(
            drop=True
        ),
        first.sort_values(by=["مرکز گلستان صدرا", "نام مدیر"], kind="stable").reset_index(
            drop=True
        ),
    )
