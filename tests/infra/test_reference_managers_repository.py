from pathlib import Path

import pandas as pd
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
