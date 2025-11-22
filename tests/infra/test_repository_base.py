from pathlib import Path

import pandas as pd

from app.infra.local_database import LocalDatabase
from app.infra.repository_base import ReferenceRepository, SQLiteReferenceRepository


def test_sqlite_reference_repository_roundtrip(tmp_path: Path):
    db = LocalDatabase(tmp_path / "ref.db")
    repo: ReferenceRepository = SQLiteReferenceRepository(
        db=db,
        table_name="sample_ref",
        int_columns=("کد مدرسه",),
        unique_columns=("کد مدرسه",),
        join_keys=("کدرشته",),
    )

    df = pd.DataFrame({"کد مدرسه": [101, "102", None], "کدرشته": [1, 1, 2], "title": ["a", "b", "c"]})
    repo.upsert_frame(df, source="unit-test")

    loaded = repo.load_frame()
    assert str(loaded["کد مدرسه"].dtype) == "Int64"
    assert loaded.shape == df.shape

    meta = repo.last_refresh_meta()
    assert meta is not None
    assert meta.table_name == "sample_ref"
    assert meta.source == "unit-test"
    assert meta.row_count == len(df)
