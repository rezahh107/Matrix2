from pathlib import Path

import pandas as pd

from app.infra.local_database import LocalDatabase
from app.infra.reference_repository import SQLiteReferenceRepository


def test_upsert_load_and_meta_roundtrip(tmp_path: Path):
    db = LocalDatabase(tmp_path / "ref.db")
    repo = SQLiteReferenceRepository(
        db=db,
        table_name="sample_ref",
        int_columns=("id",),
        join_keys=("code",),
        unique_columns=("id",),
    )

    df = pd.DataFrame({"id": [1, 2], "code": [10, 20], "name": ["a", "b"]})
    repo.upsert_frame(df, source="unit-test")

    loaded = repo.load_frame()
    assert loaded.shape == (2, 3)
    assert str(loaded["id"].dtype) == "Int64"
    assert str(loaded["code"].dtype) == "Int64"
    assert loaded.set_index("id").loc[1, "name"] == "a"

    meta = repo.last_refresh_meta()
    assert meta is not None
    assert meta.table_name == "sample_ref"
    assert meta.source == "unit-test"
    assert meta.row_count == 2

    df_updated = pd.DataFrame({"id": [2, 3], "code": [20, 30], "name": ["b", "c"]})
    repo.upsert_frame(df_updated, source="unit-test-2")
    loaded_updated = repo.load_frame().set_index("id")
    assert list(loaded_updated.index) == [2, 3]
    assert loaded_updated.loc[3, "name"] == "c"
    assert repo.last_refresh_meta().row_count == 2


def test_index_creation_for_unique_and_join_keys(tmp_path: Path):
    db = LocalDatabase(tmp_path / "idx.db")
    repo = SQLiteReferenceRepository(
        db=db,
        table_name="idx_ref",
        int_columns=("id", "code"),
        join_keys=("code",),
        unique_columns=("id",),
    )
    df = pd.DataFrame({"id": [1], "code": [5]})
    repo.upsert_frame(df, source="src")

    with db.connect() as conn:
        cursor = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='idx_ref'"
        )
        rows = cursor.fetchall()
    index_names = {row[0] for row in rows}
    assert "idx_idx_ref_id_uniq" in index_names
    assert "idx_idx_ref_code" in index_names
    for _, ddl in rows:
        assert "idx_ref" in ddl
