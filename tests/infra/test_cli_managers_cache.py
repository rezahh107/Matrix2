from pathlib import Path

import pandas as pd

from app.infra import cli
from app.infra.local_database import LocalDatabase
from app.infra.reference_managers_repository import load_managers_from_cache


def test_cli_import_managers(tmp_path: Path) -> None:
    path = tmp_path / "mgr.xlsx"
    pd.DataFrame({"manager name": ["Mgr"], "center": [1]}).to_excel(path, index=False)

    db_path = tmp_path / "cache.sqlite"
    exit_code = cli.main(
        ["import-managers", "--manager-report", str(path), "--local-db", str(db_path)]
    )

    assert exit_code == 0
    loaded = load_managers_from_cache(LocalDatabase(db_path))
    assert loaded.shape[0] == 1
    assert int(loaded.iloc[0]["مرکز گلستان صدرا"]) == 1
    assert loaded.iloc[0]["نام مدیر"] == "Mgr"
