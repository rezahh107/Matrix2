from pathlib import Path

import pandas as pd
import pytest

from app.infra import cli
from app.infra.local_database import LocalDatabase, _SCHEMA_VERSION
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


def test_cli_import_managers_help_includes_command() -> None:
    parser = cli._build_parser()
    help_text = parser.format_help()

    assert "import-managers" in help_text


def test_cli_import_managers_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "mgr.xlsx"
    pd.DataFrame({"manager name": ["Mgr"], "center": [2]}).to_excel(path, index=False)
    db_path = tmp_path / "cache.sqlite"

    first = cli.main(["import-managers", "--manager-report", str(path), "--local-db", str(db_path)])
    second = cli.main(["import-managers", "--manager-report", str(path), "--local-db", str(db_path)])

    assert first == 0
    assert second == 0
    loaded = load_managers_from_cache(LocalDatabase(db_path))
    assert loaded.shape[0] == 1
    assert int(loaded.iloc[0]["مرکز گلستان صدرا"]) == 2


def test_cli_import_managers_newer_schema_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "newer.sqlite"
    db = LocalDatabase(db_path)
    with db.connect() as conn:
        LocalDatabase._ensure_schema_meta_table(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta(id, schema_version, policy_version, ssot_version, created_at)
            VALUES (1, ?, '1.0.3', '1.0.2', '2024-01-01T00:00:00Z')
            """,
            (_SCHEMA_VERSION + 1,),
        )
        conn.commit()

    path = tmp_path / "mgr.xlsx"
    pd.DataFrame({"manager name": ["Mgr"], "center": [3]}).to_excel(path, index=False)

    exit_code = cli.main(
        ["import-managers", "--manager-report", str(path), "--local-db", str(db_path)]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "نسخهٔ پایگاه داده ناسازگار" in captured.err
