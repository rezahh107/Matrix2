from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.infra import cli
from app.infra.exporter_archive_repository import ExporterArchiveConfig, ExporterArchiveRepository
from app.infra.local_database import LocalDatabase


def test_exporter_archive_cli_list_and_compare(capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "local.db"
    db = LocalDatabase(db_path)
    db.initialize()
    repo = ExporterArchiveRepository(db=db)
    df_a = pd.DataFrame({"col": [1]})
    df_b = pd.DataFrame({"col": [1, 2]})
    snap_a = repo.archive_snapshot(
        rows_df=df_a,
        exporter_version="1",
        run_uuid="a",
        config=ExporterArchiveConfig(enabled=True),
    )
    snap_b = repo.archive_snapshot(
        rows_df=df_b,
        exporter_version="1",
        run_uuid="b",
        config=ExporterArchiveConfig(enabled=True),
    )

    exit_code = cli.main(
        [
            "exporter-archive",
            "list",
            "--local-db",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "id=" in captured.out
    assert "truncated" in captured.out

    exit_code = cli.main(
        [
            "exporter-archive",
            "compare",
            "--local-db",
            str(db_path),
            "--a",
            str(snap_a),
            "--b",
            str(snap_b),
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "row_hash_equal" in captured.out
    assert "added" in captured.out


def test_exporter_archive_cli_compare_truncated(capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "local.db"
    db = LocalDatabase(db_path)
    db.initialize()
    repo = ExporterArchiveRepository(db=db)
    df_big = pd.DataFrame({"col": list(range(5))})
    df_small = pd.DataFrame({"col": [1, 2]})
    snap_truncated = repo.archive_snapshot(
        rows_df=df_big,
        exporter_version="1",
        run_uuid="a",
        config=ExporterArchiveConfig(enabled=True, row_limit=2),
    )
    snap_full = repo.archive_snapshot(
        rows_df=df_small,
        exporter_version="1",
        run_uuid="b",
        config=ExporterArchiveConfig(enabled=True, row_limit=10),
    )

    exit_code = cli.main(
        [
            "exporter-archive",
            "compare",
            "--local-db",
            str(db_path),
            "--a",
            str(snap_truncated),
            "--b",
            str(snap_full),
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "truncated" in captured.out
