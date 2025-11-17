from __future__ import annotations

from pathlib import Path

import pandas as pd
from PySide6.QtCore import QCoreApplication, QEventLoop, QTimer

from app.ui.loaders import ExcelLoader


def _ensure_app() -> QCoreApplication:
    app = QCoreApplication.instance()
    if app is None:
        app = QCoreApplication([])
    return app


def test_excel_loader_success(tmp_path: Path) -> None:
    _ensure_app()
    csv_path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1, 2]}).to_csv(csv_path, index=False)

    loader = ExcelLoader(csv_path)
    results: list[pd.DataFrame] = []
    loop = QEventLoop()
    loader.loaded.connect(lambda df: (results.append(df), loop.quit()))
    loader.failed.connect(lambda msg: (_ for _ in ()).throw(AssertionError(msg)))

    loader.start()
    QTimer.singleShot(3000, loop.quit)
    loop.exec()

    assert results and results[0]["a"].tolist() == [1, 2]


def test_excel_loader_failure(tmp_path: Path) -> None:
    _ensure_app()
    missing = tmp_path / "missing.xlsx"
    loader = ExcelLoader(missing)
    errors: list[str] = []
    loop = QEventLoop()
    loader.loaded.connect(lambda *_: (_ for _ in ()).throw(AssertionError("expected failure")))
    loader.failed.connect(lambda msg: (errors.append(msg), loop.quit()))

    loader.start()
    QTimer.singleShot(3000, loop.quit)
    loop.exec()

    assert errors and "missing.xlsx" in errors[0]
