from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.ui.helpers.manager_helpers import extract_manager_names, load_manager_names_from_pool


def test_extract_manager_names() -> None:
    df = pd.DataFrame({"manager_name": [" Alice ", "Bob", "Alice"]})
    names = extract_manager_names(df)
    assert names == ["Alice", "Bob"]


def test_extract_manager_names_missing_column() -> None:
    df = pd.DataFrame({"wrong": [1, 2]})
    with pytest.raises(ValueError):
        extract_manager_names(df)


def test_load_manager_names_from_pool(tmp_path: Path) -> None:
    path = tmp_path / "pool.csv"
    pd.DataFrame({"manager_name": ["X", "Y"]}).to_csv(path, index=False)
    names = load_manager_names_from_pool(path)
    assert names == ["X", "Y"]
