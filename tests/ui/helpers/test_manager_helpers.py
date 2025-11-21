from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.ui.helpers.manager_helpers import extract_manager_names, load_manager_names_from_pool


def test_extract_manager_names_accepts_persian_manager_column() -> None:
    df = pd.DataFrame({"مدیر": ["  علی ", "زهرا", "علی", "  "]})

    names = extract_manager_names(df)

    assert names == ["علی", "زهرا"]


def test_extract_manager_names_raises_when_manager_column_missing() -> None:
    df = pd.DataFrame({"wrong": [1, 2]})

    with pytest.raises(ValueError) as exc_info:
        extract_manager_names(df)

    assert "manager_name" in str(exc_info.value)
    assert "مدیر" in str(exc_info.value)


def test_extract_manager_names_empty_after_cleaning() -> None:
    df = pd.DataFrame({"manager_name": ["  ", None, " \t "]})

    with pytest.raises(ValueError) as exc_info:
        extract_manager_names(df)

    assert "هیچ مدیری" in str(exc_info.value)


def test_load_manager_names_from_pool(tmp_path: Path) -> None:
    path = tmp_path / "pool.csv"
    pd.DataFrame({"manager_name": ["X", "Y"]}).to_csv(path, index=False)

    names = load_manager_names_from_pool(path)

    assert names == ["X", "Y"]
