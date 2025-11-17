from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.ui.helpers.counter_helpers import autodetect_academic_year, detect_year_candidates


def test_detect_year_candidates_strict() -> None:
    df = pd.DataFrame({"student_id": ["540000001", "540000099"]})
    strict, fallback = detect_year_candidates(df)
    assert strict == 1404
    assert fallback == 1404


def test_detect_year_candidates_none() -> None:
    df = pd.DataFrame({"student_id": []})
    strict, fallback = detect_year_candidates(df)
    assert strict is None
    assert fallback is None


def test_autodetect_academic_year(tmp_path: Path) -> None:
    path = tmp_path / "roster.csv"
    pd.DataFrame({"student_id": ["550000001"]}).to_csv(path, index=False)
    year = autodetect_academic_year(path)
    assert year == 1405
