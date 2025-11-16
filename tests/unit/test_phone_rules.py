from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

_HERE = Path(__file__).resolve()
for candidate in _HERE.parents:
    if (candidate / "pyproject.toml").exists():
        sys.path.insert(0, str(candidate))
        break

from app.core.common.phone_rules import (
    fix_guardian_phones,
    normalize_digits,
    normalize_digits_series,
    normalize_mobile,
    normalize_mobile_series,
)


def test_normalize_digits_handles_farsi_and_punctuation() -> None:
    assert normalize_digits("۰۲۱-۱۲۳ ۴۵۶۷") == "0211234567"
    assert normalize_digits(None) is None


def test_normalize_mobile_accepts_valid() -> None:
    assert normalize_mobile("۰۹۱۲۳۴۵۶۷۸۹") == "09123456789"


def test_normalize_mobile_rejects_invalid_lengths_or_prefix() -> None:
    assert normalize_mobile("9123456789") is None
    assert normalize_mobile("001234567890") is None
    assert normalize_mobile("08123456789") is None


def test_normalize_mobile_series_preserves_index() -> None:
    series = pd.Series(["۰۹۱۲۳۴۵۶۷۸۹", "9123456789"], index=["a", "b"])
    normalized = normalize_mobile_series(series)
    assert normalized.index.tolist() == ["a", "b"]
    assert normalized.loc["a"] == "09123456789"
    assert pd.isna(normalized.loc["b"])


def test_fix_guardian_phones_moves_second_if_first_missing() -> None:
    first, second = fix_guardian_phones(None, "۰۹۳۵۱۱۱۲۲۳۳")
    assert first == "09351112233"
    assert second is None


def test_fix_guardian_phones_drops_duplicate_second() -> None:
    first, second = fix_guardian_phones("۰۹۱۲۳۴۵۶۷۸۹", "09123456789")
    assert first == "09123456789"
    assert second is None


def test_normalize_digits_series_returns_string_dtype() -> None:
    series = pd.Series(["۰۲۱", None])
    normalized = normalize_digits_series(series)
    assert normalized.dtype == "string"
    assert normalized.iloc[0] == "021"
    assert pd.isna(normalized.iloc[1])
