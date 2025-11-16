from pathlib import Path
import sys

import pandas as pd

_HERE = Path(__file__).resolve()
for candidate in _HERE.parents:
    if (candidate / "pyproject.toml").exists():
        sys.path.insert(0, str(candidate))
        break

from app.core.common.phone_rules import (
    HEKMAT_LANDLINE_FALLBACK,
    HEKMAT_STATUS_CODE,
    HEKMAT_TRACKING_CODE,
    apply_hekmat_contact_policy,
    fix_guardian_phones,
    normalize_digits,
    normalize_digits_series,
    normalize_landline,
    normalize_landline_series,
    normalize_mobile,
    normalize_mobile_series,
)


def test_normalize_mobile_adds_leading_zero_for_10_digit_phones() -> None:
    assert normalize_mobile("9357174851") == "09357174851"


def test_normalize_digits_handles_farsi_and_punctuation() -> None:
    assert normalize_digits("۰۲۱-۱۲۳ ۴۵۶۷") == "0211234567"
    assert normalize_digits(None) is None


def test_normalize_mobile_accepts_valid() -> None:
    assert normalize_mobile("۰۹۱۲۳۴۵۶۷۸۹") == "09123456789"


def test_normalize_mobile_rejects_invalid_lengths_or_prefix() -> None:
    assert normalize_mobile("001234567890") is None
    assert normalize_mobile("08123456789") is None
    assert normalize_mobile("935") is None


def test_normalize_mobile_series_preserves_index() -> None:
    series = pd.Series(["۰۹۱۲۳۴۵۶۷۸۹", "9123456789"], index=["a", "b"])
    normalized = normalize_mobile_series(series)
    assert normalized.index.tolist() == ["a", "b"]
    assert normalized.loc["a"] == "09123456789"
    assert normalized.loc["b"] == "09123456789"


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


def test_normalize_landline_accepts_only_3_or_5_prefix() -> None:
    assert normalize_landline("۳۳۳۴۴۴۵۵۵۵") == "3334445555"
    assert normalize_landline("۵۱۲۳۴۵۶۷") == "51234567"
    assert normalize_landline("0123456789") is None


def test_normalize_landline_series_handles_special_zero() -> None:
    series = pd.Series(["51234567", "00000000000", "6123456"])
    normalized_default = normalize_landline_series(series)
    assert normalized_default.loc[0] == "51234567"
    assert normalized_default.isna().tolist() == [False, True, True]
    normalized_with_special = normalize_landline_series(series, allow_special_zero=True)
    assert normalized_with_special.loc[1] == "00000000000"
    assert normalized_with_special.isna().tolist() == [False, False, True]


def test_apply_hekmat_contact_policy_fills_landline_and_tracking() -> None:
    df = pd.DataFrame(
        {
            "student_registration_status": [HEKMAT_STATUS_CODE, HEKMAT_STATUS_CODE],
            "student_landline": [pd.NA, "  "],
            "hekmat_tracking": ["", None],
        }
    )
    enriched = apply_hekmat_contact_policy(
        df,
        status_column="student_registration_status",
        landline_column="student_landline",
        tracking_code_column="hekmat_tracking",
    )
    assert enriched["student_landline"].tolist() == [
        HEKMAT_LANDLINE_FALLBACK,
        HEKMAT_LANDLINE_FALLBACK,
    ]
    assert enriched["hekmat_tracking"].tolist() == [
        HEKMAT_TRACKING_CODE,
        HEKMAT_TRACKING_CODE,
    ]


def test_apply_hekmat_contact_policy_respects_non_hekmat_rows() -> None:
    df = pd.DataFrame(
        {
            "student_registration_status": [HEKMAT_STATUS_CODE, 0],
            "student_landline": [None, "51234567"],
            "hekmat_tracking": ["", "custom"],
        }
    )
    enriched = apply_hekmat_contact_policy(
        df,
        status_column="student_registration_status",
        landline_column="student_landline",
        tracking_code_column="hekmat_tracking",
    )
    assert enriched["student_landline"].tolist() == [
        HEKMAT_LANDLINE_FALLBACK,
        "51234567",
    ]
    assert enriched["hekmat_tracking"].tolist() == [
        HEKMAT_TRACKING_CODE,
        "",
    ]
