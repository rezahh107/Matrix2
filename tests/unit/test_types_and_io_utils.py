import warnings

import pandas as pd
import pytest

from app.core.common.types import JoinKeyValues, natural_key
from app.infra.io_utils import _coalesce_duplicate_columns


CANONICAL_JOIN_KEYS = [
    "کدرشته",
    "جنسیت",
    "دانش آموز فارغ",
    "مرکز گلستان صدرا",
    "مالی حکمت بنیاد",
    "کد مدرسه",
]


def test_join_key_values_from_policy_enforces_order_and_int_cast():
    payload = {key: str(index) for index, key in enumerate(CANONICAL_JOIN_KEYS, start=1)}
    join_keys = JoinKeyValues.from_policy(payload, CANONICAL_JOIN_KEYS)
    assert list(join_keys.keys()) == CANONICAL_JOIN_KEYS
    assert list(join_keys.values()) == [1, 2, 3, 4, 5, 6]


def test_join_key_values_from_policy_missing_key_raises():
    payload = {"کدرشته": 1}
    with pytest.raises(ValueError):
        JoinKeyValues.from_policy(payload, CANONICAL_JOIN_KEYS)


def test_natural_key_orders_strings_naturally():
    assert natural_key("EMP-2") < natural_key("EMP-10")
    assert natural_key(" ") == ("",)


def test_coalesce_duplicate_columns_avoids_downcast_warning_and_preserves_values():
    df = pd.DataFrame(
        [
            [None, "الف", 2, None],
            [1, None, None, "ب"],
        ],
        columns=["code", "name", "code", "name"],
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", FutureWarning)
        result = _coalesce_duplicate_columns(df)

    assert not caught
    assert result.shape == (2, 2)
    assert list(result.columns) == ["code", "name"]
    assert result.loc[0, "code"] == 2
    assert result.loc[1, "name"] == "ب"
