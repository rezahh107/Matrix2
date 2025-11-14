import pandas as pd
import pytest

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    COL_GROUP,
    COL_MANAGER_NAME,
    COL_MENTOR_ID,
    COL_MENTOR_NAME,
    COL_POSTAL,
    COL_SCHOOL_COUNT,
    COL_SCHOOL_CODE,
    assert_inspactor_schema,
)
from app.core.policy_loader import load_policy


def _valid_inspactor_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            COL_MENTOR_NAME: ["الف"],
            COL_MANAGER_NAME: ["ب"],
            COL_MENTOR_ID: ["1001"],
            COL_POSTAL: ["1234567890"],
            COL_SCHOOL_COUNT: [1],
            CAPACITY_CURRENT_COL: [10],
            CAPACITY_SPECIAL_COL: [0],
            COL_GROUP: ["ریاضی"],
            "کدرشته": [101],
            COL_SCHOOL_CODE: [501],
            "کد مدرسه 1": [501],
            "کد مدرسه 2": [0],
            "کد مدرسه 3": [0],
            "کد مدرسه 4": [0],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
        }
    )


def test_assert_inspactor_schema_missing_required_column() -> None:
    policy = load_policy()
    broken = _valid_inspactor_frame().drop(columns=[COL_GROUP])

    with pytest.raises(KeyError) as excinfo:
        assert_inspactor_schema(broken, policy)

    assert COL_GROUP in str(excinfo.value)
