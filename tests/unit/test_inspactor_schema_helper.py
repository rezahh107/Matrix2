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
from app.core.common.columns import CANON_EN_TO_FA, ensure_required_columns, resolve_aliases
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


def test_assert_inspactor_schema_accepts_name_aliases() -> None:
    policy = load_policy()
    aliased = _valid_inspactor_frame().rename(
        columns={COL_MENTOR_NAME: "پشتیبان", COL_MANAGER_NAME: "مدیر"}
    )

    ensured = assert_inspactor_schema(aliased, policy)

    assert COL_MENTOR_NAME in ensured.columns
    assert COL_MANAGER_NAME in ensured.columns


def test_resolve_aliases_handles_common_variants() -> None:
    aliased = pd.DataFrame(
        {
            "پشتیبان": ["الف"],
            "نام و نام خانوادگی مدیر": ["ب"],
            "mentor_id": ["1001"],
            "کد رشته": [101],
            "کد مدرسه": [501],
        }
    )

    resolved = resolve_aliases(aliased, "inspactor")

    assert CANON_EN_TO_FA["mentor_name"] in resolved.columns
    assert CANON_EN_TO_FA["manager_name"] in resolved.columns
    assert resolved[CANON_EN_TO_FA["mentor_id"]].tolist() == ["1001"]


def test_ensure_required_columns_with_aliases() -> None:
    required = {
        CANON_EN_TO_FA["mentor_name"],
        CANON_EN_TO_FA["manager_name"],
        CANON_EN_TO_FA["mentor_id"],
    }
    aliased = pd.DataFrame(
        {"mentor name": ["الف"], "manager": ["ب"], "کد کارمندی پشتیبان": ["1002"]}
    )

    ensured = ensure_required_columns(aliased, required, "inspactor")

    assert required.issubset(set(ensured.columns))


def test_inspactor_aliases_golden_headers() -> None:
    policy = load_policy()
    raw = pd.DataFrame(
        {
            "کد رشته": [101],
            "گروه آزمایشی": ["ریاضی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [501],
            "کد مدرسه 1": [501],
            "کد مدرسه 2": [0],
            "کد مدرسه 3": [0],
            "کد مدرسه 4": [0],
            "پشتیبان": ["الف"],
            "نام و نام خانوادگی مدیر": ["ب"],
            "mentor_id": ["1001"],
            "کدپستی": ["1234567890"],
            "تعداد مدارس تحت پوشش": [1],
            "تعداد داوطلبان تحت پوشش": [10],
            "تعداد تحت پوشش خاص": [0],
        }
    )

    ensured = assert_inspactor_schema(raw, policy)

    assert CANON_EN_TO_FA["mentor_name"] in ensured.columns
    assert CANON_EN_TO_FA["manager_name"] in ensured.columns
    assert ensured[CANON_EN_TO_FA["mentor_id"]].iloc[0] == "1001"
