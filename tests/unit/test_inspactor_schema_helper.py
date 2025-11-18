import pandas as pd
import pandas.api.types as pat
import pytest

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    COL_GROUP,
    COL_MANAGER_NAME,
    COL_MENTOR_ID,
    COL_MENTOR_NAME,
    COL_POSTAL,
    COL_SCHOOL1,
    COL_SCHOOL2,
    COL_SCHOOL3,
    COL_SCHOOL4,
    COL_SCHOOL_COUNT,
    COL_SCHOOL_CODE,
    DERIVED_INSPACTOR_COLUMNS,
    REQUIRED_INSPACTOR_COLUMNS,
    assert_inspactor_schema,
)
from app.core.common.columns import (
    CANON_EN_TO_FA,
    coerce_semantics,
    ensure_required_columns,
    resolve_aliases,
)
from app.core.inspactor_schema_helper import infer_school_count
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


def test_fully_populated_inspactor_no_defaulting() -> None:
    policy = load_policy()
    frame = _valid_inspactor_frame()

    ensured = assert_inspactor_schema(frame, policy)

    for column in DERIVED_INSPACTOR_COLUMNS:
        if column == COL_POSTAL:
            pd.testing.assert_series_equal(
                frame[column], ensured[column], check_names=False, check_dtype=False
            )
        else:
            pd.testing.assert_series_equal(frame[column], ensured[column], check_names=False)


def test_partial_derived_missing_are_defaulted() -> None:
    policy = load_policy()
    raw = _valid_inspactor_frame().drop(
        columns=[COL_SCHOOL_COUNT, COL_POSTAL, CAPACITY_CURRENT_COL, CAPACITY_SPECIAL_COL]
    )

    ensured = assert_inspactor_schema(raw, policy)

    assert set(DERIVED_INSPACTOR_COLUMNS).issubset(ensured.columns)
    assert ensured[COL_SCHOOL_COUNT].iloc[0] == 2
    assert pat.is_integer_dtype(ensured[COL_SCHOOL_COUNT])
    assert ensured[CAPACITY_CURRENT_COL].iloc[0] == 0
    assert ensured[CAPACITY_SPECIAL_COL].iloc[0] == 0
    assert pat.is_integer_dtype(ensured[CAPACITY_CURRENT_COL])
    assert pat.is_integer_dtype(ensured[CAPACITY_SPECIAL_COL])
    assert pd.isna(ensured[COL_POSTAL]).iloc[0]


def test_mixed_derived_preserve_existing_values() -> None:
    policy = load_policy()
    raw = _valid_inspactor_frame().assign(
        **{COL_SCHOOL_COUNT: [3], CAPACITY_CURRENT_COL: [7]}
    ).drop(columns=[COL_POSTAL])

    ensured = assert_inspactor_schema(raw, policy)

    assert ensured[COL_SCHOOL_COUNT].iloc[0] == 3
    assert ensured[CAPACITY_CURRENT_COL].iloc[0] == 7
    assert pat.is_string_dtype(ensured[COL_POSTAL])
    assert pd.isna(ensured[COL_POSTAL]).iloc[0]


def test_missing_required_non_derived_columns_fail() -> None:
    policy = load_policy()
    broken = _valid_inspactor_frame().drop(columns=[COL_MENTOR_NAME])

    with pytest.raises(KeyError) as excinfo:
        assert_inspactor_schema(broken, policy)

    message = str(excinfo.value)
    assert COL_MENTOR_NAME in message
    assert COL_MENTOR_NAME not in DERIVED_INSPACTOR_COLUMNS


def test_missing_required_columns_report_diagnostics() -> None:
    policy = load_policy()
    broken = _valid_inspactor_frame().drop(
        columns=[COL_MENTOR_NAME, COL_MANAGER_NAME, COL_GROUP]
    )

    with pytest.raises(KeyError) as excinfo:
        assert_inspactor_schema(broken, policy)

    message = str(excinfo.value)
    assert "accepted:" in message
    assert COL_GROUP in message
    assert "mentor_name" in message
    assert COL_MENTOR_ID in message  # در پیش‌نمایش ستون‌های موجود


def test_pipeline_ordering_requires_defaults_before_ensure() -> None:
    policy = load_policy()
    aliased = pd.DataFrame(
        {
            "پشتیبان": ["الف"],
            "نام و نام خانوادگی مدیر": ["ب"],
            "mentor_id": ["1001"],
            "گروه آزمایشی": ["ریاضی"],
            "کد رشته": [101],
            "کد مدرسه": [501],
            "کد مدرسه 1": [501],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
        }
    )

    normalized = resolve_aliases(aliased, "inspactor")
    coerced = coerce_semantics(normalized, "inspactor")
    with pytest.raises(ValueError):
        ensure_required_columns(coerced, REQUIRED_INSPACTOR_COLUMNS, "inspactor")

    ensured = assert_inspactor_schema(aliased, policy)
    assert set(REQUIRED_INSPACTOR_COLUMNS).issubset(set(ensured.columns))


def test_infer_school_count_deterministic_on_shuffled_columns() -> None:
    frame = pd.DataFrame(
        {
            COL_SCHOOL_CODE: [101, None],
            COL_SCHOOL1: [pd.NA, "0"],
            COL_SCHOOL2: [303, 202],
            COL_SCHOOL3: [0, 0],
        }
    )
    shuffled = frame[[COL_SCHOOL2, COL_SCHOOL_CODE, COL_SCHOOL1, COL_SCHOOL3]]
    columns = (
        COL_SCHOOL_CODE,
        COL_SCHOOL1,
        COL_SCHOOL2,
        COL_SCHOOL3,
        COL_SCHOOL4,
    )

    result_a = infer_school_count(frame, columns)
    result_b = infer_school_count(shuffled, columns)
    expected_counts = pd.Series([2, 1], dtype="Int64")
    pd.testing.assert_series_equal(result_a, expected_counts, check_names=False)
    pd.testing.assert_series_equal(result_a, result_b, check_names=False)
    pd.testing.assert_series_equal(result_a, infer_school_count(frame, columns), check_names=False)


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


