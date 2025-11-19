from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd
import pandas.testing as pdt

from app.core.build_matrix import (
    BuildConfig,
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    _as_domain_config,
    _explode_rows,
    _prepare_base_rows,
    build_school_maps,
    prepare_crosswalk_mappings,
)
from app.core.common.domain import COL_SCHOOL
from tests.unit.test_vectorized_equivalence import _create_sample_inputs

SNAPSHOT_DIR = Path(__file__).resolve().parents[1] / "snapshots"
NORMAL_SNAPSHOT = SNAPSHOT_DIR / "explode_rows_normal.json"
SCHOOL_SNAPSHOT = SNAPSHOT_DIR / "explode_rows_school.json"

_SORT_KEYS = (
    "کد کارمندی پشتیبان",
    "کدرشته",
    "جنسیت",
    "دانش آموز فارغ",
    "مالی حکمت بنیاد",
    "کد مدرسه",
)

_COLUMN_ORDER_BASE = (
    "جایگزین",
    "پشتیبان",
    "کد کارمندی پشتیبان",
    "مدیر",
    "ردیف پشتیبان",
    "نام رشته",
    "کدرشته",
    "جنسیت",
    "دانش آموز فارغ",
    "مرکز گلستان صدرا",
    "مالی حکمت بنیاد",
    "کد مدرسه",
    "نام مدرسه",
    "عادی مدرسه",
    "mentor_school_binding_mode",
    "has_school_constraint",
    "جنسیت2",
    "دانش آموز فارغ2",
    "مرکز گلستان صدرا3",
)


def _build_fixture_base() -> tuple[
    pd.DataFrame,
    dict[str, str],
    BuildConfig,
    Tuple[str, str, str, str],
]:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df = insp_df.copy()
    # Postal code should be 4 digits so that alias_normal is non-empty → normal rows exist.
    insp_df.loc[0, "کدپستی"] = "1234"

    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    name_to_code, code_to_name, buckets, synonyms = prepare_crosswalk_mappings(crosswalk_df)
    code_to_name_school, school_name_to_code = build_school_maps(schools_df)
    base_df, _, _ = _prepare_base_rows(
        insp_df,
        cfg=cfg,
        domain_cfg=domain_cfg,
        name_to_code=name_to_code,
        code_to_name=code_to_name,
        buckets=buckets,
        synonyms=synonyms,
        school_name_to_code=school_name_to_code,
        code_to_name_school=code_to_name_school,
        group_cols=["گروه آزمایشی"],
        school_cols=["نام مدرسه 1"],
        gender_col="جنسیت",
        included_col=None,
    )

    cap_current_col = cfg.capacity_current_column or CAPACITY_CURRENT_COL
    cap_special_col = cfg.capacity_special_column or CAPACITY_SPECIAL_COL
    remaining_col = cfg.remaining_capacity_column or "remaining_capacity"
    school_code_col = cfg.school_code_column or COL_SCHOOL

    return base_df, code_to_name_school, cfg, (cap_current_col, cap_special_col, remaining_col, school_code_col)


def _canonicalize(
    df: pd.DataFrame,
    *,
    cap_current_col: str,
    cap_special_col: str,
    remaining_col: str,
    school_code_col: str,
) -> pd.DataFrame:
    column_order = list(_COLUMN_ORDER_BASE)
    if "کد مدرسه" in column_order:
        idx = column_order.index("کد مدرسه")
        column_order[idx] = school_code_col
    elif school_code_col not in column_order:
        column_order.append(school_code_col)
    column_order.extend([cap_current_col, cap_special_col, remaining_col])

    if df.empty:
        keep_cols = [col for col in column_order if col in df.columns]
        return df.reindex(columns=keep_cols)

    df = df.copy()
    ordered_cols = [col for col in column_order if col in df.columns]
    extra_cols = [col for col in df.columns if col not in ordered_cols]
    df = df.loc[:, ordered_cols + extra_cols]

    sort_cols = [col for col in _SORT_KEYS if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    int_columns = {
        "ردیف پشتیبان",
        "کدرشته",
        "مرکز گلستان صدرا",
        "مالی حکمت بنیاد",
        "جنسیت",
        "دانش آموز فارغ",
        cap_current_col,
        cap_special_col,
        remaining_col,
        school_code_col,
    }
    for column in int_columns:
        if column in df.columns:
            numeric = pd.to_numeric(df[column], errors="coerce")
            df[column] = numeric.astype("Int64")

    string_columns = {"mentor_school_binding_mode"}
    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].astype("string")

    return df


def _generate_canonical_rows() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    tuple[str, str, str, str],
]:
    base_df, code_to_name_school, cfg, cols = _build_fixture_base()
    cap_current_col, cap_special_col, remaining_col, school_code_col = cols
    domain_cfg = _as_domain_config(cfg)

    normal_df = _explode_rows(
        base_df.loc[base_df["can_normal"]],
        alias_col="alias_normal",
        status_col="statuses_normal",
        school_col="schools_normal",
        type_label="عادی",
        code_to_name_school=code_to_name_school,
        cfg=cfg,
        domain_cfg=domain_cfg,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )
    school_df = _explode_rows(
        base_df.loc[base_df["can_school"]],
        alias_col="alias_school",
        status_col="statuses_school",
        school_col="school_codes",
        type_label="مدرسه‌ای",
        code_to_name_school=code_to_name_school,
        cfg=cfg,
        domain_cfg=domain_cfg,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )

    normal_canonical = _canonicalize(
        normal_df,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )
    school_canonical = _canonicalize(
        school_df,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )
    return normal_canonical, school_canonical, cols


def _load_snapshot(
    path: Path,
    *,
    cap_current_col: str,
    cap_special_col: str,
    remaining_col: str,
    school_code_col: str,
) -> pd.DataFrame:
    assert path.exists(), f"snapshot missing: {path}"
    loaded = pd.read_json(path, orient="records", dtype=False)
    return _canonicalize(
        loaded,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )


def test_explode_rows_matches_snapshot() -> None:
    normal_df, school_df, cols = _generate_canonical_rows()
    cap_current_col, cap_special_col, remaining_col, school_code_col = cols

    expected_normal = _load_snapshot(
        NORMAL_SNAPSHOT,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )
    expected_school = _load_snapshot(
        SCHOOL_SNAPSHOT,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )

    pdt.assert_frame_equal(normal_df, expected_normal)
    pdt.assert_frame_equal(school_df, expected_school)
