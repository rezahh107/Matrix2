from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    BuildConfig,
    build_matrix,
    build_school_maps,
    generate_row_variants,
    prepare_crosswalk_mappings,
    _prepare_base_rows,
    _as_domain_config,
)


def _create_sample_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    insp_df = pd.DataFrame(
        {
            "نام پشتیبان": ["زهرا", "علی"],
            "نام مدیر": ["شهدخت کشاورز", "آینا هوشمند"],
            "کد کارمندی پشتیبان": ["EMP-1", "EMP-2"],
            "ردیف پشتیبان": [1, 2],
            "گروه آزمایشی": ["تجربی", "ریاضی"],
            "جنسیت": ["دختر", "پسر"],
            "دانش آموز فارغ": [0, 1],
            "کدپستی": ["12345", "999"],
            "تعداد داوطلبان تحت پوشش": [5, 3],
            "تعداد تحت پوشش خاص": [10, 4],
            "نام مدرسه 1": ["", "مدرسه نمونه 1"],
            "تعداد مدارس تحت پوشش": [0, 1],
            "امکان جذب دانش آموز": ["بلی", "بلی"],
            "مالی حکمت بنیاد": [0, 0],
            "مرکز گلستان صدرا": [0, 0],
        }
    )

    schools_df = pd.DataFrame(
        {
            "کد مدرسه": ["5001"],
            "نام مدرسه 1": ["مدرسه نمونه 1"],
        }
    )

    crosswalk_df = pd.DataFrame(
        {
            "گروه آزمایشی": ["تجربی", "ریاضی"],
            "کد گروه": [1201, 2201],
            "مقطع تحصیلی": ["دهم", "دهم"],
        }
    )

    return insp_df, schools_df, crosswalk_df


def _build_reference_matrix(
    base_df: pd.DataFrame,
    cfg: BuildConfig,
    code_to_name_school: dict[str, str],
) -> pd.DataFrame:
    rows: list[dict] = []
    for record in base_df.to_dict("records"):
        common_base = {
            "alias": record.get("alias_normal") if record.get("alias_normal") is not None else "",
            "supporter": record["supporter"],
            "manager": record["manager"],
            "mentor_id": record["mentor_id"],
            "row_id": record.get("mentor_row_id", ""),
            "center_code": record["center_code"],
            "center_text": record["center_text"],
            "capacity_current": record.get("capacity_current", 0),
            "capacity_special": record.get("capacity_special", 0),
            "capacity_remaining": record.get("capacity_remaining", 0),
        }
        if record.get("can_normal"):
            rows.extend(
                generate_row_variants(
                    base=common_base,
                    group_pairs=record["group_pairs"],
                    genders=record["genders"],
                    statuses=record["statuses_normal"],
                    schools_raw=[""],
                    finance_variants=cfg.finance_variants,
                    code_to_name_school=code_to_name_school,
                )
            )
        if record.get("can_school") and record.get("school_codes"):
            school_base = common_base.copy()
            school_base["alias"] = record.get("alias_school", "")
            rows.extend(
                generate_row_variants(
                    base=school_base,
                    group_pairs=record["group_pairs"],
                    genders=record["genders"],
                    statuses=record["statuses_school"],
                    schools_raw=record["school_codes"],
                    finance_variants=cfg.finance_variants,
                    code_to_name_school=code_to_name_school,
                )
            )
    manual = pd.DataFrame(rows)
    if manual.empty:
        return manual
    manual["ردیف پشتیبان"] = manual["ردیف پشتیبان"].apply(
        lambda v: int(v) if str(v).strip().isdigit() else str(v).strip()
    )
    manual["کد مدرسه"] = manual["کد مدرسه"].astype(int)
    manual["جایگزین"] = manual["جایگزین"].apply(
        lambda v: int(v) if str(v).strip().isdigit() else str(v).strip()
    )
    return manual


def test_vectorized_matrix_matches_reference() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    cfg = BuildConfig()

    matrix, _, _, unmatched_df, unseen_df, invalid_df = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=cfg,
    )

    assert invalid_df.empty

    name_to_code, code_to_name, buckets, synonyms = prepare_crosswalk_mappings(crosswalk_df)
    code_to_name_school, school_name_to_code = build_school_maps(schools_df)
    domain_cfg = _as_domain_config(cfg)
    base_df, unseen_ref, unmatched_ref = _prepare_base_rows(
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

    manual = _build_reference_matrix(base_df, cfg, code_to_name_school)

    matrix_cmp = matrix.drop(columns=["counter"]).sort_values(
        by=["مرکز گلستان صدرا", "کدرشته", "کد مدرسه", "جایگزین"], kind="stable"
    )
    manual_cmp = manual.sort_values(
        by=["مرکز گلستان صدرا", "کدرشته", "کد مدرسه", "جایگزین"], kind="stable"
    )

    matrix_cmp = matrix_cmp.reset_index(drop=True)
    manual_cmp = manual_cmp[matrix_cmp.columns].reset_index(drop=True)

    for column in [
        "جنسیت",
        "دانش آموز فارغ",
        "مالی حکمت بنیاد",
        CAPACITY_CURRENT_COL,
        CAPACITY_SPECIAL_COL,
        "remaining_capacity",
    ]:
        matrix_cmp[column] = matrix_cmp[column].astype("Int64")
        manual_cmp[column] = manual_cmp[column].astype("Int64")

    pdt.assert_frame_equal(matrix_cmp, manual_cmp)

    assert unseen_df.empty
    assert unmatched_df.empty
    assert not unseen_ref
    assert not unmatched_ref


def test_duplicate_mentors_are_filtered_before_row_generation() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    duplicate_row = insp_df.iloc[[0]].copy()
    duplicate_row.loc[:, "نام پشتیبان"] = ["زهرا تکراری"]
    insp_with_duplicate = pd.concat([insp_df, duplicate_row], ignore_index=True)

    matrix, _, _, _, _, invalid_df = build_matrix(
        insp_with_duplicate,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    duplicate_reasons = invalid_df.loc[
        invalid_df["reason"] == "duplicate mentor employee code"
    ]
    assert len(duplicate_reasons) == 2
    assert set(duplicate_reasons["پشتیبان"]) == {"زهرا", "زهرا تکراری"}
    assert matrix["کد کارمندی پشتیبان"].eq("EMP-1").sum() == 0
