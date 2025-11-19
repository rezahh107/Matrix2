from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pytest

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
            "کدپستی": ["1234", "5678"],
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

    matrix, _, _, unmatched_df, unseen_df, invalid_df, _, _ = build_matrix(
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

    drop_columns = [
        col
        for col in ("counter", "mentor_school_binding_mode", "has_school_constraint")
        if col in matrix.columns
    ]
    matrix_cmp = matrix.drop(columns=drop_columns).sort_values(
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
        "کدرشته",
        CAPACITY_CURRENT_COL,
        CAPACITY_SPECIAL_COL,
        "remaining_capacity",
    ]:
        matrix_cmp[column] = matrix_cmp[column].astype("Int64")
        manual_cmp[column] = manual_cmp[column].astype("Int64")

    for column in ["جایگزین"]:
        matrix_cmp[column] = matrix_cmp[column].astype(str)
        manual_cmp[column] = manual_cmp[column].astype(str)

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

    matrix, _, _, _, _, invalid_df, _, _ = build_matrix(
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

    sorted_invalid = duplicate_reasons.sort_values("پشتیبان").reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "پشتیبان": ["زهرا", "زهرا تکراری"],
            "reason": ["duplicate mentor employee code", "duplicate mentor employee code"],
        }
    )
    pdt.assert_frame_equal(
        sorted_invalid[["پشتیبان", "reason"]].reset_index(drop=True),
        expected,
        check_dtype=False,
    )
    assert matrix["کد کارمندی پشتیبان"].eq("EMP-1").sum() == 0


def test_validation_captures_unmatched_school_counts() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[0, "نام مدرسه 1"] = "123456"

    cfg = BuildConfig(min_coverage_ratio=0.0, school_lookup_mismatch_threshold=1.0)
    _, validation, _, unmatched_df, _, _, _, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=cfg,
    )

    assert len(unmatched_df) == 1
    assert validation["unmatched_school_count"].iat[0] == 1
    assert validation["join_key_duplicate_rows"].iat[0] == 0
    assert "coverage_ratio" in validation.columns
    total_rows = validation["total_rows"].iat[0]
    total_candidates = validation["total_candidates"].iat[0]
    assert total_candidates > 0
    assert validation["coverage_ratio"].iat[0] == pytest.approx(
        total_rows / total_candidates
    )


def test_global_mentors_with_zero_school_values_remain_valid() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[:, "نام مدرسه 1"] = 0

    matrix, validation, _, _, _, invalid_df, _, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(min_coverage_ratio=0.0, school_lookup_mismatch_threshold=0.0),
    )

    assert not matrix.empty
    assert invalid_df.empty
    assert validation["school_lookup_mismatch_count"].iat[0] == 0


def test_build_matrix_reports_school_lookup_mismatches_without_coverage_failure() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[0, "نام مدرسه 1"] = "123456"

    matrix, validation, _, _, _, invalid_df, _, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(
            min_coverage_ratio=1.0,
            school_lookup_mismatch_threshold=1.0,
        ),
    )

    assert not matrix.empty
    assert validation["school_lookup_mismatch_count"].iat[0] == 1
    assert not invalid_df.empty
    assert any("unknown school" in str(reason) for reason in invalid_df["reason"])


def test_school_lookup_mismatches_are_logged_in_invalid_sheet() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[0, "نام مدرسه 1"] = "مدرسه ناشناخته"

    matrix, _, _, _, _, invalid_df, _, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(min_coverage_ratio=0.0, school_lookup_mismatch_threshold=1.0),
    )

    assert not matrix.empty
    assert not invalid_df.empty
    reasons = invalid_df["reason"].astype(str).tolist()
    assert any("unknown school name" in reason for reason in reasons)


def test_school_lookup_gate_raises_when_threshold_exceeded() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[0, "نام مدرسه 1"] = "مدرسه ناشناخته"

    with pytest.raises(ValueError) as excinfo:
        build_matrix(
            insp_df,
            schools_df,
            crosswalk_df,
            cfg=BuildConfig(
                min_coverage_ratio=0.0,
                school_lookup_mismatch_threshold=0.0,
                fail_on_school_lookup_threshold=True,
            ),
        )

    assert getattr(excinfo.value, "is_school_lookup_threshold_error", False)
    invalid_df = getattr(excinfo.value, "invalid_mentors_df", pd.DataFrame())
    assert not invalid_df.empty
    assert any("unknown school" in str(reason) for reason in invalid_df["reason"])


def test_school_lookup_threshold_can_warn_instead_of_raise() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    insp_df.loc[0, "نام مدرسه 1"] = "مدرسه ناشناخته"

    matrix, _, _, _, _, invalid_df, _, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(
            min_coverage_ratio=0.0,
            school_lookup_mismatch_threshold=0.0,
            fail_on_school_lookup_threshold=False,
        ),
    )

    assert not matrix.empty
    assert not invalid_df.empty
    assert any("unknown school" in str(reason) for reason in invalid_df["reason"])


def test_build_matrix_reports_join_key_duplicates() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()
    first_row = insp_df.iloc[[0]].copy()
    first_row.loc[:, "کدرشته"] = [1201]
    first_row.loc[:, "کد مدرسه"] = [0]
    duplicate = first_row.copy()
    duplicate.loc[:, "نام پشتیبان"] = ["زهرا دوم"]
    duplicate.loc[:, "کد کارمندی پشتیبان"] = ["EMP-99"]
    insp_df = pd.concat([first_row, duplicate], ignore_index=True)

    _, validation, _, _, _, _, duplicate_join_keys, _ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    assert len(duplicate_join_keys) == 2
    assert duplicate_join_keys["کد کارمندی پشتیبان"].tolist() == ["EMP-1", "EMP-99"]
    assert validation["join_key_duplicate_rows"].iat[0] == 2
    warnings_df = validation[validation["warning_type"].notna()]
    assert not warnings_df.empty
    assert "join_key_duplicate" in warnings_df["warning_type"].unique()
    assert any("EMP-1" in str(msg) for msg in warnings_df["warning_message"].dropna())


def test_validation_reports_dedup_metrics() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()

    _, validation, _, _, _, _, _, progress_log = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    assert "dedup_removed_rows" in validation.columns
    assert validation["dedup_removed_rows"].iat[0] == 0
    assert "dedup_removed_rows" in progress_log.columns
    dedup_row = progress_log.loc[progress_log["step"] == "deduplicate_matrix"].iloc[0]
    assert dedup_row["dedup_removed_rows"] == 0


def test_validation_includes_policy_version_metadata() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()

    cfg = BuildConfig(expected_policy_version="1.0.3")
    _, validation, *_ = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=cfg,
    )

    assert "policy_version" in validation.columns
    assert validation["policy_version"].iat[0] == cfg.policy_version
    assert validation["policy_version_expected"].iat[0] == cfg.expected_policy_version


def test_progress_log_captures_normalization_alias_diffs() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()

    _, _, _, _, _, _, _, progress_log = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    school_row = progress_log.loc[progress_log["dataset"] == "schools"].iloc[0]
    assert school_row["aliases_added_count"] >= 1
    assert "school_code" in str(school_row["aliases_added"])

    schools_df_with_alias = schools_df.copy()
    schools_df_with_alias["school_code"] = schools_df_with_alias["کد مدرسه"]
    _, _, _, _, _, _, _, progress_log_alias = build_matrix(
        insp_df,
        schools_df_with_alias,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    school_row_alias = progress_log_alias.loc[
        progress_log_alias["dataset"] == "schools"
    ].iloc[0]
    assert school_row_alias["aliases_added_count"] < school_row["aliases_added_count"]
    assert "school_code" not in str(school_row_alias["aliases_added"])


def test_progress_log_exposes_normalization_reports_in_attrs() -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()

    _, _, _, _, _, _, _, progress_log = build_matrix(
        insp_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    reports = progress_log.attrs.get("column_normalization_reports")
    assert reports
    assert {"inspactor", "schools", "crosswalk"}.issubset(reports.keys())
    assert "school_code" in reports["schools"]["aliases_added"]


def test_build_matrix_raises_when_dedup_threshold_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    insp_df, schools_df, crosswalk_df = _create_sample_inputs()

    from app.core import build_matrix as build_module

    original_drop_duplicates = pd.DataFrame.drop_duplicates

    def _fake_drop_duplicates(self, subset=None, keep="first", *args, **kwargs):
        result = original_drop_duplicates(
            self, subset=subset, keep=keep, *args, **kwargs
        )
        subset_cols = subset or []
        if any(col == "پشتیبان" for col in subset_cols) and len(result) > 1:
            return result.iloc[:-1].copy()
        return result

    monkeypatch.setattr(pd.DataFrame, "drop_duplicates", _fake_drop_duplicates)
    monkeypatch.setattr(
        build_module, "_validate_finance_invariants", lambda *_, **__: None
    )

    with pytest.raises(ValueError, match="حذف رکوردهای تکراری") as excinfo:
        build_matrix(
            insp_df,
            schools_df,
            crosswalk_df,
            cfg=BuildConfig(dedup_removed_ratio_threshold=0.0),
        )

    assert getattr(excinfo.value, "is_dedup_removed_threshold_error", False)
