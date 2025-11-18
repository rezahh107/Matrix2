import pandas as pd
import pytest

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    BuildConfig,
    DomainBuildConfig,
    _as_domain_config,
    _explode_rows,
    center_text,
)
from app.core.common.domain import COL_SCHOOL
from app.core.matrix.coverage import CoveragePolicyConfig, compute_coverage_metrics
from app.core.qa.coverage_validation import build_coverage_validation_fields


JOIN_KEYS = [
    "کدرشته",
    "جنسیت",
    "دانش آموز فارغ",
    "مرکز گلستان صدرا",
    "مالی حکمت بنیاد",
    "کد مدرسه",
]


def _base_row(
    *,
    group_code: int,
    can_generate: bool = True,
    mentor_id: str = "m1",
) -> dict:
    return {
        "supporter": "پشتیبان",
        "manager": "مدیر",
        "mentor_id": mentor_id,
        "mentor_row_id": 1,
        "center_code": 1,
        "center_text": "مرکز",
        "group_pairs": [("رشته", group_code)],
        "genders": [1],
        "statuses_normal": [1],
        "statuses_school": [1],
        "finance": [0],
        "school_codes": [0],
        "schools_normal": [0],
        "alias_normal": "a",
        "alias_school": None,
        "can_normal": can_generate,
        "can_school": False,
        "capacity_current": 0,
        "capacity_special": 0,
        "capacity_remaining": 0,
        "school_count": 0,
    }


def test_compute_coverage_metrics_excludes_blocked_candidates() -> None:
    base_df = pd.DataFrame(
        [
            _base_row(group_code=101, can_generate=True, mentor_id="m1"),
            _base_row(group_code=102, can_generate=False, mentor_id="m2"),
            _base_row(group_code=103, can_generate=True, mentor_id="m3"),
        ]
    )
    matrix_df = pd.DataFrame(
        [
            {
                "کدرشته": 101,
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
                "کد کارمندی پشتیبان": "m1",
            }
        ]
    )

    policy = CoveragePolicyConfig(
        denominator_mode="mentors",
        require_student_presence=False,
        include_blocked_candidates_in_denominator=False,
    )
    metrics, coverage_df, _ = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=None,
        join_keys=JOIN_KEYS,
        policy=policy,
        unmatched_school_count=0,
        invalid_group_token_count=2,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    assert metrics.total_groups == 2  # only viable candidates
    assert metrics.covered_groups == 1
    assert metrics.unseen_viable_groups == 1
    assert metrics.invalid_group_token_count == 2
    assert metrics.blocked_groups == 1  # recorded for debug even if excluded
    assert (
        coverage_df.loc[coverage_df["is_unseen_viable"], "کدرشته"].iat[0] == 103
    )


def test_compute_coverage_metrics_intersects_with_students_when_requested() -> None:
    base_df = pd.DataFrame(
        [
            _base_row(group_code=201, can_generate=True, mentor_id="m1"),
            _base_row(group_code=202, can_generate=True, mentor_id="m2"),
        ]
    )
    students_df = pd.DataFrame(
        [
            {
                "کدرشته": 202,
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
            }
        ]
    )
    matrix_df = pd.DataFrame(
        [
            {
                "کدرشته": 202,
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
                "کد کارمندی پشتیبان": "m2",
            }
        ]
    )
    policy = CoveragePolicyConfig(
        denominator_mode="mentors_students_intersection",
        require_student_presence=True,
        include_blocked_candidates_in_denominator=False,
    )
    metrics, coverage_df, _ = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=students_df,
        join_keys=JOIN_KEYS,
        policy=policy,
        unmatched_school_count=0,
        invalid_group_token_count=0,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    assert metrics.total_groups == 1
    assert metrics.covered_groups == 1
    assert metrics.unseen_viable_groups == 0
    assert coverage_df["in_coverage_denominator"].sum() == 1


def test_build_coverage_validation_fields_aligns_with_metrics() -> None:
    base_df = pd.DataFrame([_base_row(group_code=301)])
    matrix_df = pd.DataFrame(
        [
            {
                "کدرشته": 301,
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
                "کد کارمندی پشتیبان": "m1",
            }
        ]
    )
    policy = CoveragePolicyConfig(
        denominator_mode="mentors",
        require_student_presence=False,
        include_blocked_candidates_in_denominator=False,
    )
    metrics, _, _ = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=None,
        join_keys=JOIN_KEYS,
        policy=policy,
        unmatched_school_count=7,
        invalid_group_token_count=5,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    result = build_coverage_validation_fields(
        metrics=metrics,
        coverage_threshold=0.95,
        total_rows=len(matrix_df),
    )

    assert set(result.keys()) == {
        "coverage_ratio",
        "unseen_group_count",
        "invalid_group_token_count",
        "coverage_denominator_groups",
        "total_candidates",
        "covered_groups",
        "unmatched_school_count",
        "coverage_threshold",
    }
    assert result["coverage_ratio"] == metrics.coverage_ratio
    assert result["unseen_group_count"] == metrics.unseen_viable_groups
    assert result["invalid_group_token_count"] == metrics.invalid_group_token_count
    assert result["coverage_denominator_groups"] == metrics.total_groups
    assert result["total_candidates"] == int(round(len(matrix_df) / metrics.coverage_ratio))
    assert result["covered_groups"] == metrics.covered_groups
    assert result["unmatched_school_count"] == metrics.unmatched_school_count


def test_coverage_metrics_regression_many_invalid_tokens_all_viable_groups_covered() -> None:
    valid_groups = [
        (101, 1, 0, 10, 1, 1001),
        (101, 2, 0, 10, 1, 1001),
        (102, 1, 1, 11, 2, 1002),
    ]

    valid_rows = [
        _base_row(group_code=group_vals[0], can_generate=True, mentor_id=f"M{i}")
        | {
            "genders": [group_vals[1]],
            "statuses_normal": [group_vals[2]],
            "center_code": group_vals[3],
            "finance": [group_vals[4]],
            "school_codes": [group_vals[5]],
            "schools_normal": [group_vals[5]],
        }
        for i, group_vals in enumerate(valid_groups, start=1)
    ]

    blocked_row = _base_row(group_code=103, can_generate=False, mentor_id="BLOCKED") | {
        "genders": [1],
        "statuses_normal": [0],
        "center_code": 12,
        "finance": [1],
        "school_codes": [1003],
        "schools_normal": [1003],
    }

    invalid_token_count = 50

    base_df = pd.DataFrame(valid_rows + [blocked_row])

    matrix_df = pd.DataFrame(
        [dict(zip(JOIN_KEYS, group_vals), row_id=f"R{i}") for i, group_vals in enumerate(valid_groups, start=1)]
    )

    policy_cfg = CoveragePolicyConfig(
        denominator_mode="mentors",
        require_student_presence=False,
        include_blocked_candidates_in_denominator=False,
    )

    metrics, coverage_df, _ = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=None,
        join_keys=JOIN_KEYS,
        policy=policy_cfg,
        unmatched_school_count=0,
        invalid_group_token_count=invalid_token_count,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    assert metrics.total_groups == len(valid_groups)
    assert metrics.covered_groups == len(valid_groups)
    assert metrics.unseen_viable_groups == 0
    assert metrics.invalid_group_token_count == invalid_token_count
    assert metrics.unmatched_school_count == 0
    assert metrics.coverage_ratio == pytest.approx(1.0)
    assert coverage_df["in_coverage_denominator"].sum() == len(valid_groups)
    assert coverage_df["is_unseen_viable"].sum() == 0


def test_coverage_metrics_normalizes_blank_gender_and_status_to_zero() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)

    base_df = pd.DataFrame(
        [
            {
                "supporter": "پشتیبان",
                "manager": "مدیر",
                "mentor_id": "EMP-1",
                "mentor_row_id": 1,
                "center_code": 1,
                "center_text": center_text(1),
                "group_pairs": [("رشته", 401)],
                "genders": [""],
                "statuses_normal": [""],
                "statuses_school": [""],
                "finance": [0],
                "school_codes": [0],
                "schools_normal": [0],
                "alias_normal": "1234",
                "alias_school": None,
                "can_normal": True,
                "can_school": False,
                "capacity_current": 0,
                "capacity_special": 0,
                "capacity_remaining": 0,
                "school_count": 0,
            }
        ]
    )

    cap_current_col = cfg.capacity_current_column or CAPACITY_CURRENT_COL
    cap_special_col = cfg.capacity_special_column or CAPACITY_SPECIAL_COL
    remaining_col = cfg.remaining_capacity_column or "remaining_capacity"
    school_code_col = cfg.school_code_column or COL_SCHOOL

    matrix_df = _explode_rows(
        base_df.loc[base_df["can_normal"]],
        alias_col="alias_normal",
        status_col="statuses_normal",
        school_col="schools_normal",
        type_label="عادی",
        code_to_name_school={},
        cfg=cfg,
        domain_cfg=domain_cfg,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )

    policy = CoveragePolicyConfig(
        denominator_mode="mentors",
        require_student_presence=False,
        include_blocked_candidates_in_denominator=False,
    )

    metrics, coverage_df, summary = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=None,
        join_keys=cfg.policy.join_keys,
        policy=policy,
        unmatched_school_count=0,
        invalid_group_token_count=0,
        center_column=cfg.policy.stage_column("center"),
        finance_column=cfg.policy.stage_column("finance"),
        school_code_column=school_code_col,
    )

    assert int(metrics.covered_groups) == 1
    assert int(metrics.unseen_viable_groups) == 0
    assert metrics.coverage_ratio == 1.0
    assert summary["candidate_only_groups"] == 0
    assert matrix_df["جنسیت"].iat[0] == 0
    assert matrix_df["دانش آموز فارغ"].iat[0] == 0
    assert coverage_df.loc[0, "status"] == "covered"


def test_coverage_metrics_normalizes_missing_join_keys_to_zero_int64() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)

    base_df = pd.DataFrame(
        [
            {
                "supporter": "پشتیبان",
                "manager": "مدیر",
                "mentor_id": "EMP-2",
                "mentor_row_id": 1,
                "center_code": pd.NA,
                "center_text": "",
                "group_pairs": [("رشته", 402)],
                "genders": [1],
                "statuses_normal": [1],
                "statuses_school": [1],
                "finance": [pd.NA],
                "school_codes": [pd.NA],
                "schools_normal": [pd.NA],
                "alias_normal": "9999",
                "alias_school": None,
                "can_normal": True,
                "can_school": False,
                "capacity_current": 0,
                "capacity_special": 0,
                "capacity_remaining": 0,
                "school_count": 0,
            }
        ]
    )

    cap_current_col = cfg.capacity_current_column or CAPACITY_CURRENT_COL
    cap_special_col = cfg.capacity_special_column or CAPACITY_SPECIAL_COL
    remaining_col = cfg.remaining_capacity_column or "remaining_capacity"
    school_code_col = cfg.school_code_column or COL_SCHOOL

    matrix_df = _explode_rows(
        base_df.loc[base_df["can_normal"]],
        alias_col="alias_normal",
        status_col="statuses_normal",
        school_col="schools_normal",
        type_label="عادی",
        code_to_name_school={},
        cfg=cfg,
        domain_cfg=domain_cfg,
        cap_current_col=cap_current_col,
        cap_special_col=cap_special_col,
        remaining_col=remaining_col,
        school_code_col=school_code_col,
    )

    join_keys = cfg.policy.join_keys
    assert matrix_df[join_keys].isna().sum().sum() == 0
    assert matrix_df.dtypes.loc[join_keys].apply(str).str.contains("Int64").all()
    assert matrix_df["مرکز گلستان صدرا"].iat[0] == 0
    assert matrix_df[school_code_col].iat[0] == 0
    assert matrix_df["مالی حکمت بنیاد"].iat[0] == 0

    policy = CoveragePolicyConfig(
        denominator_mode="mentors",
        require_student_presence=False,
        include_blocked_candidates_in_denominator=False,
    )

    metrics, coverage_df, summary = compute_coverage_metrics(
        matrix_df=matrix_df,
        base_df=base_df,
        students_df=None,
        join_keys=join_keys,
        policy=policy,
        unmatched_school_count=0,
        invalid_group_token_count=0,
        center_column=cfg.policy.stage_column("center"),
        finance_column=cfg.policy.stage_column("finance"),
        school_code_column=school_code_col,
    )

    assert metrics.coverage_ratio == 1.0
    assert summary["candidate_only_groups"] == 0
    assert coverage_df.loc[0, "status"] == "covered"
