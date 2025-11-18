import pandas as pd

from app.core.matrix.coverage import (
    CoveragePolicyConfig,
    compute_coverage_metrics,
)


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
        invalid_group_tokens=2,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    assert metrics.total_groups == 2  # only viable candidates
    assert metrics.covered_groups == 1
    assert metrics.unseen_viable_groups == 1
    assert metrics.invalid_group_tokens == 2
    assert metrics.blocked_groups == 1  # recorded for debug even if excluded
    assert coverage_df[coverage_df["is_unseen_viable"] == True].iloc[0]["کدرشته"] == 103


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
        invalid_group_tokens=0,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    assert metrics.total_groups == 1
    assert metrics.covered_groups == 1
    assert metrics.unseen_viable_groups == 0
    assert coverage_df["in_coverage_denominator"].sum() == 1
