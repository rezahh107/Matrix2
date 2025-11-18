from __future__ import annotations

import pandas as pd

import pandas as pd

from app.core.matrix.coverage import compute_group_coverage_debug


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
    mentor_id: str,
    genders: list[int | str] | None = None,
    alias_normal: str | None = None,
    can_normal: bool = True,
) -> dict:
    return {
        "supporter": "پشتیبان",
        "manager": "مدیر",
        "mentor_id": mentor_id,
        "mentor_row_id": 1,
        "center_code": 1,
        "center_text": "مرکز",
        "group_pairs": [("رشته", group_code)],
        "genders": genders or [1],
        "statuses_normal": [1],
        "statuses_school": [1],
        "finance": [0],
        "school_codes": [0],
        "schools_normal": [0],
        "alias_normal": alias_normal,
        "alias_school": None,
        "can_normal": can_normal,
        "can_school": False,
        "capacity_current": 0,
        "capacity_special": 0,
        "capacity_remaining": 0,
        "school_count": 0,
    }


def test_compute_group_coverage_flags_candidate_and_matrix_states() -> None:
    base_df = pd.DataFrame(
        [
            _base_row(group_code=101, mentor_id="m1", alias_normal="a101"),
            _base_row(group_code=102, mentor_id="m2", alias_normal=None, can_normal=False),
            _base_row(group_code=104, mentor_id="m3", alias_normal="a104"),
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
            },
            {
                "کدرشته": 103,
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
                "کد کارمندی پشتیبان": "ghost",
            },
        ]
    )

    coverage_df, summary = compute_group_coverage_debug(
        matrix_df,
        base_df,
        join_keys=JOIN_KEYS,
        center_column="مرکز گلستان صدرا",
        finance_column="مالی حکمت بنیاد",
        school_code_column="کد مدرسه",
    )

    status_map = {
        tuple(row[j] for j in JOIN_KEYS): row["status"] for _, row in coverage_df.iterrows()
    }

    assert status_map[(101, 1, 1, 1, 0, 0)] == "covered"
    assert status_map[(102, 1, 1, 1, 0, 0)] == "blocked_candidate"
    assert status_map[(104, 1, 1, 1, 0, 0)] == "candidate_only"
    assert status_map[(103, 1, 1, 1, 0, 0)] == "matrix_only"

    assert summary["covered_groups"] == 1
    assert summary["blocked_candidate_groups"] == 1
    assert summary["candidate_only_groups"] == 1
    assert summary["matrix_only_groups"] == 1
