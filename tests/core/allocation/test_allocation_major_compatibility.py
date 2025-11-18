import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy
from app.core.common.columns import canonicalize_headers


def _base_student_frame(policy: object, major: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": ["S1"],
            "کدرشته": [major],
            "گروه آزمایشی": [major],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [0],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [0],
        }
    )


def _duplicate_pool_frame(policy: object) -> pd.DataFrame:
    data = [
        ["M1", "M1", "Mentor One", 35, 35, 1, 0, 0, 0, 0, 1, 0, 0.0],
        ["M2", "M2", "Mentor Two", 3, 3, 1, 0, 0, 0, 0, 1, 0, 0.0],
    ]
    columns = [
        "mentor_id",
        "کد کارمندی پشتیبان",
        "نام پشتیبان",
        "کدرشته",
        "گروه آزمایشی",
        "جنسیت",
        "دانش آموز فارغ",
        "مرکز گلستان صدرا",
        "مالی حکمت بنیاد",
        "کد مدرسه",
        "remaining_capacity",
        "allocations_new",
        "occupancy_ratio",
    ]
    return pd.DataFrame(data, columns=columns)


def test_allocation_with_duplicate_identifier_columns_respects_major_filter():
    policy = load_policy()
    students = _base_student_frame(policy, major=35)
    pool = _duplicate_pool_frame(policy)

    allocations, _, _, _ = allocate_batch(
        students,
        pool,
        policy=policy,
        frames_already_canonical=False,
        capacity_column=policy.columns.remaining_capacity,
    )

    allocations_fa = canonicalize_headers(allocations, header_mode="fa")
    selected = allocations_fa.iloc[0]["کد کارمندی پشتیبان"]
    assert selected == "M1"


def test_allocation_ranking_stable_after_dedupe():
    policy = load_policy()
    students = _base_student_frame(policy, major=35)
    data = [
        [
            "M1",
            "M1",
            "Mentor One",
            35,
            35,
            1,
            0,
            0,
            0,
            0,
            1,
            1,
            0.5,
        ],
        [
            "M0",
            "M0",
            "Mentor Zero",
            35,
            35,
            1,
            0,
            0,
            0,
            0,
            2,
            0,
            0.0,
        ],
    ]
    columns = [
        "mentor_id",
        "کد کارمندی پشتیبان",
        "نام پشتیبان",
        "کدرشته",
        "گروه آزمایشی",
        "جنسیت",
        "دانش آموز فارغ",
        "مرکز گلستان صدرا",
        "مالی حکمت بنیاد",
        "کد مدرسه",
        "remaining_capacity",
        "allocations_new",
        "occupancy_ratio",
    ]
    pool = pd.DataFrame(data, columns=columns)

    allocations, _, _, _ = allocate_batch(
        students,
        pool,
        policy=policy,
        frames_already_canonical=False,
        capacity_column=policy.columns.remaining_capacity,
    )

    allocations_fa = canonicalize_headers(allocations, header_mode="fa")
    selected = allocations_fa.iloc[0]["کد کارمندی پشتیبان"]
    assert selected == "M0"
