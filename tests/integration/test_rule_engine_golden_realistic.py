import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy


_GROUP_NAME = "تجربی"
ALLOWED_GROUP_CODES = frozenset(
    {1, 3, 5, 7, 8, 9, 20, 22, 23, 24, 25, 26, 27, 31, 33, 35, 36, 37}
)
_UNMATCHED_MAJORS = (8, 9, 24, 22, 25)
_CENTER_CODES = (0, 1, 2)
_FINANCE_VARIANTS = (0, 1, 3)


def _student_row(idx: int, *, major: int, gender: int, grad: int, center: int, finance: int, school: int | str) -> dict[str, object]:
    return {
        "student_id": f"STU-{idx:03d}",
        "کدرشته": major,
        "گروه آزمایشی": _GROUP_NAME,
        "جنسیت": gender,
        "دانش آموز فارغ": grad,
        "مرکز گلستان صدرا": center,
        "مالی حکمت بنیاد": finance,
        "کد مدرسه": school,
    }


def _golden_pool() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "پشتیبان": "Mentor-A",
                "کد کارمندی پشتیبان": "EMP-001",
                "کدرشته": 1,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 501,
                "remaining_capacity": 3,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
            },
            {
                "پشتیبان": "Mentor-B",
                "کد کارمندی پشتیبان": "EMP-002",
                "کدرشته": 3,
                "گروه آزمایشی": "ریاضی",
                "جنسیت": 0,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 1,
                "کد مدرسه": 601,
                "remaining_capacity": 2,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
            },
            {
                "پشتیبان": "Mentor-C",
                "کد کارمندی پشتیبان": "EMP-003",
                "کدرشته": 5,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 2,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 701,
                "remaining_capacity": 1,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
            },
            {
                "پشتیبان": "Mentor-D",
                "کد کارمندی پشتیبان": "EMP-004",
                "کدرشته": 7,
                "گروه آزمایشی": "تجربی",
                "جنسیت": 1,
                "دانش آموز فارغ": 1,
                "مرکز گلستان صدرا": 0,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 0,
                "remaining_capacity": 4,
                "allocations_new": 0,
                "occupancy_ratio": 0.0,
            },
        ]
    )


def _students_high_no_match() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    idx = 1
    # Mentor-A candidates (6 students, capacity 3)
    for _ in range(6):
        rows.append(
            _student_row(
                idx,
                major=1,
                gender=1,
                grad=1,
                center=0,
                finance=0,
                school=501,
            )
        )
        idx += 1
    # Mentor-B candidates (4 students, capacity 2)
    for _ in range(4):
        rows.append(
            _student_row(
                idx,
                major=3,
                gender=0,
                grad=0,
                center=1,
                finance=1,
                school=601,
            )
        )
        idx += 1
    # Mentor-C candidates (2 students, capacity 1)
    for _ in range(2):
        rows.append(
            _student_row(
                idx,
                major=5,
                gender=1,
                grad=0,
                center=2,
                finance=0,
                school=701,
            )
        )
        idx += 1
    # Mentor-D candidates (4 students, capacity 4) with hyphen/zero schools
    d_schools = ["5-01", 0, "0501", " 501 "]
    for school in d_schools:
        rows.append(
            _student_row(
                idx,
                major=7,
                gender=1,
                grad=1,
                center=0,
                finance=0,
                school=school,
            )
        )
        idx += 1
    # Remaining students intentionally mismatched to trigger ELIGIBILITY_NO_MATCH
    while len(rows) < 82:
        major = _UNMATCHED_MAJORS[idx % len(_UNMATCHED_MAJORS)]
        center = _CENTER_CODES[idx % len(_CENTER_CODES)]
        finance = _FINANCE_VARIANTS[idx % len(_FINANCE_VARIANTS)]
        rows.append(
            _student_row(
                idx,
                major=major,
                gender=(idx + 1) % 2,
                grad=1,
                center=center,
                finance=finance,
                school=9000 + idx,
            )
        )
        idx += 1
    students = pd.DataFrame(rows)
    assert set(students["کدرشته"]).issubset(ALLOWED_GROUP_CODES)
    return students


def test_realistic_high_no_match_scenario_golden() -> None:
    """Golden Test: سناریوی ۸۲ نفره با نرخ بالای ELIGIBILITY_NO_MATCH."""

    policy = load_policy()
    students = _students_high_no_match()
    pool = _golden_pool()
    alloc1, pool1, logs1, trace1 = allocate_batch(students, pool, policy=policy)
    alloc2, pool2, logs2, trace2 = allocate_batch(students, pool, policy=policy)

    def _sort_alloc(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("student_id").reset_index(drop=True)

    pd.testing.assert_frame_equal(_sort_alloc(alloc1), _sort_alloc(alloc2))
    pd.testing.assert_frame_equal(
        pool1.sort_index(axis=1).sort_index(),
        pool2.sort_index(axis=1).sort_index(),
    )
    pd.testing.assert_frame_equal(
        logs1.sort_index(axis=1).sort_values(by="student_id").reset_index(drop=True),
        logs2.sort_index(axis=1).sort_values(by="student_id").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        trace1.sort_index(axis=1)
        .sort_values(by=["student_id", "stage"], ignore_index=True),
        trace2.sort_index(axis=1)
        .sort_values(by=["student_id", "stage"], ignore_index=True),
    )

    assert logs1.shape[0] == 82
    assert alloc1.shape[0] == 10
    no_match_mask = logs1["error_type"] == "ELIGIBILITY_NO_MATCH"
    capacity_full_mask = logs1["error_type"] == "CAPACITY_FULL"
    assert int(no_match_mask.sum()) == 66
    assert int(capacity_full_mask.sum()) == 6
    success_mask = logs1["allocation_status"] == "success"
    assert int(success_mask.sum()) == 10
    assert (logs1.loc[success_mask, "candidate_count"] > 0).all()
    assert (
        logs1.loc[capacity_full_mask, "stage_candidate_counts"].apply(
            lambda stage: stage["capacity_gate"]
        )
        == 0
    ).all()
    assert (
        logs1.loc[no_match_mask, "stage_candidate_counts"].apply(
            lambda stage: stage["type"]
        )
        == 0
    ).all()

    first_success = logs1.loc[success_mask].iloc[0]
    student_trace = trace1[trace1["student_id"] == first_success["student_id"]]
    trace_counts = student_trace.set_index("stage")["total_after"].to_dict()
    assert trace_counts == first_success["stage_candidate_counts"]

    expected_allocations = pd.DataFrame(
        [
            {
                "student_id": "STU-001",
                "student_national_code": "",
                "mentor": "Mentor-A",
                "mentor_id": "EMP-001",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-002",
                "student_national_code": "",
                "mentor": "Mentor-A",
                "mentor_id": "EMP-001",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-003",
                "student_national_code": "",
                "mentor": "Mentor-A",
                "mentor_id": "EMP-001",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-007",
                "student_national_code": "",
                "mentor": "Mentor-B",
                "mentor_id": "EMP-002",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-008",
                "student_national_code": "",
                "mentor": "Mentor-B",
                "mentor_id": "EMP-002",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-011",
                "student_national_code": "",
                "mentor": "Mentor-C",
                "mentor_id": "EMP-003",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-013",
                "student_national_code": "",
                "mentor": "Mentor-D",
                "mentor_id": "EMP-004",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-014",
                "student_national_code": "",
                "mentor": "Mentor-D",
                "mentor_id": "EMP-004",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-015",
                "student_national_code": "",
                "mentor": "Mentor-D",
                "mentor_id": "EMP-004",
                "mentor_alias_code": "",
            },
            {
                "student_id": "STU-016",
                "student_national_code": "",
                "mentor": "Mentor-D",
                "mentor_id": "EMP-004",
                "mentor_alias_code": "",
            },
        ]
    )
    pd.testing.assert_frame_equal(
        _sort_alloc(alloc1), _sort_alloc(expected_allocations)
    )
