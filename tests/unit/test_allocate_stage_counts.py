import pandas as pd

from app.core.allocate_students import allocate_student
from app.core.policy_loader import load_policy


def _pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": ["Mentor-X", "Mentor-Y"],
            "کد کارمندی پشتیبان": ["EMP-900", "EMP-901"],
            "کدرشته": [1201, 1201],
            "جنسیت": [1, 1],
            "دانش آموز فارغ": [1, 1],
            "مرکز گلستان صدرا": [2, 2],
            "مالی حکمت بنیاد": [0, 0],
            "کد مدرسه": [3581, 3581],
            "remaining_capacity": [2, 0],
            "allocations_new": [0, 0],
            "occupancy_ratio": [0.0, 0.0],
        }
    )


def _student() -> dict[str, object]:
    return {
        "student_id": "ST-TRACE",
        "کدرشته": 1201,
        "جنسیت": 1,
        "دانش آموز فارغ": 1,
        "مرکز گلستان صدرا": 2,
        "مالی حکمت بنیاد": 0,
        "کد مدرسه": 3581,
    }


def test_stage_candidate_counts_align_with_trace() -> None:
    """Rule R2 instrumentation: stage counts باید با Trace برابر باشد."""

    policy = load_policy()
    result = allocate_student(_student(), _pool(), policy=policy)
    counts = result.log["stage_candidate_counts"]
    assert set(counts.keys()) == {
        "type",
        "group",
        "gender",
        "graduation_status",
        "center",
        "finance",
        "school",
        "capacity_gate",
    }
    trace_counts = {record["stage"]: record["total_after"] for record in result.trace}
    for stage, expected_total in trace_counts.items():
        assert counts[stage] == expected_total
    assert counts["capacity_gate"] <= counts["school"]
