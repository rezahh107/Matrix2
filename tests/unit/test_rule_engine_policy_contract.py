import pandas as pd

from app.core.allocate_students import _collect_join_key_map
from app.core.common.filters import resolve_student_school_code
from app.core.common.ranking import apply_ranking_policy
from app.core.common.trace import build_trace_plan
from app.core.policy_loader import load_policy


def test_policy_join_keys_unique_and_int_enforced() -> None:
    """Rule R1: تضمین ۶ کلید Join و تبدیل به int در collect."""

    policy = load_policy()
    assert len(policy.join_keys) == 6
    student = {
        "student_id": "ST-100",
        "کدرشته": "1201",
        "جنسیت": "1",
        "دانش_آموز_فارغ": "0",
        "مرکز_گلستان_صدرا": "2",
        "مالی_حکمت_بنیاد": "0",
        "کد_مدرسه": "35-81",
    }
    join_map, missing = _collect_join_key_map(student, policy)
    assert missing == ()
    assert len(join_map) == 6
    assert all(isinstance(value, int) for value in join_map.values())


def test_trace_plan_matches_policy_order() -> None:
    """Rule R2: ترتیب تریس باید مطابق Policy (type→capacity) باشد."""

    policy = load_policy()
    plan = build_trace_plan(policy)
    stage_order = [stage.stage for stage in plan]
    assert stage_order == [
        "type",
        "group",
        "gender",
        "graduation_status",
        "center",
        "finance",
        "school",
        "capacity_gate",
    ]


def test_ranking_policy_respects_order_and_natural_sort() -> None:
    """Rule R3: ترتیب occ→alloc→natural mentor_id باید پایدار باشد."""

    policy = load_policy()
    candidate_pool = pd.DataFrame(
        {
            "کد کارمندی پشتیبان": ["EMP-2", "EMP-010", "EMP-3", "EMP-11"],
            "کدرشته": [1201, 1201, 1201, 1201],
            "جنسیت": [1, 1, 1, 1],
            "دانش آموز فارغ": [1, 1, 1, 1],
            "مرکز گلستان صدرا": [2, 2, 2, 2],
            "مالی حکمت بنیاد": [0, 0, 0, 0],
            "کد مدرسه": [3581, 3581, 3581, 3581],
        }
    )
    state = {
        "EMP-2": {"initial": 4, "remaining": 3, "alloc_new": 1},
        "EMP-010": {"initial": 4, "remaining": 2, "alloc_new": 0},
        "EMP-3": {"initial": 4, "remaining": 2, "alloc_new": 0},
        "EMP-11": {"initial": 4, "remaining": 1, "alloc_new": 0},
    }
    ranked = apply_ranking_policy(candidate_pool, state=state, policy=policy)
    assert list(ranked["کد کارمندی پشتیبان"]) == [
        "EMP-2",
        "EMP-3",
        "EMP-010",
        "EMP-11",
    ]
    ranking_names = [rule.name for rule in policy.ranking_rules]
    assert ranking_names == [
        "min_occupancy_ratio",
        "max_remaining_capacity",
        "min_allocations_new",
        "min_mentor_id",
    ]


def test_school_code_zero_behaves_as_wildcard() -> None:
    """Rule R6: صفر به‌عنوان wildcard مدرسه باید فعال باشد."""

    policy = load_policy()
    student = {"کد_مدرسه": 0}
    school_code = resolve_student_school_code(student, policy)
    assert school_code.value == 0
    assert school_code.wildcard is True
