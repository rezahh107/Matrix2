"""تست‌های واحد برای ماژول‌های ids و ranking."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.columns import canonicalize_headers
from app.core.common.domain import BuildConfig, MentorType, compute_alias
from app.core.common.ids import (
    build_mentor_alias_map,
    build_mentor_id_map,
    ensure_ranking_columns,
    extract_alias_code_series,
    inject_mentor_id,
)
from app.core.common.ranking import (
    apply_ranking_policy,
    build_mentor_state,
    consume_capacity,
)
from app.core.policy_loader import PolicyConfig, parse_policy_dict


def _base_policy_payload() -> dict[str, object]:
    return {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "postal_valid_range": [1000, 9999],
        "finance_variants": [0, 1, 3],
        "center_map": {"شهدخت کشاورز": 1, "آیناز هوشمند": 2, "*": 0},
        "school_code_empty_as_zero": True,
        "prefer_major_code": True,
        "coverage_threshold": 0.95,
        "dedup_removed_ratio_threshold": 0.05,
        "school_lookup_mismatch_threshold": 0.0,
        "alias_rule": {"normal": "postal_or_fallback_mentor_id", "school": "mentor_id"},
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "gender_codes": {
            "male": {"value": 1, "counter_code": "357"},
            "female": {"value": 0, "counter_code": "373"},
        },
        "columns": {
            "postal_code": "کدپستی",
            "school_count": "تعداد مدارس تحت پوشش",
            "school_code": "کد مدرسه",
            "capacity_current": "تعداد داوطلبان تحت پوشش",
            "capacity_special": "تعداد تحت پوشش خاص",
            "remaining_capacity": "remaining_capacity",
        },
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio", "ascending": True},
            {"name": "min_allocations_new", "column": "allocations_new", "ascending": True},
            {"name": "min_mentor_id", "column": "mentor_sort_key", "ascending": True},
        ],
        "trace_stages": [
            {"stage": "type", "column": "کدرشته"},
            {"stage": "group", "column": "گروه آزمایشی"},
            {"stage": "gender", "column": "جنسیت"},
            {"stage": "graduation_status", "column": "دانش آموز فارغ"},
            {"stage": "center", "column": "مرکز گلستان صدرا"},
            {"stage": "finance", "column": "مالی حکمت بنیاد"},
            {"stage": "school", "column": "کد مدرسه"},
            {"stage": "capacity_gate", "column": "remaining_capacity"},
        ],
        "virtual_alias_ranges": [[7000, 7999]],
        "virtual_name_patterns": ["در\\s+انتظار\\s+تخصیص"],
        "excel": {
            "rtl": True,
            "font_name": "Tahoma",
            "font_size": 8,
            "header_mode_internal": "en",
            "header_mode_write": "fa_en",
        },
    }


@pytest.fixture()
def _policy() -> PolicyConfig:
    """سیاست نمونه مطابق نسخهٔ 1.0.3 برای تست‌های رتبه‌بندی."""

    return parse_policy_dict(_base_policy_payload())


def test_build_mentor_id_map_normalizes_inputs() -> None:
    matrix = pd.DataFrame(
        {
            "پشتیبان": [" زهرا ", "علی", "زهرا"],
            "کد کارمندی پشتیبان": ["001", "EMP-010", "001"],
        }
    )

    mapping = build_mentor_id_map(matrix)

    assert mapping["زهرا"] == "001"
    assert mapping["علی"] == "EMP-010"
    assert len(mapping) == 2


def test_build_mentor_alias_map_collects_stats() -> None:
    pool = pd.DataFrame(
        {
            "alias": ["0012345678", "009876543", None],
            "کد کارمندی پشتیبان": ["EMP-001", "", "EMP-777"],
        }
    )

    alias_series = extract_alias_code_series(pool)
    mapping, stats = build_mentor_alias_map(pool, alias_series=alias_series)

    assert mapping["12345678"] == "EMP-001"
    assert stats.total_alias_rows == 2
    assert stats.alias_rows_with_mentor == 1
    assert stats.alias_rows_without_mentor == 1
    assert stats.unique_aliases == 1


def test_inject_mentor_id_preserves_original_dataframe() -> None:
    pool = pd.DataFrame(
        {
            "پشتیبان": ["زهرا", "علی"],
            "کد کارمندی پشتیبان": ["", "EMP-010"],
            "occupancy_ratio": [0.3, 0.2],
            "allocations_new": [1, 2],
        }
    )
    id_map = {"زهرا": "EMP-001"}

    injected = inject_mentor_id(pool, id_map)

    assert injected.loc[0, "کد کارمندی پشتیبان"] == "EMP-001"
    assert pool.loc[0, "کد کارمندی پشتیبان"] == ""
    assert "mentor_id_str" not in pool.columns


def test_ensure_ranking_columns_adds_mentor_columns() -> None:
    pool = pd.DataFrame(
        {
            "پشتیبان": ["زهرا"],
            "کد کارمندی پشتیبان": ["EMP-001"],
            "occupancy_ratio": [0.1],
            "allocations_new": [0],
        }
    )

    prepared = ensure_ranking_columns(pool)

    assert "mentor_id_str" in prepared.columns
    assert prepared.loc[0, "mentor_id_str"] == "EMP-001"
    assert "mentor_sort_key" in prepared.columns
    assert prepared.loc[0, "mentor_sort_key"] == ("emp-", 1)
    assert "mentor_id_str" not in pool.columns
    assert "mentor_sort_key" not in pool.columns


def test_apply_ranking_policy_natural_tie_break(_policy: PolicyConfig) -> None:
    pool = pd.DataFrame(
        {
            "پشتیبان": ["الف", "ب", "ج"],
            "کد کارمندی پشتیبان": ["EMP-010", "EMP-002", "EMP-001"],
            "occupancy_ratio": [0.4, 0.4, 0.4],
            "allocations_new": [2, 2, 2],
        }
    )

    ranked = apply_ranking_policy(pool, policy=_policy)

    assert ranked["کد کارمندی پشتیبان"].tolist() == ["EMP-001", "EMP-002", "EMP-010"]
    assert ranked["mentor_id_str"].tolist() == ["EMP-001", "EMP-002", "EMP-010"]
    assert ranked["mentor_sort_key"].tolist() == [
        ("emp-", 1),
        ("emp-", 2),
        ("emp-", 10),
    ]
    assert "mentor_id_str" not in pool.columns
    assert "mentor_sort_key" not in pool.columns


@pytest.mark.parametrize(
    "payload",
    (
        {
            **_base_policy_payload(),
            "ranking": [
                "min_occupancy_ratio",
                "min_allocations_new",
                "min_mentor_id",
            ],
            "ranking_rules": [],
        },
        _base_policy_payload(),
    ),
    ids=["legacy", "extended"],
)
def test_ranking_payloads_equivalent(payload: dict[str, object]) -> None:
    policy_data = dict(payload)
    if not policy_data.get("ranking_rules"):
        policy_data.pop("ranking_rules", None)
    policy = parse_policy_dict(policy_data)
    pool = pd.DataFrame(
        {
            "پشتیبان": ["الف", "ب", "ج"],
            "کد کارمندی پشتیبان": ["EMP-010", "EMP-002", "EMP-001"],
            "occupancy_ratio": [0.4, 0.4, 0.4],
            "allocations_new": [2, 2, 2],
        }
    )

    ranked = apply_ranking_policy(pool, policy=policy)

    assert ranked["کد کارمندی پشتیبان"].tolist() == [
        "EMP-001",
        "EMP-002",
        "EMP-010",
    ]


def test_consume_capacity_handles_str_values_and_updates_state() -> None:
    state = {
        "EMP-1": {"remaining": "3", "initial": "5", "alloc_new": "2"},
    }

    before, after, ratio = consume_capacity(state, "EMP-1")

    assert before == 3
    assert after == 2
    assert ratio == pytest.approx((5 - 2) / 5)
    entry = state["EMP-1"]
    assert entry["remaining"] == 2
    assert entry["alloc_new"] == 3
    assert entry["occupancy_ratio"] == pytest.approx(ratio)


def test_consume_capacity_underflow_raises_value_error() -> None:
    state = {"EMP-2": {"remaining": "0", "initial": "4", "alloc_new": 1}}

    with pytest.raises(ValueError, match="CAPACITY_UNDERFLOW"):
        consume_capacity(state, "EMP-2")


def test_build_mentor_state_handles_persian_headers_and_normalizes_values(
    _policy: PolicyConfig,
) -> None:
    pool = pd.DataFrame(
        {
            "کد کارمندی پشتیبان": ["EMP-1", "EMP-2", "EMP-2", "EMP-NEG"],
            "remaining_capacity": [5, 3.5, None, -4],
        }
    )

    state = build_mentor_state(pool, policy=_policy)

    assert state["EMP-1"] == {
        "initial": 5,
        "remaining": 5,
        "alloc_new": 0,
        "occupancy_ratio": 0.0,
    }
    assert state["EMP-2"]["initial"] == 3
    assert state["EMP-2"]["remaining"] == 3
    assert state["EMP-NEG"]["initial"] == 0
    assert state["EMP-NEG"]["remaining"] == 0


def test_build_mentor_state_returns_empty_when_capacity_missing(
    _policy: PolicyConfig,
) -> None:
    pool = pd.DataFrame({"کد کارمندی پشتیبان": ["EMP-1", "EMP-2"]})

    state = build_mentor_state(pool, policy=_policy)

    assert state == {}


def test_build_mentor_state_prefers_explicit_capacity_column(
    _policy: PolicyConfig,
) -> None:
    custom_capacity_fa = _policy.columns.capacity_current
    pool = pd.DataFrame(
        {
            "کد کارمندی پشتیبان": ["EMP-1", "EMP-2"],
            custom_capacity_fa: [7, 0],
            "remaining_capacity": [1, 5],
        }
    )
    canonical_pool = canonicalize_headers(pool, header_mode="en")

    state = build_mentor_state(
        canonical_pool, capacity_column="capacity_current", policy=_policy
    )

    assert state["EMP-1"]["initial"] == 7
    assert state["EMP-1"]["remaining"] == 7
    assert state["EMP-2"]["initial"] == 0
    assert state["EMP-2"]["remaining"] == 0


def test_build_mentor_state_returns_empty_without_mentor_id(
    _policy: PolicyConfig,
) -> None:
    pool = pd.DataFrame({"remaining_capacity": [3, 4]})

    state = build_mentor_state(pool, policy=_policy)

    assert state == {}


def test_compute_alias_respects_policy_rules() -> None:
    cfg = BuildConfig()

    assert compute_alias(MentorType.NORMAL, "4001", "EMP-9", cfg=cfg) == "4001"
    assert compute_alias(MentorType.NORMAL, "401", "EMP-9", cfg=cfg) == ""
    assert compute_alias(MentorType.SCHOOL, "4001", 4001.0, cfg=cfg) == "4001"
