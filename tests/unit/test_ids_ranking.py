"""تست‌های واحد برای ماژول‌های ids و ranking."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.domain import BuildConfig, MentorType, compute_alias
from app.core.common.ids import build_mentor_id_map, ensure_ranking_columns, inject_mentor_id
from app.core.common.ranking import apply_ranking_policy
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
        "alias_rule": {"normal": "postal_or_fallback_mentor_id", "school": "mentor_id"},
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
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


def test_compute_alias_respects_policy_rules() -> None:
    cfg = BuildConfig()

    assert compute_alias(MentorType.NORMAL, "4001", "EMP-9", cfg=cfg) == "4001"
    assert compute_alias(MentorType.NORMAL, "401", "EMP-9", cfg=cfg) == ""
    assert compute_alias(MentorType.SCHOOL, "4001", 4001.0, cfg=cfg) == "4001"
