"""تست‌های واحد برای ماژول‌های ids و ranking."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.ids import build_mentor_id_map, ensure_ranking_columns, inject_mentor_id
from app.core.common.ranking import apply_ranking_policy
from app.core.policy_loader import PolicyConfig


@pytest.fixture()
def _policy() -> PolicyConfig:
    """سیاست نمونه مطابق نسخهٔ 1.0.3 برای تست‌های رتبه‌بندی."""

    return PolicyConfig(
        version="1.0.3",
        normal_statuses=[1, 0],
        school_statuses=[1],
        join_keys=[
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        ranking=[
            "min_occupancy_ratio",
            "min_allocations_new",
            "min_mentor_id",
        ],
    )


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


def test_ensure_ranking_columns_adds_mentor_id_str() -> None:
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
    assert "mentor_id_str" not in pool.columns


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
    assert "mentor_id_str" not in pool.columns
