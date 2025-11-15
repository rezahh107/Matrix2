from __future__ import annotations

import math

import pandas as pd
import pytest

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    COL_GROUP,
    COL_MANAGER_NAME,
    COL_MENTOR_NAME,
    COL_SCHOOL1,
    BuildConfig,
    capacity_gate,
)


@pytest.fixture()
def _capacity_gate_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            COL_MENTOR_NAME: ["منتور الف", "منتور ب", "منتور پ"],
            COL_MANAGER_NAME: ["مدیر 1", "مدیر 2", "مدیر 3"],
            COL_GROUP: ["ریاضی", "ریاضی", "تجربی"],
            COL_SCHOOL1: ["مدرسه 1", "مدرسه 2", "مدرسه 3"],
            CAPACITY_CURRENT_COL: [7, 3, 1],
            CAPACITY_SPECIAL_COL: [5, 3, 4],
        }
    )


def test_capacity_gate_reports_metrics_when_special_consumed(
    _capacity_gate_pool: pd.DataFrame,
) -> None:
    cfg = BuildConfig()

    kept, removed, metrics, skipped = capacity_gate(_capacity_gate_pool, cfg=cfg)

    assert skipped is False
    assert metrics.total_removed == len(removed) == 2
    expected_loss = int(
        _capacity_gate_pool.loc[
            _capacity_gate_pool[CAPACITY_CURRENT_COL]
            >= _capacity_gate_pool[CAPACITY_SPECIAL_COL],
            CAPACITY_SPECIAL_COL,
        ].sum()
    )
    assert metrics.total_special_capacity_lost == expected_loss
    expected_kept_ratio = len(kept) / len(_capacity_gate_pool)
    assert math.isclose(metrics.percent_pool_kept, expected_kept_ratio)
