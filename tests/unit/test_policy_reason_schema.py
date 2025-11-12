"""تست‌های مربوط به اسکیما و ستون‌های شیت دلایل پشتیبان."""

from __future__ import annotations

import pytest

from app.core.common.policy import load_selection_reason_policy
from app.core.policy_loader import load_policy


def test_policy_reason_columns_default() -> None:
    policy = load_policy()
    config = load_selection_reason_policy(policy)
    assert config.columns == (
        "شمارنده",
        "کدملی",
        "نام",
        "نام خانوادگی",
        "شناسه پشتیبان",
        "دلیل انتخاب پشتیبان",
    )


def test_policy_reason_columns_override() -> None:
    payload = {
        "version": "1.0.3",
        "emission": {
            "selection_reasons": {
                "columns": ["A", "B", "A", "C"],
            }
        },
    }
    with pytest.raises(ValueError):
        load_selection_reason_policy(payload)
