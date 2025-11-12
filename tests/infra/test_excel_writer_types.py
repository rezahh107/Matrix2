from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.policy_loader import load_policy
from app.infra.excel.exporter import write_selection_reasons_sheet


def test_writer_enforces_types_and_counter() -> None:
    policy = load_policy()
    df = pd.DataFrame(
        [
            {
                "شمارنده": 5,
                "کدملی": 12345,
                "نام": "الف",
                "نام خانوادگی": "ب",
                "شناسه پشتیبان": 201,
                "دلیل انتخاب پشتیبان": "متن"
            },
            {
                "شمارنده": 2,
                "کدملی": 67890,
                "نام": "ج",
                "نام خانوادگی": "د",
                "شناسه پشتیبان": 202,
                "دلیل انتخاب پشتیبان": "متن"
            },
        ]
    )

    sheet_name, sanitized = write_selection_reasons_sheet(df, writer=None, policy=policy)
    assert sheet_name == policy.emission.selection_reasons.sheet_name
    assert list(sanitized.columns) == list(policy.emission.selection_reasons.columns)
    assert sanitized["شمارنده"].tolist() == [1, 2]
    assert sanitized["کدملی"].dtype.name.startswith("string")
    assert sanitized["شناسه پشتیبان"].dtype.name.startswith("string")
    assert sanitized.attrs["schema_hash"] == policy.emission.selection_reasons.schema_hash
