"""ابزارهای الحاق کانال تخصیص به خروجی تریس/خلاصه."""

from __future__ import annotations

import pandas as pd

from app.core.policy_loader import PolicyConfig

from .engine import derive_channel_map


def attach_allocation_channel(
    summary_df: pd.DataFrame, students_df: pd.DataFrame, *, policy: PolicyConfig
) -> pd.DataFrame:
    """کپی summary با ستون «allocation_channel» مبتنی بر Policy."""

    if summary_df.empty or "student_id" not in summary_df.columns:
        return summary_df.copy()
    channel_map = derive_channel_map(students_df, policy)
    result = summary_df.copy()
    result["allocation_channel"] = result["student_id"].map(channel_map).fillna("")
    return result


__all__ = ["attach_allocation_channel"]
