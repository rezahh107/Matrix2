"""توابع کمکی لایهٔ Core برای برچسب زدن وضعیت صلاحیت و مراحل."""

from __future__ import annotations

from typing import Mapping

from app.core.policy_loader import PolicyConfig, load_policy
from .types import TraceStageLiteral

__all__ = ["build_stage_pass_flags"]


def build_stage_pass_flags(
    stage_candidate_counts: Mapping[str, int] | None,
    *,
    policy: PolicyConfig | None = None,
) -> dict[TraceStageLiteral, bool]:
    """تبدیل شمارندهٔ مراحل به فلگ عبور/عدم‌عبور برای Trace."""

    if policy is None:
        policy = load_policy()
    flags: dict[TraceStageLiteral, bool] = {
        stage: False for stage in policy.trace_stage_names
    }
    if not stage_candidate_counts:
        return flags
    for stage in policy.trace_stage_names:
        try:
            flags[stage] = int(stage_candidate_counts.get(stage, 0)) > 0
        except Exception:
            flags[stage] = False
    return flags

