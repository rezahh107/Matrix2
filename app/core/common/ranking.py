"""منطق رتبه‌بندی پشتیبان‌ها طبق Policy نسخهٔ 1.0.3."""

from __future__ import annotations

import re
from pathlib import Path
from numbers import Number
from typing import Any, Dict, Mapping

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.core.policy_loader import PolicyConfig, load_policy
from .ids import ensure_ranking_columns

__all__ = [
    "natural_key",
    "build_mentor_state",
    "apply_ranking_policy",
    "consume_capacity",
    "ensure_ranking_columns",
]

_DEFAULT_POLICY_PATH = Path("config/policy.json")
_NUMERIC = re.compile(r"(\d+)")


def natural_key(value: Any) -> tuple[Any, ...]:
    """تولید کلید طبیعی برای مرتب‌سازی شناسه‌ها.

    مثال::

        >>> natural_key("EMP-10") > natural_key("EMP-2")
        True
    """

    if value is None:
        return ("",)
    text = str(value).strip()
    if not text:
        return ("",)
    parts: list[Any] = []
    for token in _NUMERIC.split(text):
        if not token:
            continue
        parts.append(int(token) if token.isdigit() else token.lower())
    return tuple(parts)


def build_mentor_state(
    pool_df: pd.DataFrame,
    *,
    capacity_column: str = "remaining_capacity",
    policy: PolicyConfig | None = None,
) -> Dict[Any, Dict[str, int]]:
    """ساخت وضعیت ظرفیت اولیهٔ پشتیبان‌ها برای تخصیص."""

    if policy is None:
        policy = load_policy()

    canonical = canonicalize_headers(pool_df, header_mode="en")
    if "mentor_id" not in canonical.columns:
        return {}

    candidates = [capacity_column]
    policy_defined = policy.columns.remaining_capacity
    if policy_defined not in candidates:
        candidates.append(policy_defined)
    canonical_candidate = canonicalize_headers(
        pd.DataFrame(columns=[capacity_column]), header_mode="en"
    ).columns[0]
    if canonical_candidate not in candidates:
        candidates.append(canonical_candidate)
    if "remaining_capacity" not in candidates:
        candidates.append("remaining_capacity")

    resolved_capacity: str | None = None
    for candidate in candidates:
        if candidate in canonical.columns:
            resolved_capacity = candidate
            break

    if resolved_capacity is None:
        return {}

    grouped = canonical.groupby("mentor_id", dropna=True)[resolved_capacity]
    initial = pd.to_numeric(grouped.max(), errors="coerce").fillna(0).astype(int)
    state: Dict[Any, Dict[str, int]] = {}
    for mentor_id, capacity in initial.items():
        value = int(max(capacity, 0))
        state[mentor_id] = {
            "initial": value,
            "remaining": value,
            "alloc_new": 0,
            "occupancy_ratio": 0.0,
        }
    return state


def apply_ranking_policy(
    candidate_pool: pd.DataFrame,
    *,
    state: Mapping[Any, Mapping[str, int]] | None = None,
    policy: PolicyConfig | None = None,
    policy_path: str | Path = _DEFAULT_POLICY_PATH,
) -> pd.DataFrame:
    """مرتب‌سازی استخر کاندید با قوانین Policy و حالت ظرفیت."""

    if policy is None:
        policy = load_policy(policy_path)

    ranked = ensure_ranking_columns(candidate_pool.copy())
    en_view = canonicalize_headers(ranked, header_mode="en")
    mentor_ids = en_view.get("mentor_id")
    if mentor_ids is None:
        raise KeyError("candidate pool must include 'mentor_id' column after canonicalization")

    state_view: Mapping[Any, Mapping[str, int]]
    state_view = (
        state if state is not None else build_mentor_state(en_view, policy=policy)
    )

    def _state_value(mentor: Any, key: str) -> int:
        entry = state_view.get(mentor)
        if not entry:
            return 0
        raw = entry.get(key, 0)
        try:
            return int(raw)  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - نگهبان ورودی پیش‌بینی‌نشده
            return 0

    initial = mentor_ids.map(lambda mentor: _state_value(mentor, "initial"))
    remaining = mentor_ids.map(lambda mentor: _state_value(mentor, "remaining"))
    allocations = mentor_ids.map(lambda mentor: _state_value(mentor, "alloc_new"))

    safe_initial = initial.mask(initial <= 0, 1)
    occupancy = (initial - remaining) / safe_initial

    ranked["occupancy_ratio"] = occupancy.astype(float)
    ranked["allocations_new"] = allocations.astype(int)
    ranked["mentor_sort_key"] = mentor_ids.map(natural_key)
    ranked["mentor_id_en"] = mentor_ids

    sort_columns: list[str] = []
    ascending_flags: list[bool] = []
    for rule in policy.ranking_rules:
        if rule.column not in ranked.columns:
            raise KeyError(f"Ranking column '{rule.column}' missing from candidate pool")
        sort_columns.append(rule.column)
        ascending_flags.append(bool(rule.ascending))

    ranked = ranked.sort_values(by=sort_columns, ascending=ascending_flags, kind="stable")
    return ranked.reset_index(drop=True)


def _coerce_capacity_value(value: Any) -> int:
    """تبدیل امن مقادیر ظرفیت به عدد صحیح غیرمنفی."""

    if isinstance(value, Number):
        if pd.isna(value):  # type: ignore[arg-type]
            return 0
        return int(value)

    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except Exception:  # pragma: no cover - نگهبان ورودی غیرمنتظره
        return 0


def consume_capacity(state: Dict[Any, Dict[str, int]], mentor_id: Any) -> tuple[int, int, float]:
    """به‌روزرسانی ظرفیت پشتیبان پس از تخصیص و بازگشت ظرفیت قبل/بعد."""

    if mentor_id not in state:
        raise KeyError(f"Mentor '{mentor_id}' missing from state")
    entry = state[mentor_id]
    before = _coerce_capacity_value(entry.get("remaining", 0))
    if before <= 0:
        raise ValueError("CAPACITY_UNDERFLOW")
    after = before - 1
    entry["remaining"] = after
    entry["alloc_new"] = _coerce_capacity_value(entry.get("alloc_new", 0)) + 1
    initial = _coerce_capacity_value(entry.get("initial", before))
    if initial <= 0:
        initial = max(before, 1)
    denominator = max(initial, 1)
    occupancy_ratio = (initial - after) / denominator
    entry["occupancy_ratio"] = float(occupancy_ratio)
    return before, after, float(occupancy_ratio)
