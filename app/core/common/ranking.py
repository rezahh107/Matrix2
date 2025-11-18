"""منطق رتبه‌بندی پشتیبان‌ها طبق Policy نسخهٔ 1.0.3."""

from __future__ import annotations

from hashlib import blake2b
from pathlib import Path
from numbers import Number
from typing import Any, Dict, Mapping, Sequence

import pandas as pd

from app.core.common.columns import canonicalize_headers, dedupe_columns
from app.core.policy_loader import PolicyConfig, load_policy
from .types import natural_key
from .ids import ensure_ranking_columns
from .reasons import ReasonCode, build_reason

__all__ = [
    "natural_key",
    "build_mentor_state",
    "apply_ranking_policy",
    "consume_capacity",
    "ensure_ranking_columns",
]

_DEFAULT_POLICY_PATH = Path("config/policy.json")


def build_mentor_state(
    pool_df: pd.DataFrame,
    *,
    capacity_column: str = "remaining_capacity",
    policy: PolicyConfig | None = None,
) -> Dict[Any, Dict[str, float | int]]:
    """ساخت وضعیت ظرفیت اولیهٔ پشتیبان‌ها برای تخصیص و Rule Engine."""

    if policy is None:
        policy = load_policy()

    canonical = dedupe_columns(canonicalize_headers(pool_df, header_mode="en"))
    if "mentor_sort_key" not in canonical.columns and "mentor_id" in canonical.columns:
        canonical = canonical.copy()
        canonical["mentor_sort_key"] = canonical["mentor_id"].map(natural_key)
    sort_candidates = [
        column
        for column in ("occupancy_ratio", "allocations_new", "mentor_sort_key")
        if column in canonical.columns
    ]
    if sort_candidates:
        canonical = canonical.sort_values(
            by=sort_candidates,
            ascending=[True] * len(sort_candidates),
            kind="stable",
        ).reset_index(drop=True)
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
    state: Dict[Any, Dict[str, float | int]] = {}
    for mentor_id, capacity in initial.items():
        value = int(max(capacity, 0))
        state[mentor_id] = {
            "initial": value,
            "remaining": value,
            "alloc_new": 0,
            "occupancy_ratio": 0.0,
            "total_capacity": value,
            "current_allocations": 0,
            "remaining_capacity": value,
        }
    return state


def apply_ranking_policy(
    candidate_pool: pd.DataFrame,
    *,
    state: Mapping[Any, Mapping[str, object]] | None = None,
    policy: PolicyConfig | None = None,
    policy_path: str | Path = _DEFAULT_POLICY_PATH,
) -> pd.DataFrame:
    """مرتب‌سازی استخر کاندید با قوانین Policy و حالت ظرفیت."""

    if policy is None:
        policy = load_policy(policy_path)

    ranked = candidate_pool.copy()
    en_view = dedupe_columns(canonicalize_headers(ranked, header_mode="en"))
    state_source = en_view.copy()

    if "allocations_new" not in ranked.columns and "allocations_new" in en_view.columns:
        ranked["allocations_new"] = en_view["allocations_new"]
    if "allocations_new" not in ranked.columns:
        ranked["allocations_new"] = 0
    if "occupancy_ratio" not in ranked.columns and "occupancy_ratio" in en_view.columns:
        ranked["occupancy_ratio"] = en_view["occupancy_ratio"]
    if "occupancy_ratio" not in ranked.columns:
        ranked["occupancy_ratio"] = 0.0
    if "کد کارمندی پشتیبان" not in ranked.columns:
        mentor_id_series = en_view.get("mentor_id")
        if mentor_id_series is None:
            raise KeyError("candidate pool must include mentor identifier column")
        if isinstance(mentor_id_series, pd.DataFrame):
            mentor_id_series = mentor_id_series.iloc[:, 0]
        ranked["کد کارمندی پشتیبان"] = mentor_id_series

    ranked = ensure_ranking_columns(ranked)
    en_view = dedupe_columns(canonicalize_headers(ranked, header_mode="en"))
    mentor_ids = en_view.get("mentor_id")
    if isinstance(mentor_ids, pd.DataFrame):
        mentor_ids = mentor_ids.iloc[:, 0]
    if mentor_ids is None:
        raise KeyError("candidate pool must include 'mentor_id' column after canonicalization")

    state_view: Mapping[Any, Mapping[str, object]]
    state_view = (
        state if state is not None else build_mentor_state(state_source, policy=policy)
    )

    def _state_value(mentor: Any, key: str) -> int:
        entry = state_view.get(mentor)
        if not entry:
            return 0
        raw = entry.get(key, 0)
        try:
            return int(raw)  # type: ignore[arg-type]
        except (ValueError, TypeError):  # pragma: no cover - نگهبان ورودی پیش‌بینی‌نشده
            return 0

    def _series_as_int(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce").fillna(0)
        return numeric.astype(int)

    initial = mentor_ids.map(lambda mentor: _state_value(mentor, "initial"))
    remaining = mentor_ids.map(lambda mentor: _state_value(mentor, "remaining"))
    allocations = mentor_ids.map(lambda mentor: _state_value(mentor, "alloc_new"))

    initial_int = _series_as_int(initial)
    remaining_int = _series_as_int(remaining)
    allocations_int = _series_as_int(allocations)

    safe_initial = initial_int.mask(initial_int <= 0, 1)
    occupancy = (initial_int - remaining_int) / safe_initial

    ranked["occupancy_ratio"] = occupancy.astype(float)
    ranked["allocations_new"] = allocations_int
    ranked["remaining_capacity"] = remaining_int
    ranked["remaining_capacity_desc"] = (-remaining_int).astype(int)
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
    ranked = ranked.reset_index(drop=True)
    tie_columns: Sequence[str]
    if len(sort_columns) > 1:
        tie_columns = tuple(sort_columns[:-1])
    else:
        tie_columns = tuple(sort_columns)
    ranked["__fair_origin__"] = ranked.index
    strategy = getattr(policy, "fairness_strategy", "none") or "none"
    ranked, fairness_applied = _apply_fairness_strategy(
        ranked,
        strategy=strategy,
        tie_columns=tie_columns,
    )
    if fairness_applied:
        ranked.attrs["fairness_reason"] = build_reason(ReasonCode.FAIRNESS_ORDER)
    ranked.attrs["fairness_strategy"] = strategy
    ranked = ranked.drop(columns=["__fair_origin__"], errors="ignore")
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


def consume_capacity(
    state: Dict[Any, Dict[str, float | int]], mentor_id: Any
) -> tuple[int, int, float]:
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
    entry["remaining_capacity"] = after
    entry["current_allocations"] = _coerce_capacity_value(
        entry.get("current_allocations", 0)
    ) + 1
    initial = _coerce_capacity_value(entry.get("initial", before))
    if initial <= 0:
        initial = max(before, 1)
    entry["total_capacity"] = max(
        initial, _coerce_capacity_value(entry.get("total_capacity", initial))
    )
    denominator = max(initial, 1)
    occupancy_ratio = (initial - after) / denominator
    entry["occupancy_ratio"] = float(occupancy_ratio)
    return before, after, float(occupancy_ratio)


_FAIRNESS_COUNTER_CANDIDATES: tuple[str, ...] = (
    "counter",
    "allocation_counter",
    "student_id",
    "row_number",
    "شمارنده",
)


def _hash_counter_series(series: pd.Series) -> pd.Series:
    from app.core.counter import stable_counter_hash, validate_counter

    def _hash(value: object) -> int:
        text = str(value or "").strip()
        if not text:
            text = "0"
        try:
            normalized = validate_counter(text)
        except ValueError:
            fallback = re.sub(r"\D", "", text) or text or "0"
            digest = blake2b(fallback.encode("utf-8"), digest_size=8)
            return int.from_bytes(digest.digest(), "big")
        return stable_counter_hash(normalized)

    return series.map(_hash)


def _apply_deterministic_jitter(df: pd.DataFrame, tie_columns: Sequence[str]) -> pd.DataFrame:
    source: pd.Series | None = None
    for column in _FAIRNESS_COUNTER_CANDIDATES:
        if column in df.columns:
            source = df[column].astype("string")
            break
    if source is None:
        source = df.index.astype("string")
    jitter = _hash_counter_series(source)
    order = list(tie_columns) + ["__fairness_key__"]
    df = df.assign(__fairness_key__=jitter)
    df = df.sort_values(order, kind="stable")
    return df.drop(columns=["__fairness_key__"])


def _hash_text(value: object) -> int:
    text = str(value or "").strip() or "0"
    digest = blake2b(text.encode("utf-8"), digest_size=8)
    return int.from_bytes(digest.digest(), "big")


def _apply_round_robin(df: pd.DataFrame, tie_columns: Sequence[str]) -> pd.DataFrame:
    if "mentor_id_en" not in df.columns:
        return df
    groups = df.groupby(list(tie_columns), sort=False, group_keys=False)
    frames: list[pd.DataFrame] = []
    for _, block in groups:
        if len(block) <= 1:
            frames.append(block)
            continue
        block = block.copy()
        block["__fairness_key__"] = block["mentor_id_en"].astype("string").map(_hash_text)
        block = block.sort_values("__fairness_key__", kind="stable")
        block = block.drop(columns=["__fairness_key__"])
        frames.append(block)
    if not frames:
        return df
    return pd.concat(frames, ignore_index=True)


def _apply_fairness_strategy(
    ranked: pd.DataFrame,
    *,
    strategy: str,
    tie_columns: Sequence[str],
) -> tuple[pd.DataFrame, bool]:
    if strategy == "none" or ranked.empty or not tie_columns:
        return ranked, False
    working = ranked.copy()
    original = tuple(working.get("__fair_origin__", working.index))
    if strategy == "deterministic_jitter":
        working = _apply_deterministic_jitter(working, tie_columns)
    elif strategy == "round_robin":
        working = _apply_round_robin(working, tie_columns)
    else:
        return ranked, False
    applied = tuple(working.get("__fair_origin__", working.index)) != original
    return working, applied
