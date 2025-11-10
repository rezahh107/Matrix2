"""اجرای سیاست رتبه‌بندی پشتیبان‌ها طبق Policy و SSoT.

این ماژول فقط منطق مرتب‌سازی را پیاده می‌کند و هیچ I/Oای انجام نمی‌دهد.
مسیر پیش‌فرض سیاست از ``config/policy.json`` خوانده می‌شود تا اصل
Policy-First حفظ گردد.

مثال::

    >>> import pandas as pd
    >>> from app.core.common.ranking import apply_ranking_policy
    >>> df = pd.DataFrame({
    ...     "پشتیبان": ["الف", "ب", "ج"],
    ...     "کد کارمندی پشتیبان": ["EMP-010", "EMP-002", "EMP-001"],
    ...     "occupancy_ratio": [0.2, 0.2, 0.2],
    ...     "allocations_new": [1, 1, 1],
    ... })
    >>> sorted_df = apply_ranking_policy(df)
    >>> sorted_df["کد کارمندی پشتیبان"].tolist()
    ['EMP-001', 'EMP-002', 'EMP-010']
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
from .ids import ensure_ranking_columns, natural_key

__all__ = ["apply_ranking_policy"]

_DEFAULT_POLICY_PATH = Path("config/policy.json")


def _natural_sort_series(series: pd.Series) -> pd.Series:
    """تبدیل سری mentor_id به کلیدهای قابل sort طبیعی."""

    return series.fillna("").map(natural_key)


@dataclass(frozen=True)
class _SortRule:
    column: str
    ascending: bool
    key: Callable[[pd.Series], pd.Series] | None = None


_RULE_MAP: dict[str, _SortRule] = {
    "min_occupancy_ratio": _SortRule("occupancy_ratio", True, None),
    "min_allocations_new": _SortRule("allocations_new", True, None),
    "min_mentor_id": _SortRule("mentor_id_str", True, _natural_sort_series),
}


def _resolve_rules(order: Sequence[str]) -> Iterable[_SortRule]:
    for item in order:
        rule = _RULE_MAP.get(item)
        if rule is None:
            raise ValueError(f"Unsupported ranking rule: {item}")
        yield rule


def apply_ranking_policy(
    candidate_pool: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
    policy_path: str | Path = _DEFAULT_POLICY_PATH,
) -> pd.DataFrame:
    """مرتب‌سازی استخر کاندیدها بر اساس سیاست رسمی.

    Args:
        candidate_pool: دیتافریم شامل ستون‌های مورد نیاز برای رتبه‌بندی.
        policy: در تست‌ها می‌توان :class:`PolicyConfig` آماده پاس داد تا از
            خواندن فایل جلوگیری شود.
        policy_path: مسیر جایگزین فایل سیاست در صورت نیاز.

    Returns:
        pd.DataFrame: دیتافریم جدید مرتب‌شده با حفظ sort پایدار.

    Raises:
        ValueError: اگر قانون ناشناخته‌ای در Policy وجود داشته باشد.
        KeyError: در صورت فقدان ستون‌های ضروری.
    """

    if policy is None:
        policy = load_policy(policy_path)

    ranked = ensure_ranking_columns(candidate_pool)
    for rule in reversed(list(_resolve_rules(policy.ranking))):
        if rule.key is not None:
            ranked = ranked.sort_values(
                by=rule.column,
                ascending=rule.ascending,
                kind="stable",
                key=rule.key,
            )
        else:
            ranked = ranked.sort_values(
                by=rule.column,
                ascending=rule.ascending,
                kind="stable",
            )
    return ranked
