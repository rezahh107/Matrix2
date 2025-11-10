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
from typing import Iterable, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
from .ids import ensure_ranking_columns

__all__ = ["apply_ranking_policy"]

_DEFAULT_POLICY_PATH = Path("config/policy.json")


@dataclass(frozen=True)
class _SortRule:
    column: str
    ascending: bool


_RULE_MAP: dict[str, _SortRule] = {
    "min_occupancy_ratio": _SortRule("occupancy_ratio", True),
    "min_allocations_new": _SortRule("allocations_new", True),
    "min_mentor_id": _SortRule("mentor_sort_key", True),
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
    resolved_rules = list(_resolve_rules(policy.ranking))
    for rule in reversed(resolved_rules):
        ranked = ranked.sort_values(
            by=rule.column,
            ascending=rule.ascending,
            kind="stable",
        )
    # پس از اجرای همهٔ قوانین، ایندکس بازتنظیم می‌شود تا بازتولیدپذیری کامل حفظ گردد.
    return ranked.reset_index(drop=True)
