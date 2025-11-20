"""توابع حاکمیت استخر منتورها (POOL_01) با ورودی Override ساده.

این ماژول هیچ I/O یا وابستگی به Qt ندارد و صرفاً روی DataFrame
کار می‌کند تا بر اساس پیکربندی Policy یا overrideهای UI/CLI منتورها
را فعال/غیرفعال کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd


@dataclass(frozen=True)
class MentorPoolGovernanceConfig:
    """پیکربندی حاکمیت استخر منتورها.

    مثال::

        >>> import pandas as pd
        >>> from app.core.allocation.mentor_pool import (
        ...     MentorPoolGovernanceConfig, apply_mentor_pool_governance,
        ... )
        >>> pool = pd.DataFrame({"mentor_id": [1, 2], "mentor_status": ["ACTIVE", "FROZEN"]})
        >>> cfg = MentorPoolGovernanceConfig(enabled=True)
        >>> filtered = apply_mentor_pool_governance(pool, cfg)
        >>> list(filtered["mentor_id"])
        [1]

    """

    enabled: bool = False
    status_column: str = "mentor_status"
    active_values: tuple[str | int | bool, ...] = (
        "ACTIVE",
        "ENABLED",
        "true",
        1,
        "1",
        True,
    )
    inactive_values: tuple[str | int | bool, ...] = (
        "FROZEN",
        "INACTIVE",
        "DISABLED",
        "false",
        0,
        "0",
        False,
    )
    default_active: bool = True

    @classmethod
    def default(cls) -> "MentorPoolGovernanceConfig":
        """برگشت پیکربندی پیش‌فرض بدون تغییر رفتار موجود."""

        return cls()


def _normalize_mentor_id(value: object) -> str:
    text = str(value).strip()
    return text


def _normalize_override_map(overrides: Mapping[object, bool] | None) -> dict[str, bool]:
    if not overrides:
        return {}
    normalized: dict[str, bool] = {}
    for key, enabled in overrides.items():
        mentor_id = _normalize_mentor_id(key)
        if mentor_id:
            normalized[mentor_id] = bool(enabled)
    return normalized


def _coerce_status(value: object, *, config: MentorPoolGovernanceConfig) -> bool | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text in map(str, config.active_values) or text.upper() in {
        str(v).upper() for v in config.active_values
    }:
        return True
    if text in map(str, config.inactive_values) or text.upper() in {
        str(v).upper() for v in config.inactive_values
    }:
        return False
    if text.isdigit():
        return bool(int(text))
    return None


def apply_mentor_pool_governance(
    pool: pd.DataFrame,
    config: MentorPoolGovernanceConfig,
    *,
    overrides: Mapping[object, bool] | None = None,
) -> pd.DataFrame:
    """اعمال حاکمیت استخر روی DataFrame منتورها.

    Args:
        pool: دیتافریم canonical منتورها.
        config: پیکربندی فعال/غیرفعال کردن پیش‌فرض از Policy.
        overrides: نگاشت اختیاری ``mentor_id → enabled`` از UI/CLI.

    Returns:
        DataFrame فیلترشده. در attrs اطلاعات تعداد حذف شده درج می‌شود.
    """

    if pool is None or pool.empty:
        return pool.copy() if pool is not None else pd.DataFrame()

    normalized_overrides = _normalize_override_map(overrides)
    frame = pool.copy()
    if "mentor_id" not in frame.columns:
        frame.attrs["mentor_pool_governance"] = {"removed": 0}
        return frame

    enabled_mask = pd.Series(True, index=frame.index)
    disabled_by_status = 0

    if config.enabled and config.status_column in frame.columns:
        status_series = frame[config.status_column]
        derived = status_series.map(lambda value: _coerce_status(value, config=config))
        defaults = derived.fillna(config.default_active)
        enabled_mask &= defaults.astype(bool)
        disabled_by_status = int((~defaults.astype(bool)).sum())

    if normalized_overrides:
        mentor_ids = frame["mentor_id"].map(_normalize_mentor_id)
        override_mask = mentor_ids.map(lambda mid: normalized_overrides.get(mid, True))
        enabled_mask &= override_mask.astype(bool)

    filtered = frame.loc[enabled_mask].copy()
    removed_count = int((~enabled_mask).sum())
    filtered.attrs["mentor_pool_governance"] = {
        "removed": removed_count,
        "removed_by_status": disabled_by_status,
        "overrides_count": len(normalized_overrides),
    }
    return filtered

