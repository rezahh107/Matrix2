from __future__ import annotations

"""توابع حاکمیت استخر منتورها (POOL_01) با ورودی Override ساده.

این ماژول هیچ I/O یا وابستگی به Qt ندارد و صرفاً روی DataFrame
کار می‌کند تا بر اساس پیکربندی Policy یا overrideهای UI/CLI منتورها
را فعال/غیرفعال کند.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd

from app.core.policy_loader import MentorPoolGovernanceConfig, MentorStatus

__all__ = [
    "compute_effective_status",
    "filter_active_mentors",
]


def compute_effective_status(
    mentors_df: pd.DataFrame,
    governance: MentorPoolGovernanceConfig,
    overrides: Mapping[int | str | float, bool] | None = None,
) -> pd.Series:
    """محاسبهٔ وضعیت مؤثر هر پشتیبان بر اساس Policy و overrideهای نوبت جاری.

    پارامترها
    ----------
    mentors_df:
        دیتافریم اولیهٔ پشتیبان‌ها که باید ستون ``mentor_id`` داشته باشد.
    governance:
        تنظیمات حاکمیت استخر از Policy.
    overrides:
        نگاشت اختیاری ``mentor_id`` → ``enabled`` برای فعال/غیرفعال‌سازی نوبتی.

    مثال
    -----
    >>> import pandas as pd
    >>> from app.core.policy_loader import MentorPoolGovernanceConfig, MentorStatus
    >>> df = pd.DataFrame({"mentor_id": [1, 2]})
    >>> config = MentorPoolGovernanceConfig(
    ...     default_status=MentorStatus.ACTIVE,
    ...     mentor_status_map={2: MentorStatus.INACTIVE},
    ...     allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
    ... )
    >>> compute_effective_status(df, config).tolist()
    [<MentorStatus.ACTIVE: 'active'>, <MentorStatus.INACTIVE: 'inactive'>]
    >>> compute_effective_status(df, config, overrides={2: True}).tolist()
    [<MentorStatus.ACTIVE: 'active'>, <MentorStatus.ACTIVE: 'active'>]
    """

    if "mentor_id" not in mentors_df.columns:
        raise KeyError("mentors_df must contain 'mentor_id' column")

    mentor_ids = pd.to_numeric(mentors_df["mentor_id"], errors="coerce")
    statuses = pd.Series(
        governance.default_status, index=mentors_df.index, dtype=object
    )

    policy_status = mentor_ids.map(governance.mentor_status_map)
    statuses = statuses.where(policy_status.isna(), policy_status)

    override_map: dict[int, MentorStatus] = {}
    if overrides:
        for raw_id, enabled in overrides.items():
            try:
                mentor_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            override_map[mentor_id] = MentorStatus.ACTIVE if bool(enabled) else MentorStatus.INACTIVE

    if override_map:
        override_status = mentor_ids.map(override_map)
        statuses = statuses.where(override_status.isna(), override_status)

    return statuses


def filter_active_mentors(
    mentors_df: pd.DataFrame,
    governance: MentorPoolGovernanceConfig,
    overrides: Mapping[int | str | float, bool] | None = None,
    *,
    attach_status: bool = False,
    status_column: str = "mentor_status",
) -> pd.DataFrame:
    """اعمال حاکمیت استخر و بازگرداندن استخر فعال.

    این تابع تغییری در ورودی ایجاد نمی‌کند و دیتافریم جدیدی می‌سازد که
    تنها پشتیبان‌های با وضعیت ``active`` را نگه می‌دارد. در صورت نیاز می‌توان
    وضعیت مؤثر را به‌صورت ستونی جداگانه نیز ضمیمه کرد.

    مثال
    -----
    >>> import pandas as pd
    >>> from app.core.policy_loader import MentorPoolGovernanceConfig, MentorStatus
    >>> df = pd.DataFrame({"mentor_id": [10, 20], "نام": ["الف", "ب"]})
    >>> config = MentorPoolGovernanceConfig(
    ...     default_status=MentorStatus.ACTIVE,
    ...     mentor_status_map={20: MentorStatus.INACTIVE},
    ...     allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
    ... )
    >>> filter_active_mentors(df, config)
       mentor_id نام
    0        10  الف
    >>> filter_active_mentors(df, config, overrides={20: True}, attach_status=True)
       mentor_id نام mentor_status
    0        10  الف         active
    1        20   ب         active
    """

    statuses = compute_effective_status(mentors_df, governance, overrides)
    active_mask = statuses == MentorStatus.ACTIVE
    filtered = mentors_df.loc[active_mask].copy()

    if attach_status:
        filtered.loc[:, status_column] = statuses.loc[filtered.index].map(lambda s: s.value)

    return filtered
