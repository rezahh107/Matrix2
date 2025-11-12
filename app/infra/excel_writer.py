"""واسط سازگار برای توابع خروجی اکسل legacy."""

from __future__ import annotations

from typing import Tuple

import pandas as pd

from app.core.policy_loader import PolicyConfig
from .excel.exporter import write_selection_reasons_sheet as _write_selection_reasons_sheet

__all__ = ["write_selection_reasons_sheet"]


def write_selection_reasons_sheet(
    df_reasons: pd.DataFrame | None,
    writer: pd.ExcelWriter | None,
    policy: PolicyConfig,
) -> Tuple[str, pd.DataFrame]:
    """پوشش سازگار برای پیاده‌سازی جدید واقع در ``app.infra.excel.exporter``."""

    return _write_selection_reasons_sheet(df_reasons, writer, policy)
