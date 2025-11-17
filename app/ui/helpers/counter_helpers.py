from __future__ import annotations

"""توابع کمکی خالص برای پیشنهاد سال تحصیلی بر اساس شمارنده‌ها.

مثال:
    >>> autodetect_academic_year(Path("roster.xlsx"))
"""

from pathlib import Path
from typing import Tuple

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.core.counter import (
    detect_academic_year_from_counters,
    infer_year_strict,
    pick_counter_sheet_name,
)


def _load_counter_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        with pd.ExcelFile(path) as workbook:
            sheet_name = pick_counter_sheet_name(workbook.sheet_names)
            if sheet_name is None:
                sheet_name = workbook.sheet_names[0]
            return workbook.parse(sheet_name)
    if suffix == ".csv":
        return pd.read_csv(path)
    with pd.ExcelFile(path) as workbook:
        sheet_name = pick_counter_sheet_name(workbook.sheet_names)
        if sheet_name is None:
            sheet_name = workbook.sheet_names[0]
        return workbook.parse(sheet_name)


def detect_year_candidates(dataframe: pd.DataFrame) -> Tuple[int | None, int | None]:
    """محاسبهٔ سال تحصیلی (سخت‌گیرانه و تقریبی) از دیتافریم شمارنده."""

    canonical = canonicalize_headers(dataframe, header_mode="en")
    strict_year = infer_year_strict(canonical)
    fallback_year = detect_academic_year_from_counters(canonical)
    return strict_year, fallback_year


def autodetect_academic_year(roster_path: Path) -> int | None:
    """بازگرداندن سال تحصیلی پیشنهادی از فایل شمارنده."""

    dataframe = _load_counter_dataframe(roster_path)
    strict_year, fallback_year = detect_year_candidates(dataframe)
    return strict_year or fallback_year
