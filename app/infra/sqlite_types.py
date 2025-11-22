"""تبدیل‌های نوعی مشترک برای SQLite و pandas."""

from __future__ import annotations

from typing import Iterable, Mapping

import numpy as np
import pandas as pd


def coerce_int_like(value: object) -> int | pd.NA:
    """تبدیل مقدار به ``int`` در صورت امکان؛ در غیر این صورت ``pd.NA``.

    مثال
    ----
    >>> coerce_int_like("123")
    123
    >>> coerce_int_like("123.0")
    123
    >>> coerce_int_like(" ") is pd.NA
    False
    >>> pd.isna(coerce_int_like(" "))
    True
    """

    if value is None:
        return pd.NA
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return pd.NA
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return pd.NA
        try:
            return int(float(stripped))
        except ValueError:
            return pd.NA
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        try:
            return int(float(value))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return pd.NA


def coerce_int_series(series: pd.Series, *, fill_value: int | None = None) -> pd.Series:
    """تبدیل امن یک سری به نوع ``Int64`` با مقادیر تهی پایدار."""

    coerced = series.map(coerce_int_like)
    result = coerced.astype("Int64")
    if fill_value is not None:
        result = result.fillna(int(fill_value))
    return result


def coerce_int_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    *,
    fill_values: Mapping[str, int | None] | None = None,
) -> pd.DataFrame:
    """کپی دیتافریم با ستون‌های عددی تبدیل‌شده به ``Int64``.

    اگر ستونی موجود نباشد نادیده گرفته می‌شود و سایر ستون‌ها دست‌نخورده
    باقی می‌مانند.
    """

    if df is None:
        return pd.DataFrame()
    coerced = df.copy()
    for col in columns:
        if col in coerced.columns:
            fill_value = None
            if fill_values is not None and col in fill_values:
                fill_value = fill_values[col]
            coerced[col] = coerce_int_series(coerced[col], fill_value=fill_value)
    return coerced


__all__ = ["coerce_int_like", "coerce_int_series", "coerce_int_columns"]
