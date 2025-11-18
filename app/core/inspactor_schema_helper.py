from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Collection, Iterable, Mapping

import pandas as pd

from app.core.common.columns import accepted_synonyms
from app.core.common.normalization import to_numlike_str


@dataclass(frozen=True)
class InspactorDefaultConfig:
    """پیکربندی ستون‌های قابل پرشدن پیش‌فرض برای گزارش Inspactor."""

    school_code_columns: tuple[str, ...]
    school_count_column: str
    derived_factories: Mapping[str, Callable[[pd.DataFrame], pd.Series]]

def missing_inspactor_columns(df: pd.DataFrame, required: Collection[str]) -> list[str]:
    columns = set(map(str, df.columns))
    return sorted(col for col in required if col not in columns)


def infer_school_count(df: pd.DataFrame, school_code_columns: Iterable[str]) -> pd.Series:
    """تخمین تعداد مدارس پوشش داده‌شده بر اساس ستون‌های کد مدرسه.

    هر سطر با شمارش مقدارهای عددی/متنی غیرتهی در ستون‌های کد مدرسه برآورد می‌شود.
    خروجی همیشه `Int64` و پایدار نسبت به ترتیب ستون‌ها است.
    """

    present = [col for col in school_code_columns if col in df.columns]
    if not present:
        return pd.Series([0] * len(df), index=df.index, dtype="Int64")

    counts = [
        sum(1 for value in row if to_numlike_str(value))
        for row in df[present].itertuples(index=False)
    ]
    return pd.Series(counts, index=df.index, dtype="Int64")


def with_default_inspactor_columns(df: pd.DataFrame, cfg: InspactorDefaultConfig) -> pd.DataFrame:
    """اضافه‌کردن ستون‌های مشتق‌شدنی Inspactor با مقادیر پیش‌فرض پایدار.

    فقط ستون‌های غیروحیاتی/مشتق‌شدنی (ظرفیت، کدپستی، تعداد مدارس) در صورت فقدان
    ساخته می‌شوند. ستون‌های موجود دست‌نخورده باقی می‌مانند.
    """

    result = df.copy()
    fillers: dict[str, Callable[[pd.DataFrame], pd.Series]] = {
        cfg.school_count_column: lambda frame: infer_school_count(frame, cfg.school_code_columns),
        **cfg.derived_factories,
    }

    for column, factory in fillers.items():
        if column not in result.columns:
            result[column] = factory(result)

    return result


def schema_error_message(missing: Collection[str], policy: object) -> str:
    columns = list(missing) or ["<unknown>"]
    joined = ", ".join(columns)
    version = getattr(policy, "version", "<unknown>")
    return f"[policy {version}] missing Inspactor columns: {joined}"


def missing_inspactor_diagnostics(df: pd.DataFrame, missing: Collection[str]) -> str:
    """تولید گزارش خطای غنی برای ستون‌های مفقود Inspactor.

    - فهرست سینونیم‌های قابل قبول برای هر ستون مفقود را بر اساس Policy بازمی‌گرداند.
    - چند ستون اول موجود در ورودی را برای خطایابی سریع نشان می‌دهد.

    Args:
        df: دیتافریم خام ورودی.
        missing: ستون‌های اجباری که پیدا نشده‌اند.

    Returns:
        str: متن کمکی برای الحاق به پیام خطا.
    """

    if not missing:
        return ""

    synonyms = {
        column: accepted_synonyms("inspactor", column) for column in missing
    }
    seen = [str(column) for column in df.columns]
    preview = ", ".join(seen[:8]) if seen else "<no columns>"
    return f" | accepted: {synonyms} | seen: {preview} (total={len(seen)})"
