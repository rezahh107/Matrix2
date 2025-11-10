"""فیلترهای خالص برای کلیدهای اتصال دانش‌آموز به پشتیبان (Core-only).

این ماژول هیچ I/O انجام نمی‌دهد و تنها عملیات برداری روی DataFrameهای
pandas را اجرا می‌کند. هر تابع یکی از مراحل «Allocation 7-Pack» را پوشش
می‌دهد و در نهایت `apply_join_filters` ترتیب استاندارد را اعمال می‌کند.

مثال ساده::

    >>> import pandas as pd
    >>> from app.core.common.filters import apply_join_filters
    >>> pool = pd.DataFrame({
    ...     "کدرشته": [1201, 1202],
    ...     "گروه آزمایشی": ["تجربی", "ریاضی"],
    ...     "جنسیت": [1, 1],
    ...     "دانش آموز فارغ": [0, 0],
    ...     "مرکز گلستان صدرا": [1, 1],
    ...     "مالی حکمت بنیاد": [0, 0],
    ...     "کد مدرسه": [3581, 4001],
    ... })
    >>> student = {
    ...     "کدرشته": 1201,
    ...     "گروه_آزمایشی": "تجربی",
    ...     "جنسیت": 1,
    ...     "دانش_آموز_فارغ": 0,
    ...     "مرکز_گلستان_صدرا": 1,
    ...     "مالی_حکمت_بنیاد": 0,
    ...     "کد_مدرسه": 3581,
    ... }
    >>> apply_join_filters(pool, student).shape[0]
    1
"""

from __future__ import annotations

from typing import Callable, Mapping, Sequence

import pandas as pd

FilterFunc = Callable[[pd.DataFrame, Mapping[str, object]], pd.DataFrame]

__all__ = [
    "filter_by_type",
    "filter_by_group",
    "filter_by_gender",
    "filter_by_graduation_status",
    "filter_by_center",
    "filter_by_finance",
    "filter_by_school",
    "apply_join_filters",
]


def _student_value(student: Mapping[str, object], column: str) -> object:
    """بازیابی مقدار ستون از دانش‌آموز با پشتیبانی از آندرلاین/فاصله."""

    if column in student:
        return student[column]
    normalized = column.replace(" ", "_")
    if normalized in student:
        return student[normalized]
    raise KeyError(f"Student row missing value for '{column}'")


def _eq_filter(frame: pd.DataFrame, column: str, value: object) -> pd.DataFrame:
    """اعمال فیلتر مساوی روی دیتافریم بدون تغییر ورودی اصلی."""

    return frame.loc[frame[column] == value]


def filter_by_type(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ type بر اساس ستون «کدرشته»."""

    return _eq_filter(pool, "کدرشته", _student_value(student, "کدرشته"))


def filter_by_group(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ group بر اساس ستون «گروه آزمایشی» در DataFrame."""

    return _eq_filter(pool, "گروه آزمایشی", _student_value(student, "گروه آزمایشی"))


def filter_by_gender(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ gender بر اساس ستون «جنسیت»"""

    return _eq_filter(pool, "جنسیت", _student_value(student, "جنسیت"))


def filter_by_graduation_status(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ graduation_status بر اساس ستون «دانش آموز فارغ»"""

    return _eq_filter(pool, "دانش آموز فارغ", _student_value(student, "دانش آموز فارغ"))


def filter_by_center(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ center بر اساس ستون «مرکز گلستان صدرا»"""

    return _eq_filter(pool, "مرکز گلستان صدرا", _student_value(student, "مرکز گلستان صدرا"))


def filter_by_finance(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ finance بر اساس ستون «مالی حکمت بنیاد»"""

    return _eq_filter(pool, "مالی حکمت بنیاد", _student_value(student, "مالی حکمت بنیاد"))


def filter_by_school(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """فیلتر مرحلهٔ school بر اساس ستون «کد مدرسه»"""

    return _eq_filter(pool, "کد مدرسه", _student_value(student, "کد مدرسه"))


def apply_join_filters(pool: pd.DataFrame, student: Mapping[str, object]) -> pd.DataFrame:
    """اجرای ترتیبی هفت فیلتر join روی استخر کاندید بدون mutate کردن ورودی."""

    current = pool
    for fn in _FILTER_SEQUENCE:
        current = fn(current, student)
        if current.empty:
            break
    return current


_FILTER_SEQUENCE: Sequence[FilterFunc] = (
    filter_by_type,
    filter_by_group,
    filter_by_gender,
    filter_by_graduation_status,
    filter_by_center,
    filter_by_finance,
    filter_by_school,
)
