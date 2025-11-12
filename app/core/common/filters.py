"""فیلترهای خالص برای کلیدهای اتصال دانش‌آموز به پشتیبان (Core-only).

این ماژول هیچ I/O انجام نمی‌دهد و تنها عملیات برداری روی DataFrameهای
pandas را اجرا می‌کند. هر تابع یکی از مراحل «Allocation 7-Pack» را پوشش
می‌دهد و در نهایت `apply_join_filters` ترتیب استاندارد را اعمال می‌کند.
نام ستون‌ها به‌طور کامل از Policy خوانده می‌شود تا تغییرات بدون دستکاری
کد اعمال شوند.

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

from numbers import Number
from typing import Callable, Mapping, Sequence

import pandas as pd

from ..policy_loader import PolicyConfig, load_policy
from .normalization import to_numlike_str

FilterFunc = Callable[[pd.DataFrame, Mapping[str, object], PolicyConfig], pd.DataFrame]

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


def _filter_by_stage(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig,
    stage: str,
) -> pd.DataFrame:
    column = policy.stage_column(stage)
    return _eq_filter(pool, column, _student_value(student, column))


def filter_by_type(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر مرحلهٔ type بر اساس ستون اعلام‌شده در Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "type")


def filter_by_group(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر مرحلهٔ group با ستون پویا از Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "group")


def filter_by_gender(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر gender با ستون تعریف‌شده در Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "gender")


def filter_by_graduation_status(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر graduation_status با ستون پویا."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "graduation_status")


def filter_by_center(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر center با ستون پویا."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "center")


def filter_by_finance(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر finance با ستون پویا."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(pool, student, policy, "finance")


def filter_by_school(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """فیلتر school با ستون پویا."""

    if policy is None:
        policy = load_policy()
    column = policy.stage_column("school")
    allow_zero = policy.school_code_empty_as_zero and (
        column == policy.columns.school_code
    )
    if allow_zero:
        try:
            value = _student_value(student, column)
        except KeyError:
            return pool

        if isinstance(value, Number):
            if pd.isna(value):  # type: ignore[arg-type]
                return pool
            normalized_value = int(value)
        else:
            text = to_numlike_str(value).strip()
            if not text:
                return pool
            try:
                normalized_value = int(float(text))
            except ValueError:
                return pool

        if normalized_value == 0:
            return pool
        return _eq_filter(pool, column, normalized_value)

    return _filter_by_stage(pool, student, policy, "school")


def apply_join_filters(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    *,
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """اجرای ترتیبی هفت فیلتر join روی استخر کاندید بدون mutate کردن ورودی."""

    if policy is None:
        policy = load_policy()

    current = pool
    for fn in _FILTER_SEQUENCE:
        current = fn(current, student, policy)
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
