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

from dataclasses import dataclass
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
    "resolve_student_school_code",
    "StudentSchoolCode",
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


def _coerce_school_candidate(value: object) -> tuple[int | None, bool]:
    """تبدیل امن مقدار ستون مدرسه به int یا تشخیص کمبود داده."""

    if value is None:
        return None, True
    if isinstance(value, Number):
        if pd.isna(value):  # type: ignore[arg-type]
            return None, True
        try:
            return int(value), False
        except (TypeError, ValueError):
            return None, True
    text = to_numlike_str(value).strip()
    if not text:
        return None, True
    try:
        return int(float(text)), False
    except ValueError:
        return None, True


@dataclass(frozen=True)
class StudentSchoolCode:
    """نتیجهٔ استخراج کد مدرسهٔ دانش‌آموز با توجه به Policy."""

    value: int | None
    missing: bool
    wildcard: bool


def resolve_student_school_code(
    student: Mapping[str, object],
    policy: PolicyConfig,
) -> StudentSchoolCode:
    """بازیابی کد مدرسه دانش‌آموز با درنظرگرفتن فallback Policy.

    مثال ساده::

        >>> from app.core.common.filters import resolve_student_school_code
        >>> from app.core.policy_loader import load_policy
        >>> policy = load_policy()
        >>> student = {"school_code_norm": None}
        >>> resolve_student_school_code(student, policy).value
        0

    Returns:
        نمونهٔ ``StudentSchoolCode`` شامل مقدار نرمال‌شده، پرچم کمبود داده و
        وضعیت wildcard برای عبور از فیلتر مدرسه.
    """

    column = policy.stage_column("school")
    allow_zero = policy.school_code_empty_as_zero and (
        column == policy.columns.school_code
    )
    normalized = column.replace(" ", "_")
    candidate_keys = (
        column,
        normalized,
        "school_code_norm",
        "school_code",
        "school_code_raw",
    )
    candidates: list[object] = []
    for key in candidate_keys:
        if key in student:
            candidates.append(student[key])

    for candidate in candidates:
        value, missing = _coerce_school_candidate(candidate)
        if not missing:
            wildcard = bool(allow_zero and value == 0)
            return StudentSchoolCode(value=value, missing=False, wildcard=wildcard)

    if allow_zero:
        return StudentSchoolCode(value=0, missing=False, wildcard=True)

    return StudentSchoolCode(value=None, missing=True, wildcard=False)


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
    school_code = resolve_student_school_code(student, policy)
    if school_code.missing or school_code.wildcard:
        return pool
    if school_code.value is None:
        return pool
    return _eq_filter(pool, column, school_code.value)


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
