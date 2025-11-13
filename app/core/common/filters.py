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
from .normalization import strip_school_code_separators, to_numlike_str

_SCHOOL_CODE_TRANSLATION = str.maketrans(
    {
        "-": " ",
        "−": " ",  # minus sign
        "‑": " ",  # non-breaking hyphen
        "–": " ",  # en dash
        "—": " ",  # em dash
        "―": " ",  # horizontal bar
        "﹘": " ",  # small em dash
        "﹣": " ",  # small hyphen-minus
        "／": " ",  # full-width slash
        "/": " ",
        "\\": " ",
        "⁄": " ",
        "ـ": "",  # kashida
    }
)


@dataclass(frozen=True)
class StudentSchoolCode:
    """نمایش نرمال‌شدهٔ «کد مدرسه» همراه با وضعیت کمبود و wildcard."""

    value: int | None
    missing: bool
    wildcard: bool


def _coerce_school_candidate(candidate: object) -> tuple[int | None, bool]:
    """تبدیل مقدار خام کد مدرسه به int یا علامت‌گذاری کمبود.

    این تابع پیش از تفسیر مقدار، همهٔ جداکننده‌های رایج (خط تیره، اسلش، کشیده)
    را حذف می‌کند تا مقادیر نظیر «۳۵-۸۱» یا «35/81» نیز به‌درستی به 3581 تبدیل شوند.
    """

    if candidate is None or candidate is pd.NA:
        return None, True
    if isinstance(candidate, Number) and not isinstance(candidate, bool):
        if pd.isna(candidate):  # type: ignore[arg-type]
            return None, True
        return int(candidate), False
    if isinstance(candidate, (bytes, bytearray)):
        try:
            candidate = candidate.decode("utf-8", "ignore")
        except Exception:
            return None, True
    if isinstance(candidate, str):
        candidate = candidate.translate(_SCHOOL_CODE_TRANSLATION)
    text = to_numlike_str(candidate).strip()
    if not text:
        return None, True
    try:
        return int(float(text)), False
    except ValueError:
        return None, True


def _sanitize_school_series(series: pd.Series) -> pd.Series:
    """بازگرداندن Series از مقادیر نرمال‌شدهٔ کد مدرسه بدون mutate ورودی.

    مثال کوتاه::

        >>> import pandas as pd
        >>> _sanitize_school_series(pd.Series(["35-81", "۳۵/۸۱"]))
        0    3581
        1    3581
        dtype: Int64
    """

    cleaned: list[object] = []
    for value in series.tolist():
        coerced, missing = _coerce_school_candidate(value)
        cleaned.append(pd.NA if missing else coerced)
    result = pd.Series(cleaned, index=series.index)
    numeric = pd.to_numeric(result, errors="coerce")
    return numeric.astype("Int64")


def filter_school_by_value(
    frame: pd.DataFrame, column: str, target: int
) -> tuple[pd.DataFrame, bool]:
    """فیلتر کردن ستون مدرسه با رعایت نرمال‌سازی و گزارش تطبیق."""

    column_series = frame[column]
    if pd.api.types.is_integer_dtype(column_series):
        mask = column_series == target
    else:
        sanitized = _sanitize_school_series(column_series)
        mask = sanitized == target
    matched = bool(mask.any())
    if not matched:
        return frame, False
    return frame.loc[mask], True


def resolve_student_school_code(
    student: Mapping[str, object],
    policy: PolicyConfig,
) -> StudentSchoolCode:
    """استخراج مقدار استاندارد کد مدرسه با درنظرگرفتن سیاست wildcard."""

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

FilterFunc = Callable[
    [
        pd.DataFrame,
        Mapping[str, object],
        PolicyConfig,
        Mapping[str, int] | None,
    ],
    pd.DataFrame,
]
FilterTracker = Callable[[str, int], None]

__all__ = [
    "StudentSchoolCode",
    "FilterTracker",
    "filter_by_type",
    "filter_by_group",
    "filter_by_gender",
    "filter_by_graduation_status",
    "filter_by_center",
    "filter_by_finance",
    "filter_by_school",
    "filter_school_by_value",
    "resolve_student_school_code",
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


def _coerce_center_candidate(candidate: object) -> int | None:
    """تبدیل مقدار مرکز به عدد صحیح یا None برای حالت wildcard."""

    if candidate is None or candidate is pd.NA:
        return None
    if isinstance(candidate, Number) and not isinstance(candidate, bool):
        if pd.isna(candidate):  # type: ignore[arg-type]
            return None
        return int(candidate)
    text = to_numlike_str(candidate)
    if text is None:
        return None
    text = text.strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _student_center_value(student: Mapping[str, object], column: str) -> int | None:
    """استخراج مقدار مرکز دانش‌آموز با مدیریت ستون‌های معادل."""

    try:
        raw = _student_value(student, column)
    except KeyError:
        return None
    return _coerce_center_candidate(raw)


def _center_wildcard(policy: PolicyConfig) -> int | None:
    """خواندن مقدار wildcard (مانند '*': 0) از policy.center_map."""

    wildcard = policy.center_map.get("*")
    if wildcard is None:
        return None
    return int(wildcard)


def _is_center_wildcard(value: int | None, policy: PolicyConfig) -> bool:
    """بررسی می‌کند که آیا مقدار مرکز باید فیلتر را غیرفعال کند یا خیر."""

    if value is None:
        return True
    wildcard = _center_wildcard(policy)
    if wildcard is None:
        return False
    return value == wildcard


def _eq_filter(frame: pd.DataFrame, column: str, value: object) -> pd.DataFrame:
    """اعمال فیلتر مساوی روی دیتافریم بدون تغییر ورودی اصلی."""

    return frame.loc[frame[column] == value]


def _filter_by_stage(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig,
    stage: str,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    column = policy.stage_column(stage)
    normalized = column.replace(" ", "_")
    if student_join_map and normalized in student_join_map:
        value = student_join_map[normalized]
    else:
        value = _student_value(student, column)
    return _eq_filter(pool, column, value)


def filter_by_type(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر مرحلهٔ type بر اساس ستون اعلام‌شده در Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(
        pool,
        student,
        policy,
        "type",
        student_join_map=student_join_map,
    )


def filter_by_group(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر مرحلهٔ group با ستون پویا از Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(
        pool,
        student,
        policy,
        "group",
        student_join_map=student_join_map,
    )


def filter_by_gender(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر gender با ستون تعریف‌شده در Policy."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(
        pool,
        student,
        policy,
        "gender",
        student_join_map=student_join_map,
    )


def filter_by_graduation_status(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر graduation_status با ستون پویا."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(
        pool,
        student,
        policy,
        "graduation_status",
        student_join_map=student_join_map,
    )


def filter_by_center(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر center با ستون پویا."""

    if policy is None:
        policy = load_policy()
    column = policy.stage_column("center")
    center_value = _student_center_value(student, column)
    if _is_center_wildcard(center_value, policy):
        return pool
    if center_value is None:
        return pool
    return _eq_filter(pool, column, center_value)


def filter_by_finance(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر finance با ستون پویا."""

    if policy is None:
        policy = load_policy()
    return _filter_by_stage(
        pool,
        student,
        policy,
        "finance",
        student_join_map=student_join_map,
    )


def filter_by_school(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    policy: PolicyConfig | None = None,
    *,
    student_join_map: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """فیلتر school با ستون پویا."""

    if policy is None:
        policy = load_policy()
    column = policy.stage_column("school")
    school_code = resolve_student_school_code(student, policy)
    if school_code.wildcard or school_code.missing:
        return pool
    if school_code.value is None:
        return pool
    target = int(school_code.value)
    filtered, matched = filter_school_by_value(pool, column, target)
    if not matched:
        return pool
    return filtered


def apply_join_filters(
    pool: pd.DataFrame,
    student: Mapping[str, object],
    *,
    policy: PolicyConfig | None = None,
    student_join_map: Mapping[str, int] | None = None,
    tracker: FilterTracker | None = None,
) -> pd.DataFrame:
    """اجرای ترتیبی هفت فیلتر join روی استخر کاندید بدون mutate کردن ورودی."""

    if policy is None:
        policy = load_policy()

    current = pool
    for index, (stage_name, fn) in enumerate(
        zip(_FILTER_STAGE_NAMES, _FILTER_SEQUENCE)
    ):
        current = fn(
            current,
            student,
            policy,
            student_join_map=student_join_map,
        )
        if tracker is not None:
            tracker(stage_name, int(current.shape[0]))
        if current.empty and tracker is not None:
            for remaining in _FILTER_STAGE_NAMES[index + 1 :]:
                tracker(remaining, 0)
            break
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

_FILTER_STAGE_NAMES: Sequence[str] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
)
