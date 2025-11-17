"""تعریف قراردادهای دادهٔ حوزهٔ Eligibility Matrix (Core-only, بدون I/O).

این ماژول صرفاً تایپ‌ها را نگه می‌دارد و منطق ندارد. نگاشت کلیدهای join به
کمک :class:`JoinKeyValues` در قالب ساختار فقط‌خواندنی و با اجبار «۶ مقدار عددی»
ذخیره می‌شود تا خطاهای داده‌ای به‌سرعت شناسایی شوند.

مثال:
    >>> from app.core.common.types import JoinKeyValues
    >>> keys = JoinKeyValues({
    ...   "کدرشته": 1201,
    ...   "گروه_آزمایشی": 1,
    ...   "جنسیت": 1,
    ...   "دانش_آموز_فارغ": 0,
    ...   "مرکز_گلستان_صدرا": 1,
    ...   "مالی_حکمت_بنیاد": 0,
    ... })
    >>> keys["کدرشته"]
    1201
"""

from __future__ import annotations

from collections import OrderedDict
from types import MappingProxyType
from typing import Any, Iterator, KeysView, Mapping, MutableMapping
from typing import Dict, Iterable, List, Literal, Optional, Tuple, TypedDict

import re


_NUM = re.compile(r"(\d+)")


def natural_key(s: str) -> Tuple[object, ...]:
    """کلید طبیعی برای sort پایدار شناسه‌ها (EMP-2 قبل از EMP-10).

    مثال::

        >>> natural_key("EMP-2") < natural_key("EMP-10")
        True
    """

    text = str(s or "").strip()
    if not text:
        return ("",)

    parts: list[object] = []
    has_text = False
    for token in _NUM.split(text):
        if not token:
            continue
        if token.isdecimal():
            number = int(token)
            if not parts:
                parts.append("")
            parts.append(number)
        else:
            parts.append(token.lower())
            has_text = True

    if not parts:
        return ("",)
    if not has_text and not isinstance(parts[0], str):
        parts.insert(0, "")
    return tuple(parts)


class JoinKeyValues(Mapping[str, int]):
    """نگهدارندهٔ فقط‌خواندنی برای کلیدهای join با اجبار ۶ مقدار صحیح."""

    __slots__ = ("_items", "_mapping")

    def __init__(
        self,
        data: Mapping[str, int] | MutableMapping[str, int],
        *,
        expected_keys: Iterable[str] | None = None,
    ):
        ordered = OrderedDict[str, int]()
        for key, value in data.items():
            normalized_key = str(key)
            try:
                coerced_value = int(value)
            except (TypeError, ValueError) as exc:  # pragma: no cover - guard
                raise TypeError(
                    f"Join key '{normalized_key}' must be int-compatible",
                ) from exc
            ordered[normalized_key] = coerced_value

        if len(ordered) != 6:
            raise ValueError("JoinKeyValues must contain exactly six entries")

        if expected_keys is not None:
            expected = tuple(str(key) for key in expected_keys)
            if tuple(ordered.keys()) != expected:
                raise ValueError(
                    "JoinKeyValues order/key mismatch; expected "
                    f"{expected} got {tuple(ordered.keys())}"
                )

        object.__setattr__(self, "_items", tuple(ordered.items()))
        object.__setattr__(self, "_mapping", MappingProxyType(dict(ordered)))

    def __getitem__(self, key: str) -> int:  # pragma: no cover - Mapping API
        return self._mapping[key]

    def __iter__(self) -> Iterator[str]:  # pragma: no cover - Mapping API
        return (key for key, _ in self._items)

    def __len__(self) -> int:  # pragma: no cover - Mapping API
        return len(self._items)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"JoinKeyValues({dict(self._items)!r})"

    def keys(self) -> KeysView[str]:
        """دسترسی به کلیدها با حفظ ترتیب درج."""

        return self._mapping.keys()

    def items(self) -> tuple[tuple[str, int], ...]:
        """برگشت زوج‌های (کلید، مقدار) به‌ترتیب Policy."""

        return self._items

    def as_dict(self) -> Dict[str, int]:
        """کپی معمولی دیکشنری برای سازگاری با pandas/JSON."""

        return dict(self._items)

    @classmethod
    def from_policy(cls, data: Mapping[str, int], join_keys: Iterable[str]) -> "JoinKeyValues":
        """ساخت نمونه از روی Policy با اجبار ترتیب کلیدها.

        Args:
            data: نگاشت ورودی شامل مقادیر کلیدها.
            join_keys: ترتیب دقیق کلیدها که باید ۶ تایی و int باشد.
        """

        expected_keys = tuple(str(key) for key in join_keys)
        missing = [key for key in expected_keys if key not in data]
        if missing:
            raise ValueError(f"join key values missing for: {', '.join(missing)}")
        ordered = {key: data[key] for key in expected_keys}
        return cls(ordered, expected_keys=expected_keys)


# سازگاری نامی با نسخه‌های قبلی
JoinKeys = JoinKeyValues
JoinKeysDF = JoinKeyValues

__all__ = [
    "natural_key",
    "JoinKeyValues",
    "JoinKeys",
    "JoinKeysDF",
    "StudentRow",
    "MentorRow",
    "AllocationErrorLiteral",
    "AllocationAlertRecord",
    "MentorStateSnapshot",
    "MentorStateDelta",
    "AllocationLogRecord",
    "TraceStageLiteral",
    "TraceStageRecord",
    "CANONICAL_TRACE_ORDER",
]


class StudentRow(TypedDict, total=False):
    """نمایندهٔ یک ردیف دانش‌آموز پس از نرمال‌سازی."""

    student_id: str
    کدرشته: int
    جنسیت: int
    دانش_آموز_فارغ: int
    مرکز_گلستان_صدرا: int
    مالی_حکمت_بنیاد: int
    کد_مدرسه: int
    گروه_آزمایشی: str
    نام: str


class MentorRow(TypedDict, total=False):
    """اطلاعات پشتیبان برای تخصیص ظرفیت و ردیابی."""

    پشتیبان: str
    کد_کارمندی_پشتیبان: str
    occupancy_ratio: float
    allocations_new: int
    remaining_capacity: int
    covered_now: int
    special_limit: int


class MentorStateSnapshot(TypedDict):
    """وضعیت خلاصه‌شدهٔ ظرفیت یک پشتیبان برای ثبت در Trace.

    مثال::

        >>> MentorStateSnapshot(remaining=3, alloc_new=1, occupancy_ratio=0.5)
    """

    remaining: int
    alloc_new: int
    occupancy_ratio: float


class MentorStateDelta(TypedDict):
    """تغییرات وضعیت پشتیبان قبل و بعد از تخصیص.

    مثال::

        >>> MentorStateDelta(
        ...     before=MentorStateSnapshot(remaining=2, alloc_new=0, occupancy_ratio=0.0),
        ...     after=MentorStateSnapshot(remaining=1, alloc_new=1, occupancy_ratio=0.5),
        ...     diff=MentorStateSnapshot(remaining=-1, alloc_new=1, occupancy_ratio=0.5),
        ... )
    """

    before: MentorStateSnapshot
    after: MentorStateSnapshot
    diff: MentorStateSnapshot


AllocationErrorLiteral = Literal[
    "ELIGIBILITY_NO_MATCH",
    "CAPACITY_FULL",
    "DATA_MISSING",
    "INTERNAL_ERROR",
]


class AllocationAlertRecord(TypedDict, total=False):
    """هشدار ساخت‌یافته برای گزارش مرحلهٔ حذف کاندید."""

    code: str
    stage: str
    message: str
    context: Dict[str, Any]


class AllocationLogRecord(TypedDict, total=False):
    """ساختار استاندارد برای ثبت Trace تصمیمات تخصیص."""

    row_index: int
    student_id: str
    allocation_status: Literal["success", "failed"]
    mentor_selected: Optional[str]
    mentor_id: Optional[str]
    occupancy_ratio: Optional[float]
    join_keys: JoinKeyValues
    candidate_count: int
    selection_reason: Optional[str]
    tie_breakers: Dict[str, Any]
    error_type: Optional[AllocationErrorLiteral]
    detailed_reason: Optional[str]
    suggested_actions: List[str]
    capacity_before: Optional[int]
    capacity_after: Optional[int]
    mentor_state_delta: Optional[MentorStateDelta]
    stage_candidate_counts: Dict[str, int]
    rule_reason_code: Optional[str]
    rule_reason_text: Optional[str]
    rule_reason_details: Optional[Mapping[str, Any]]
    fairness_reason_code: Optional[str]
    fairness_reason_text: Optional[str]
    alerts: List[AllocationAlertRecord]
    alias_autofill: int
    alias_unmatched: int
    phase_rule_trace: List[Mapping[str, Any]]


TraceStageLiteral = Literal[
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
    "capacity_gate",
]


CANONICAL_TRACE_ORDER: tuple[TraceStageLiteral, ...] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
    "capacity_gate",
)


class TraceStageRecord(TypedDict):
    """نتایج هر مرحلهٔ تریس تخصیص برای مقاصد Explainability."""

    stage: TraceStageLiteral
    column: str
    expected_value: Any
    total_before: int
    total_after: int
    matched: bool
    expected_op: str | None
    expected_threshold: Any | None
    extras: Mapping[str, Any] | None
