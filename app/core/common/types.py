"""تعریف قراردادهای دادهٔ حوزهٔ Eligibility Matrix (Core-only, بدون I/O).

این ماژول صرفاً تایپ‌ها را نگه می‌دارد و منطق ندارد.
برای هم‌راستایی با SSoT، دو نمایش از JoinKeys ارائه می‌شود:

1. Pythonic (با آندرلاین) برای مصرف داخل کد/تست
2. SSOT/DF (با فاصله) برای هم‌نامی دقیق با ستون‌های DataFrame

مثال:
    >>> from app.core.common.types import StudentRow, JoinKeys, JoinKeysDF
    >>> student: StudentRow = {
    ...   "student_id": "STD-1", "نام": "علی",
    ...   "کدرشته": 1201, "جنسیت": 1, "دانش_آموز_فارغ": 0,
    ...   "مرکز_گلستان_صدرا": 1, "مالی_حکمت_بنیاد": 0, "کد_مدرسه": 3581,
    ... }
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict

__all__ = [
    "JoinKeys",
    "JoinKeysDF",
    "StudentRow",
    "MentorRow",
    "AllocationErrorLiteral",
    "AllocationLogRecord",
]


class JoinKeys(TypedDict):
    """کلیدهای اتصال بین جداول (int)."""

    کدرشته: int
    جنسیت: int
    دانش_آموز_فارغ: int
    مرکز_گلستان_صدرا: int
    مالی_حکمت_بنیاد: int
    کد_مدرسه: int


# نمایش SSOT/DF (با فاصله) برای هم‌نامی با ستون‌های اکسل/دیتافریم
JoinKeysDF = TypedDict(
    "JoinKeysDF",
    {
        "کدرشته": int,
        "جنسیت": int,
        "دانش آموز فارغ": int,
        "مرکز گلستان صدرا": int,
        "مالی حکمت بنیاد": int,
        "کد مدرسه": int,
    },
)


class StudentRow(TypedDict, total=False):
    """نمایندهٔ یک ردیف دانش‌آموز پس از نرمال‌سازی."""

    student_id: str
    کدرشته: int
    جنسیت: int
    دانش_آموز_فارغ: int
    مرکز_گلستان_صدرا: int
    مالی_حکمت_بنیاد: int
    کد_مدرسه: int
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


AllocationErrorLiteral = Literal[
    "ELIGIBILITY_NO_MATCH",
    "CAPACITY_FULL",
    "DATA_MISSING",
    "INTERNAL_ERROR",
]


class AllocationLogRecord(TypedDict, total=False):
    """ساختار استاندارد برای ثبت Trace تصمیمات تخصیص."""

    row_index: int
    student_id: str
    allocation_status: Literal["success", "failed"]
    mentor_selected: Optional[str]
    mentor_id: Optional[str]
    occupancy_ratio: Optional[float]
    join_keys: JoinKeys
    candidate_count: int
    selection_reason: Optional[str]
    tie_breakers: Dict[str, Any]
    error_type: Optional[AllocationErrorLiteral]
    detailed_reason: Optional[str]
    suggested_actions: List[str]
