"""سیستم مرکزی مدیریت کد/متن دلایل (Core-only).

این ماژول تنها یک SSoT برای ReasonCode فراهم می‌کند تا تمامی لایه‌ها
بتوانند پیام فارسی یکسانی تولید کنند.

مثال::

    >>> build_reason(ReasonCode.GENDER_MISMATCH)
    LocalizedReason(code=<ReasonCode.GENDER_MISMATCH: 'GENDER_MISMATCH'>,
                    message_fa='عدم تطابق جنسیت دانش‌آموز و پشتیبان.')
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping

__all__ = ["ReasonCode", "LocalizedReason", "build_reason", "reason_message"]


class ReasonCode(StrEnum):
    """کدهای یکتای دلایل قابل گزارش مطابق Policy/SSoT."""

    OK = "OK"
    ELIGIBILITY_NO_MATCH = "ELIGIBILITY_NO_MATCH"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    GROUP_MISMATCH = "GROUP_MISMATCH"
    GENDER_MISMATCH = "GENDER_MISMATCH"
    GRADUATION_STATUS_MISMATCH = "GRADUATION_STATUS_MISMATCH"
    CENTER_MISMATCH = "CENTER_MISMATCH"
    FINANCE_MISMATCH = "FINANCE_MISMATCH"
    SCHOOL_STATUS_MISMATCH = "SCHOOL_STATUS_MISMATCH"
    CAPACITY_FULL = "CAPACITY_FULL"
    FILTERED_OUT = "FILTERED_OUT"
    FAIRNESS_ORDER = "FAIRNESS_ORDER"


@dataclass(frozen=True, slots=True)
class LocalizedReason:
    """متن بومی‌شدهٔ دلایل برای گزارش‌گیری انسانی."""

    code: ReasonCode
    message_fa: str


_REASON_MESSAGES_FA: Mapping[ReasonCode, str] = {
    ReasonCode.OK: "انتخاب موفق.",
    ReasonCode.ELIGIBILITY_NO_MATCH: "هیچ پشتیبان واجد شرایط با کلیدهای Policy یافت نشد.",
    ReasonCode.TYPE_MISMATCH: "گروه یا نوع درخواستی دانش‌آموز با استخر فعلی هم‌خوان نبود.",
    ReasonCode.GROUP_MISMATCH: "گروه آزمایشی با معیارهای Policy تطابق نداشت.",
    ReasonCode.GENDER_MISMATCH: "عدم تطابق جنسیت دانش‌آموز و پشتیبان.",
    ReasonCode.GRADUATION_STATUS_MISMATCH: "وضعیت فارغ‌التحصیلی با سیاست تخصیص ناسازگار است.",
    ReasonCode.CENTER_MISMATCH: "مرکز گلستان صدرا با ورودی دانش‌آموز یا Policy متفاوت است.",
    ReasonCode.FINANCE_MISMATCH: "وضعیت مالی حکمت‌بنیاد با سیاست فعلی انطباق ندارد.",
    ReasonCode.SCHOOL_STATUS_MISMATCH: "مدرسه یا وضعیت پسامدرسه‌ای دانش‌آموز اجازهٔ ادامه مسیر نداد.",
    ReasonCode.CAPACITY_FULL: "هیچ ظرفیت فعالی در پشتیبان‌های واجد شرایط باقی نمانده است.",
    ReasonCode.FILTERED_OUT: "دانش‌آموز در این مرحله از فیلتر Policy حذف شد.",
    ReasonCode.FAIRNESS_ORDER: "بازچینش عدالت‌محور طبق Policy انجام شد.",
}


def reason_message(code: ReasonCode) -> str:
    """برگرداندن متن فارسی ذخیره‌شده برای یک کد دلیل."""

    try:
        return _REASON_MESSAGES_FA[code]
    except KeyError as exc:  # pragma: no cover - نگهبان نسخه‌های آینده
        raise ValueError(f"Reason code '{code}' تعریف نشده است") from exc


def build_reason(code: ReasonCode) -> LocalizedReason:
    """ساخت شیء :class:`LocalizedReason` با پیام فارسی پایدار."""

    return LocalizedReason(code=code, message_fa=reason_message(code))
