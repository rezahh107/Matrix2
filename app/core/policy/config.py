"""تعریف پیکربندی سیاست برای کانال‌های تخصیص (Core only)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class AllocationChannelConfig:
    """پیکربندی کانال تخصیص مدرسه/مرکز.

    این ساختار نشان می‌دهد چه کدهای مدرسه‌ای در کانال «مدرسه» (SCHOOL) قرار
    می‌گیرند، کدام شناسه‌های مرکز به کانال‌های GOLESTAN/SADRA تعلق دارند و در
    صورت نیاز چه ستون‌هایی برای تشخیص وضعیت ثبت‌نام و فعال بودن دانش‌آموز
    استفاده شود. تمام داده‌ها از policy.json خوانده می‌شوند و هیچ مقدار
    هاردکد در Core وجود ندارد.

    مثال:
        >>> config = AllocationChannelConfig(
        ...     school_codes=(10, 11),
        ...     center_channels={"GOLESTAN": (1,), "SADRA": (2,)},
        ...     registration_center_column="registration_center",
        ...     educational_status_column="student_educational_status",
        ...     active_status_values=(0,),
        ... )
        >>> 10 in config.school_codes
        True
    """

    school_codes: tuple[int, ...]
    center_channels: Mapping[str, tuple[int, ...]]
    registration_center_column: str | None
    educational_status_column: str | None
    active_status_values: tuple[int, ...]

    @staticmethod
    def empty() -> "AllocationChannelConfig":
        """نسخهٔ بدون قانون ویژه برای fallback Policy."""

        return AllocationChannelConfig(
            school_codes=tuple(),
            center_channels={},
            registration_center_column=None,
            educational_status_column=None,
            active_status_values=tuple(),
        )


__all__ = ["AllocationChannelConfig"]
