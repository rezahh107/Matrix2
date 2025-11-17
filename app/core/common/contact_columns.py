"""SSOT ستون‌های تماس و نرمال‌سازی امن موبایل برای خروجی اکسل.

این ماژول تنها منبع نام ستون‌های موبایل/تلفن است تا همهٔ مصرف‌کننده‌ها
از یک مجموعه‌ی واحد استفاده کنند. علاوه‌بر آن، تابعی برای تبدیل ورودی‌های
موبایل (رشته، عدد، float) به رشتهٔ ۱۱ رقمی با صفر پیشتاز ارائه می‌دهد تا
در خروجی اکسل به‌عنوان شناسهٔ متنی ذخیره شوند.
"""

from __future__ import annotations

import re
from typing import Final, Iterable

import pandas as pd

from app.core.common.phone_rules import normalize_mobile

__all__ = [
    "MOBILE_COLUMN_NAMES",
    "MOBILE_COLUMN_KEYWORDS",
    "TEXT_SENSITIVE_COLUMN_NAMES",
    "TRACKING_CODE_COLUMN_NAMES",
    "is_mobile_header",
    "normalize_mobile_series_for_export",
]


_HEADER_CLEANUP_RE: Final[re.Pattern[str]] = re.compile(r"[_\-|\u200c]")


TRACKING_CODE_COLUMN_NAMES: Final[frozenset[str]] = frozenset(
    {
        "student_hekmat_tracking_code",
        "student_hekmat_tracking",
        "student_tracking_code",
        "tracking_code",
        "tracking_code_hekmat",
        "کد رهگیری حکمت",
    }
)


MOBILE_COLUMN_NAMES: Final[frozenset[str]] = frozenset(
    {
        "student_mobile",
        "student_mobile_raw",
        "student_mobile_number",
        "student_contact1_mobile",
        "student_contact2_mobile",
        "student_GF_Mobile",
        "GF_Mobile",
        "contact1_mobile",
        "contact2_mobile",
        "guardian_phone_1",
        "guardian_phone_2",
        "guardian1_mobile",
        "guardian2_mobile",
        "parent_mobile_1",
        "parent_mobile_2",
        "تلفن همراه پدر",
        "تلفن همراه مادر",
        "تلفن منزل",
        "student_landline",
        "landline",
        "student_phone",
        "student_home_phone",
        "تلفن همراه",
        "تلفن همراه | student_mobile",
        "تلفن همراه داوطلب",
        "موبایل دانش آموز",
        "موبایل دانش‌آموز",
        "موبایل رابط 1",
        "موبایل رابط 2",
        "تلفن رابط 1",
        "تلفن رابط 1 | contact1_mobile",
        "تلفن رابط 2",
        "تلفن رابط 2 | contact2_mobile",
        "تلفن ثابت",
        "تلفن",
    }
)

TEXT_SENSITIVE_COLUMN_NAMES: Final[frozenset[str]] = (
    MOBILE_COLUMN_NAMES | TRACKING_CODE_COLUMN_NAMES
)

MOBILE_COLUMN_KEYWORDS: Final[tuple[str, ...]] = (
    "mobile",
    "cell phone",
    "cellphone",
    "موبایل",
    "تلفن همراه",
    "شماره همراه",
)


def is_mobile_header(label: object) -> bool:
    """تشخیص ستون موبایل بر اساس نام صریح یا کلیدواژه‌های رایج."""

    label_text = str(label)
    if label_text in MOBILE_COLUMN_NAMES:
        return True

    normalized = " ".join(_HEADER_CLEANUP_RE.sub(" ", label_text).casefold().split())

    return any(keyword in normalized for keyword in MOBILE_COLUMN_KEYWORDS)


def normalize_mobile_series_for_export(series: pd.Series) -> pd.Series:
    """نرمال‌سازی امن ستون موبایل برای خروجی اکسل (همیشه dtype=string).

    - ورودی می‌تواند float، int، رشته یا مقادیر خالی باشد؛ تمام مقادیر ابتدا
      به رشته تبدیل و با :func:`normalize_mobile` پالایش می‌شوند.
    - خروجی همیشه از نوع ``string[python]`` است و فقط ارقام معتبر را نگه
      می‌دارد؛ شماره‌های ۱۰ رقمی که با «9» شروع شوند به‌صورت ۱۱ رقمی با صفر
      پیشتاز بازگردانده می‌شوند.
    - مقادیر نامعتبر به ``<NA>`` تبدیل می‌شوند تا در اکسل به‌عنوان سلول خالی
      نمایش داده شوند.
    """

    normalized = series.astype("object").map(normalize_mobile)
    # normalize_mobile already returns 11-digit strings when valid; no extra padding needed.
    return pd.Series(normalized, index=series.index, dtype="string")
