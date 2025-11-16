from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd

from app.core.common.normalization import extract_ascii_digits

MOBILE_REQUIRED_PREFIX = "09"
MOBILE_REQUIRED_LENGTH = 11
HEKMAT_TRACKING_CODE = "1111111111111111"
HEKMAT_LANDLINE_FALLBACK = "00000000000"

__all__ = [
    "MOBILE_REQUIRED_PREFIX",
    "MOBILE_REQUIRED_LENGTH",
    "HEKMAT_TRACKING_CODE",
    "HEKMAT_LANDLINE_FALLBACK",
    "normalize_digits",
    "normalize_mobile",
    "normalize_mobile_series",
    "normalize_digits_series",
    "fix_guardian_phones",
    "fix_guardian_phone_columns",
]


def normalize_digits(value: object | None) -> Optional[str]:
    """بازگرداندن فقط digits انگلیسی از ورودی متنی/عددی.

    مثال::

        >>> normalize_digits("۰۲۱-۱۲۳ ۴۵۶۷")
        '0211234567'
    """

    digits = extract_ascii_digits(value)
    return digits or None


def normalize_mobile(value: object | None) -> Optional[str]:
    """اعتبارسنجی شماره موبایل ایران طبق Policy فعلی.

    تنها شماره‌هایی پذیرفته می‌شوند که بعد از حذف نویز:

    - دقیقاً 11 رقم داشته باشند، و
    - با «09» شروع شوند.

    مثال::

        >>> normalize_mobile("۰۹۱۲-۳۴۵ ۶۷۸۹")
        '09123456789'
        >>> normalize_mobile("+989123456789")
        None
    """

    digits = normalize_digits(value)
    if digits is None:
        return None
    if len(digits) != MOBILE_REQUIRED_LENGTH:
        return None
    if not digits.startswith(MOBILE_REQUIRED_PREFIX):
        return None
    return digits


def normalize_mobile_series(series: pd.Series | None) -> pd.Series:
    """اعمال ``normalize_mobile`` به‌صورت element-wise روی Series.

    خروجی با dtype="string" و مقدار «<NA>» برای ورودی‌های نامعتبر است.
    """

    if series is None:
        return pd.Series(dtype="string")
    normalized = series.astype("object").map(normalize_mobile)
    return pd.Series(normalized, index=series.index, dtype="string")


def normalize_digits_series(series: pd.Series | None) -> pd.Series:
    """تبدیل Series به فقط digits انگلیسی (بدون محدودیت طول)."""

    if series is None:
        return pd.Series(dtype="string")
    normalized = series.astype("object").map(normalize_digits)
    return pd.Series(normalized, index=series.index, dtype="string")


def fix_guardian_phones(
    phone1: object | None,
    phone2: object | None,
) -> Tuple[Optional[str], Optional[str]]:
    """تعمیر شماره رابط اول و دوم با رعایت قواعد برابری/جابجایی.

    قواعد:
        - هر دو شماره ابتدا با :func:`normalize_mobile` پالایش می‌شوند.
        - اگر شمارهٔ اول خالی و دومی پر باشد، شمارهٔ دوم به اول منتقل می‌شود.
        - اگر هر دو برابر باشند، شمارهٔ دوم حذف می‌شود.
    """

    first = normalize_mobile(phone1)
    second = normalize_mobile(phone2)
    if first is None and second is not None:
        return second, None
    if first is not None and second is not None and first == second:
        return first, None
    return first, second


def _ensure_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([pd.NA] * len(df), dtype="string", index=df.index)


def _assign_series(
    frame: pd.DataFrame,
    canonical: str,
    selected: str,
    values: pd.Series,
) -> None:
    frame[canonical] = values
    if selected != canonical:
        frame[selected] = values


def fix_guardian_phone_columns(
    df: pd.DataFrame,
    col1: str,
    col2: str,
    *,
    canonical1: str | None = None,
    canonical2: str | None = None,
) -> pd.DataFrame:
    """اعمال ``fix_guardian_phones`` روی دو ستون دیتافریم.

    Args:
        df: دیتافریم منبع.
        col1: نام ستون رابط اول (خام).
        col2: نام ستون رابط دوم (خام).
        canonical1: در صورت نیاز ستون مقصد استاندارد برای رابط اول.
        canonical2: در صورت نیاز ستون مقصد استاندارد برای رابط دوم.
    """

    if col1 not in df.columns and col2 not in df.columns:
        return df

    result = df.copy()
    result.attrs.update(df.attrs)

    series1 = _ensure_series(result, col1)
    series2 = _ensure_series(result, col2)

    fixed_values: list[Tuple[Optional[str], Optional[str]]] = []
    for value1, value2 in zip(series1.tolist(), series2.tolist()):
        fixed_values.append(fix_guardian_phones(value1, value2))

    first_series = pd.Series((pair[0] for pair in fixed_values), index=result.index, dtype="string")
    second_series = pd.Series((pair[1] for pair in fixed_values), index=result.index, dtype="string")

    target1 = canonical1 or col1
    target2 = canonical2 or col2

    _assign_series(result, target1, col1, first_series)
    _assign_series(result, target2, col2, second_series)

    return result
