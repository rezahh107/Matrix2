from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd

from app.core.common.normalization import extract_ascii_digits
from app.core.common.domain import FinanceCode

MOBILE_REQUIRED_PREFIX = "09"
MOBILE_REQUIRED_LENGTH = 11
HEKMAT_TRACKING_CODE = "1111111111111111"
HEKMAT_LANDLINE_FALLBACK = "00000000000"
HEKMAT_STATUS_CODE = int(FinanceCode.HEKMAT)

__all__ = [
    "MOBILE_REQUIRED_PREFIX",
    "MOBILE_REQUIRED_LENGTH",
    "HEKMAT_TRACKING_CODE",
    "HEKMAT_LANDLINE_FALLBACK",
    "HEKMAT_STATUS_CODE",
    "normalize_digits",
    "normalize_mobile",
    "normalize_landline",
    "normalize_mobile_series",
    "normalize_landline_series",
    "normalize_digits_series",
    "fix_guardian_phones",
    "fix_guardian_phone_columns",
    "apply_hekmat_contact_policy",
    "apply_hekmat_contact_policy_series",
]


def normalize_digits(value: object | None) -> Optional[str]:
    """بازگرداندن تنها digits انگلیسی از ورودی.

    - ارقام فارسی/عربی به انگلیسی برگردانده می‌شوند.
    - تمامی نویزها (فاصله، خط تیره و …) حذف می‌شوند.
    - اگر خروجی خالی باشد ``None`` بازگردانده می‌شود.
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
    if len(digits) == 10 and digits.startswith("9"):
        digits = f"0{digits}"
    if len(digits) != MOBILE_REQUIRED_LENGTH:
        return None
    if not digits.startswith(MOBILE_REQUIRED_PREFIX):
        return None
    return digits


def normalize_landline(
    value: object | None,
    *,
    allow_special_zero: bool = False,
) -> Optional[str]:
    """نرمال‌سازی تلفن ثابت طبق Policy.

    قواعد:
    - فقط digits انگلیسی نگه داشته می‌شوند.
    - در حالت عادی تنها شماره‌هایی که با «3» یا «5» شروع شوند پذیرفته می‌شوند.
    - اگر ``allow_special_zero`` فعال باشد، مقدار ویژهٔ حکمت ``00000000000`` بدون تغییر
      بازگردانده می‌شود تا قانون «شروع با 3 یا 5» آن را حذف نکند.
    """

    digits = normalize_digits(value)
    if digits is None:
        return None
    if allow_special_zero and digits == HEKMAT_LANDLINE_FALLBACK:
        return digits
    if digits.startswith("3") or digits.startswith("5"):
        return digits
    return None


def normalize_mobile_series(series: pd.Series | None) -> pd.Series:
    """اعمال ``normalize_mobile`` به‌صورت element-wise روی Series.

    خروجی با dtype="string" و مقدار «<NA>» برای ورودی‌های نامعتبر است.
    """

    if series is None:
        return pd.Series(dtype="string")
    normalized = series.astype("object").map(normalize_mobile)
    return pd.Series(normalized, index=series.index, dtype="string")


def normalize_landline_series(
    series: pd.Series | None,
    *,
    allow_special_zero: bool = False,
) -> pd.Series:
    """اعمال ``normalize_landline`` روی Series با حفظ index.

    Args:
        series: ستون ورودی.
        allow_special_zero: برای نگه‌داشت مقدار خاص حکمت فعال می‌شود.
    """

    if series is None:
        return pd.Series(dtype="string")
    normalized = series.astype("object").map(
        lambda value: normalize_landline(value, allow_special_zero=allow_special_zero)
    )
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


def _to_int64(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(numeric, index=series.index, dtype="Int64")


def _ensure_string_series(series: pd.Series) -> pd.Series:
    if series.dtype == "string":
        return series
    return series.astype("string")


def _blank_mask(series: pd.Series) -> pd.Series:
    stripped = series.str.strip()
    stripped_na = stripped.isna()
    return stripped_na | (stripped == "")


def apply_hekmat_contact_policy_series(
    status: pd.Series,
    landline: pd.Series,
    tracking: pd.Series | None = None,
) -> tuple[pd.Series, pd.Series | None]:
    """اعمال قانون حکمت روی سری‌های وضعیت، تلفن ثابت و کد رهگیری."""

    status_int = _to_int64(status)
    landline_series = _ensure_string_series(landline)
    hekmat_mask = (status_int == HEKMAT_STATUS_CODE).fillna(False)
    empty_landline = _blank_mask(landline_series)
    updated_landline = landline_series.mask(
        hekmat_mask & empty_landline, HEKMAT_LANDLINE_FALLBACK
    )

    updated_tracking: pd.Series | None = None
    if tracking is not None:
        tracking_series = _ensure_string_series(tracking)
        updated_tracking = tracking_series.mask(hekmat_mask, HEKMAT_TRACKING_CODE)
        updated_tracking = updated_tracking.mask(~hekmat_mask, "")

    return updated_landline, updated_tracking


def apply_hekmat_contact_policy(
    df: pd.DataFrame,
    *,
    status_column: str,
    landline_column: str,
    tracking_code_column: str | None = None,
) -> pd.DataFrame:
    """اعمال قوانین تلفن ثابت و کد رهگیری حکمت روی دیتافریم."""

    if status_column not in df.columns or landline_column not in df.columns:
        return df.copy()

    result = df.copy()
    result.attrs.update(df.attrs)

    tracking_series = None
    if tracking_code_column is not None:
        if tracking_code_column not in result.columns:
            result[tracking_code_column] = pd.Series(
                [pd.NA] * len(result), dtype="string", index=result.index
            )
        tracking_series = result[tracking_code_column]

    updated_landline, updated_tracking = apply_hekmat_contact_policy_series(
        result[status_column],
        result[landline_column],
        tracking_series,
    )
    result[landline_column] = updated_landline

    if tracking_code_column is not None and updated_tracking is not None:
        result[tracking_code_column] = updated_tracking

    return result
