"""توابع کمکی برای کنترل قالب ستون‌های حساس در خروجی اکسل."""

from __future__ import annotations

from typing import Iterable, Sequence

from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

__all__ = ["ensure_text_columns", "TEXT_COLUMN_NAMES"]

TEXT_COLUMN_NAMES = frozenset(
    {
        "student_mobile",
        "student_mobile_raw",
        "student_mobile_number",
        "student_contact1_mobile",
        "student_contact2_mobile",
        "contact1_mobile",
        "contact2_mobile",
        "student_landline",
        "landline",
        "student_phone",
        "student_home_phone",
        "hekmat_tracking",
        "student_hekmat_tracking_code",
        "student_hekmat_tracking",
        "student_tracking_code",
        "tracking_code",
        "tracking_code_hekmat",
        "تلفن همراه",
        "موبایل دانش آموز",
        "موبایل دانش‌آموز",
        "موبایل رابط 1",
        "تلفن رابط 1",
        "موبایل رابط 2",
        "تلفن رابط 2",
        "تلفن ثابت",
        "تلفن",
        "کد رهگیری حکمت",
    }
)


def ensure_text_columns(
    ws: Worksheet,
    headers: Sequence[str],
    *,
    extra_columns: Iterable[str] | None = None,
) -> None:
    """اعمال قالب «Text» برای ستون‌های شماره تلفن و کد رهگیری."""

    target_names = set(TEXT_COLUMN_NAMES)
    if extra_columns is not None:
        target_names.update(str(name) for name in extra_columns)
    header_list = [str(label) for label in headers]
    indexes = [idx for idx, label in enumerate(header_list) if label in target_names]
    if not indexes:
        return
    max_row = ws.max_row or 1
    for idx in indexes:
        column_letter = get_column_letter(idx + 1)
        for row in range(1, max_row + 1):
            cell = ws[f"{column_letter}{row}"]
            cell.number_format = "@"
