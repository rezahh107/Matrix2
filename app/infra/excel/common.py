"""ابزارهای مشترک خروجی‌های اکسل در لایهٔ Sabt."""

from __future__ import annotations

from typing import Iterable, Sequence, TYPE_CHECKING

import pandas as pd

from app.core.common.columns import CANON_EN_TO_FA, ensure_series
from app.core.common.normalization import normalize_fa
from app.core.pipeline import (
    CONTACT_POLICY_ALIAS_GROUPS,
    CONTACT_POLICY_COLUMNS,
)

if TYPE_CHECKING:  # pragma: no cover - فقط برای type checking
    from app.infra.excel.export_allocations import AllocationExportColumn

__all__ = [
    "identify_code_headers",
    "enforce_text_columns",
    "attach_contact_columns",
]

_CODE_HEADER_KEYWORDS = ("کد", "شماره", "رهگیری", "قبض")
_CODE_FIELD_HINTS = frozenset(
    normalize_fa(value)
    for value in (
        "mentor_id",
        "mentor_alias_code",
        "student_id",
        "student_national_code",
        "national_id",
        "کدملی",
        "کد ملی",
        "کد پستی",
        "کدرشته",
        "کد رهگیری حکمت",
        "شماره قبض",
        "شماره صندلی",
        "شماره کلاس",
    )
)


def _normalize_hint(value: str | None) -> str:
    """نرمال‌سازی متن برای مقایسهٔ ستونی."""

    return normalize_fa(value or "").strip()


def identify_code_headers(
    profile: Sequence["AllocationExportColumn"],
) -> set[str]:
    """ستون‌هایی که باید همیشه به‌صورت متن نوشته شوند را بر اساس پروفایل برمی‌گرداند."""

    headers: set[str] = set()
    for column in profile:
        if column.source_kind == "literal":
            continue
        header_hint = _normalize_hint(column.header)
        field_hint = _normalize_hint(column.source_field)
        if field_hint in _CODE_FIELD_HINTS or header_hint in _CODE_FIELD_HINTS:
            headers.add(column.header)
            continue
        if any(keyword in header_hint for keyword in _CODE_HEADER_KEYWORDS):
            headers.add(column.header)
    return headers


def enforce_text_columns(
    frame: pd.DataFrame,
    *,
    headers: Iterable[str],
) -> pd.DataFrame:
    """تضمین می‌کند ستون‌های تعیین‌شده به‌صورت متن ذخیره شوند."""

    headers = list(headers)
    if not headers:
        return frame.copy()

    output = frame.copy()
    for header in headers:
        if header in output.columns:
            output[header] = ensure_series(output[header]).astype("string")
    return output


def attach_contact_columns(
    target: pd.DataFrame, contacts: pd.DataFrame
) -> pd.DataFrame:
    """افزودن ستون‌های تماس نرمال‌شده و همهٔ نام‌های فارسی متناظر."""

    for column in CONTACT_POLICY_COLUMNS:
        if column not in contacts.columns:
            continue
        series = ensure_series(contacts[column]).reindex(target.index)
        target[column] = series
        alias = CANON_EN_TO_FA.get(column)
        if alias:
            target[alias] = series
        for extra in CONTACT_POLICY_ALIAS_GROUPS.get(column, ()):
            target[extra] = series
    return target
