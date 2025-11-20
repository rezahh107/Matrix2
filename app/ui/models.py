from __future__ import annotations

"""مدل‌های ساده‌ی UI برای نگاشت داده‌های استخر منتورها."""

from dataclasses import dataclass
from typing import Iterable, Mapping

import pandas as pd

from app.core.common.columns import canonicalize_headers


@dataclass
class MentorPoolEntry:
    """نمایش سطری منتور برای UI.

    Attributes:
        mentor_id: شناسهٔ منتور (الزامی).
        mentor_name: نام برای نمایش.
        manager: نام مدیر مربوطه.
        center: مرکز یا شناسهٔ مرکز.
        school: مدرسه/مرکز وابسته در صورت وجود.
        capacity: ظرفیت باقی‌مانده یا ظرفیت کل.
        enabled: وضعیت فعال بودن در UI.
    """

    mentor_id: str
    mentor_name: str = ""
    manager: str | None = None
    center: str | int | None = None
    school: str | None = None
    capacity: int | float | None = None
    enabled: bool = True


def _coerce_boolish(value: object) -> bool | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "active", "enabled"}:
        return True
    if text in {"0", "false", "no", "frozen", "disabled", "inactive"}:
        return False
    return None


def _first_present(record: Mapping[str, object], candidates: Iterable[str]) -> object:
    for key in candidates:
        if key in record:
            return record[key]
    return None


def _string_value(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def build_mentor_entries_from_dataframe(
    df: pd.DataFrame, *, existing_overrides: Mapping[str, bool] | None = None
) -> list[MentorPoolEntry]:
    """تبدیل DataFrame استخر منتورها به ورودی‌های مدل UI.

    Args:
        df: دیتافریم خام (خروجی Inspactor/استخر).
        existing_overrides: نگاشت قبلی ``mentor_id → enabled`` برای حفظ وضعیت.

    Returns:
        لیست ``MentorPoolEntry`` با اعمال overrideهای قبلی.
    """

    normalized = canonicalize_headers(df, header_mode="en") if isinstance(df, pd.DataFrame) else pd.DataFrame()
    overrides = {str(k).strip(): bool(v) for k, v in (existing_overrides or {}).items() if str(k).strip()}
    entries: list[MentorPoolEntry] = []

    for record in normalized.to_dict(orient="records"):
        mentor_id = _string_value(
            _first_present(record, ("mentor_id", "alias", "mentorid"))
        )
        if not mentor_id:
            continue

        status_value = record.get("mentor_status")
        enabled = overrides.get(mentor_id)
        if enabled is None:
            enabled = _coerce_boolish(status_value)
        if enabled is None:
            enabled = True

        entries.append(
            MentorPoolEntry(
                mentor_id=mentor_id,
                mentor_name=_string_value(
                    _first_present(record, ("mentor_name", "name", "full_name"))
                ),
                manager=_string_value(_first_present(record, ("manager", "manager_name"))) or None,
                center=_string_value(_first_present(record, ("center", "center_id"))) or None,
                school=_string_value(
                    _first_present(record, ("school_code", "school", "school_name"))
                )
                or None,
                capacity=_first_present(
                    record,
                    (
                        "remaining_capacity",
                        "capacity",
                        "capacity_current",
                        "capacity_special",
                    ),
                ),
                enabled=bool(enabled),
            )
        )

    return entries

