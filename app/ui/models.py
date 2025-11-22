from __future__ import annotations

"""مدل‌های ساده‌ی UI برای نگاشت داده‌های استخر منتورها."""

from dataclasses import dataclass
from typing import Iterable, Mapping

import pandas as pd

from app.core.common.columns import canonicalize_headers, resolve_aliases


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

    if isinstance(df, pd.DataFrame):
        resolved = resolve_aliases(df, source="matrix")
        normalized = canonicalize_headers(resolved, header_mode="en")
    else:
        normalized = pd.DataFrame()
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

        manager_raw = _first_present(
            record,
            (
                "manager_name",
                "manager",
            ),
        )
        manager_value = _string_value(manager_raw)
        if not manager_value:
            manager_value = "(بدون مدیر)"

        center_name_value = _first_present(
            record,
            (
                "center_name",
                "مرکز",
            ),
        )
        center_value: str | int | None
        if _string_value(center_name_value):
            center_value = _string_value(center_name_value)
        else:
            center_id_value = _first_present(
                record,
                (
                    "center",
                    "center_id",
                ),
            )
            if isinstance(center_id_value, float) and pd.isna(center_id_value):
                center_value = None
            else:
                center_value = center_id_value

        school_name_value = _first_present(
            record,
            (
                "school_name",
            ),
        )
        school_value: str | None
        if _string_value(school_name_value):
            school_value = _string_value(school_name_value)
        else:
            school_code_value = _first_present(
                record,
                (
                    "school",
                    "school_code",
                ),
            )
            if isinstance(school_code_value, float) and pd.isna(school_code_value):
                school_value = None
            else:
                school_value = school_code_value if school_code_value is not None else None

        entries.append(
            MentorPoolEntry(
                mentor_id=mentor_id,
                mentor_name=_string_value(
                    _first_present(
                        record,
                        (
                            "mentor_name",
                            "name",
                            "full_name",
                            "mentor",
                        ),
                    )
                ),
                manager=manager_value,
                center=center_value,
                school=school_value,
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

