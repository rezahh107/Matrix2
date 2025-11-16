"""وضعیت داشبورد بر اساس AppPreferences."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Sequence

from ..app_preferences import AppPreferences

__all__ = [
    "FileStatusLevel",
    "FileStatusViewModel",
    "LastRunInfo",
    "collect_file_statuses",
    "format_last_run_label",
    "read_last_run_info",
]


class FileStatusLevel(str, Enum):
    """سطح وضعیت فایل برای تعیین رنگ و پیام."""

    READY = "ready"
    WARNING = "warning"


@dataclass(frozen=True)
class FileStatusViewModel:
    """مدل نمایشی هر مسیر در کارت وضعیت فایل‌ها."""

    key: str
    label: str
    description: str
    path: str
    level: FileStatusLevel
    exists: bool


@dataclass(frozen=True)
class LastRunInfo:
    """اطلاعات آخرین اجرای موفق سناریو."""

    run_type: str
    timestamp: datetime


_FILE_STATUS_DEFINITIONS: Sequence[tuple[str, str, str, str]] = (
    ("matrix_output", "خروجی ماتریس", "آخرین فایل ماتریس ساخته‌شده", "last_matrix_path"),
    ("alloc_output", "خروجی تخصیص", "فایل نهایی تخصیص دانش‌آموز-منتور", "last_alloc_output"),
    (
        "sabt_allocate",
        "Sabt تخصیص",
        "آخرین فایل خروجی برای سامانه ثبت از تب تخصیص",
        "last_sabt_output_allocate",
    ),
    (
        "sabt_rule",
        "Sabt موتور قواعد",
        "آخرین فایل خروجی برای سامانه ثبت از تب موتور قواعد",
        "last_sabt_output_rule",
    ),
    ("output_folder", "پوشه خروجی", "آخرین پوشه‌ای که خروجی در آن ذخیره شد", "last_output_dir"),
)

_RUN_TYPE_LABELS = {
    "build": "ساخت ماتریس",
    "allocate": "تخصیص",
    "rule-engine": "موتور قواعد",
}


def _path_from_prefs(prefs: AppPreferences, attr: str) -> str:
    value = getattr(prefs, attr, "")
    return str(value or "").strip()


def collect_file_statuses(prefs: AppPreferences) -> List[FileStatusViewModel]:
    """ساخت مدل وضعیت فایل‌ها بر اساس مسیرهای ذخیره‌شده."""

    statuses: List[FileStatusViewModel] = []
    for key, label, description, attr in _FILE_STATUS_DEFINITIONS:
        path_text = _path_from_prefs(prefs, attr)
        exists = bool(path_text and Path(path_text).exists())
        level = FileStatusLevel.READY if exists else FileStatusLevel.WARNING
        statuses.append(
            FileStatusViewModel(
                key=key,
                label=label,
                description=description,
                path=path_text,
                level=level,
                exists=exists,
            )
        )
    return statuses


def read_last_run_info(prefs: AppPreferences) -> LastRunInfo | None:
    """خواندن اطلاعات آخرین اجرا از Prefs و تبدیل به مدل."""

    run_type = prefs.last_run_type.strip()
    timestamp_text = prefs.last_run_timestamp.strip()
    if not run_type or not timestamp_text:
        return None
    try:
        timestamp = datetime.fromisoformat(timestamp_text)
    except ValueError:
        return None
    return LastRunInfo(run_type=run_type, timestamp=timestamp)


def format_last_run_label(info: LastRunInfo | None) -> str:
    """ساخت متن قابل‌نمایش برای برچسب آخرین اجرا."""

    if info is None:
        return "آخرین اجرا: هنوز اجرایی ثبت نشده است"
    run_title = _RUN_TYPE_LABELS.get(info.run_type, info.run_type)
    formatted_time = info.timestamp.strftime("%Y/%m/%d %H:%M")
    return f"آخرین اجرا: {run_title} • {formatted_time}"
