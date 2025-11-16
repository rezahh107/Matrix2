"""وضعیت داشبورد بر اساس AppPreferences."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Sequence

from ..app_preferences import AppPreferences
from ..texts import UiTranslator

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


_FILE_STATUS_DEFINITIONS: Sequence[tuple[str, str]] = (
    ("matrix_output", "last_matrix_path"),
    ("alloc_output", "last_alloc_output"),
    ("sabt_allocate", "last_sabt_output_allocate"),
    ("sabt_rule", "last_sabt_output_rule"),
    ("output_folder", "last_output_dir"),
)

_FILE_STATUS_TEXT = {
    "matrix_output": ("files.matrix_output", "آخرین فایل ماتریس ساخته‌شده"),
    "alloc_output": ("files.alloc_output", "فایل نهایی تخصیص دانش‌آموز-منتور"),
    "sabt_allocate": ("files.sabt_output", "آخرین فایل خروجی برای سامانه ثبت از تب تخصیص"),
    "sabt_rule": ("files.sabt_output", "آخرین فایل خروجی برای سامانه ثبت از تب موتور قواعد"),
    "output_folder": ("files.output_folder", "آخرین پوشه‌ای که خروجی در آن ذخیره شد"),
}

def _path_from_prefs(prefs: AppPreferences, attr: str) -> str:
    value = getattr(prefs, attr, "")
    return str(value or "").strip()


def collect_file_statuses(prefs: AppPreferences, translator: UiTranslator) -> List[FileStatusViewModel]:
    """ساخت مدل وضعیت فایل‌ها بر اساس مسیرهای ذخیره‌شده."""

    statuses: List[FileStatusViewModel] = []
    for key, attr in _FILE_STATUS_DEFINITIONS:
        label_key, description_fallback = _FILE_STATUS_TEXT.get(key, (key, key))
        label = translator.text(label_key, label_key)
        description = translator.text(label_key, description_fallback)
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


def format_last_run_label(info: LastRunInfo | None, translator: UiTranslator) -> str:
    """ساخت متن قابل‌نمایش برای برچسب آخرین اجرا."""

    if info is None:
        return translator.text("status.no_runs", "آخرین اجرا: هنوز اجرایی ثبت نشده است")
    run_title = {
        "build": translator.text("action.build", info.run_type),
        "allocate": translator.text("action.allocate", info.run_type),
        "rule-engine": translator.text("action.rule_engine", info.run_type),
    }.get(info.run_type, translator.text("status.ready", info.run_type))
    formatted_time = info.timestamp.strftime("%Y/%m/%d %H:%M")
    prefix = translator.text("status.last_run_prefix", "آخرین اجرا")
    return f"{prefix}: {run_title} • {formatted_time}"
