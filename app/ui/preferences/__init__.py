"""تنظیمات و مدل‌های ترجیحی UI."""

from .dashboard_texts import ChecklistItem, DashboardTextBundle, load_dashboard_texts
from .dashboard_state import (
    FileStatusLevel,
    FileStatusViewModel,
    LastRunInfo,
    collect_file_statuses,
    format_last_run_label,
    read_last_run_info,
)

__all__ = [
    "ChecklistItem",
    "DashboardTextBundle",
    "FileStatusLevel",
    "FileStatusViewModel",
    "LastRunInfo",
    "collect_file_statuses",
    "format_last_run_label",
    "load_dashboard_texts",
    "read_last_run_info",
]
