"""ابزارهای کمکی مسیر برای پشتیبانی از حالت PyInstaller و توسعه."""

from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Iterable

__all__ = [
    "get_app_base_path",
    "resource_path",
    "get_user_data_dir",
    "get_log_directory",
]


@lru_cache(maxsize=1)
def get_app_base_path() -> Path:
    """بازگرداندن مسیر پایهٔ برنامه چه در حالت عادی چه فریز شده."""

    if getattr(sys, "frozen", False):  # PyInstaller onefile/onedir
        base = getattr(sys, "_MEIPASS", None)
        if base:
            return Path(base)
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def _normalize_parts(parts: Iterable[str | os.PathLike[str]]) -> Path:
    path = Path()
    for piece in parts:
        candidate = Path(os.fspath(piece))
        if candidate.is_absolute():
            path = candidate
        else:
            path = path / candidate
    return path


def resource_path(*parts: str | os.PathLike[str]) -> Path:
    """دسترسی به فایل‌های باندل‌شده (config و ...)."""

    if not parts:
        return get_app_base_path()
    candidate = _normalize_parts(parts)
    if candidate.is_absolute():
        return candidate
    return get_app_base_path() / candidate


@lru_cache(maxsize=None)
def get_user_data_dir(app_name: str = "Matrix2") -> Path:
    """پوشهٔ کاربر برای نگهداری لاگ و Prefs جانبی."""

    base = Path(os.path.expanduser("~")) / app_name
    base.mkdir(parents=True, exist_ok=True)
    return base


@lru_cache(maxsize=None)
def get_log_directory(subdir: str = "logs", app_name: str = "Matrix2") -> Path:
    """ساخت/بازگرداندن پوشهٔ لاگ عمومی برنامه."""

    root = get_user_data_dir(app_name)
    log_dir = root / subdir
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir
