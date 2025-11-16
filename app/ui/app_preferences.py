"""مدیریت تنظیمات UI با اعتبارسنجی مرکزمحور."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Set

from PySide6.QtCore import QByteArray, QSettings

from app.core.policy_loader import load_policy

__all__ = ["AppPreferences"]


class AppPreferences:
    """مدیریت تنظیمات برنامه با اعتبارسنجی پیشرفته."""

    def __init__(self) -> None:
        self._settings = QSettings("YourOrganization", "MentorAllocation")
        self._valid_centers = self._load_valid_centers()

    # ------------------------------------------------------------------ داخلی
    def _load_valid_centers(self) -> Set[int]:
        """بارگذاری مراکز معتبر از Policy."""

        try:
            policy = load_policy()
            return {center.id for center in policy.center_management.centers}
        except Exception:
            return {0, 1, 2}

    def _get_string(self, key: str, default: str = "") -> str:
        value = self._settings.value(key, default)
        if isinstance(value, str):
            return value
        if value is None:
            return default
        return str(value)

    def _set_string(self, key: str, value: str) -> None:
        self._settings.setValue(key, value)
        self._settings.sync()

    def _get_bool(self, key: str, default: bool = False) -> bool:
        value = self._settings.value(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes"}
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return default

    def _get_float(self, key: str, default: float = 0.0) -> float:
        value = self._settings.value(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _get_default_manager(self, center_id: int) -> str:
        """دریافت مدیر پیش‌فرض یک مرکز از Policy."""

        try:
            policy = load_policy()
            center = policy.center_management.get_center(center_id)
            if center and center.default_manager:
                return center.default_manager.strip()
        except Exception:
            pass
        return ""

    def _log_invalid_setting(self, key: str, value: Any, reason: str) -> None:
        """ثبت خطای تنظیمات نامعتبر."""

        logger = logging.getLogger(__name__)
        logger.warning("تنظیمات نامعتبر: key=%s, value=%s, reason=%s", key, value, reason)

    # ------------------------------------------------------------------ مسیرهای فایل
    @property
    def last_output_dir(self) -> str:
        """آخرین پوشه خروجی انتخاب شده."""

        return self._get_string("ui/last_output_dir")

    @last_output_dir.setter
    def last_output_dir(self, value: str) -> None:
        self._set_string("ui/last_output_dir", value)

    @property
    def last_matrix_path(self) -> str:
        """آخرین مسیر ماتریس."""

        return self._get_string("ui/last_matrix_path")

    @last_matrix_path.setter
    def last_matrix_path(self, value: str) -> None:
        self._set_string("ui/last_matrix_path", value)

    @property
    def last_alloc_output(self) -> str:
        """آخرین فایل خروجی تخصیص."""

        return self._get_string("ui/last_alloc_output")

    @last_alloc_output.setter
    def last_alloc_output(self, value: str) -> None:
        self._set_string("ui/last_alloc_output", value)

    @property
    def last_sabt_output_allocate(self) -> str:
        """آخرین خروجی Sabt در تب تخصیص."""

        return self._get_string("ui/last_sabt_output_allocate")

    @last_sabt_output_allocate.setter
    def last_sabt_output_allocate(self, value: str) -> None:
        self._set_string("ui/last_sabt_output_allocate", value)

    @property
    def last_sabt_output_rule(self) -> str:
        """آخرین خروجی Sabt برای تب موتور قواعد."""

        return self._get_string("ui/last_sabt_output_rule")

    @last_sabt_output_rule.setter
    def last_sabt_output_rule(self, value: str) -> None:
        self._set_string("ui/last_sabt_output_rule", value)

    @property
    def last_sabt_config_path(self) -> str:
        """آخرین فایل تنظیمات Exporter."""

        return self._get_string("ui/last_sabt_config")

    @last_sabt_config_path.setter
    def last_sabt_config_path(self, value: str) -> None:
        self._set_string("ui/last_sabt_config", value)

    # ------------------------------------------------------------------ متادیتای اجرا
    @property
    def last_run_type(self) -> str:
        """نوع آخرین سناریوی اجرا شده (build/allocate/rule-engine)."""

        return self._get_string("ui/last_run/type")

    @last_run_type.setter
    def last_run_type(self, value: str) -> None:
        self._set_string("ui/last_run/type", value)

    @property
    def last_run_timestamp(self) -> str:
        """زمان آخرین اجرا به صورت ISO string."""

        return self._get_string("ui/last_run/timestamp")

    @last_run_timestamp.setter
    def last_run_timestamp(self, value: str) -> None:
        self._set_string("ui/last_run/timestamp", value)

    def record_last_run(self, run_type: str, timestamp: datetime | None = None) -> None:
        """ثبت متادیتای آخرین اجرای موفق سناریو."""

        moment = timestamp or datetime.now()
        self.last_run_type = run_type
        self.last_run_timestamp = moment.isoformat(timespec="minutes")

    # ------------------------------------------------------------------ تنظیمات تخصیص
    @property
    def max_occupancy(self) -> float:
        """حداکثر درصد اشغال منتورها."""

        return self._get_float("allocation/max_occupancy", 0.95)

    @max_occupancy.setter
    def max_occupancy(self, value: float) -> None:
        self._settings.setValue("allocation/max_occupancy", value)
        self._settings.sync()

    @property
    def priority_new_mentors(self) -> bool:
        """اولویت منتورهای جدید."""

        return self._get_bool("allocation/priority_new_mentors", True)

    @priority_new_mentors.setter
    def priority_new_mentors(self, value: bool) -> None:
        self._settings.setValue("allocation/priority_new_mentors", value)
        self._settings.sync()

    @property
    def priority_high_capacity(self) -> bool:
        """اولویت ظرفیت بالا."""

        return self._get_bool("allocation/priority_high_capacity", True)

    @priority_high_capacity.setter
    def priority_high_capacity(self, value: bool) -> None:
        self._settings.setValue("allocation/priority_high_capacity", value)
        self._settings.sync()

    @property
    def enable_capacity_gate(self) -> bool:
        """فعال‌سازی Capacity Gate."""

        return self._get_bool("matrix/enable_capacity_gate", True)

    @enable_capacity_gate.setter
    def enable_capacity_gate(self, value: bool) -> None:
        self._settings.setValue("matrix/enable_capacity_gate", value)
        self._settings.sync()

    # ------------------------------------------------------------------ مدیران مراکز
    @property
    def golestan_manager(self) -> str:
        """نام مدیر پیش‌فرض مرکز گلستان."""

        return self.get_center_manager(1, "شهدخت کشاورز")

    @golestan_manager.setter
    def golestan_manager(self, value: str) -> None:
        self.set_center_manager(1, value)

    @property
    def sadra_manager(self) -> str:
        """نام مدیر پیش‌فرض مرکز صدرا."""

        return self.get_center_manager(2, "آیناز هوشمند")

    @sadra_manager.setter
    def sadra_manager(self, value: str) -> None:
        self.set_center_manager(2, value)

    def get_center_manager(self, center_id: int, default: str = "") -> str:
        """دریافت مدیر یک مرکز با اعتبارسنجی کامل."""

        if center_id not in self._valid_centers:
            raise ValueError(f"مرکز {center_id} معتبر نیست")
        key = f"center_{center_id}_manager"
        raw_value = self._settings.value(key, "")
        if not isinstance(raw_value, str):
            raw_value = str(raw_value or "")
        cleaned_value = raw_value.strip()
        fallback = default.strip() if default else self._get_default_manager(center_id)
        if not cleaned_value:
            return fallback
        if len(cleaned_value) > 100:
            self._log_invalid_setting(key, raw_value, "طول نام مدیر بسیار زیاد است")
            return fallback
        return cleaned_value

    def set_center_manager(self, center_id: int, manager_name: str) -> None:
        """ذخیره مدیر یک مرکز با اعتبارسنجی."""

        if center_id not in self._valid_centers:
            raise ValueError(f"مرکز {center_id} معتبر نیست")
        if not isinstance(manager_name, str) or not manager_name.strip():
            raise ValueError("نام مدیر باید یک رشته غیرخالی باشد")
        cleaned_name = manager_name.strip()
        if len(cleaned_name) > 100:
            raise ValueError("نام مدیر نمی‌تواند بیش از ۱۰۰ کاراکتر باشد")
        key = f"center_{center_id}_manager"
        self._settings.setValue(key, cleaned_name)
        self._settings.sync()

    def clear_center_manager(self, center_id: int) -> None:
        """حذف override مدیر برای مرکز مشخص."""

        if center_id not in self._valid_centers:
            raise ValueError(f"مرکز {center_id} معتبر نیست")
        key = f"center_{center_id}_manager"
        self._settings.remove(key)
        self._settings.sync()

    # ------------------------------------------------------------------ UI state
    @property
    def window_geometry(self) -> bytes | None:
        """موقعیت و اندازه پنجره."""

        value = self._settings.value("ui/window_geometry")
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, QByteArray):
            return bytes(value)
        return None

    @window_geometry.setter
    def window_geometry(self, value: bytes) -> None:
        self._settings.setValue("ui/window_geometry", value)
        self._settings.sync()

    def load_recent_files(self, category: str, max_count: int = 10) -> List[str]:
        """بارگذاری لیست فایل‌های اخیر."""

        key = f"recent/{category}"
        stored = self._settings.value(key, [])
        if isinstance(stored, str):
            stored_list = [stored]
        elif isinstance(stored, list):
            stored_list = stored
        elif isinstance(stored, (tuple, set)):
            stored_list = list(stored)
        else:
            stored_list = []
        normalized = [str(item) for item in stored_list if str(item).strip()]
        return normalized[:max_count]

    def add_recent_file(self, category: str, file_path: str, max_count: int = 10) -> None:
        """افزودن فایل به لیست اخیر."""

        recent = self.load_recent_files(category, max_count)
        if file_path in recent:
            recent.remove(file_path)
        recent.insert(0, file_path)
        trimmed = recent[:max_count]
        self._settings.setValue(f"recent/{category}", trimmed)
        self._settings.sync()
