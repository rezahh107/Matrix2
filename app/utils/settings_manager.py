"""
مدیریت تنظیمات با ماندگاری و پشتیبان‌گیری خودکار
الگوی Singleton برای دسترسی سراسری
"""

from PySide6.QtCore import QSettings, QByteArray
from typing import Any, Optional, Dict
from pathlib import Path
import json
import base64
from datetime import datetime


class SettingsManager:
    """
    مدیریت تنظیمات برنامه با قابلیت ذخیره‌سازی دائمی
    
    مثال استفاده:
        settings = SettingsManager.instance()
        settings.set('last_output_dir', '/path/to/dir')
        output_dir = settings.get('last_output_dir', default='/tmp')
    """
    
    _instance: Optional['SettingsManager'] = None
    
    def __init__(self, org_name: str = "YourOrg", app_name: str = "AllocationApp"):
        """
        Args:
            org_name: نام سازمان (برای مسیر ذخیره)
            app_name: نام برنامه
        """
        self._settings = QSettings(org_name, app_name)
        self._cache: Dict[str, Any] = {}
        self._load_cache()
        
    @classmethod
    def instance(cls, org_name: str = "YourOrg", 
                 app_name: str = "AllocationApp") -> 'SettingsManager':
        """دریافت نمونه Singleton"""
        if cls._instance is None:
            cls._instance = cls(org_name, app_name)
        return cls._instance
    
    
    def _serialize_value(self, value: Any) -> Any:
        """Serialize values safely for JSON (handles QByteArray/bytes)."""
        try:
            from PySide6.QtCore import QByteArray
        except Exception:
            QByteArray = None
        if QByteArray and isinstance(value, QByteArray):
            value = bytes(value)
        if isinstance(value, (bytes, bytearray)):
            return {"__type__": "bytes", "encoding": "base64", "data": base64.b64encode(bytes(value)).decode("ascii")}
        return value

    def _deserialize_value(self, value: Any) -> Any:
        """Deserialize values serialized by _serialize_value."""
        try:
            from PySide6.QtCore import QByteArray
        except Exception:
            QByteArray = None
        if isinstance(value, dict) and value.get("__type__") == "bytes" and value.get("encoding") == "base64":
            raw = base64.b64decode(value.get("data", ""))
            return QByteArray(raw) if QByteArray else raw
        return value

    def _load_cache(self):
        """بارگذاری تنظیمات در کش"""
        for key in self._settings.allKeys():
            self._cache[key] = self._settings.value(key)
    
    def set(self, key: str, value: Any) -> None:
        """
        ذخیره یک تنظیم
        
        Args:
            key: کلید تنظیم
            value: مقدار (باید JSON-serializable باشد)
        """
        self._settings.setValue(key, value)
        self._cache[key] = value
        self._settings.sync()  # ذخیره فوری
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        دریافت یک تنظیم
        
        Args:
            key: کلید تنظیم
            default: مقدار پیش‌فرض در صورت عدم وجود
            
        Returns:
            مقدار ذخیره شده یا default
        """
        if key in self._cache:
            return self._cache[key]
        
        value = self._settings.value(key, default)
        self._cache[key] = value
        return value
    
    def get_int(self, key: str, default: int = 0) -> int:
        """دریافت عدد صحیح"""
        value = self.get(key, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        """دریافت عدد اعشاری"""
        value = self.get(key, default)
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """دریافت مقدار بولین"""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return bool(value)
    
    def remove(self, key: str) -> None:
        """حذف یک تنظیم"""
        self._settings.remove(key)
        self._cache.pop(key, None)
    
    def clear(self) -> None:
        """پاکسازی تمام تنظیمات"""
        self._settings.clear()
        self._cache.clear()
    
    def export_to_file(self, file_path: Path) -> None:
        """
        صادرات تنظیمات به فایل JSON (برای پشتیبان) با serialize ایمن
        """
        safe_settings = {k: self._serialize_value(v) for k, v in self._cache.items()}
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'settings': safe_settings
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
    
    def import_from_file(self, file_path: Path) -> None:
        """وارد کردن تنظیمات از فایل JSON با deserialize ایمن"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if 'settings' in data:
            for key, value in data['settings'].items():
                self.set(key, self._deserialize_value(value))


class AppPreferences:
    """
    کلاس کمکی برای دسترسی آسان به تنظیمات رایج برنامه
    
    مثال:
        prefs = AppPreferences()
        prefs.last_output_dir = "/path/to/output"
        print(prefs.last_output_dir)
    """
    
    def __init__(self, settings: Optional[SettingsManager] = None):
        self._settings = settings or SettingsManager.instance()
    
    @property
    def last_output_dir(self) -> str:
        """آخرین پوشه خروجی انتخاب شده"""
        return self._settings.get('ui/last_output_dir', '')
    
    @last_output_dir.setter
    def last_output_dir(self, value: str):
        self._settings.set('ui/last_output_dir', value)
    
    @property
    def last_matrix_path(self) -> str:
        """آخرین مسیر ماتریس"""
        return self._settings.get('ui/last_matrix_path', '')
    
    @last_matrix_path.setter
    def last_matrix_path(self, value: str):
        self._settings.set('ui/last_matrix_path', value)
    
    @property
    def max_occupancy(self) -> float:
        """حداکثر درصد اشغال منتورها"""
        return self._settings.get_float('allocation/max_occupancy', 0.95)
    
    @max_occupancy.setter
    def max_occupancy(self, value: float):
        self._settings.set('allocation/max_occupancy', value)
    
    @property
    def priority_new_mentors(self) -> bool:
        """اولویت منتورهای جدید"""
        return self._settings.get_bool('allocation/priority_new_mentors', True)
    
    @priority_new_mentors.setter
    def priority_new_mentors(self, value: bool):
        self._settings.set('allocation/priority_new_mentors', value)
    
    @property
    def priority_high_capacity(self) -> bool:
        """اولویت ظرفیت بالا"""
        return self._settings.get_bool('allocation/priority_high_capacity', True)
    
    @priority_high_capacity.setter
    def priority_high_capacity(self, value: bool):
        self._settings.set('allocation/priority_high_capacity', value)
    
    @property
    def enable_capacity_gate(self) -> bool:
        """فعال‌سازی Capacity Gate"""
        return self._settings.get_bool('matrix/enable_capacity_gate', True)
    
    @enable_capacity_gate.setter
    def enable_capacity_gate(self, value: bool):
        self._settings.set('matrix/enable_capacity_gate', value)
    
    @property
    def window_geometry(self) -> Optional[bytes]:
        """موقعیت و اندازه پنجره"""
        return self._settings.get('ui/window_geometry')
    
    @window_geometry.setter
    def window_geometry(self, value: bytes):
        self._settings.set('ui/window_geometry', value)
    
    def load_recent_files(self, category: str, max_count: int = 10) -> list:
        """
        بارگذاری لیست فایل‌های اخیر
        
        Args:
            category: دسته‌بندی (مثلاً 'inspector', 'school')
            max_count: حداکثر تعداد
            
        Returns:
            لیست مسیرهای اخیر
        """
        key = f'recent/{category}'
        recent = self._settings.get(key, [])
        
        if isinstance(recent, str):
            recent = [recent]
        
        return recent[:max_count]
    
    def add_recent_file(self, category: str, file_path: str, 
                       max_count: int = 10) -> None:
        """
        افزودن فایل به لیست اخیر
        
        Args:
            category: دسته‌بندی
            file_path: مسیر فایل
            max_count: حداکثر تعداد نگهداری
        """
        recent = self.load_recent_files(category, max_count)
        
        # حذف دوباره‌ها
        if file_path in recent:
            recent.remove(file_path)
        
        # افزودن به اول لیست
        recent.insert(0, file_path)
        
        # محدود کردن تعداد
        recent = recent[:max_count]
        
        self._settings.set(f'recent/{category}', recent)


# ============= مثال استفاده =============
if __name__ == "__main__":
    print("✅ SettingsManager آماده استفاده است")
    
    # تست
    prefs = AppPreferences()
    prefs.last_output_dir = "/test/path"
    print(f"✓ پوشه ذخیره شد: {prefs.last_output_dir}")
    
    prefs.add_recent_file('inspector', '/path/to/inspector.xlsx')
    print(f"✓ فایل‌های اخیر: {prefs.load_recent_files('inspector')}")
