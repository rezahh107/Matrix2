"""
سیستم اعتبارسنجی با Decorator Pattern و پیام‌های کاربرپسند
"""

from pathlib import Path
from typing import Callable, Any
import pandas as pd
from functools import wraps


class ValidationError(Exception):
    """خطای اعتبارسنجی با پیام فارسی"""
    pass


def validate_with_friendly_error(validator_func: Callable) -> Callable:
    """
    دکوراتور برای تبدیل خطاهای فنی به پیام‌های کاربرپسند
    
    مثال:
        @validate_with_friendly_error
        def validate_excel_file(path):
            return Path(path)
    """
    @wraps(validator_func)
    def wrapper(*args, **kwargs):
        try:
            return validator_func(*args, **kwargs)
        except ValidationError:
            raise  # خطاهای قبلی را حفظ کن
        except Exception as e:
            raise ValidationError(f"خطای غیرمنتظره: {str(e)}")
    return wrapper


@validate_with_friendly_error
def validate_excel_file(path_str: str, file_desc: str = "فایل") -> Path:
    """
    اعتبارسنجی کامل فایل اکسل
    
    Args:
        path_str: مسیر فایل
        file_desc: توضیح فایل برای پیام‌های خطا
    
    Returns:
        Path: مسیر معتبر
        
    Raises:
        ValidationError: با پیام فارسی کاربرپسند
    """
    if not path_str or not path_str.strip():
        raise ValidationError(f"لطفاً {file_desc} را انتخاب کنید")
    
    file_path = Path(path_str)
    
    # بررسی وجود
    if not file_path.exists():
        raise ValidationError(
            f"{file_desc} یافت نشد\n"
            f"مسیر: {file_path}"
        )
    
    # بررسی نوع فایل
    if not file_path.is_file():
        raise ValidationError(f"'{file_path.name}' یک فایل نیست")
    
    # بررسی پسوند - فقط xlsx
    if file_path.suffix.lower() != '.xlsx':
        raise ValidationError(
            f"فرمت فایل نامعتبر است\n"
            f"فرمت مورد نیاز: Excel 2007+ (.xlsx)\n"
            f"فرمت دریافتی: {file_path.suffix}\n\n"
            f"توجه: فایل‌های .xls قدیمی پشتیبانی نمی‌شوند"
        )
    
    # تست قابل خواندن بودن
    try:
        pd.read_excel(file_path, nrows=1)
    except Exception as e:
        raise ValidationError(
            f"{file_desc} قابل خواندن نیست\n"
            f"ممکن است فایل خراب یا رمزدار باشد\n"
            f"جزئیات: {str(e)[:100]}"
        )
    
    return file_path


@validate_with_friendly_error
def validate_output_directory(path_str: str) -> Path:
    """
    اعتبارسنجی پوشه خروجی با بررسی مجوز نوشتن
    
    Returns:
        Path: مسیر پوشه معتبر
        
    Raises:
        ValidationError: در صورت عدم معتبر بودن یا نبود مجوز
    """
    if not path_str or not path_str.strip():
        raise ValidationError("لطفاً پوشه خروجی را انتخاب کنید")
    
    dir_path = Path(path_str)
    
    # بررسی وجود و نوع
    if not dir_path.exists():
        raise ValidationError(
            f"پوشه خروجی یافت نشد\n"
            f"مسیر: {dir_path}"
        )
    
    if not dir_path.is_dir():
        raise ValidationError(f"'{dir_path}' یک پوشه نیست")
    
    # بررسی مجوز نوشتن
    test_file = dir_path / ".write_test_temp"
    try:
        test_file.write_text("test")
        test_file.unlink()
    except Exception:
        raise ValidationError(
            f"مجوز نوشتن در پوشه خروجی وجود ندارد\n"
            f"لطفاً یک پوشه دیگر انتخاب کنید"
        )
    
    return dir_path


@validate_with_friendly_error
def validate_numeric_range(value: float, min_val: float, max_val: float, 
                          name: str = "مقدار") -> float:
    """
    اعتبارسنجی محدوده عددی
    
    مثال:
        validate_numeric_range(0.95, 0.5, 1.0, "درصد اشغال")
    """
    if not (min_val <= value <= max_val):
        raise ValidationError(
            f"{name} باید بین {min_val} و {max_val} باشد\n"
            f"مقدار وارد شده: {value}"
        )
    return value


class InputValidator:
    """
    کلاس کمکی برای اعتبارسنجی دسته‌ای ورودی‌ها
    
    مثال:
        validator = InputValidator()
        validator.add_check(lambda: validate_excel_file(path, "گزارش"))
        validator.add_check(lambda: validate_output_directory(output))
        
        if validator.validate():
            # همه ورودی‌ها معتبر هستند
    """
    
    def __init__(self):
        self.checks = []
        self.errors = []
        
    def add_check(self, check_func: Callable) -> 'InputValidator':
        """افزودن یک بررسی"""
        self.checks.append(check_func)
        return self  # برای chain کردن
        
    def validate(self) -> bool:
        """
        اجرای تمام بررسی‌ها
        
        Returns:
            bool: True اگر همه معتبر باشند
        """
        self.errors.clear()
        
        for check in self.checks:
            try:
                check()
            except ValidationError as e:
                self.errors.append(str(e))
        
        return len(self.errors) == 0
    
    def get_first_error(self) -> str:
        """دریافت اولین خطا (برای نمایش به کاربر)"""
        return self.errors[0] if self.errors else ""
    
    def get_all_errors(self) -> str:
        """دریافت تمام خطاها با فاصله‌گذاری"""
        return "\n\n".join(f"❌ {err}" for err in self.errors)


# ============= مثال استفاده =============
def validate_build_matrix_inputs(inspector_path: str, school_path: str, 
                                 crosswalk_path: str) -> dict:
    """
    اعتبارسنجی کامل ورودی‌های ساخت ماتریس
    
    Returns:
        dict: دیکشنری مسیرهای معتبر
        
    Raises:
        ValidationError: در صورت نامعتبر بودن هر ورودی
    """
    validator = InputValidator()
    
    results = {}
    
    # بررسی‌ها را اضافه کن
    validator.add_check(
        lambda: results.update({
            'inspector': validate_excel_file(inspector_path, "گزارش inspectors")
        })
    )
    
    validator.add_check(
        lambda: results.update({
            'school': validate_excel_file(school_path, "گزارش schools")
        })
    )
    
    validator.add_check(
        lambda: results.update({
            'crosswalk': validate_excel_file(crosswalk_path, "فایل crosswalk")
        })
    )
    
    # اعتبارسنجی
    if not validator.validate():
        raise ValidationError(validator.get_first_error())
    
    return results


# تست
if __name__ == "__main__":
    print("✅ Validator آماده استفاده است")
    
    # تست اعتبارسنجی
    try:
        validate_excel_file("", "تست")
    except ValidationError as e:
        print(f"✓ خطای مورد انتظار: {e}")
