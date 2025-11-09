"""
نقطه ورود برنامه تخصیص دانشجو-منتور
مدیریت: Singleton، DPI Scaling، خطاهای بحرانی
"""

import sys
import os
from pathlib import Path
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtCore import Qt, QSharedMemory
from PySide6.QtGui import QFont


__version__ = "1.0.0"


def setup_environment():
    """پیکربندی محیط اجرا"""
    # DPI Scaling
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
    os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"
    
    # اضافه کردن مسیر فعلی به sys.path برای import
    current_dir = Path(__file__).resolve().parent
    root_dir = current_dir.parent
    for p in (str(root_dir), str(current_dir)):
        if p not in sys.path:
            sys.path.insert(0, p)


class SingleInstanceGuard:
    """
    مدیریت Singleton با QSharedMemory
    جلوگیری از اجرای چند نمونه همزمان برنامه
    """
    
    def __init__(self, key: str = "AllocationApp_SingleInstance"):
        self.key = key
        self.shared_memory = QSharedMemory(key)
        
    def is_already_running(self) -> bool:
        """
        بررسی اجرای قبلی برنامه
        
        Returns:
            True اگر برنامه قبلاً در حال اجراست
        """
        # تلاش برای attach به shared memory موجود
        if self.shared_memory.attach():
            return True
            
        # تلاش برای ایجاد shared memory جدید
        if self.shared_memory.create(1):
            return False
            
        # در صورت خطا، فرض می‌کنیم برنامه در حال اجراست
        return True
        
    def release(self):
        """آزادسازی منابع"""
        if self.shared_memory.isAttached():
            self.shared_memory.detach()


def show_already_running_message():
    """نمایش پیام برنامه در حال اجرا"""
    msg = QMessageBox()
    msg.setIcon(QMessageBox.Information)
    msg.setWindowTitle("برنامه در حال اجرا")
    msg.setText("برنامه تخصیص دانشجو-منتور قبلاً اجرا شده است.")
    msg.setInformativeText(
        "لطفاً پنجره برنامه را از نوار وظیفه پیدا کنید.\n"
        "در صورت عدم دسترسی، برنامه را از Task Manager ببندید."
    )
    msg.setStandardButtons(QMessageBox.Ok)
    msg.setDefaultButton(QMessageBox.Ok)
    msg.exec()


def setup_application() -> QApplication:
    """
    راه‌اندازی QApplication با تنظیمات بهینه
    
    Returns:
        QApplication: نمونه برنامه
    """
    app = QApplication(sys.argv)
    
    # فعال‌سازی High DPI
    app.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    app.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    
    # تنظیمات برنامه
    app.setApplicationName("AllocationApp")
    app.setOrganizationName("YourOrg")
    app.setApplicationVersion(__version__)
    
    # فونت پیش‌فرض
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    return app


def main():
    """
    تابع اصلی اجرای برنامه
    
    Returns:
        int: کد خروج (0 = موفق، 1 = خطا)
    """
    try:
        # پیکربندی محیط
        setup_environment()
        
        # بررسی Singleton
        guard = SingleInstanceGuard()
        if guard.is_already_running():
            # ایجاد QApplication موقت برای نمایش پیام
            temp_app = QApplication(sys.argv)
            show_already_running_message()
            return 1
        
        # راه‌اندازی برنامه اصلی
        app = setup_application()
        
        # import پنجره اصلی (بعد از setup)
        try:
            from app.ui.main_window import MainWindow
        except ImportError:
            QMessageBox.critical(
                None,
                "خطای بارگذاری",
                "خطا در بارگذاری ماژول‌های برنامه.\n"
                "لطفاً یکپارچگی فایل‌ها را بررسی کنید."
            )
            return 1
        
        # ایجاد و نمایش پنجره
        window = MainWindow()
        window.show()
        
        # اجرای حلقه رویداد
        exit_code = app.exec()
        
        # آزادسازی منابع
        guard.release()
        
        return exit_code
        
    except Exception as e:
        # مدیریت خطاهای بحرانی
        import traceback
        
        error_message = (
            f"خطای بحرانی در اجرای برنامه:\n\n"
            f"{str(e)}\n\n"
            f"جزئیات فنی:\n"
            f"{traceback.format_exc()}"
        )
        
        print(error_message)
        
        # تلاش برای نمایش به کاربر
        try:
            temp_app = QApplication(sys.argv)
            QMessageBox.critical(
                None,
                "خطای بحرانی",
                f"برنامه با خطا مواجه شد:\n{str(e)}\n\n"
                f"لطفاً به توسعه‌دهنده اطلاع دهید."
            )
        except:
            pass
            
        return 1


if __name__ == "__main__":
    sys.exit(main())
