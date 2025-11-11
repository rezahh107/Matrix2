"""
نقطه ورود برنامه تخصیص دانشجو-منتور
مدیریت: Singleton، DPI Scaling، خطاهای بحرانی
نسخه بهبود یافته
"""

import sys
import os
import logging
import atexit
import traceback
import getpass
from pathlib import Path
from typing import Callable, Optional, NoReturn
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtCore import Qt, QSharedMemory, QTimer
from PySide6.QtGui import QFont, QGuiApplication

from app.infra.logging import LoggingContext, configure_logging, install_exception_hook


__version__ = "1.0.1"
__author__ = "Your Name"
__description__ = "سیستم تخصیص دانشجو-منتور"


logger = logging.getLogger("app.ui.main")
_LOGGING_CONTEXT: LoggingContext | None = None
_RESTORE_EXCEPTION_HOOK: Callable[[], None] | None = None


def _bootstrap_logging() -> LoggingContext:
    """راه‌اندازی زیرساخت لاگ با ذخیرهٔ کانتکست سراسری.

    مثال::

        >>> ctx = _bootstrap_logging()  # doctest: +SKIP
    """

    global _LOGGING_CONTEXT, _RESTORE_EXCEPTION_HOOK
    if _LOGGING_CONTEXT is None:
        context = configure_logging(
            app_name="AllocationApp",
            app_version=__version__,
            logger_name=logger.name,
        )
        _LOGGING_CONTEXT = context
        _RESTORE_EXCEPTION_HOOK = install_exception_hook(logger, context)
    return _LOGGING_CONTEXT


def setup_environment() -> None:
    """
    پیکربندی محیط اجرا با مدیریت خطا و بهینه‌سازی تنظیمات
    """
    try:
        # تنظیمات DPI Scaling برای نمایش بهینه در صفحه‌های High DPI
        os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
        os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"
        os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
        
        # اضافه کردن مسیرهای مورد نیاز به sys.path
        current_dir = Path(__file__).resolve().parent
        root_dir = current_dir.parent
        
        paths_to_add = [str(root_dir), str(current_dir)]
        for path in paths_to_add:
            if path not in sys.path:
                sys.path.insert(0, path)
                logger.info(f"مسیر اضافه شد: {path}")
        
        logger.info("پیکربندی محیط با موفقیت انجام شد")
        
    except Exception as e:
        logger.error(f"خطا در پیکربندی محیط: {e}")
        # ادامه اجرا با تنظیمات پیش‌فرض
        logger.warning("ادامه اجرا با تنظیمات پیش‌فرض")


class SingleInstanceGuard:
    """
    کلاس مدیریت نمونه یکتا (Singleton) با استفاده از QSharedMemory
    جلوگیری از اجرای چند نمونه همزمان برنامه
    """
    
    def __init__(self, key: str = "AllocationApp_SingleInstance_v1") -> None:
        """
        مقداردهی اولیه کلاس
        
        Args:
            key: کلید منحصر به فرد برای شناسایی نمونه برنامه
        """
        # اضافه کردن شناسه کاربر برای جلوگیری از تداخل
        user_specific_key = f"{key}_{getpass.getuser()}"
        
        self.key = user_specific_key
        self.shared_memory = QSharedMemory(user_specific_key)
        self._is_attached = False
        atexit.register(self.cleanup)
        
    def is_already_running(self) -> bool:
        """
        بررسی اجرای قبلی برنامه با timeout
        
        Returns:
            bool: True اگر برنامه قبلاً در حال اجراست
        """
        try:
            # تلاش برای attach به shared memory موجود
            if self.shared_memory.attach():
                self._is_attached = True
                logger.warning("نمونه دیگری از برنامه در حال اجراست")
                return True
                
            # تلاش برای ایجاد shared memory جدید
            if self.shared_memory.create(1):
                self._is_attached = True
                logger.info("Shared memory ایجاد شد - اولین نمونه برنامه")
                return False
                
            # خطا در ایجاد - احتمالاً نمونه دیگری در حال اجراست
            error = self.shared_memory.error()
            logger.error(f"خطا در ایجاد shared memory: {error}")
            return True
            
        except Exception as e:
            logger.error(f"خطا در بررسی singleton: {e}")
            return True
    
    def cleanup(self) -> None:
        """آزادسازی منابع با مدیریت خطا"""
        try:
            if self.shared_memory.isAttached():
                self.shared_memory.detach()
                logger.info("Shared memory آزاد شد")
        except Exception as e:
            logger.error(f"خطا در آزادسازی shared memory: {e}")
    
    def __enter__(self):
        """پشتیبانی از context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """تمیزکاری خودکار هنگام خروج از context"""
        self.cleanup()


def show_already_running_message() -> None:
    """نمایش پیام برنامه در حال اجرا با تنظیمات بهتر"""
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    msg_box = QMessageBox()
    msg_box.setIcon(QMessageBox.Icon.Warning)
    msg_box.setWindowTitle("برنامه در حال اجرا")
    msg_box.setText("📱 برنامه تخصیص دانشجو-منتور قبلاً اجرا شده است.")
    msg_box.setInformativeText(
        "لطفاً پنجره برنامه را از نوار وظیفه پیدا کنید.\n\n"
        "📍 در صورت عدم دسترسی:\n"
        "• از Task Manager (Ctrl+Shift+Esc) استفاده کنید\n"
        "• process های مربوطه را ببندید\n"
        "• سپس مجدداً برنامه را اجرا کنید"
    )
    msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
    msg_box.setDefaultButton(QMessageBox.StandardButton.Ok)
    
    # تنظیمات ظاهری
    msg_box.setStyleSheet("""
        QMessageBox {
            background-color: #f8f9fa;
            font-family: Segoe UI;
        }
        QMessageBox QPushButton {
            background-color: #007bff;
            color: white;
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            min-width: 80px;
        }
    """)
    
    msg_box.exec()


def setup_application() -> QApplication:
    """
    راه‌اندازی QApplication با تنظیمات بهینه و مدیریت خطا
    
    Returns:
        QApplication: نمونه برنامه
    """
    try:
        app = QApplication.instance()
        if app is None:
            app = QApplication(sys.argv)
        
        # فعال‌سازی High DPI با fallback
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
        app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
        
        # تنظیمات برنامه
        app.setApplicationName("AllocationApp")
        app.setOrganizationName("YourOrg")
        app.setApplicationVersion(__version__)
        app.setQuitOnLastWindowClosed(True)
        
        # فونت پیش‌فرض با پشتیبانی از زبان فارسی
        preferred_fonts = [
            "Segoe UI", 
            "Tahoma", 
            "Arial", 
            "Verdana",
            "Microsoft Sans Serif"  # پشتیبانی بهتر از فارسی
        ]
        font = QFont()
        for font_name in preferred_fonts:
            if font_name in QFont().families():
                font.setFamily(font_name)
                logger.info(f"فونت انتخاب شده: {font_name}")
                break
        
        font.setPointSize(10)
        font.setStyleHint(QFont.StyleHint.AnyStyle)
        app.setFont(font)
        
        logger.info("QApplication با موفقیت راه‌اندازی شد")
        return app
        
    except Exception as e:
        logger.error(f"خطا در راه‌اندازی QApplication: {e}")
        raise


def load_main_window():
    """
    بارگذاری ماژول پنجره اصلی با مدیریت خطای دقیق
    
    Returns:
        MainWindow: کلاس پنجره اصلی
    """
    try:
        from app.ui.main_window import MainWindow
        logger.info("ماژول MainWindow با موفقیت بارگذاری شد")
        return MainWindow
        
    except ImportError as e:
        logger.error(f"خطای Import در بارگذاری MainWindow: {e}")
        
        # تشخیص نوع خطای import
        if "app.ui.main_window" in str(e):
            raise ImportError(
                "خطا در بارگذاری ماژول‌های برنامه.\n"
                "لطفاً از صحت ساختار پوشه‌ها و فایل‌ها اطمینان حاصل کنید."
            ) from e
        else:
            raise ImportError(
                "خطا در وابستگی‌های برنامه.\n"
                "لطفاً از نصب بودن تمام کتابخانه‌های مورد نیاز اطمینان حاصل کنید."
            ) from e


def show_critical_error(
    message: str,
    technical_details: str = "",
    *,
    log_path: Path | None = None,
) -> None:
    """
    نمایش خطای بحرانی با جزئیات
    
    Args:
        message: پیام خطا برای کاربر
        technical_details: جزئیات فنی برای توسعه‌دهنده
        log_path: مسیر فایل گزارش خطا برای اشتراک‌گذاری
    """
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    error_msg = QMessageBox()
    error_msg.setIcon(QMessageBox.Icon.Critical)
    error_msg.setWindowTitle("خطای بحرانی")
    error_msg.setText("❌ برنامه با خطای غیرمنتظره مواجه شد")

    info_text = message
    if log_path:
        info_text += f"\n\n📄 مسیر گزارش خطا:\n{log_path}"
    error_msg.setInformativeText(info_text)
    
    if technical_details:
        error_msg.setDetailedText(technical_details)
    
    error_msg.setStandardButtons(QMessageBox.StandardButton.Ok)
    error_msg.exec()


def main() -> int:
    """
    تابع اصلی اجرای برنامه با مدیریت خطای جامع
    
    Returns:
        int: کد خروج (0 = موفق، 1 = خطا)
    """
    context = _bootstrap_logging()
    guard = None
    app = None
    
    try:
        # لاگ اطلاعات سیستم
        logger.info(f"شروع راه‌اندازی برنامه - نسخه {__version__}")
        logger.info(f"Python: {sys.version}")
        logger.info(f"Platform: {sys.platform}")
        
        # پیکربندی محیط
        setup_environment()
        
        # بررسی Singleton
        guard = SingleInstanceGuard()
        if guard.is_already_running():
            logger.warning("تلاش برای اجرای نمونه دوم برنامه")
            show_already_running_message()
            return 1
        
        # راه‌اندازی برنامه اصلی
        app = setup_application()
        
        # بارگذاری و ایجاد پنجره اصلی
        MainWindowClass = load_main_window()
        window = MainWindowClass()
        window.show()
        
        logger.info("برنامه با موفقیت راه‌اندازی شد و پنجره اصلی نمایش داده شد")
        
        # اجرای حلقه رویداد
        exit_code = app.exec()
        logger.info(f"برنامه با کد خروج {exit_code} بسته شد")
        
        return exit_code
        
    except ImportError as e:
        # خطاهای مربوط به import ماژول‌ها
        error_msg = str(e)
        error_details = traceback.format_exc()
        error_id = context.new_error_id()
        report_path = context.write_error_report(
            error_id=error_id,
            message=error_msg,
            traceback_text=error_details,
        )
        logger.error(
            f"خطای Import: {error_msg}",
            extra={"error_id": error_id, "report_path": str(report_path)},
        )
        show_critical_error(
            "خطا در بارگذاری کامپوننت‌های برنامه.\n\n"
            "راه‌حل‌های احتمالی:\n"
            "• از کامل بودن فایل‌های برنامه اطمینان حاصل کنید\n"
            "• مجدداً برنامه را نصب کنید\n"
            "• با پشتیبانی تماس بگیرید",
            f"ImportError: {error_msg}\nPython Path: {sys.path}",
            log_path=report_path,
        )
        return 1

    except Exception as e:
        # مدیریت خطاهای بحرانی
        error_message = f"خطای غیرمنتظره: {str(e)}"
        technical_details = traceback.format_exc()
        error_id = context.new_error_id()
        report_path = context.write_error_report(
            error_id=error_id,
            message=error_message,
            traceback_text=technical_details,
        )

        logger.critical(
            f"خطای بحرانی: {error_message}\n{technical_details}",
            extra={"error_id": error_id, "report_path": str(report_path)},
        )

        show_critical_error(
            "برنامه با یک خطای غیرمنتظره مواجه شد.\n\n"
            "لطفاً:\n"
            "• شرایط را بررسی کنید\n"
            "• مجدداً تلاش کنید\n"
            "• در صورت تکرار، با پشتیبانی تماس بگیرید",
            technical_details,
            log_path=report_path,
        )
        return 1

    finally:
        # تمیزکاری منابع - تضمین آزادسازی در همه شرایط
        if guard:
            guard.cleanup()
        logger.info("تمیزکاری منابع انجام شد")
        global _RESTORE_EXCEPTION_HOOK
        if _RESTORE_EXCEPTION_HOOK:
            _RESTORE_EXCEPTION_HOOK()
            _RESTORE_EXCEPTION_HOOK = None


if __name__ == "__main__":
    sys.exit(main())
