"""
موتور اجرای غیرهمزمان وظایف با قابلیت لغو و گزارش پیشرفت
الگوهای استفاده‌شده: Command, Observer, Context Manager
"""

from PySide6.QtCore import QObject, Signal, QThread
from typing import Any, Callable, Optional
import traceback
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class TaskResult:
    """نتیجه اجرای تسک"""
    success: bool
    data: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None


class TaskRunner(QObject):
    """
    اجراکننده تسک‌های سنگین در thread جداگانه
    
    مثال استفاده:
        runner = TaskRunner(my_heavy_task, arg1, arg2)
        runner.progress.connect(update_progress_bar)
        runner.finished.connect(handle_result)
        runner.start()
    """
    
    progress = Signal(int, str)    # (درصد، پیام)
    finished = Signal(object)
    
    def __init__(self, task_func: Callable, *args, **kwargs):
        super().__init__()
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs
        self._cancelled = False
        self._thread: Optional[QThread] = None
        
    def run(self) -> None:
        """اجرای تسک با مدیریت خطا"""
        try:
            self._cancelled = False
            
            # اجرای تابع با ارسال سیگنال پیشرفت
            result = self.task_func(
                self.progress, 
                self.check_cancel,
                *self.args, 
                **self.kwargs
            )
            
            self.finished.emit(TaskResult(success=True, data=result))
            
        except Exception as e:
            error_result = TaskResult(
                success=False,
                error=str(e),
                traceback=traceback.format_exc()
            )
            self.finished.emit(error_result)
            
    def start(self) -> None:
        """شروع اجرا در thread جدید"""
        self._thread = QThread()
        self.moveToThread(self._thread)
        
        self._thread.started.connect(self.run)
        self.finished.connect(self._thread.quit)
        self.finished.connect(self.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        
        self._thread.start()
        
    def cancel(self) -> None:
        """درخواست لغو تسک"""
        self._cancelled = True
        
    def check_cancel(self) -> None:
        """بررسی لغو - باید در نقاط کلیدی تسک فراخوانی شود"""
        if self._cancelled:
            raise RuntimeError("عملیات توسط کاربر لغو شد")


@contextmanager
def task_execution_context(progress_signal, start_msg: str, end_msg: str):
    """
    Context manager برای مدیریت خودکار پیشرفت
    
    مثال:
        with task_execution_context(progress, "شروع خواندن", "خواندن تمام شد"):
            data = read_large_file()
    """
    progress_signal.emit(0, start_msg)
    try:
        yield
    finally:
        progress_signal.emit(100, end_msg)


# ============= مثال استفاده =============
def sample_heavy_task(progress_signal, check_cancel, file_path: str):
    """تسک نمونه که طول می‌کشد"""
    
    with task_execution_context(progress_signal, "شروع پردازش", "اتمام"):
        
        # مرحله 1: خواندن
        progress_signal.emit(20, f"در حال خواندن {file_path}")
        check_cancel()  # بررسی لغو
        
        # شبیه‌سازی کار سنگین
        import time
        time.sleep(1)
        
        # مرحله 2: پردازش
        progress_signal.emit(60, "در حال پردازش داده‌ها")
        check_cancel()
        time.sleep(1)
        
        # مرحله 3: ذخیره
        progress_signal.emit(90, "در حال ذخیره نتایج")
        check_cancel()
        time.sleep(0.5)
        
    return {"status": "موفق", "rows": 1000}


# تست
if __name__ == "__main__":
    print("✅ TaskRunner آماده استفاده است")
