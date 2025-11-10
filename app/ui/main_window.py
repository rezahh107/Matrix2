"""پنجرهٔ اصلی PySide6 با پل Progress و لغو امن."""

from __future__ import annotations

from typing import Any, Callable

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .task_runner import ProgressFn, Worker

__all__ = ["MainWindow"]


class MainWindow(QMainWindow):
    """رابط کاربری مینیمال برای اجرای تسک‌های Core در حالت UI."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("سامانه تخصیص دانشجو-منتور")
        self.setMinimumSize(480, 320)
        self.setLayoutDirection(Qt.RightToLeft)

        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._status = QLabel("آماده")
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._demo_btn = QPushButton("اجرای تست")
        self._demo_btn.clicked.connect(self._start_demo_task)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(self._status)
        layout.addWidget(self._progress)
        layout.addWidget(self._log)
        layout.addWidget(self._demo_btn)
        self.setCentralWidget(container)

        self._worker: Worker | None = None

    # ------------------------------------------------------------------ UI API
    def run_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Worker:
        """اجرای تابع در Thread جداگانه با مدیریت progress."""

        if self._worker is not None and self._worker.isRunning():
            raise RuntimeError("task already running")

        worker = Worker(func, *args, **kwargs)
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_finished)
        self._worker = worker
        self._log.append("شروع تسک")
        worker.start()
        return worker

    # ----------------------------------------------------------------- Handlers
    @Slot(int, str)
    def _on_progress(self, pct: int, message: str) -> None:
        self._progress.setValue(int(pct))
        self._status.setText(message)
        self._log.append(f"{pct}% | {message}")

    @Slot(bool, object)
    def _on_finished(self, success: bool, error: object | None) -> None:
        if error is not None:
            self._log.append(f"خطا: {error}")
        elif not success:
            self._log.append("تسک لغو شد")
        else:
            self._log.append("تسک با موفقیت تمام شد")
        self._worker = None

    def _start_demo_task(self) -> None:
        """اجرای نمونهٔ بسیار سریع برای نمایش progress."""

        if self._worker is not None and self._worker.isRunning():
            return

        def _demo(*, progress: ProgressFn) -> None:
            for pct, msg in ((0, "آغاز"), (30, "میانی"), (60, "پایان"), (100, "کامل")):
                progress(pct, msg)

        self.run_task(_demo)

    # -------------------------------------------------------------- Qt events
    def closeEvent(self, event: QCloseEvent) -> None:  # noqa: D401
        """در صورت اجرای تسک، ابتدا لغو کرده و سپس بسته می‌شود."""

        if self._worker is not None and self._worker.isRunning():
            self._worker.request_cancel()
            self._worker.wait(3000)
        super().closeEvent(event)


def run_demo() -> None:  # pragma: no cover - فقط برای تست دستی
    """اجرای نمونهٔ دستی UI."""

    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":  # pragma: no cover
    run_demo()
