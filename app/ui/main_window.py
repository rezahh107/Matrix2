"""پنجرهٔ اصلی PySide6 برای سناریوهای سامانه تخصیص دانشجو-منتور."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, List, Sequence, Tuple

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.infra import cli

from .task_runner import ProgressFn, Worker

__all__ = ["MainWindow", "run_demo", "FilePicker"]


class FilePicker(QWidget):
    def __init__(self, parent=None, save=False, placeholder=""):
        super().__init__(parent)
        self.save = save

        self.edit = QLineEdit(self)
        self.edit.setPlaceholderText(placeholder)

        self.btn = QPushButton("انتخاب…", self)
        self.btn.clicked.connect(self._pick)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.edit)
        layout.addWidget(self.btn)

    def text(self) -> str:
        return self.edit.text().strip()

    def setText(self, s: str) -> None:
        self.edit.setText(s)

    def _pick(self) -> None:
        if self.save:
            path, _ = QFileDialog.getSaveFileName(
                self,
                "ذخیره خروجی",
                "",
                "All Files (*.*)",
            )
        else:
            path, _ = QFileDialog.getOpenFileName(
                self,
                "انتخاب فایل",
                "",
                "All Files (*.*)",
            )

        if path:
            self.edit.setText(path)


class MainWindow(QMainWindow):
    """پنجرهٔ اصلی PySide6 برای اجرای سناریوهای Build و Allocate."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("سامانه تخصیص دانشجو-منتور")
        self.setMinimumSize(960, 640)
        self.setLayoutDirection(Qt.RightToLeft)

        self._worker: Worker | None = None
        policy_file = Path("config/policy.json")
        self._default_policy_path = str(policy_file) if policy_file.exists() else ""

        central = QWidget(self)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(16)

        dashboard = self._build_dashboard()
        main_layout.addWidget(dashboard)

        self._tabs = QTabWidget(self)
        self._tabs.setDocumentMode(True)
        self._tabs.setTabPosition(QTabWidget.North)
        self._tabs.addTab(self._build_build_page(), "ساخت ماتریس")
        self._tabs.addTab(self._build_allocate_page(), "تخصیص")
        self._tabs.addTab(self._build_validate_page(), "اعتبارسنجی")
        self._tabs.addTab(self._build_explain_page(), "توضیحات")
        main_layout.addWidget(self._tabs)

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(12)
        self._status = QLabel("آماده")
        self._status.setObjectName("labelStatus")
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setObjectName("progressBar")
        status_layout.addWidget(self._status, 0)
        status_layout.addWidget(self._progress, 1)
        main_layout.addLayout(status_layout)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setObjectName("textLog")
        main_layout.addWidget(self._log, 1)

        bottom_layout = QHBoxLayout()
        bottom_layout.setContentsMargins(0, 0, 0, 0)
        bottom_layout.setSpacing(12)
        bottom_layout.addStretch(1)
        self._btn_demo = QPushButton("اجرای تست (دمو Progress)")
        self._btn_demo.setObjectName("btnDemo")
        self._btn_demo.clicked.connect(self._start_demo_task)
        bottom_layout.addWidget(self._btn_demo)
        main_layout.addLayout(bottom_layout)

        self.setCentralWidget(central)

        self._interactive: List[QWidget] = []
        self._register_interactive_controls()

    # ------------------------------------------------------------------ UI setup
    def _build_dashboard(self) -> QWidget:
        """ایجاد کارت داشبورد سبک با وضعیت Policy و میانبرها."""

        frame = QFrame(self)
        frame.setFrameShape(QFrame.StyledPanel)
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(24)

        policy_display = self._default_policy_path or "config/policy.json"
        info = QLabel(
            f"<b>سیاست فعال:</b> {policy_display}<br/>"
            "<b>نسخه Policy:</b> 1.0.3<br/>"
            "<b>نسخه SSoT:</b> 1.0.2"
        )
        info.setTextFormat(Qt.RichText)
        info.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        layout.addWidget(info, 1)

        shortcuts = QVBoxLayout()
        shortcuts.setContentsMargins(0, 0, 0, 0)
        shortcuts.setSpacing(8)
        label = QLabel("میان‌بر سناریوها")
        label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        shortcuts.addWidget(label)

        btn_build = QPushButton("شروع ساخت ماتریس")
        btn_build.clicked.connect(self._start_build)
        shortcuts.addWidget(btn_build)

        btn_allocate = QPushButton("شروع تخصیص")
        btn_allocate.clicked.connect(self._start_allocate)
        shortcuts.addWidget(btn_allocate)

        shortcuts.addStretch(1)
        layout.addLayout(shortcuts, 0)

        return frame

    def _build_build_page(self) -> QWidget:
        """فرم ورودی‌های سناریوی ساخت ماتریس."""

        page = QWidget(self)
        form = QFormLayout(page)
        form.setLabelAlignment(Qt.AlignRight)
        form.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_inspactor = FilePicker(page, placeholder="فایل Inspactor")
        self._picker_inspactor.setObjectName("editInspactor")
        form.addRow("گزارش Inspactor", self._picker_inspactor)

        self._picker_schools = FilePicker(page, placeholder="فایل مدارس")
        self._picker_schools.setObjectName("editSchools")
        form.addRow("گزارش مدارس", self._picker_schools)

        self._picker_crosswalk = FilePicker(page, placeholder="فایل Crosswalk")
        self._picker_crosswalk.setObjectName("editCrosswalk")
        form.addRow("Crosswalk", self._picker_crosswalk)

        self._picker_policy_build = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_build.setObjectName("editPolicy1")
        if self._default_policy_path:
            self._picker_policy_build.setText(self._default_policy_path)
        form.addRow("سیاست", self._picker_policy_build)

        self._picker_output_matrix = FilePicker(
            page, save=True, placeholder="فایل خروجی ماتریس (*.xlsx)"
        )
        self._picker_output_matrix.setObjectName("editMatrixOut")
        form.addRow("خروجی ماتریس", self._picker_output_matrix)

        self._btn_build = QPushButton("ساخت ماتریس")
        self._btn_build.setObjectName("btnBuildMatrix")
        self._btn_build.clicked.connect(self._start_build)
        form.addRow("", self._btn_build)

        return page

    def _build_allocate_page(self) -> QWidget:
        """فرم ورودی‌های سناریوی تخصیص."""

        page = QWidget(self)
        form = QFormLayout(page)
        form.setLabelAlignment(Qt.AlignRight)
        form.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_students = FilePicker(
            page, placeholder="دانش‌آموزان (*.xlsx یا *.csv)"
        )
        self._picker_students.setObjectName("editStudents")
        form.addRow("فایل دانش‌آموزان", self._picker_students)

        self._picker_pool = FilePicker(page, placeholder="استخر منتورها (*.xlsx)")
        self._picker_pool.setObjectName("editPool")
        form.addRow("استخر منتورها", self._picker_pool)

        self._picker_policy_allocate = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_allocate.setObjectName("editPolicy2")
        if self._default_policy_path:
            self._picker_policy_allocate.setText(self._default_policy_path)
        form.addRow("سیاست", self._picker_policy_allocate)

        self._picker_alloc_out = FilePicker(
            page, save=True, placeholder="فایل خروجی تخصیص (*.xlsx)"
        )
        self._picker_alloc_out.setObjectName("editAllocOut")
        form.addRow("خروجی تخصیص", self._picker_alloc_out)

        self._edit_capacity = QLineEdit(page)
        self._edit_capacity.setPlaceholderText("remaining_capacity")
        self._edit_capacity.setText("remaining_capacity")
        self._edit_capacity.setObjectName("editCapacityCol")
        form.addRow("ستون ظرفیت", self._edit_capacity)

        self._btn_allocate = QPushButton("تخصیص")
        self._btn_allocate.setObjectName("btnAllocate")
        self._btn_allocate.clicked.connect(self._start_allocate)
        form.addRow("", self._btn_allocate)

        return page

    def _build_validate_page(self) -> QWidget:
        """صفحهٔ سبک اعتبارسنجی بدون اتصال به زیرساخت."""

        page = QWidget(self)
        page.setObjectName("pageValidate")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(12)

        label = QLabel(
            "برای اجرای کنترل کیفیت، خروجی ماتریس را در ابزار QA جداگانه بررسی کنید."
        )
        label.setWordWrap(True)
        layout.addWidget(label)

        self._btn_run_validate = QPushButton("اجرای کنترل (غیرفعال)")
        self._btn_run_validate.setObjectName("btnRunValidate")
        self._btn_run_validate.setEnabled(False)
        layout.addWidget(self._btn_run_validate)

        layout.addStretch(1)
        return page

    def _build_explain_page(self) -> QWidget:
        """صفحهٔ سبک توضیح گزارش Explain."""

        page = QWidget(self)
        page.setObjectName("pageExplain")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(12)

        label = QLabel(
            "گزارش Explain در زمان اجرای سناریوهای Build/Allocate به‌صورت خودکار"
            " در خروجی‌های تولیدشده ذخیره می‌شود."
        )
        label.setWordWrap(True)
        layout.addWidget(label)
        layout.addStretch(1)

        return page

    def _register_interactive_controls(self) -> None:
        """ثبت کنترل‌هایی که هنگام اجرای تسک غیرفعال می‌شوند."""

        self._interactive = [
            self._btn_build,
            self._btn_allocate,
            self._btn_demo,
            self._picker_inspactor,
            self._picker_schools,
            self._picker_crosswalk,
            self._picker_policy_build,
            self._picker_output_matrix,
            self._picker_students,
            self._picker_pool,
            self._picker_policy_allocate,
            self._picker_alloc_out,
            self._edit_capacity,
        ]

    # ------------------------------------------------------------------ Actions
    def _start_build(self) -> None:
        """اجرای سناریوی ساخت ماتریس با فراخوانی CLI."""

        if self._worker is not None and self._worker.isRunning():
            QMessageBox.warning(self, "تسک در حال اجرا", "لطفاً تا پایان عملیات جاری صبر کنید.")
            return

        required = [
            (self._picker_inspactor, "گزارش Inspactor"),
            (self._picker_schools, "گزارش مدارس"),
            (self._picker_crosswalk, "Crosswalk"),
            (self._picker_output_matrix, "خروجی ماتریس"),
        ]
        if not self._ensure_filled(required):
            return

        policy_path = self._picker_policy_build.text() or self._default_policy_path or "config/policy.json"
        argv = [
            "build-matrix",
            "--inspactor",
            self._picker_inspactor.text(),
            "--schools",
            self._picker_schools.text(),
            "--crosswalk",
            self._picker_crosswalk.text(),
            "--output",
            self._picker_output_matrix.text(),
            "--policy",
            policy_path,
        ]
        self._launch_cli(argv, "ساخت ماتریس")

    def _start_allocate(self) -> None:
        """اجرای سناریوی تخصیص با فراخوانی CLI."""

        if self._worker is not None and self._worker.isRunning():
            QMessageBox.warning(self, "تسک در حال اجرا", "لطفاً تا پایان عملیات جاری صبر کنید.")
            return

        required = [
            (self._picker_students, "فایل دانش‌آموزان"),
            (self._picker_pool, "استخر منتورها"),
            (self._picker_alloc_out, "خروجی تخصیص"),
        ]
        if not self._ensure_filled(required):
            return

        capacity = self._edit_capacity.text().strip() or "remaining_capacity"
        self._edit_capacity.setText(capacity)
        policy_path = self._picker_policy_allocate.text() or self._default_policy_path or "config/policy.json"

        argv = [
            "allocate",
            "--students",
            self._picker_students.text(),
            "--pool",
            self._picker_pool.text(),
            "--output",
            self._picker_alloc_out.text(),
            "--capacity-column",
            capacity,
            "--policy",
            policy_path,
        ]
        self._launch_cli(argv, "تخصیص")

    def _start_demo_task(self) -> None:
        """اجرای پیش‌نمایش پیشرفت برای تست UI."""

        if self._worker is not None and self._worker.isRunning():
            QMessageBox.information(self, "مشغول", "یک عملیات دیگر در حال اجراست.")
            return

        def _demo_task(*, progress: ProgressFn) -> None:
            for pct, msg in ((0, "آغاز"), (30, "در حال پردازش"), (60, "گام پایانی"), (100, "کامل")):
                progress(pct, msg)

        self._launch_worker(_demo_task, "دموی پیشرفت")

    def _launch_cli(self, argv: Sequence[str], action: str) -> None:
        """اجرای فرمان CLI با Worker و رعایت قرارداد progress."""

        def _task(*, progress: ProgressFn) -> None:
            exit_code = cli.main(argv, progress_factory=lambda: progress)
            if exit_code != 0:
                raise RuntimeError(f"کد خروج غیرصفر: {exit_code}")

        self._launch_worker(_task, action)

    def _launch_worker(self, func: Callable[..., None], action: str) -> None:
        """اجرای تابع در Worker با آماده‌سازی UI."""

        self._progress.setValue(0)
        self._status.setText(f"{action} در حال اجرا…")
        self._log.append(f"<b>▶️ شروع {action}</b>")
        self._disable_controls(True)

        worker = Worker(func)
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_finished)
        self._worker = worker
        worker.start()

    # ----------------------------------------------------------------- Helpers
    def _disable_controls(self, disabled: bool) -> None:
        """فعال/غیرفعال کردن کنترل‌های تعاملی."""

        for widget in self._interactive:
            widget.setEnabled(not disabled)

    def _ensure_filled(self, fields: Iterable[Tuple[FilePicker | QLineEdit, str]]) -> bool:
        """بررسی پر بودن فیلدهای ضروری و نمایش هشدار در صورت نقص."""

        missing = [label for widget, label in fields if not widget.text().strip()]
        if missing:
            details = "\n".join(f"- {label}" for label in missing)
            QMessageBox.warning(self, "ورودی ناقص", f"لطفاً فیلدهای زیر را تکمیل کنید:\n{details}")
            return False
        return True

    # ----------------------------------------------------------------- Signals
    @Slot(int, str)
    def _on_progress(self, pct: int, message: str) -> None:
        """به‌روزرسانی نوار پیشرفت و ثبت لاگ."""

        self._progress.setValue(max(0, min(100, int(pct))))
        self._status.setText(message or "در حال پردازش")
        safe_msg = message or "(بدون پیام)"
        self._log.append(f"{pct}% | {safe_msg}")

    @Slot(bool, object)
    def _on_finished(self, success: bool, error: object | None) -> None:
        """پایان عملیات را مدیریت کرده و پیام مناسب را نمایش می‌دهد."""

        self._disable_controls(False)
        self._worker = None

        if error is not None:
            msg = str(error)
            if isinstance(error, FileNotFoundError):
                color = "#c00"
                QMessageBox.critical(self, "فایل یافت نشد", msg)
            elif isinstance(error, PermissionError):
                color = "#c00"
                QMessageBox.critical(self, "عدم دسترسی", msg)
            elif isinstance(error, ValueError):
                color = "#b58900"
                QMessageBox.warning(self, "داده نامعتبر", msg)
            else:
                color = "#c00"
                QMessageBox.critical(self, "خطای ناشناخته", msg)
            self._status.setText("خطا")
            self._log.append(f'<span style="color:{color}">❌ {msg}</span>')
            return

        if not success:
            self._status.setText("لغو شد")
            self._log.append("⚠️ عملیات متوقف شد")
            return

        self._progress.setValue(100)
        self._status.setText("کامل")
        self._log.append('<span style="color:#2e7d32">✅ عملیات با موفقیت پایان یافت</span>')

    # -------------------------------------------------------------- Qt events
    def closeEvent(self, event: QCloseEvent) -> None:
        """در صورت اجرای تسک فعال، تلاش برای لغو امن و سپس بستن."""

        if self._worker is not None and self._worker.isRunning():
            self._worker.request_cancel()
            self._worker.wait(3000)
        super().closeEvent(event)


def run_demo() -> None:  # pragma: no cover - اجرای دستی UI
    """اجرای سادهٔ پنجره برای تست دستی."""

    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":  # pragma: no cover
    run_demo()
