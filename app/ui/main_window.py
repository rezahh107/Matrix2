"""
رابط کاربری اصلی برنامه تخصیص دانشجو-منتور
ویژگی‌ها: اجرای async، اعتبارسنجی، ماندگاری تنظیمات، لاگ زمان‌دار
"""

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QFileDialog,
    QTextEdit, QProgressBar, QCheckBox, QDoubleSpinBox,
    QMessageBox, QTabWidget, QApplication
)
from PySide6.QtCore import Qt, QThread, QTime, QByteArray
from PySide6.QtGui import QFont
from pathlib import Path
from typing import Optional
import pandas as pd

# این importها را باید از ماژول‌های قبلی انجام دهید
from app.utils.task_runner import TaskRunner
from app.utils.validator import (
    validate_excel_file,
    validate_output_directory,
    validate_numeric_range,
    validate_build_matrix_inputs,
    ValidationError,
)
from app.utils.settings_manager import AppPreferences


__version__ = "1.0.0"


class MainWindow(QMainWindow):
    """
    پنجره اصلی برنامه با قابلیت‌های:
    - اجرای async بدون فریز UI
    - اعتبارسنجی هوشمند ورودی‌ها
    - ماندگاری تنظیمات
    - لغو عملیات
    - لاگ سطح‌بندی‌شده
    """
    
    def __init__(self):
        super().__init__()
        
        # مدیریت تنظیمات
        self.preferences = AppPreferences()
        
        # مدیریت Thread
        self.current_thread: Optional[QThread] = None
        self.current_runner = None  # TaskRunner
        
        self._setup_ui()
        self._setup_connections()
        self._load_saved_settings()
        
    def _setup_ui(self):
        """ساخت رابط کاربری"""
        self.setWindowTitle(f"سامانه تخصیص دانشجو-منتور - نسخه {__version__}")
        self.setMinimumSize(900, 750)
        
        # RTL و فونت
        self.setLayoutDirection(Qt.RightToLeft)
        self.setFont(QFont("Segoe UI", 10))
        
        # ویجت مرکزی
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        
        # تب‌ها
        self.tabs = QTabWidget()
        self._create_matrix_tab()
        self._create_allocation_tab()
        main_layout.addWidget(self.tabs)
        
        # بخش پیشرفت و لاگ
        self._create_progress_section(main_layout)
        self._create_log_section(main_layout)
        
    def _create_matrix_tab(self):
        """تب ساخت ماتریس اهلیت"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("ساخت ماتریس اهلیت")
        form = QFormLayout(group)
        
        # ورودی‌ها
        self.inspector_edit = QLineEdit()
        self.inspector_edit.setPlaceholderText("مسیر فایل گزارش inspectors...")
        inspector_layout = QHBoxLayout()
        inspector_layout.addWidget(self.inspector_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.inspector_edit, "گزارش inspectors"))
        btn.setMaximumWidth(100)
        inspector_layout.addWidget(btn)
        form.addRow("گزارش Inspectors:", inspector_layout)
        
        self.school_edit = QLineEdit()
        self.school_edit.setPlaceholderText("مسیر فایل گزارش schools...")
        school_layout = QHBoxLayout()
        school_layout.addWidget(self.school_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.school_edit, "گزارش schools"))
        btn.setMaximumWidth(100)
        school_layout.addWidget(btn)
        form.addRow("گزارش Schools:", school_layout)
        
        self.crosswalk_edit = QLineEdit()
        self.crosswalk_edit.setPlaceholderText("مسیر فایل crosswalk...")
        crosswalk_layout = QHBoxLayout()
        crosswalk_layout.addWidget(self.crosswalk_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.crosswalk_edit, "فایل crosswalk"))
        btn.setMaximumWidth(100)
        crosswalk_layout.addWidget(btn)
        form.addRow("فایل Crosswalk:", crosswalk_layout)
        
        # تنظیمات
        self.capacity_gate_check = QCheckBox("فعال‌سازی Capacity Gate")
        self.capacity_gate_check.setChecked(True)
        self.capacity_gate_check.setToolTip("بررسی ظرفیت در زمان ساخت ماتریس")
        form.addRow("", self.capacity_gate_check)
        
        # دکمه اجرا
        self.build_matrix_btn = QPushButton("🔨 ساخت ماتریس اهلیت")
        self.build_matrix_btn.setMinimumHeight(40)
        form.addRow("", self.build_matrix_btn)
        
        layout.addWidget(group)
        layout.addStretch()
        
        self.tabs.addTab(widget, "ساخت ماتریس")
        
    def _create_allocation_tab(self):
        """تب تخصیص دانشجویان"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("تخصیص دانشجویان به منتورها")
        form = QFormLayout(group)
        
        # ورودی‌ها
        self.matrix_edit = QLineEdit()
        self.matrix_edit.setPlaceholderText("مسیر ماتریس اهلیت...")
        matrix_layout = QHBoxLayout()
        matrix_layout.addWidget(self.matrix_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.matrix_edit, "ماتریس اهلیت"))
        btn.setMaximumWidth(100)
        matrix_layout.addWidget(btn)
        form.addRow("ماتریس اهلیت:", matrix_layout)
        
        self.students_edit = QLineEdit()
        self.students_edit.setPlaceholderText("مسیر لیست دانشجویان...")
        students_layout = QHBoxLayout()
        students_layout.addWidget(self.students_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.students_edit, "لیست دانشجویان"))
        btn.setMaximumWidth(100)
        students_layout.addWidget(btn)
        form.addRow("لیست دانشجویان:", students_layout)
        
        self.capacity_edit = QLineEdit()
        self.capacity_edit.setPlaceholderText("مسیر ظرفیت منتورها...")
        capacity_layout = QHBoxLayout()
        capacity_layout.addWidget(self.capacity_edit)
        btn = QPushButton("انتخاب")
        btn.clicked.connect(lambda: self._select_file(self.capacity_edit, "ظرفیت منتورها"))
        btn.setMaximumWidth(100)
        capacity_layout.addWidget(btn)
        form.addRow("ظرفیت منتورها:", capacity_layout)
        
        # پوشه خروجی
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("پوشه ذخیره نتایج...")
        output_layout = QHBoxLayout()
        output_layout.addWidget(self.output_dir_edit)
        btn = QPushButton("انتخاب پوشه")
        btn.clicked.connect(self._select_output_directory)
        btn.setMaximumWidth(100)
        output_layout.addWidget(btn)
        form.addRow("پوشه خروجی:", output_layout)
        
        # تنظیمات تخصیص
        settings_group = QGroupBox("تنظیمات تخصیص")
        settings_form = QFormLayout(settings_group)
        
        self.occupancy_spin = QDoubleSpinBox()
        self.occupancy_spin.setRange(0.50, 1.00)
        self.occupancy_spin.setSingleStep(0.01)
        self.occupancy_spin.setValue(0.95)
        self.occupancy_spin.setToolTip("حداکثر درصد اشغال ظرفیت منتورها (0.95 = 95%)")
        settings_form.addRow("حداکثر اشغال:", self.occupancy_spin)
        
        self.priority_new_check = QCheckBox("اولویت منتورهای جدید")
        self.priority_new_check.setChecked(True)
        self.priority_new_check.setToolTip("منتورهای بدون دانشجو اولویت بالاتر دارند")
        settings_form.addRow("", self.priority_new_check)
        
        self.priority_capacity_check = QCheckBox("اولویت ظرفیت بالا")
        self.priority_capacity_check.setChecked(True)
        self.priority_capacity_check.setToolTip("منتورهای با ظرفیت بیشتر اولویت دارند")
        settings_form.addRow("", self.priority_capacity_check)
        
        form.addRow(settings_group)
        
        # دکمه اجرا
        self.allocate_btn = QPushButton("🎯 اجرای تخصیص")
        self.allocate_btn.setMinimumHeight(40)
        form.addRow("", self.allocate_btn)
        
        layout.addWidget(group)
        layout.addStretch()
        
        self.tabs.addTab(widget, "تخصیص")
        
    def _create_progress_section(self, parent_layout):
        """بخش نمایش پیشرفت"""
        progress_layout = QHBoxLayout()
        
        progress_layout.addWidget(QLabel("پیشرفت:"))
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        progress_layout.addWidget(self.progress_bar, 1)
        
        self.cancel_btn = QPushButton("⏹️ لغو")
        self.cancel_btn.setVisible(False)
        self.cancel_btn.setMaximumWidth(100)
        progress_layout.addWidget(self.cancel_btn)
        
        parent_layout.addLayout(progress_layout)
        
    def _create_log_section(self, parent_layout):
        """بخش لاگ"""
        log_group = QGroupBox("گزارش عملیات")
        log_layout = QVBoxLayout(log_group)
        
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setMaximumHeight(200)
        self.log_area.setPlaceholderText(
            "خروجی و گزارش عملیات اینجا نمایش داده می‌شود...\n"
            "ℹ️ اطلاعات | ✅ موفقیت | ⚠️ هشدار | ❌ خطا"
        )
        log_layout.addWidget(self.log_area)
        
        parent_layout.addWidget(log_group)
        
    def _setup_connections(self):
        """اتصال سیگنال‌ها"""
        self.build_matrix_btn.clicked.connect(self._on_build_matrix_clicked)
        self.allocate_btn.clicked.connect(self._on_allocate_clicked)
        self.cancel_btn.clicked.connect(self._on_cancel_clicked)
        
    def _load_saved_settings(self):
        """بارگذاری تنظیمات ذخیره‌شده"""
        if not self.preferences:
            return
            
        # بارگذاری مقادیر
        self.output_dir_edit.setText(self.preferences.last_output_dir)
        self.matrix_edit.setText(self.preferences.last_matrix_path)
        self.occupancy_spin.setValue(self.preferences.max_occupancy)
        self.priority_new_check.setChecked(self.preferences.priority_new_mentors)
        self.priority_capacity_check.setChecked(self.preferences.priority_high_capacity)
        self.capacity_gate_check.setChecked(self.preferences.enable_capacity_gate)
        
        # بارگذاری geometry پنجره
        geometry = self.preferences.window_geometry
        if geometry:
            self.restoreGeometry(geometry)
            
        self._log("✅ تنظیمات قبلی بارگذاری شد", "success")
        
    def _save_settings(self):
        """ذخیره تنظیمات فعلی"""
        if not self.preferences:
            return
            
        self.preferences.last_output_dir = self.output_dir_edit.text()
        self.preferences.last_matrix_path = self.matrix_edit.text()
        self.preferences.max_occupancy = self.occupancy_spin.value()
        self.preferences.priority_new_mentors = self.priority_new_check.isChecked()
        self.preferences.priority_high_capacity = self.priority_capacity_check.isChecked()
        self.preferences.enable_capacity_gate = self.capacity_gate_check.isChecked()
        self.preferences.window_geometry = self.saveGeometry()
        
    def _select_file(self, line_edit: QLineEdit, title: str):
        """انتخاب فایل اکسل"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            f"انتخاب {title}",
            "",
            "Excel Files (*.xlsx);;All Files (*)"
        )
        if file_path:
            line_edit.setText(file_path)
            
    def _select_output_directory(self):
        """انتخاب پوشه خروجی"""
        dir_path = QFileDialog.getExistingDirectory(
            self,
            "انتخاب پوشه خروجی"
        )
        if dir_path:
            self.output_dir_edit.setText(dir_path)
            
    def _on_build_matrix_clicked(self):
        """مدیریت کلیک دکمه ساخت ماتریس"""
        try:
            valid = validate_build_matrix_inputs(
                self.inspector_edit.text(),
                self.school_edit.text(),
                self.crosswalk_edit.text(),
            )
            use_gate = self.capacity_gate_check.isChecked()
            self._run_async_task(
                self._execute_build_matrix,
                valid['inspector'], valid['school'], valid['crosswalk'], use_gate
            )
        except ValidationError as e:
            self._show_error(str(e))
        except Exception as e:
            self._show_error(str(e))
            
    def _on_allocate_clicked(self):
        """مدیریت کلیک دکمه تخصیص"""
        try:
            matrix = validate_excel_file(self.matrix_edit.text(), "ماتریس اهلیت")
            students = validate_excel_file(self.students_edit.text(), "لیست دانشجویان")
            capacity = validate_excel_file(self.capacity_edit.text(), "ظرفیت منتورها")
            output_dir = validate_output_directory(self.output_dir_edit.text())
            validate_numeric_range(self.occupancy_spin.value(), 0.50, 1.00, "حداکثر اشغال")
            rules = {
                'max_occupancy_threshold': self.occupancy_spin.value(),
                'priority_new_mentors': self.priority_new_check.isChecked(),
                'priority_high_capacity': self.priority_capacity_check.isChecked()
            }
            self._run_async_task(
                self._execute_allocation,
                matrix, students, capacity, output_dir, rules
            )
        except ValidationError as e:
            self._show_error(str(e))
        except Exception as e:
            self._show_error(str(e))
            
    def _run_async_task(self, task_func, *args):
        """
        اجرای یک تسک در thread جداگانه با TaskRunner
        """
        if self.current_thread and self.current_thread.isRunning():
            self._show_warning("یک عملیات در حال اجراست.")
            return
        self._set_ui_processing(True)
        self.cancel_btn.setEnabled(True)
        # Thread + Runner
        self.current_thread = QThread()
        self.current_runner = TaskRunner(task_func, *args)
        self.current_runner.moveToThread(self.current_thread)
        self.current_runner.progress.connect(self._on_progress_update)
        self.current_runner.finished.connect(self._on_task_finished)
        self.current_runner.finished.connect(self.current_thread.quit)
        self.current_thread.started.connect(self.current_runner.run)
        self.current_thread.start()
        self._log("⏳ عملیات شروع شد...", "info")
        
    def _on_progress_update(self, percent: int, message: str):
        """به‌روزرسانی نوار پیشرفت"""
        self.progress_bar.setValue(percent)
        self._log(message, "info")
        
    def _on_task_finished(self, result):
        """پردازش نتیجه نهایی تسک"""
        self._set_ui_processing(False)
        # TaskResult dataclass or plain object/dict
        success = False
        error_msg = None
        data = None
        try:
            # PySide may deliver as Python object
            if hasattr(result, 'success'):
                success = bool(result.success)
                error_msg = getattr(result, 'error', None)
                data = getattr(result, 'data', None)
            elif isinstance(result, dict):
                success = result.get('success', False)
                error_msg = result.get('error')
                data = result.get('data')
        except Exception:
            error_msg = 'خطای ناشناخته'
        if success:
            self._log("✅ عملیات با موفقیت انجام شد", "success")
            self._save_settings()
        else:
            self._log(f"❌ خطا: {error_msg or 'نامشخص'}", "error")
            
    def _on_cancel_clicked(self):
        """لغو عملیات جاری"""
        if self.current_runner:
            self.current_runner.cancel()
            self._log("⏹️ درخواست لغو ارسال شد...", "warning")
            
    def _set_ui_processing(self, is_processing: bool):
        """فعال/غیرفعال کردن UI در حین پردازش"""
        self.build_matrix_btn.setEnabled(not is_processing)
        self.allocate_btn.setEnabled(not is_processing)
        self.progress_bar.setVisible(is_processing)
        self.cancel_btn.setVisible(is_processing)
        
        if is_processing:
            self.progress_bar.setValue(0)
        else:
            self.cancel_btn.setEnabled(False)
            
    def _log(self, message: str, level: str = "info"):
        """
        افزودن پیام به لاگ با زمان و آیکون
        
        Args:
            message: متن پیام
            level: سطح (info, success, warning, error, debug)
        """
        icons = {
            'info': 'ℹ️',
            'success': '✅',
            'warning': '⚠️',
            'error': '❌',
            'debug': '🐛'
        }
        
        timestamp = QTime.currentTime().toString("HH:mm:ss")
        icon = icons.get(level, '📝')
        
        formatted = f"[{timestamp}] {icon} {message}"
        self.log_area.append(formatted)
        
        # اسکرول به آخر
        scrollbar = self.log_area.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
    def _show_error(self, message: str):
        """نمایش پیام خطا"""
        QMessageBox.critical(self, "خطا", message)
        self._log(f"❌ {message}", "error")
        
    def _show_warning(self, message: str):
        """نمایش هشدار"""
        QMessageBox.warning(self, "هشدار", message)
        self._log(f"⚠️ {message}", "warning")
        
    def closeEvent(self, event):
        """ذخیره تنظیمات هنگام بستن"""
        self._save_settings()
        event.accept()
        
    # ===== توابع اجرایی (باید با منطق واقعی جایگزین شوند) =====
    
    def _execute_build_matrix(self, progress_signal, check_cancel, 
                              inspector, school, crosswalk, use_gate):
        """اجرای ساخت ماتریس با هستهٔ واقعی"""
        from pathlib import Path
        from app.core import build_matrix as bm
        import pandas as pd
        outdir = self.output_dir_edit.text().strip()
        outdir_path = Path(outdir) if outdir else Path(inspector).parent
        outdir_path.mkdir(parents=True, exist_ok=True)
        progress_signal.emit(10, "در حال خواندن فایل‌ها...")
        check_cancel()
        cfg = bm.BuildConfig(enable_capacity_gate=bool(use_gate))
        # اجرای اصلی
        progress_signal.emit(50, "در حال ساخت ماتریس...")
        check_cancel()
        matrix, validation, removed, unmatched_schools, unseen_groups, invalid_mentors, meta = bm.build_matrix(
            Path(inspector), Path(school), Path(crosswalk), cfg
        )
        # ذخیره خروجی‌ها
        progress_signal.emit(85, "در حال ذخیره فایل‌ها...")
        check_cancel()
        out_csv = outdir_path / "eligibility_matrix.csv"
        out_xlsx = outdir_path / "eligibility_matrix.xlsx"
        matrix.to_csv(out_csv, index=False)
        sheets = {"matrix": matrix, "validation": validation}
        if not removed.empty:
            sheets["removed_mentors"] = removed
        if not unmatched_schools.empty:
            sheets["unmatched_schools"] = unmatched_schools
        if not unseen_groups.empty:
            sheets["unseen_groups"] = unseen_groups
        if not invalid_mentors.empty:
            sheets["invalid_mentors"] = invalid_mentors
        # استفاده از نویسندهٔ امن در build_matrix
        bm.write_xlsx_atomic(out_xlsx, sheets)
        progress_signal.emit(100, "اتمام")
        return {
            "type": "matrix",
            "output_csv": str(out_csv),
            "output_xlsx": str(out_xlsx),
            "rows": int(len(matrix)),
        }

    def _execute_allocation(self, progress_signal, check_cancel,
                           matrix, students, capacity, output_dir, rules):
        """اجرای تخصیص با هستهٔ واقعی"""
        from pathlib import Path
        from app.core import allocate_students as alloc
        outdir = Path(output_dir)
        outdir.mkdir(parents=True, exist_ok=True)
        out_import = outdir / "import_to_sabt.xlsx"
        out_log = outdir / "allocation_log.xlsx"
        progress_signal.emit(10, "در حال آماده‌سازی...")
        check_cancel()
        progress_signal.emit(30, "در حال تخصیص...")
        check_cancel()
        result = alloc.allocate_students_optimized(
            str(matrix), str(students), str(capacity), str(out_import), str(out_log), rules or {}
        )
        progress_signal.emit(85, "در حال ذخیره نتایج...")
        check_cancel()
        progress_signal.emit(100, "اتمام")
        return {"type": "allocation", **result}


# ===== تست سریع =====
if __name__ == "__main__":
    import sys
    
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
