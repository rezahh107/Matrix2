"""پنجرهٔ اصلی PySide6 برای سناریوهای سامانه تخصیص دانشجو-منتور."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, List, Sequence, Tuple

import pandas as pd
from PySide6.QtCore import QByteArray, QSettings, Qt, Slot
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFormLayout,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.core.common.columns import HeaderMode, canonicalize_headers
from app.core.counter import (
    detect_academic_year_from_counters,
    find_max_sequence_by_prefix,
    infer_year_strict,
    pick_counter_sheet_name,
    year_to_yy,
)
from app.core.policy_loader import get_policy
from app.infra import cli

from .task_runner import ProgressFn, Worker
from .widgets import FilePicker

__all__ = ["MainWindow", "run_demo", "FilePicker"]


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

        self._splitter = QSplitter(Qt.Vertical, self)
        self._splitter.setChildrenCollapsible(False)

        top_pane = QWidget(self._splitter)
        top_layout = QVBoxLayout(top_pane)
        top_layout.setContentsMargins(16, 16, 16, 8)
        top_layout.setSpacing(16)

        dashboard = self._build_dashboard()
        top_layout.addWidget(dashboard)

        self._tabs = QTabWidget(self)
        self._tabs.setDocumentMode(True)
        self._tabs.setTabPosition(QTabWidget.North)
        self._tabs.addTab(self._build_build_page(), "ساخت ماتریس")
        self._tabs.addTab(self._build_allocate_page(), "تخصیص")
        self._tabs.addTab(self._build_rule_engine_page(), "موتور قواعد")
        self._tabs.addTab(self._build_validate_page(), "اعتبارسنجی")
        self._tabs.addTab(self._build_explain_page(), "توضیحات")
        top_layout.addWidget(self._tabs)

        bottom_pane = QWidget(self._splitter)
        bottom_layout = QVBoxLayout(bottom_pane)
        bottom_layout.setContentsMargins(16, 0, 16, 16)
        bottom_layout.setSpacing(12)

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
        bottom_layout.addLayout(status_layout)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setObjectName("textLog")
        bottom_layout.addWidget(self._log, 1)

        controls_layout = QHBoxLayout()
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(12)
        controls_layout.addStretch(1)
        self._btn_demo = QPushButton("اجرای تست (دمو Progress)")
        self._btn_demo.setObjectName("btnDemo")
        self._btn_demo.clicked.connect(self._start_demo_task)
        controls_layout.addWidget(self._btn_demo)
        bottom_layout.addLayout(controls_layout)

        self._splitter.addWidget(top_pane)
        self._splitter.addWidget(bottom_pane)
        self._splitter.setStretchFactor(0, 3)
        self._splitter.setStretchFactor(1, 1)

        self.setCentralWidget(self._splitter)

        settings = QSettings()
        state = settings.value("ui/main_splitter")
        if isinstance(state, QByteArray):
            self._splitter.restoreState(state)

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

        btn_rule = QPushButton("اجرای موتور قواعد")
        btn_rule.clicked.connect(self._start_rule_engine)
        shortcuts.addWidget(btn_rule)

        shortcuts.addStretch(1)
        layout.addLayout(shortcuts, 0)

        return frame

    def _build_build_page(self) -> QWidget:
        """فرم ورودی‌های سناریوی ساخت ماتریس."""

        page = QWidget(self)
        outer = QVBoxLayout(page)
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)

        inputs_group = QGroupBox("ورودی‌ها", page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_inspactor = FilePicker(page, placeholder="فایل Inspactor")
        self._picker_inspactor.setObjectName("editInspactor")
        self._picker_inspactor.setToolTip("خروجی گزارش Inspactor که فهرست پشتیبان‌ها را دارد")
        inputs_layout.addRow("گزارش Inspactor", self._picker_inspactor)

        self._picker_schools = FilePicker(page, placeholder="فایل مدارس")
        self._picker_schools.setObjectName("editSchools")
        self._picker_schools.setToolTip("فایل رسمی مدارس برای تطبیق کد و نام مدرسه")
        inputs_layout.addRow("گزارش مدارس", self._picker_schools)

        self._picker_crosswalk = FilePicker(page, placeholder="فایل Crosswalk")
        self._picker_crosswalk.setObjectName("editCrosswalk")
        self._picker_crosswalk.setToolTip("جدول Crosswalk جهت نگاشت رشته‌ها و گروه‌ها")
        inputs_layout.addRow("Crosswalk", self._picker_crosswalk)

        outer.addWidget(inputs_group)

        policy_group = QGroupBox("سیاست", page)
        policy_layout = QFormLayout(policy_group)
        policy_layout.setLabelAlignment(Qt.AlignRight)
        policy_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_policy_build = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_build.setObjectName("editPolicy1")
        if self._default_policy_path:
            self._picker_policy_build.setText(self._default_policy_path)
        policy_layout.addRow("سیاست", self._picker_policy_build)
        outer.addWidget(policy_group)

        output_group = QGroupBox("خروجی", page)
        output_layout = QFormLayout(output_group)
        output_layout.setLabelAlignment(Qt.AlignRight)
        output_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_output_matrix = FilePicker(
            page, save=True, placeholder="فایل خروجی ماتریس (*.xlsx)"
        )
        self._picker_output_matrix.setObjectName("editMatrixOut")
        self._picker_output_matrix.setToolTip("مسیر ذخیرهٔ فایل خروجی ماتریس اهلیت")
        output_layout.addRow("خروجی ماتریس", self._picker_output_matrix)
        outer.addWidget(output_group)

        self._btn_build = QPushButton("ساخت ماتریس")
        self._btn_build.setObjectName("btnBuildMatrix")
        self._btn_build.clicked.connect(self._start_build)
        action_layout = QHBoxLayout()
        action_layout.addStretch(1)
        action_layout.addWidget(self._btn_build)
        outer.addLayout(action_layout)
        outer.addStretch(1)

        return page

    def _build_allocate_page(self) -> QWidget:
        """فرم ورودی‌های سناریوی تخصیص."""

        page = QWidget(self)
        outer = QVBoxLayout(page)
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)

        inputs_group = QGroupBox("ورودی‌های تخصیص", page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_students = FilePicker(
            page, placeholder="دانش‌آموزان (*.xlsx یا *.csv)"
        )
        self._picker_students.setObjectName("editStudents")
        self._picker_students.setToolTip("لیست دانش‌آموزانی که باید به پشتیبان متصل شوند")
        inputs_layout.addRow("فایل دانش‌آموزان", self._picker_students)

        self._picker_pool = FilePicker(page, placeholder="استخر منتورها (*.xlsx)")
        self._picker_pool.setObjectName("editPool")
        self._picker_pool.setToolTip("فهرست منتورها یا پشتیبان‌ها برای تخصیص")
        inputs_layout.addRow("استخر منتورها", self._picker_pool)

        outer.addWidget(inputs_group)

        advanced_group = QGroupBox("تنظیمات پیشرفته", page)
        advanced_layout = QFormLayout(advanced_group)
        advanced_layout.setLabelAlignment(Qt.AlignRight)
        advanced_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_policy_allocate = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_allocate.setObjectName("editPolicy2")
        if self._default_policy_path:
            self._picker_policy_allocate.setText(self._default_policy_path)
        advanced_layout.addRow("سیاست", self._picker_policy_allocate)

        self._picker_alloc_out = FilePicker(
            page, save=True, placeholder="فایل خروجی تخصیص (*.xlsx)"
        )
        self._picker_alloc_out.setObjectName("editAllocOut")
        self._picker_alloc_out.setToolTip("مسیر ذخیرهٔ نتیجه نهایی تخصیص دانش‌آموز-منتور")

        self._edit_capacity = QLineEdit(page)
        self._edit_capacity.setPlaceholderText("remaining_capacity")
        self._edit_capacity.setText("remaining_capacity")
        self._edit_capacity.setObjectName("editCapacityCol")
        advanced_layout.addRow("ستون ظرفیت", self._edit_capacity)

        outer.addWidget(advanced_group)

        register_box = QGroupBox("شناسهٔ ثبت‌نام", page)
        register_box.setObjectName("registrationGroupBox")
        register_layout = QFormLayout(register_box)
        register_layout.setLabelAlignment(Qt.AlignRight)
        register_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._combo_academic_year = QComboBox(register_box)
        self._combo_academic_year.setEditable(True)
        self._combo_academic_year.setInsertPolicy(QComboBox.NoInsert)
        self._combo_academic_year.setObjectName("academicYearInput")
        self._combo_academic_year.lineEdit().setPlaceholderText("مثلاً 1404")
        self._combo_academic_year.setToolTip(
            "سال تحصیلی شروع شمارنده‌ها را تعیین کنید تا کدها درست ادامه یابند"
        )
        for year in range(1395, 1411):
            self._combo_academic_year.addItem(str(year))
        register_layout.addRow("سال تحصیلی", self._combo_academic_year)

        self._picker_prior_roster = FilePicker(
            register_box,
            placeholder="روستر سال قبل (اختیاری)",
        )
        self._picker_prior_roster.setObjectName("priorRosterPicker")
        self._picker_prior_roster.setToolTip("برای بازیابی شمارندهٔ سال قبل در صورت وجود")
        register_layout.addRow("روستر سال قبل", self._picker_prior_roster)

        self._picker_current_roster = FilePicker(
            register_box,
            placeholder="روستر سال جاری / شمارنده‌ها",
        )
        self._picker_current_roster.setObjectName("currentRosterPicker")
        self._picker_current_roster.setToolTip("برای کشف آخرین شمارنده‌های سال جاری")
        register_layout.addRow("روستر سال جاری", self._picker_current_roster)

        self._btn_autodetect = QPushButton("پیشنهاد خودکار", register_box)
        self._btn_autodetect.setObjectName("autodetectCountersBtn")
        self._btn_autodetect.clicked.connect(self._autodetect_counters)
        register_layout.addRow("", self._btn_autodetect)

        outer.addWidget(register_box)

        output_group = QGroupBox("خروجی", page)
        output_layout = QFormLayout(output_group)
        output_layout.setLabelAlignment(Qt.AlignRight)
        output_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)
        output_layout.addRow("خروجی تخصیص", self._picker_alloc_out)
        outer.addWidget(output_group)

        sabt_group = QGroupBox("خروجی Sabt (ImportToSabt)", page)
        sabt_layout = QFormLayout(sabt_group)
        sabt_layout.setLabelAlignment(Qt.AlignRight)
        sabt_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_sabt_output_alloc = FilePicker(
            page,
            save=True,
            placeholder="خروجی Sabt (*.xlsx)",
            dialog_filter="Excel (*.xlsx *.xlsm *.xls)",
        )
        self._picker_sabt_output_alloc.setObjectName("editSabtOutputAlloc")
        self._picker_sabt_output_alloc.setToolTip(
            "فایل ImportToSabt برای ارسال به سامانه ثبت"
        )
        sabt_layout.addRow("فایل خروجی", self._picker_sabt_output_alloc)

        self._picker_sabt_config_alloc = FilePicker(
            page,
            placeholder="SmartAlloc_Exporter_Config_v1.json",
            dialog_filter="JSON (*.json)",
        )
        self._picker_sabt_config_alloc.setObjectName("editSabtConfigAlloc")
        self._picker_sabt_config_alloc.setToolTip(
            "فایل تنظیمات SmartAlloc Exporter"
        )
        sabt_layout.addRow("فایل تنظیمات", self._picker_sabt_config_alloc)

        self._picker_sabt_template_alloc = FilePicker(
            page,
            placeholder="قالب اختیاری ImportToSabt",
            dialog_filter="Excel Template (*.xlsx *.xlsm *.xls)",
        )
        self._picker_sabt_template_alloc.setObjectName("editSabtTemplateAlloc")
        self._picker_sabt_template_alloc.setToolTip(
            "قالب اختیاری؛ در صورت خالی ساخت خودکار انجام می‌شود"
        )
        sabt_layout.addRow("فایل قالب", self._picker_sabt_template_alloc)

        sabt_hint = QLabel(
            "اگر فایل قالب خالی بماند، ensure_template_workbook براساس"
            " SmartAlloc_Exporter_Config_v1.json یک Workbook حداقلی می‌سازد و"
            " نیازی به فایل باینری جدا نیست."
        )
        sabt_hint.setWordWrap(True)
        sabt_layout.addRow("", sabt_hint)

        outer.addWidget(sabt_group)

        self._btn_allocate = QPushButton("تخصیص")
        self._btn_allocate.setObjectName("btnAllocate")
        self._btn_allocate.clicked.connect(self._start_allocate)
        action_layout = QHBoxLayout()
        action_layout.addStretch(1)
        action_layout.addWidget(self._btn_allocate)
        outer.addLayout(action_layout)
        outer.addStretch(1)

        return page

    def _build_rule_engine_page(self) -> QWidget:
        """فرم اجرای موتور قواعد بر پایه ماتریس موجود."""

        page = QWidget(self)
        outer = QVBoxLayout(page)
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)

        inputs_group = QGroupBox("ورودی‌ها", page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_rule_matrix = FilePicker(
            page, placeholder="ماتریس اهلیت (*.xlsx)"
        )
        self._picker_rule_matrix.setObjectName("editRuleMatrix")
        self._picker_rule_matrix.setToolTip("فایل ماتریس اهلیت ساخته‌شده را انتخاب کنید")
        inputs_layout.addRow("فایل ماتریس", self._picker_rule_matrix)

        self._picker_rule_students = FilePicker(
            page, placeholder="دانش‌آموزان (*.xlsx یا *.csv)"
        )
        self._picker_rule_students.setObjectName("editRuleStudents")
        self._picker_rule_students.setToolTip("لیست دانش‌آموزان برای ارزیابی مجدد با موتور قواعد")
        inputs_layout.addRow("فایل دانش‌آموزان", self._picker_rule_students)

        outer.addWidget(inputs_group)

        register_box = QGroupBox("شناسهٔ ثبت‌نام", page)
        register_box.setObjectName("ruleRegistrationGroup")
        register_layout = QFormLayout(register_box)
        register_layout.setLabelAlignment(Qt.AlignRight)
        register_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._combo_rule_academic_year = QComboBox(register_box)
        self._combo_rule_academic_year.setEditable(True)
        self._combo_rule_academic_year.setInsertPolicy(QComboBox.NoInsert)
        self._combo_rule_academic_year.setObjectName("ruleAcademicYearInput")
        self._combo_rule_academic_year.lineEdit().setPlaceholderText("مثلاً 1404")
        self._combo_rule_academic_year.setToolTip(
            "سال تحصیلی مرجع شمارنده‌ها برای موتور قواعد را مشخص کنید"
        )
        for year in range(1395, 1411):
            self._combo_rule_academic_year.addItem(str(year))
        register_layout.addRow("سال تحصیلی", self._combo_rule_academic_year)

        self._picker_rule_prior_roster = FilePicker(
            register_box,
            placeholder="روستر سال قبل (اختیاری)",
        )
        self._picker_rule_prior_roster.setObjectName("rulePriorRosterPicker")
        register_layout.addRow("روستر سال قبل", self._picker_rule_prior_roster)

        self._picker_rule_current_roster = FilePicker(
            register_box,
            placeholder="روستر سال جاری / شمارنده‌ها",
        )
        self._picker_rule_current_roster.setObjectName("ruleCurrentRosterPicker")
        register_layout.addRow("روستر سال جاری", self._picker_rule_current_roster)

        self._btn_rule_autodetect = QPushButton("پیشنهاد خودکار", register_box)
        self._btn_rule_autodetect.setObjectName("ruleAutodetectBtn")
        self._btn_rule_autodetect.clicked.connect(self._autodetect_rule_engine_counters)
        register_layout.addRow("", self._btn_rule_autodetect)

        outer.addWidget(register_box)

        advanced_group = QGroupBox("تنظیمات پیشرفته", page)
        advanced_layout = QFormLayout(advanced_group)
        advanced_layout.setLabelAlignment(Qt.AlignRight)
        advanced_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_policy_rule = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_rule.setObjectName("editRulePolicy")
        if self._default_policy_path:
            self._picker_policy_rule.setText(self._default_policy_path)
        advanced_layout.addRow("سیاست", self._picker_policy_rule)

        self._edit_rule_capacity = QLineEdit(page)
        self._edit_rule_capacity.setPlaceholderText("remaining_capacity")
        self._edit_rule_capacity.setText("remaining_capacity")
        self._edit_rule_capacity.setObjectName("editRuleCapacity")
        advanced_layout.addRow("ستون ظرفیت", self._edit_rule_capacity)

        outer.addWidget(advanced_group)

        output_group = QGroupBox("خروجی", page)
        output_layout = QFormLayout(output_group)
        output_layout.setLabelAlignment(Qt.AlignRight)
        output_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)
        self._picker_rule_output = FilePicker(
            page, save=True, placeholder="خروجی تخصیص (*.xlsx)"
        )
        self._picker_rule_output.setObjectName("editRuleOutput")
        self._picker_rule_output.setToolTip("فایل خروجی موتور قواعد برای ذخیره گزارش جدید")
        output_layout.addRow("خروجی", self._picker_rule_output)
        outer.addWidget(output_group)

        sabt_group = QGroupBox("خروجی Sabt (ImportToSabt)", page)
        sabt_layout = QFormLayout(sabt_group)
        sabt_layout.setLabelAlignment(Qt.AlignRight)
        sabt_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_sabt_output_rule = FilePicker(
            page,
            save=True,
            placeholder="خروجی Sabt (*.xlsx)",
            dialog_filter="Excel (*.xlsx *.xlsm *.xls)",
        )
        self._picker_sabt_output_rule.setObjectName("editSabtOutputRule")
        self._picker_sabt_output_rule.setToolTip(
            "فایل ImportToSabt برای خروجی سناریوی موتور قواعد"
        )
        sabt_layout.addRow("فایل خروجی", self._picker_sabt_output_rule)

        self._picker_sabt_config_rule = FilePicker(
            page,
            placeholder="SmartAlloc_Exporter_Config_v1.json",
            dialog_filter="JSON (*.json)",
        )
        self._picker_sabt_config_rule.setObjectName("editSabtConfigRule")
        self._picker_sabt_config_rule.setToolTip(
            "فایل تنظیمات SmartAlloc Exporter برای Rule-Engine"
        )
        sabt_layout.addRow("فایل تنظیمات", self._picker_sabt_config_rule)

        self._picker_sabt_template_rule = FilePicker(
            page,
            placeholder="قالب اختیاری ImportToSabt",
            dialog_filter="Excel Template (*.xlsx *.xlsm *.xls)",
        )
        self._picker_sabt_template_rule.setObjectName("editSabtTemplateRule")
        self._picker_sabt_template_rule.setToolTip(
            "قالب اختیاری؛ در صورت خالی همان ساخت خودکار اعمال می‌شود"
        )
        sabt_layout.addRow("فایل قالب", self._picker_sabt_template_rule)

        sabt_hint = QLabel(
            "خالی‌گذاشتن قالب باعث می‌شود ensure_template_workbook از تنظیمات"
            " JSON یک فایل پایه بسازد و نیازی به قالب جدا نیست."
        )
        sabt_hint.setWordWrap(True)
        sabt_layout.addRow("", sabt_hint)

        outer.addWidget(sabt_group)

        self._btn_rule_engine = QPushButton("اجرای موتور قواعد")
        self._btn_rule_engine.setObjectName("btnRuleEngine")
        self._btn_rule_engine.clicked.connect(self._start_rule_engine)
        action_layout = QHBoxLayout()
        action_layout.addStretch(1)
        action_layout.addWidget(self._btn_rule_engine)
        outer.addLayout(action_layout)
        outer.addStretch(1)

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
            self._btn_rule_engine,
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
            self._picker_sabt_output_alloc,
            self._picker_sabt_config_alloc,
            self._picker_sabt_template_alloc,
            self._edit_capacity,
            self._combo_academic_year,
            self._picker_prior_roster,
            self._picker_current_roster,
            self._btn_autodetect,
            self._picker_rule_matrix,
            self._picker_rule_students,
            self._picker_policy_rule,
            self._picker_rule_output,
            self._picker_sabt_output_rule,
            self._picker_sabt_config_rule,
            self._picker_sabt_template_rule,
            self._edit_rule_capacity,
            self._combo_rule_academic_year,
            self._picker_rule_prior_roster,
            self._picker_rule_current_roster,
            self._btn_rule_autodetect,
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
        policy_path = (
            self._picker_policy_allocate.text()
            or self._default_policy_path
            or "config/policy.json"
        )

        overrides = self._build_allocate_overrides()
        academic_year = overrides.get("academic_year")
        if academic_year is None:
            QMessageBox.warning(
                self,
                "سال تحصیلی نامشخص",
                "لطفاً سال تحصیلی را وارد کنید یا از پیشنهاد خودکار استفاده کنید.",
            )
            return

        prior_path = str(overrides.get("prior_roster") or "").strip()
        current_path = str(overrides.get("current_roster") or "").strip()
        for path, label in ((prior_path, "روستر سال قبل"), (current_path, "روستر سال جاری")):
            if path and not Path(path).exists():
                QMessageBox.warning(self, "فایل یافت نشد", f"{label} قابل دسترسی نیست: {path}")
                return

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
            "--academic-year",
            str(academic_year),
        ]

        if prior_path:
            argv.extend(["--prior-roster", prior_path])
        if current_path:
            argv.extend(["--current-roster", current_path])

        self._launch_cli(argv, "تخصیص", overrides=overrides)

    def _start_rule_engine(self) -> None:
        """اجرای موتور قواعد با استفاده از ماتریس موجود."""

        if self._worker is not None and self._worker.isRunning():
            QMessageBox.warning(self, "تسک در حال اجرا", "لطفاً تا پایان عملیات جاری صبر کنید.")
            return

        required = [
            (self._picker_rule_matrix, "فایل ماتریس"),
            (self._picker_rule_students, "فایل دانش‌آموزان"),
            (self._picker_rule_output, "خروجی"),
        ]
        if not self._ensure_filled(required):
            return

        capacity = self._edit_rule_capacity.text().strip() or "remaining_capacity"
        self._edit_rule_capacity.setText(capacity)
        policy_path = (
            self._picker_policy_rule.text()
            or self._default_policy_path
            or "config/policy.json"
        )

        overrides = self._build_rule_engine_overrides()
        academic_year = overrides.get("academic_year")
        if academic_year is None:
            QMessageBox.warning(
                self,
                "سال تحصیلی نامشخص",
                "لطفاً سال تحصیلی را برای موتور قواعد مشخص کنید.",
            )
            return

        prior_path = str(overrides.get("prior_roster") or "").strip()
        current_path = str(overrides.get("current_roster") or "").strip()
        for path, label in ((prior_path, "روستر سال قبل"), (current_path, "روستر سال جاری")):
            if path and not Path(path).exists():
                QMessageBox.warning(self, "فایل یافت نشد", f"{label} قابل دسترسی نیست: {path}")
                return

        argv = [
            "rule-engine",
            "--matrix",
            self._picker_rule_matrix.text(),
            "--students",
            self._picker_rule_students.text(),
            "--output",
            self._picker_rule_output.text(),
            "--capacity-column",
            capacity,
            "--policy",
            policy_path,
            "--academic-year",
            str(academic_year),
        ]

        if prior_path:
            argv.extend(["--prior-roster", prior_path])
        if current_path:
            argv.extend(["--current-roster", current_path])

        self._launch_cli(argv, "موتور قواعد", overrides=overrides)

    def _build_allocate_overrides(self) -> dict[str, object]:
        """ساخت دیکشنری پارامترهای شمارنده بر اساس ورودی UI."""

        overrides: dict[str, object] = {}
        year = self._get_academic_year()
        if year is not None:
            overrides["academic_year"] = year

        prior = self._picker_prior_roster.text().strip()
        if prior:
            overrides["prior_roster"] = prior

        current = self._picker_current_roster.text().strip()
        if current:
            overrides["current_roster"] = current

        sabt_output = self._picker_sabt_output_alloc.text().strip()
        if sabt_output:
            overrides["sabt_output"] = sabt_output

        sabt_config = self._picker_sabt_config_alloc.text().strip()
        if sabt_config:
            overrides["sabt_config"] = sabt_config

        sabt_template = self._picker_sabt_template_alloc.text().strip()
        if sabt_template:
            overrides["sabt_template"] = sabt_template

        return overrides

    def _build_rule_engine_overrides(self) -> dict[str, object]:
        """تنظیم ورودی‌های شمارنده برای تب موتور قواعد."""

        overrides: dict[str, object] = {}
        year = self._get_rule_engine_year()
        if year is not None:
            overrides["academic_year"] = year

        prior = self._picker_rule_prior_roster.text().strip()
        if prior:
            overrides["prior_roster"] = prior

        current = self._picker_rule_current_roster.text().strip()
        if current:
            overrides["current_roster"] = current

        sabt_output = self._picker_sabt_output_rule.text().strip()
        if sabt_output:
            overrides["sabt_output"] = sabt_output

        sabt_config = self._picker_sabt_config_rule.text().strip()
        if sabt_config:
            overrides["sabt_config"] = sabt_config

        sabt_template = self._picker_sabt_template_rule.text().strip()
        if sabt_template:
            overrides["sabt_template"] = sabt_template

        return overrides

    def _start_demo_task(self) -> None:
        """اجرای پیش‌نمایش پیشرفت برای تست UI."""

        if self._worker is not None and self._worker.isRunning():
            QMessageBox.information(self, "مشغول", "یک عملیات دیگر در حال اجراست.")
            return

        def _demo_task(*, progress: ProgressFn) -> None:
            for pct, msg in ((0, "آغاز"), (30, "در حال پردازش"), (60, "گام پایانی"), (100, "کامل")):
                progress(pct, msg)

        self._launch_worker(_demo_task, "دموی پیشرفت")

    def _autodetect_counters(self) -> None:
        """خواندن روستر سال جاری و پیشنهاد سال و آخرین شمارنده‌ها."""

        self._autodetect_counters_for(
            self._picker_current_roster, self._combo_academic_year
        )

    def _autodetect_rule_engine_counters(self) -> None:
        """پیشنهاد شمارنده‌ها برای تب موتور قواعد."""

        self._autodetect_counters_for(
            self._picker_rule_current_roster, self._combo_rule_academic_year
        )

    def _autodetect_counters_for(
        self, picker: FilePicker, combo: QComboBox
    ) -> None:
        """منطق مشترک پیشنهاد شمارنده بر اساس روستر ورودی."""

        path_text = picker.text().strip()
        if not path_text:
            QMessageBox.information(
                self,
                "فایل نامشخص",
                "ابتدا روستر سال جاری را انتخاب کنید.",
            )
            return

        try:
            dataframe = self._load_counter_dataframe(Path(path_text))
        except FileNotFoundError:
            QMessageBox.warning(self, "فایل یافت نشد", f"مسیر مشخص‌شده وجود ندارد: {path_text}")
            return
        except ValueError as exc:
            QMessageBox.warning(self, "خواندن فایل", str(exc))
            return
        except Exception as exc:  # pragma: no cover - خطای غیرمنتظره I/O
            QMessageBox.warning(self, "خواندن فایل", f"امکان خواندن فایل نبود: {exc}")
            return

        canonical = canonicalize_headers(dataframe, header_mode=HeaderMode.en)
        strict_year = infer_year_strict(canonical)
        fallback_year = detect_academic_year_from_counters(canonical)
        messages: list[str] = []
        if strict_year is not None:
            self._set_year_for_combo(combo, strict_year)
            messages.append(f"سال پیشنهادی: {strict_year}")
        elif fallback_year is not None:
            messages.append(f"سال احتمالی (غیر یکتا): {fallback_year}")
        else:
            messages.append("سال قابل تشخیص نیست")

        try:
            policy = get_policy()
        except Exception as exc:  # pragma: no cover - خطای policy در UI
            QMessageBox.warning(self, "بارگذاری سیاست", f"امکان خواندن policy نبود: {exc}")
            self._status.setText("بارگذاری policy ناموفق")
            return

        year = strict_year or self._get_year_value(combo)
        if year is not None:
            try:
                yy = year_to_yy(year)
            except ValueError:
                yy = None
            if yy is not None:
                male_mid3 = str(policy.gender_codes.male.counter_code).zfill(3)
                female_mid3 = str(policy.gender_codes.female.counter_code).zfill(3)
                male_prefix = f"{yy:02d}{male_mid3}"
                female_prefix = f"{yy:02d}{female_mid3}"
                last_male = find_max_sequence_by_prefix(canonical, male_prefix)
                last_female = find_max_sequence_by_prefix(canonical, female_prefix)
                next_male = last_male + 1 if last_male else 1
                next_female = last_female + 1 if last_female else 1
                if last_male:
                    messages.append(f"آخرین پسر: {yy:02d}{male_mid3}{last_male:04d}")
                else:
                    messages.append("آخرین پسر یافت نشد")
                messages.append(f"شروع بعدی پسر: {next_male:04d}")
                if last_female:
                    messages.append(f"آخرین دختر: {yy:02d}{female_mid3}{last_female:04d}")
                else:
                    messages.append("آخرین دختر یافت نشد")
                messages.append(f"شروع بعدی دختر: {next_female:04d}")
        else:
            messages.append("برای محاسبهٔ شمارندهٔ آخر، سال را مشخص کنید")

        status = " | ".join(messages)
        self._status.setText(status or "پیشنهاد خودکار انجام شد")
        self._log.append(f"ℹ️ پیشنهاد شمارنده: {status}")

    def _launch_cli(
        self,
        argv: Sequence[str],
        action: str,
        *,
        overrides: dict[str, object] | None = None,
    ) -> None:
        """اجرای فرمان CLI با Worker و رعایت قرارداد progress."""

        def _task(*, progress: ProgressFn) -> None:
            exit_code = cli.main(
                argv,
                progress_factory=lambda: progress,
                ui_overrides=overrides,
            )
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

    def _get_year_value(self, combo: QComboBox) -> int | None:
        """دریافت سال تحصیلی از یک ComboBox مشخص."""

        text = combo.currentText().strip()
        if not text:
            return None
        try:
            year = int(text)
        except ValueError:
            return None
        if year < 1300 or year > 1500:
            return None
        return year

    def _get_academic_year(self) -> int | None:
        """دریافت سال تحصیلی معتبر از ورودی تب تخصیص."""

        return self._get_year_value(self._combo_academic_year)

    def _get_rule_engine_year(self) -> int | None:
        """دریافت سال تحصیلی از تب موتور قواعد."""

        return self._get_year_value(self._combo_rule_academic_year)

    def _set_academic_year(self, year: int) -> None:
        """قرار دادن مقدار سال تحصیلی در کنترل مربوطه."""

        self._set_year_for_combo(self._combo_academic_year, year)

    def _set_year_for_combo(self, combo: QComboBox, year: int) -> None:
        """کمک‌کننده برای تنظیم مقدار سال در ComboBox."""

        combo.setEditText(str(year))

    def _load_counter_dataframe(self, path: Path) -> pd.DataFrame:
        """بارگذاری دیتافریم شمارنده با تشخیص شیت مناسب."""

        if not path:
            raise ValueError("مسیر فایل مشخص نشده است")
        if not path.exists():
            raise FileNotFoundError(path)

        suffix = path.suffix.lower()
        if suffix in {".xlsx", ".xls", ".xlsm"}:
            with pd.ExcelFile(path) as workbook:
                sheet_name = pick_counter_sheet_name(workbook.sheet_names)
                if sheet_name is None:
                    raise ValueError("هیچ شیت سازگار با شمارنده یافت نشد")
                return workbook.parse(sheet_name)
        if suffix == ".csv":
            return pd.read_csv(path)

        # تلاش مجدد به عنوان Excel پیش‌فرض
        with pd.ExcelFile(path) as workbook:
            sheet_name = pick_counter_sheet_name(workbook.sheet_names)
            if sheet_name is None:
                sheet_name = workbook.sheet_names[0]
            return workbook.parse(sheet_name)

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
        if hasattr(self, "_splitter"):
            settings = QSettings()
            settings.setValue("ui/main_splitter", self._splitter.saveState())
        super().closeEvent(event)


def run_demo() -> None:  # pragma: no cover - اجرای دستی UI
    """اجرای سادهٔ پنجره برای تست دستی."""

    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":  # pragma: no cover
    run_demo()
