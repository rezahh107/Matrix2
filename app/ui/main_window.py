"""پنجرهٔ اصلی PySide6 برای سناریوهای سامانه تخصیص دانشجو-منتور."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

import pandas as pd
from PySide6.QtCore import QByteArray, QSettings, QSize, Qt, Slot, QTimer, QUrl
from PySide6.QtGui import QAction, QCloseEvent, QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QStackedLayout,
    QSplitter,
    QStatusBar,
    QStyle,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app.core.common.columns import (
    CANON_EN_TO_FA,
    HeaderMode,
    canonicalize_headers,
    ensure_series,
)
from app.core.counter import (
    detect_academic_year_from_counters,
    find_max_sequence_by_prefix,
    infer_year_strict,
    pick_counter_sheet_name,
    year_to_yy,
)
from app.core.policy_loader import get_policy, load_policy
from app.infra import cli
from app.utils.path_utils import resource_path

from .task_runner import ProgressFn, Worker
from .widgets import DashboardCard, FilePicker
from .app_preferences import AppPreferences
from .preferences import (
    FileStatusLevel,
    FileStatusViewModel,
    collect_file_statuses,
    format_last_run_label,
    load_dashboard_texts,
    read_last_run_info,
)
from .preferences.language_dialog import LanguageDialog
from .texts import UiTranslator

__all__ = ["MainWindow", "run_demo", "FilePicker"]


class MainWindow(QMainWindow):
    """پنجرهٔ اصلی PySide6 برای اجرای سناریوهای Build و Allocate."""

    def __init__(self) -> None:
        super().__init__()
        self._prefs = AppPreferences()
        self._translator = UiTranslator(self._prefs.language)
        self.setWindowTitle(self._translator.text("app.title", "سامانه تخصیص دانشجو-منتور"))
        self.setMinimumSize(960, 640)
        self.setLayoutDirection(Qt.RightToLeft if self._prefs.language == "fa" else Qt.LeftToRight)

        self._worker: Worker | None = None
        self._success_hook: Callable[[], None] | None = None
        self._dashboard_texts = load_dashboard_texts(self._translator)
        self._btn_open_output_folder: QPushButton | None = None
        self._center_manager_combos: Dict[int, QComboBox] = {}
        self._btn_reset_managers: QPushButton | None = None
        self._shortcut_buttons: List[QToolButton] = []
        self._btn_open_output_shortcut: QToolButton | None = None
        self._log_placeholder: QLabel | None = None
        self._log_stack: QStackedLayout | None = None
        self._stage_badge: QLabel | None = None
        self._stage_detail: QLabel | None = None
        self._progress_caption: QLabel | None = None
        self._last_run_badge: QLabel | None = None
        self._file_status_rows: Dict[str, Tuple[QLabel, QLabel]] = {}
        self._current_action: str = self._translator.text("status.ready", "آماده")
        policy_file = resource_path("config", "policy.json")
        self._default_policy_path = str(policy_file) if policy_file.exists() else ""
        exporter_config = resource_path("config", "SmartAlloc_Exporter_Config_v1.json")
        self._default_sabt_config_path = (
            str(exporter_config) if exporter_config.exists() else ""
        )

        self._splitter = QSplitter(Qt.Vertical, self)
        self._splitter.setChildrenCollapsible(False)

        top_pane = QWidget(self._splitter)
        top_layout = QVBoxLayout(top_pane)
        top_layout.setContentsMargins(12, 12, 12, 6)
        top_layout.setSpacing(12)

        dashboard = self._build_dashboard()
        top_layout.addWidget(dashboard)

        self._tabs = QTabWidget(self)
        self._tabs.setDocumentMode(True)
        self._tabs.setTabPosition(QTabWidget.North)
        self._tabs.addTab(
            self._wrap_page(self._build_build_page()), self._t("tabs.build", "ساخت ماتریس")
        )
        self._tabs.addTab(
            self._wrap_page(self._build_allocate_page()), self._t("tabs.allocate", "تخصیص")
        )
        self._tabs.addTab(
            self._wrap_page(self._build_rule_engine_page()), self._t("tabs.rule_engine", "موتور قواعد")
        )
        self._tabs.addTab(
            self._wrap_page(self._build_validate_page()), self._t("tabs.validate", "اعتبارسنجی")
        )
        self._tabs.addTab(
            self._wrap_page(self._build_explain_page()), self._t("tabs.explain", "توضیحات")
        )
        top_layout.addWidget(self._tabs, 1)

        bottom_pane = QWidget(self._splitter)
        bottom_layout = QVBoxLayout(bottom_pane)
        bottom_layout.setContentsMargins(16, 0, 16, 16)
        bottom_layout.setSpacing(12)

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(12)
        self._stage_badge = QLabel(self._t("status.ready", "آماده"))
        self._stage_badge.setObjectName("labelStageBadge")
        self._stage_detail = QLabel(self._t("stage.pick_scenario", "برای شروع یکی از سناریوها را انتخاب کنید"))
        self._stage_detail.setWordWrap(True)
        self._stage_detail.setObjectName("labelStageDetail")
        self._status = QLabel(self._t("status.ready", "آماده"))
        self._status.setObjectName("labelStatus")
        status_column = QVBoxLayout()
        status_column.setSpacing(2)
        status_column.addWidget(self._stage_badge)
        status_column.addWidget(self._stage_detail)
        status_column.addWidget(self._status)
        self._last_run_badge = QLabel(self._t("status.no_runs", "آخرین اجرا: هنوز اجرایی ثبت نشده است"))
        self._last_run_badge.setObjectName("lastRunBadge")
        self._last_run_badge.setWordWrap(True)
        status_column.addWidget(self._last_run_badge)
        status_layout.addLayout(status_column, 1)

        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setObjectName("progressBar")
        self._progress_caption = QLabel(f"0% | {self._t('status.ready', 'آماده')}")
        self._progress_caption.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._progress_caption.setObjectName("progressCaption")
        progress_column = QVBoxLayout()
        progress_column.setSpacing(4)
        progress_column.addWidget(self._progress)
        progress_column.addWidget(self._progress_caption)
        status_layout.addLayout(progress_column, 1)
        bottom_layout.addLayout(status_layout)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setObjectName("textLog")

        log_container = QHBoxLayout()
        log_container.setContentsMargins(0, 0, 0, 0)
        log_container.setSpacing(12)
        log_panel = QFrame(self)
        log_panel.setObjectName("logPanel")
        log_stack = QStackedLayout(log_panel)
        log_stack.setContentsMargins(0, 0, 0, 0)
        self._log_placeholder = QLabel(self._t("log.placeholder", "هنوز گزارشی ثبت نشده است."))
        self._log_placeholder.setAlignment(Qt.AlignCenter)
        self._log_placeholder.setObjectName("logPlaceholder")
        self._log_placeholder.setWordWrap(True)
        log_stack.addWidget(self._log_placeholder)
        log_stack.addWidget(self._log)
        self._log_stack = log_stack
        self._log.textChanged.connect(self._sync_log_placeholder)
        self._sync_log_placeholder()
        log_container.addWidget(log_panel, 1)

        log_buttons = QVBoxLayout()
        log_buttons.setSpacing(8)
        self._btn_clear_log = QPushButton(self._t("log.clear", "پاک کردن گزارش"))
        self._btn_clear_log.setObjectName("btnClearLog")
        self._btn_clear_log.clicked.connect(self._clear_log)
        log_buttons.addWidget(self._btn_clear_log)

        self._btn_save_log = QPushButton(self._t("log.save", "ذخیره گزارش…"))
        self._btn_save_log.setObjectName("btnSaveLog")
        self._btn_save_log.clicked.connect(self._save_log_to_file)
        log_buttons.addWidget(self._btn_save_log)
        log_buttons.addStretch(1)

        log_container.addLayout(log_buttons, 0)
        bottom_layout.addLayout(log_container, 1)

        controls_layout = QHBoxLayout()
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(12)
        controls_layout.addStretch(1)
        self._btn_demo = QPushButton(self._t("action.demo", "اجرای تست (دمو Progress)"))
        self._btn_demo.setObjectName("btnDemo")
        self._btn_demo.setToolTip(self._t("log.demo.tooltip", "اجرای تست کوتاه برای نمایش انیمیشن پیشرفت"))
        self._btn_demo.clicked.connect(self._start_demo_task)
        controls_layout.addWidget(self._btn_demo)
        bottom_layout.addLayout(controls_layout)

        self._splitter.addWidget(top_pane)
        self._splitter.addWidget(bottom_pane)
        self._splitter.setStretchFactor(0, 3)
        self._splitter.setStretchFactor(1, 1)

        self.setCentralWidget(self._splitter)

        self._build_ribbon()
        self._build_status_bar()

        settings = QSettings()
        state = settings.value("ui/main_splitter")
        if isinstance(state, QByteArray):
            self._splitter.restoreState(state)

        self._interactive: List[QWidget] = []
        self._register_interactive_controls()
        self._update_output_folder_button_state()
        self._apply_theme()
        self._refresh_dashboard_state()

    def _t(self, key: str, fallback: str) -> str:
        """دسترسی سریع به ترجمهٔ UI برای زبان فعال."""

        return self._translator.text(key, fallback)

    # ------------------------------------------------------------------ UI setup
    def _build_ribbon(self) -> None:
        """ایجاد نوار ابزار بالایی با الهام از Ribbon Office."""

        toolbar = QToolBar(self._t("ribbon.actions", "اکشن‌ها"), self)
        toolbar.setIconSize(QSize(20, 20))
        toolbar.setMovable(False)

        build_action = QAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogNewFolder),
            self._t("action.build", "ساخت ماتریس"),
            self,
        )
        build_action.triggered.connect(self._start_build)
        toolbar.addAction(build_action)

        allocate_action = QAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon),
            self._t("action.allocate", "تخصیص"),
            self,
        )
        allocate_action.triggered.connect(self._start_allocate)
        toolbar.addAction(allocate_action)

        rule_action = QAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload),
            self._t("action.rule_engine", "اجرای Rule Engine"),
            self,
        )
        rule_action.triggered.connect(self._start_rule_engine)
        toolbar.addAction(rule_action)

        toolbar.addSeparator()

        output_action = QAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_DirHomeIcon),
            self._t("dashboard.button.output", "پوشه خروجی"),
            self,
        )
        output_action.triggered.connect(self._open_last_output_folder)
        toolbar.addAction(output_action)

        toolbar.addSeparator()
        prefs_action = QAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView),
            self._t("action.preferences", "تنظیمات"),
            self,
        )
        prefs_action.triggered.connect(self._open_language_dialog)
        toolbar.addAction(prefs_action)

        self.addToolBar(toolbar)

    def _build_status_bar(self) -> None:
        """نوار وضعیت پایین با نمایش زبان و وضعیت جاری."""

        status_bar = QStatusBar(self)
        language_label = QLabel(f"{self._t('status.language', 'زبان فعال')}: {self._prefs.language.upper()}")
        state_label = QLabel(self._t("statusbar.ready", "وضعیت: آماده"))
        status_bar.addWidget(state_label)
        status_bar.addPermanentWidget(language_label)
        self._language_label = language_label
        self._status_bar_state = state_label
        self.setStatusBar(status_bar)

    def _update_status_bar_state(self, key: str) -> None:
        """به‌روزرسانی متن نوار وضعیت بر اساس کلید."""

        if not hasattr(self, "_status_bar_state"):
            return
        mapping = {
            "ready": self._t("statusbar.ready", "وضعیت: آماده"),
            "running": self._t("statusbar.running", "وضعیت: در حال اجرا"),
            "error": self._t("statusbar.error", "وضعیت: خطا"),
        }
        self._status_bar_state.setText(mapping.get(key, mapping["ready"]))

    def _wrap_page(self, page: QWidget) -> QScrollArea:
        """پیچیدن صفحات فرم در اسکرول برای نمایش بهتر در اندازه‌های کوچک."""

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setObjectName(f"scroll_{page.objectName() or id(page)}")
        scroll.setWidget(page)
        return scroll

    def _build_dashboard(self) -> QWidget:
        """ایجاد پانل داشبورد با کارت‌های وضعیت، چک‌لیست و میانبرها."""

        frame = QFrame(self)
        frame.setObjectName("dashboardPanel")
        frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        frame.setMaximumHeight(160)
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(10)

        files_card = DashboardCard(
            self._dashboard_texts.files_title,
            self._dashboard_texts.files_description,
            self,
            max_height=140,
        )
        statuses = collect_file_statuses(self._prefs, self._translator)
        for status in statuses:
            files_card.body_layout().addWidget(self._create_file_status_row(status))
        layout.addWidget(files_card, 1)

        checklist_card = DashboardCard(
            self._dashboard_texts.checklist_title,
            self._dashboard_texts.checklist_description,
            self,
            max_height=140,
        )
        if self._dashboard_texts.checklist_items:
            for item in self._dashboard_texts.checklist_items:
                label = QLabel(f"• {item.text}")
                label.setWordWrap(True)
                label.setObjectName("dashboardChecklistItem")
                checklist_card.body_layout().addWidget(label)
        else:
            placeholder = QLabel(self._t("dashboard.no_checklist", "چک‌لیستی تعریف نشده است"))
            placeholder.setObjectName("dashboardChecklistItem")
            checklist_card.body_layout().addWidget(placeholder)
        layout.addWidget(checklist_card, 1)

        actions_card = DashboardCard(
            self._dashboard_texts.actions_title,
            self._dashboard_texts.actions_description,
            self,
        )
        policy_display = self._default_policy_path or "config/policy.json"
        policy_label = QLabel(
            f"<b>{self._t('dashboard.policy.info', 'سیاست فعال:')}</b> {policy_display}<br>"
            "نسخه Policy: 1.0.3 • نسخه SSoT: 1.0.2"
        )
        policy_label.setTextFormat(Qt.RichText)
        policy_label.setWordWrap(True)
        policy_label.setObjectName("dashboardPolicyInfo")
        actions_card.body_layout().addWidget(policy_label)

        buttons_container = QWidget(self)
        buttons_row = QGridLayout(buttons_container)
        buttons_row.setContentsMargins(0, 0, 0, 0)
        buttons_row.setHorizontalSpacing(6)
        buttons_row.setVerticalSpacing(6)
        buttons_row.setColumnStretch(0, 1)
        buttons_row.setColumnStretch(1, 1)
        buttons = [
            (
                self._t("dashboard.button.build", "ساخت ماتریس"),
                self._t("tooltip.build", "اجرای کامل سناریوی ساخت ماتریس"),
                self._start_build,
                QStyle.StandardPixmap.SP_FileDialogNewFolder,
            ),
            (
                self._t("dashboard.button.allocate", "تخصیص"),
                self._t("tooltip.allocate", "اجرای تخصیص دانش‌آموز به منتور"),
                self._start_allocate,
                QStyle.StandardPixmap.SP_ComputerIcon,
            ),
            (
                self._t("dashboard.button.rule_engine", "موتور قواعد"),
                self._t("tooltip.rule_engine", "اجرای Rule Engine برای تست سیاست"),
                self._start_rule_engine,
                QStyle.StandardPixmap.SP_BrowserReload,
            ),
        ]
        for idx, (text, tooltip, callback, icon_role) in enumerate(buttons):
            button = self._create_dashboard_shortcut(text, tooltip, callback, icon_role)
            buttons_row.addWidget(button, idx // 2, idx % 2)
        self._btn_open_output_shortcut = self._create_dashboard_shortcut(
            self._t("dashboard.button.output", "پوشه خروجی"),
            self._t("tooltip.output_folder", "آخرین پوشه خروجی تولید شده را باز می‌کند"),
            self._open_last_output_folder,
            QStyle.StandardPixmap.SP_DirHomeIcon,
        )
        buttons_row.addWidget(self._btn_open_output_shortcut, len(buttons) // 2, len(buttons) % 2)
        actions_card.body_layout().addWidget(buttons_container)
        layout.addWidget(actions_card, 1)

        return frame

    def _create_dashboard_shortcut(
        self,
        text: str,
        tooltip: str,
        callback: Callable[[], None],
        icon_role: QStyle.StandardPixmap,
    ) -> QToolButton:
        """ساخت دکمه میان‌بر داشبورد با آیکون استاندارد."""

        button = QToolButton(self)
        button.setObjectName("dashboardShortcut")
        button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        button.setIcon(self.style().standardIcon(icon_role))
        icon_size = self.style().pixelMetric(QStyle.PM_SmallIconSize) or 16
        button.setIconSize(QSize(icon_size, icon_size))
        button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        button.setFixedHeight(32)
        button.setText(text)
        button.setToolTip(tooltip)
        button.clicked.connect(callback)
        self._shortcut_buttons.append(button)
        return button

    def _create_file_status_row(self, model: FileStatusViewModel) -> QWidget:
        """ساخت ردیف وضعیت فایل بر اساس مدل نمایشی."""

        row = QFrame(self)
        row.setObjectName("fileStatusRow")
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        indicator = QLabel("●", row)
        indicator.setObjectName("fileStatusIndicator")
        indicator.setAlignment(Qt.AlignCenter)
        indicator.setFixedWidth(18)
        layout.addWidget(indicator, 0, Qt.AlignTop)

        text_label = QLabel(row)
        text_label.setWordWrap(True)
        text_label.setObjectName("fileStatusText")
        layout.addWidget(text_label, 1)

        row.setToolTip(model.description)
        self._file_status_rows[model.key] = (indicator, text_label)
        self._apply_file_status_model(model)
        return row

    def _apply_file_status_model(self, model: FileStatusViewModel) -> None:
        """به‌روزرسانی نما با توجه به وضعیت فایل."""

        widgets = self._file_status_rows.get(model.key)
        if widgets is None:
            return
        indicator, text_label = widgets
        color = "#16a34a" if model.level is FileStatusLevel.READY else "#d97706"
        symbol = "●" if model.level is FileStatusLevel.READY else "▲"
        indicator.setText(symbol)
        indicator.setStyleSheet(f"color:{color};font-size:16px;")
        path_display = model.path if model.path else self._t("status.waiting", "تنظیم نشده")
        state_text = self._t("status.ready", "آماده") if model.exists else self._t("status.waiting", "در انتظار اجرا")
        text_label.setText(
            f"<b>{model.label}</b><br>"
            f"<span style='color:#475569'>{path_display}</span><br>"
            f"<span style='color:#0f172a'>{state_text}</span>"
        )

    def _refresh_dashboard_state(self) -> None:
        """به‌روزرسانی کارت وضعیت و برچسب آخرین اجرا."""

        self._apply_file_statuses(collect_file_statuses(self._prefs, self._translator))
        self._refresh_last_run_badge()

    def _apply_file_statuses(self, statuses: Iterable[FileStatusViewModel]) -> None:
        """اعمال وضعیت فایل‌ها روی ویجت‌های کارت."""

        for model in statuses:
            self._apply_file_status_model(model)

    def _refresh_last_run_badge(self) -> None:
        """به‌روزرسانی برچسب آخرین اجرای ثبت‌شده."""

        if self._last_run_badge is None:
            return
        info = read_last_run_info(self._prefs)
        self._last_run_badge.setText(format_last_run_label(info, self._translator))

    def _create_page_hero(self, title: str, subtitle: str, badge: str) -> QFrame:
        """ساخت هدر Hero برای صفحات تب‌ها."""

        frame = QFrame(self)
        frame.setObjectName("heroCard")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(12)

        text_column = QVBoxLayout()
        text_column.setSpacing(4)
        title_label = QLabel(title)
        title_label.setObjectName("heroTitle")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("heroSubtitle")
        subtitle_label.setWordWrap(True)
        text_column.addWidget(title_label)
        text_column.addWidget(subtitle_label)
        layout.addLayout(text_column, 1)

        badge_label = QLabel(badge)
        badge_label.setObjectName("heroBadge")
        badge_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(badge_label, 0, Qt.AlignVCenter)

        return frame

    def _apply_theme(self) -> None:
        """اعمال استایل‌شیت سراسری برای ایجاد هماهنگی بصری."""

        stylesheet = """
            QMainWindow {
                background-color: #f5f7fb;
            }
            QWidget {
                color: #0f172a;
                font-family: "Segoe UI";
                font-size: 9pt;
            }
            QToolBar {
                background: #e9eef9;
                spacing: 8px;
                padding: 6px;
            }
            QStatusBar {
                background: #e9eef9;
                padding: 4px 8px;
            }
            QGroupBox {
                border: 1px solid #cfd6e6;
                border-radius: 8px;
                margin-top: 10px;
                padding: 10px;
                background-color: #ffffff;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 6px;
                color: #1d3a70;
                font-weight: 600;
            }
            #dashboardCard, #heroCard {
                border: 1px solid #cfd6e6;
                border-radius: 10px;
                background-color: #ffffff;
            }
            #dashboardPanel {
                background-color: transparent;
            }
            #dashboardCard QScrollArea {
                border: none;
            }
            QLabel#dashboardCardTitle {
                font-size: 10pt;
                font-weight: 600;
                color: #0f172a;
            }
            QLabel#dashboardCardDescription,
            QLabel#dashboardPolicyInfo,
            QLabel#dashboardChecklistItem {
                color: #475569;
            }
            QLabel#heroTitle {
                font-size: 11pt;
                font-weight: 600;
                color: #0f172a;
            }
            QLabel#heroSubtitle {
                color: #475569;
            }
            QLabel#heroBadge {
                padding: 4px 12px;
                border-radius: 12px;
                background-color: #e9eef9;
                color: #1d3a70;
                border: 1px solid #2b88d8;
            }
            QLabel#labelStageBadge {
                font-weight: 600;
                color: #1d3a70;
            }
            QLabel#labelStageDetail {
                color: #334155;
            }
            QLabel#progressCaption {
                color: #1d3a70;
                font-weight: 500;
            }
            QLabel#lastRunBadge {
                color: #0f172a;
                font-weight: 600;
            }
            QProgressBar#progressBar {
                background-color: #f8fafc;
                border: 1px solid #cfd6e6;
                border-radius: 8px;
                height: 18px;
            }
            QProgressBar#progressBar::chunk {
                background-color: #2b88d8;
                border-radius: 6px;
            }
            QLineEdit,
            QComboBox,
            QTextEdit,
            QPlainTextEdit,
            QSpinBox,
            QTableWidget,
            QTableView,
            QListView,
            QTreeView {
                background-color: #ffffff;
                border: 1px solid #cfd6e6;
                border-radius: 6px;
                padding: 4px;
                selection-background-color: #e2e8f0;
                selection-color: #0f172a;
            }
            QTabWidget::pane {
                border: 1px solid #cfd6e6;
                border-radius: 6px;
            }
            QTabBar::tab {
                background: #eef2fb;
                color: #0f172a;
                padding: 8px 12px;
                border: 1px solid #cfd6e6;
                border-bottom: none;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
            }
            QTabBar::tab:selected {
                background: #ffffff;
                color: #0f172a;
            }
            #dashboardShortcut {
                background: #f3f6fb;
                border: 1px solid #c3d3f5;
                border-radius: 6px;
                padding: 6px 10px;
            }
            #dashboardShortcut:hover {
                background: #e8f0fe;
            }
            #fileStatusRow {
                border-bottom: 1px solid #cfd6e6;
            }
            #fileStatusIndicator {
                font-size: 16px;
            }
            #fileStatusText {
                color: #0f172a;
            }
            #logPanel {
                border: 1px solid #cfd6e6;
                border-radius: 6px;
                background: #ffffff;
            }
            #logPlaceholder {
                color: #94a3b8;
            }
            QTextEdit#textLog {
                border: none;
                background: transparent;
            }
            QPushButton#btnDemo {
                background-color: #2b88d8;
                border: 1px solid #1d6fb8;
                color: #ffffff;
            }
            QPushButton#btnDemo:hover {
                background-color: #1d6fb8;
            }
        """

        self.setStyleSheet(stylesheet)

    def _set_stage(self, title: str | None, detail: str | None = None) -> None:
        """به‌روزرسانی عنوان و توضیح مرحلهٔ فعال."""

        if self._stage_badge is not None:
            self._stage_badge.setText((title or self._t("status.ready", "آماده")).strip())
        if detail is not None and self._stage_detail is not None:
            self._stage_detail.setText(detail.strip() or "")

    def _update_progress_caption(self, pct: int, message: str | None) -> None:
        """نمایش درصد پیشرفت همراه با توضیح مرحله."""

        if self._progress_caption is None:
            return
        safe_message = message or self._status.text() or "در حال پردازش"
        self._progress_caption.setText(f"{pct}% | {safe_message}")

    def _build_build_page(self) -> QWidget:
        """فرم ورودی‌های سناریوی ساخت ماتریس."""

        page = QWidget(self)
        outer = QVBoxLayout(page)
        browse_text = self._t("action.browse", "انتخاب…")
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)
        outer.addWidget(
            self._create_page_hero(
                self._t("hero.build.title", "ساخت ماتریس"),
                self._t(
                    "hero.build.subtitle",
                    "ورود فایل‌های Inspactor، مدارس و Crosswalk برای ساخت eligibility matrix مطابق Policy.",
                ),
                self._t("hero.build.badge", "گام ۱ از ۴"),
            )
        )

        inputs_group = QGroupBox(self._t("group.inputs", "ورودی‌ها"), page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_inspactor = FilePicker(page, placeholder=self._t("files.inspactor", "فایل Inspactor"))
        self._picker_inspactor.setObjectName("editInspactor")
        self._picker_inspactor.setToolTip(self._t("files.inspactor", "خروجی گزارش Inspactor که فهرست پشتیبان‌ها را دارد"))
        self._set_picker_button_text(self._picker_inspactor)
        inputs_layout.addRow(self._t("files.inspactor", "گزارش Inspactor"), self._picker_inspactor)

        self._picker_schools = FilePicker(page, placeholder=self._t("files.schools", "فایل مدارس"))
        self._picker_schools.setObjectName("editSchools")
        self._picker_schools.setToolTip(self._t("files.schools", "فایل رسمی مدارس برای تطبیق کد و نام مدرسه"))
        self._set_picker_button_text(self._picker_schools)
        inputs_layout.addRow(self._t("files.schools", "گزارش مدارس"), self._picker_schools)

        self._picker_crosswalk = FilePicker(page, placeholder=self._t("files.crosswalk", "فایل Crosswalk"))
        self._picker_crosswalk.setObjectName("editCrosswalk")
        self._picker_crosswalk.setToolTip(self._t("files.crosswalk", "جدول Crosswalk جهت نگاشت رشته‌ها و گروه‌ها"))
        self._set_picker_button_text(self._picker_crosswalk)
        inputs_layout.addRow(self._t("files.crosswalk", "Crosswalk"), self._picker_crosswalk)

        outer.addWidget(inputs_group)

        policy_group = QGroupBox(self._t("files.policy", "سیاست"), page)
        policy_layout = QFormLayout(policy_group)
        policy_layout.setLabelAlignment(Qt.AlignRight)
        policy_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_policy_build = FilePicker(
            page, placeholder="پیش‌فرض: config/policy.json"
        )
        self._picker_policy_build.setObjectName("editPolicy1")
        if self._default_policy_path:
            self._picker_policy_build.setText(self._default_policy_path)
        self._set_picker_button_text(self._picker_policy_build)
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
        self._picker_output_matrix.set_button_text(browse_text)
        self._apply_pref_default(self._picker_output_matrix, self._prefs.last_matrix_path)
        self._set_picker_button_text(self._picker_output_matrix)
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
        browse_text = self._t("action.browse", "انتخاب…")
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)
        outer.addWidget(
            self._create_page_hero(
                "تخصیص",
                "انتخاب فایل دانش‌آموز و استخر منتورها برای محاسبهٔ تخصیص و خروجی‌های Sabt.",
                "گام ۲ از ۴",
            )
        )

        inputs_group = QGroupBox("ورودی‌های تخصیص", page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_students = FilePicker(
            page, placeholder="دانش‌آموزان (*.xlsx یا *.csv)"
        )
        self._picker_students.setObjectName("editStudents")
        self._picker_students.setToolTip("لیست دانش‌آموزانی که باید به پشتیبان متصل شوند")
        self._set_picker_button_text(self._picker_students)
        inputs_layout.addRow("فایل دانش‌آموزان", self._picker_students)

        self._picker_pool = FilePicker(page, placeholder="استخر منتورها (*.xlsx)")
        self._picker_pool.setObjectName("editPool")
        self._picker_pool.setToolTip("فهرست منتورها یا پشتیبان‌ها برای تخصیص")
        self._picker_pool.set_button_text(browse_text)
        self._picker_pool.line_edit().textChanged.connect(
            lambda *_: self._refresh_all_manager_combos()
        )
        self._set_picker_button_text(self._picker_pool)
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
        self._set_picker_button_text(self._picker_policy_allocate)
        advanced_layout.addRow("سیاست", self._picker_policy_allocate)

        self._picker_alloc_out = FilePicker(
            page, save=True, placeholder="فایل خروجی تخصیص (*.xlsx)"
        )
        self._picker_alloc_out.setObjectName("editAllocOut")
        self._picker_alloc_out.setToolTip("مسیر ذخیرهٔ نتیجه نهایی تخصیص دانش‌آموز-منتور")
        self._picker_alloc_out.set_button_text(browse_text)
        self._apply_pref_default(self._picker_alloc_out, self._prefs.last_alloc_output)
        self._set_picker_button_text(self._picker_alloc_out)

        self._edit_capacity = QLineEdit(page)
        self._edit_capacity.setPlaceholderText("remaining_capacity")
        self._edit_capacity.setText("remaining_capacity")
        self._edit_capacity.setObjectName("editCapacityCol")
        advanced_layout.addRow("ستون ظرفیت", self._edit_capacity)

        outer.addWidget(advanced_group)

        outer.addWidget(self._create_center_management_section())

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
        self._set_picker_button_text(self._picker_prior_roster)
        register_layout.addRow("روستر سال قبل", self._picker_prior_roster)

        self._picker_current_roster = FilePicker(
            register_box,
            placeholder="روستر سال جاری / شمارنده‌ها",
        )
        self._picker_current_roster.setObjectName("currentRosterPicker")
        self._picker_current_roster.setToolTip("برای کشف آخرین شمارنده‌های سال جاری")
        self._set_picker_button_text(self._picker_current_roster)
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
        self._picker_sabt_output_alloc.set_button_text(browse_text)
        self._apply_pref_default(
            self._picker_sabt_output_alloc, self._prefs.last_sabt_output_allocate
        )
        self._set_picker_button_text(self._picker_sabt_output_alloc)
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
        self._picker_sabt_config_alloc.set_button_text(browse_text)
        self._apply_pref_default(
            self._picker_sabt_config_alloc, self._prefs.last_sabt_config_path
        )
        self._apply_resource_default(
            self._picker_sabt_config_alloc, self._default_sabt_config_path
        )
        self._set_picker_button_text(self._picker_sabt_config_alloc)
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
        self._set_picker_button_text(self._picker_sabt_template_alloc)
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

        QTimer.singleShot(0, self._refresh_all_manager_combos)

        return page

    def _build_rule_engine_page(self) -> QWidget:
        """فرم اجرای موتور قواعد بر پایه ماتریس موجود."""

        page = QWidget(self)
        outer = QVBoxLayout(page)
        browse_text = self._t("action.browse", "انتخاب…")
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(16)
        outer.addWidget(
            self._create_page_hero(
                "موتور قواعد",
                "اجرای Rule Engine روی ماتریس ساخته‌شده جهت بازبینی سیاست و شمارنده‌ها.",
                "گام ۳ از ۴",
            )
        )

        inputs_group = QGroupBox("ورودی‌ها", page)
        inputs_layout = QFormLayout(inputs_group)
        inputs_layout.setLabelAlignment(Qt.AlignRight)
        inputs_layout.setFormAlignment(Qt.AlignTop | Qt.AlignRight)

        self._picker_rule_matrix = FilePicker(
            page, placeholder="ماتریس اهلیت (*.xlsx)"
        )
        self._picker_rule_matrix.setObjectName("editRuleMatrix")
        self._picker_rule_matrix.setToolTip("فایل ماتریس اهلیت ساخته‌شده را انتخاب کنید")
        self._picker_rule_matrix.set_button_text(browse_text)
        self._apply_pref_default(
            self._picker_rule_matrix, self._prefs.last_matrix_path
        )
        self._set_picker_button_text(self._picker_rule_matrix)
        inputs_layout.addRow("فایل ماتریس", self._picker_rule_matrix)

        self._picker_rule_students = FilePicker(
            page, placeholder="دانش‌آموزان (*.xlsx یا *.csv)"
        )
        self._picker_rule_students.setObjectName("editRuleStudents")
        self._picker_rule_students.setToolTip("لیست دانش‌آموزان برای ارزیابی مجدد با موتور قواعد")
        self._set_picker_button_text(self._picker_rule_students)
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
        self._set_picker_button_text(self._picker_rule_prior_roster)
        register_layout.addRow("روستر سال قبل", self._picker_rule_prior_roster)

        self._picker_rule_current_roster = FilePicker(
            register_box,
            placeholder="روستر سال جاری / شمارنده‌ها",
        )
        self._picker_rule_current_roster.setObjectName("ruleCurrentRosterPicker")
        self._set_picker_button_text(self._picker_rule_current_roster)
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
        self._set_picker_button_text(self._picker_policy_rule)
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
        self._set_picker_button_text(self._picker_rule_output)
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
        self._picker_sabt_output_rule.set_button_text(browse_text)
        self._apply_pref_default(
            self._picker_sabt_output_rule, self._prefs.last_sabt_output_rule
        )
        self._set_picker_button_text(self._picker_sabt_output_rule)
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
        self._picker_sabt_config_rule.set_button_text(browse_text)
        self._apply_pref_default(
            self._picker_sabt_config_rule, self._prefs.last_sabt_config_path
        )
        self._apply_resource_default(
            self._picker_sabt_config_rule, self._default_sabt_config_path
        )
        self._set_picker_button_text(self._picker_sabt_config_rule)
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
        self._set_picker_button_text(self._picker_sabt_template_rule)
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
        layout.addWidget(
            self._create_page_hero(
                "کنترل کیفیت",
                "مرور خروجی‌های Sabt و گزارش‌های خطا پیش از تحویل نهایی.",
                "گام ۴ از ۴",
            )
        )

        intro = QLabel(
            "این بخش برای یادآوری مراحل کنترل کیفیت خروجی‌های Sabt و تخصیص است."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        guide = QLabel(
            "<ol>"
            "<li>فایل خروجی Sabt یا تخصیص را باز کنید و شیت‌های summary/error را بررسی کنید.</li>"
            "<li>در شیت summary مطمئن شوید که تعداد ردیف‌های موفق با گزارش سامانه برابر است.</li>"
            "<li>در شیت error، ستون توضیح خطا را مطالعه و در صورت نیاز به تیم فنی ارجاع دهید.</li>"
            "<li>در نهایت پوشه خروجی را بایگانی و با برچسب تاریخ ذخیره کنید.</li>"
            "</ol>"
        )
        guide.setWordWrap(True)
        guide.setTextFormat(Qt.RichText)
        layout.addWidget(guide)

        self._btn_open_output_folder = QPushButton("بازکردن پوشه خروجی")
        self._btn_open_output_folder.setObjectName("btnOpenOutputFolder")
        self._btn_open_output_folder.clicked.connect(self._open_last_output_folder)
        layout.addWidget(self._btn_open_output_folder, 0, Qt.AlignRight)
        self._update_output_folder_button_state()

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
        layout.addWidget(
            self._create_page_hero(
                "گزارش Explain",
                "دسترسی سریع به ساختار گزارش توضیح تصمیمات برای ممیزی و آموزش.",
                "ضمیمه",
            )
        )

        label = QLabel(
            "خلاصهٔ Explain در شیت جداگانه داخل فایل اکسل ذخیره می‌شود تا روند"
            " تصمیم‌گیری هر دانش‌آموز قابل پیگیری باشد."
        )
        label.setWordWrap(True)
        layout.addWidget(label)

        hint = QLabel(
            "هر سطر Explain شامل شناسه دانش‌آموز، پشتیبان انتخاب‌شده،"
            " قانون فعال و توضیح متناظر است."
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        columns = [
            "student_id",
            "mentor_id",
            "rule_tag",
            "reason",
            "score",
            "trace_step",
        ]
        table = QTableWidget(2, len(columns), page)
        table.setHorizontalHeaderLabels(columns)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.NoSelection)
        table.setMaximumHeight(160)
        sample_rows = [
            ("STU-125", "MENT-009", "R_CAPACITY", "ظرفیت منطقه", "0.82", "capacity_gate"),
            ("STU-230", "MENT-104", "R_PRIORITY", "اولویت دانش‌آموز فارغ", "0.76", "priority"),
        ]
        for row, values in enumerate(sample_rows):
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled)
                table.setItem(row, col, item)
        layout.addWidget(table)
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
        self._interactive.extend(self._shortcut_buttons)
        self._interactive.extend(self._center_manager_combos.values())
        if self._btn_reset_managers is not None:
            self._interactive.append(self._btn_reset_managers)

    # ------------------------------------------------------------------ Actions
    def _open_language_dialog(self) -> None:
        """باز کردن دیالوگ انتخاب زبان و ذخیره در تنظیمات."""

        dialog = LanguageDialog(self._prefs.language, self._translator, self)
        if dialog.exec() == QDialog.Accepted:
            chosen = dialog.selected_language()
            if chosen != self._prefs.language:
                self._prefs.language = chosen
                if hasattr(self, "_language_label"):
                    self._language_label.setText(
                        f"{self._t('status.language', 'زبان فعال')}: {chosen.upper()}"
                    )
                QMessageBox.information(
                    self,
                    self._t("dialog.language.title", "تنظیمات زبان"),
                    self._t("status.restart_required", "برای اعمال زبان باید برنامه را مجدد اجرا کنید."),
                )

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
        def _remember_build_output() -> None:
            output_path = self._picker_output_matrix.text().strip()
            if output_path:
                self._prefs.last_matrix_path = output_path
            self._prefs.record_last_run("build")
            self._refresh_dashboard_state()

        self._launch_cli(argv, "ساخت ماتریس", on_success=_remember_build_output)

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

        def _remember_allocate_outputs() -> None:
            alloc_out = self._picker_alloc_out.text().strip()
            if alloc_out:
                self._prefs.last_alloc_output = alloc_out
            sabt_output = self._picker_sabt_output_alloc.text().strip()
            if sabt_output:
                self._prefs.last_sabt_output_allocate = sabt_output
            sabt_config = self._picker_sabt_config_alloc.text().strip()
            if sabt_config:
                self._prefs.last_sabt_config_path = sabt_config
            self._update_output_folder_button_state()
            self._prefs.record_last_run("allocate")
            self._refresh_dashboard_state()

        self._launch_cli(
            argv,
            "تخصیص",
            overrides=overrides,
            on_success=_remember_allocate_outputs,
        )

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

        def _remember_rule_engine_outputs() -> None:
            matrix_path = self._picker_rule_matrix.text().strip()
            if matrix_path:
                self._prefs.last_matrix_path = matrix_path
            sabt_output = self._picker_sabt_output_rule.text().strip()
            if sabt_output:
                self._prefs.last_sabt_output_rule = sabt_output
            sabt_config = self._picker_sabt_config_rule.text().strip()
            if sabt_config:
                self._prefs.last_sabt_config_path = sabt_config
            self._update_output_folder_button_state()
            self._prefs.record_last_run("rule-engine")
            self._refresh_dashboard_state()

        self._launch_cli(
            argv,
            "موتور قواعد",
            overrides=overrides,
            on_success=_remember_rule_engine_outputs,
        )

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

        center_overrides = self.get_center_manager_map()
        if center_overrides:
            overrides["center_managers"] = center_overrides

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

    def _create_center_management_section(self) -> QWidget:
        """ایجاد بخش پویای مدیریت مراکز بر اساس پیکربندی Policy.

        این تابع با خواندن تنظیمات مراکز از Policy، یک ویجت پویا ایجاد می‌کند
        که شامل ComboBoxهای انتخاب مدیر برای هر مرکز (به جز مرکز 0) می‌باشد.

        Returns:
            QWidget: گروه ویجت شامل تمام کنترل‌های مدیریت مراکز

        Note:
            - برای هر مرکز تعریف شده در Policy (به جز مرکز 0) یک ردیف ایجاد می‌شود
            - مقادیر پیش‌فرض از policy.center_management.centers خوانده می‌شوند
            - در صورت خطا در بارگذاری Policy، پیام خطا نمایش داده می‌شود
        """

        group_box = QGroupBox("مدیریت مراکز")
        group_box.setObjectName("centerManagerGroup")
        main_layout = QVBoxLayout()
        self._center_manager_combos.clear()

        try:
            policy = load_policy()
            if not policy.center_management.enabled:
                label = QLabel("مدیریت مراکز غیرفعال است")
                main_layout.addWidget(label)
                group_box.setLayout(main_layout)
                return group_box
            for center in policy.center_management.centers:
                if center.id == 0:
                    continue
                row_layout = QHBoxLayout()
                label = QLabel(f"مدیر {center.name}:")
                combo = self._create_manager_combo(group_box)
                combo.setMinimumWidth(250)
                preferred = self._prefs.get_center_manager(
                    center.id, center.default_manager or ""
                )
                self._refresh_manager_combo(center.id, combo)
                if preferred:
                    combo.setCurrentText(preferred)
                combo.currentTextChanged.connect(
                    lambda text, cid=center.id: self._on_center_manager_changed(
                        cid, text
                    )
                )
                self._center_manager_combos[center.id] = combo
                row_layout.addWidget(label)
                row_layout.addWidget(combo)
                row_layout.addStretch()
                main_layout.addLayout(row_layout)
        except Exception as exc:
            error_label = QLabel(f"خطا در بارگذاری تنظیمات مراکز: {exc}")
            main_layout.addWidget(error_label)

        button_layout = QHBoxLayout()
        reset_btn = QPushButton("بازنشانی به پیش‌فرض")
        reset_btn.clicked.connect(self._reset_center_managers_to_default)
        button_layout.addWidget(reset_btn)
        refresh_btn = QPushButton("بارگذاری مجدد مدیران")
        refresh_btn.clicked.connect(self._refresh_all_manager_combos)
        button_layout.addWidget(refresh_btn)
        button_layout.addStretch()
        self._btn_reset_managers = reset_btn
        main_layout.addLayout(button_layout)
        group_box.setLayout(main_layout)
        return group_box

    def _create_manager_combo(self, parent: QWidget) -> QComboBox:
        """ساخت ComboBox قابل‌ویرایش برای انتخاب مدیران مراکز."""

        combo = QComboBox(parent)
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        combo.setMinimumContentsLength(1)
        combo.setToolTip("نام مدیر مرکز را انتخاب یا وارد کنید")
        return combo

    def _on_center_manager_changed(self, center_id: int, text: str) -> None:
        """ذخیرهٔ انتخاب مدیر مرکز پویا در تنظیمات."""

        cleaned = text.strip()
        if cleaned:
            self._prefs.set_center_manager(center_id, cleaned)
        else:
            self._prefs.clear_center_manager(center_id)

    def _refresh_manager_combo(
        self, center_id: int, combo: QComboBox, names: list[str] | None = None
    ) -> None:
        """پر کردن ComboBox با لیست مدیران."""

        combo.blockSignals(True)
        combo.clear()
        source_names = names
        if source_names is None:
            try:
                source_names = self._load_manager_names_from_pool()
            except Exception:
                source_names = []
        if not source_names:
            source_names = self._get_default_managers()
        combo.addItems(source_names)
        preferred = self._prefs.get_center_manager(center_id, "")
        if preferred:
            combo.setCurrentText(preferred)
        combo.blockSignals(False)

    def _refresh_all_manager_combos(self) -> None:
        """بارگذاری مجدد تمام ComboBoxهای مدیران."""

        if not self._center_manager_combos:
            return
        names = self._load_manager_names_from_pool()
        if not names:
            names = self._get_default_managers()
        for center_id, combo in self._center_manager_combos.items():
            self._refresh_manager_combo(center_id, combo, list(names))
        self._append_log("✅ لیست مدیران به‌روزرسانی شد")

    def _reset_center_managers_to_default(self) -> None:
        """بازنشانی تمام مدیران به مقادیر پیش‌فرض Policy."""

        try:
            policy = load_policy()
        except Exception as exc:
            QMessageBox.warning(self, "Policy", f"خطا در بارگذاری Policy: {exc}")
            return
        for center in policy.center_management.centers:
            if center.id == 0:
                continue
            combo = self._center_manager_combos.get(center.id)
            if combo is None:
                continue
            default_manager = center.default_manager or ""
            combo.blockSignals(True)
            combo.setCurrentText(default_manager)
            combo.blockSignals(False)
            self._on_center_manager_changed(center.id, default_manager)
        self._append_log("✅ مدیران به پیش‌فرض Policy بازنشانی شدند")

    def get_center_manager_map(self) -> Dict[int, List[str]]:
        """دریافت نگاشت مراکز به مدیران از UI."""

        result: Dict[int, List[str]] = {}
        for center_id, combo in self._center_manager_combos.items():
            manager = combo.currentText().strip()
            if manager:
                result[int(center_id)] = [manager]
        return result

    def _get_default_managers(self) -> List[str]:
        """دریافت لیست پیش‌فرض مدیران از Policy."""

        try:
            policy = load_policy()
            managers: List[str] = []
            seen: set[str] = set()
            for center in policy.center_management.centers:
                if center.default_manager:
                    text = center.default_manager.strip()
                    if text and text not in seen:
                        managers.append(text)
                        seen.add(text)
            return managers if managers else ["شهدخت کشاورز", "آیناز هوشمند"]
        except Exception:
            return ["شهدخت کشاورز", "آیناز هوشمند"]

    def _load_manager_names_from_pool(self) -> List[str]:
        """بارگذاری نام مدیران از فایل استخر با error handling پیشرفته.

        Returns:
            List[str]: لیست نام‌های منحصربه‌فرد مدیران

        Note:
            در صورت بروز هرگونه خطا، لیست پیش‌فرض برگردانده می‌شود و خطا ثبت می‌گردد.
        """

        try:
            path_text = self._picker_pool.text().strip()
            if not path_text:
                self._append_log("⚠️ مسیر فایل استخر مشخص نشده است")
                return self._get_default_managers()

            pool_path = Path(path_text)
            if not pool_path.exists():
                self._append_log("❌ فایل استخر یافت نشد")
                QMessageBox.warning(
                    self,
                    "فایل یافت نشد",
                    f"فایل استخر در مسیر زیر وجود ندارد:\n{pool_path}\n\n"
                    "لطفاً از تب 'فایل‌ها' فایل استخر را انتخاب کنید.",
                )
                return self._get_default_managers()

            if pool_path.is_dir():
                self._append_log("❌ مسیر انتخاب‌شده یک پوشه است")
                QMessageBox.warning(
                    self,
                    "مسیر نامعتبر",
                    "مسیر انتخاب‌شده یک پوشه است. لطفاً فایل معتبر انتخاب کنید.",
                )
                return self._get_default_managers()

            pool_df = pd.read_excel(pool_path)
            canonical_pool = canonicalize_headers(pool_df, header_mode="en")
            if "manager_name" not in canonical_pool.columns:
                self._append_log("❌ ستون manager_name در فایل استخر وجود ندارد")
                QMessageBox.warning(
                    self,
                    "ستون ضروری یافت نشد",
                    "ستون 'manager_name' در فایل استخر وجود ندارد.\n\n"
                    "لطفاً از صحت فایل اطمینان حاصل کنید.",
                )
                return self._get_default_managers()

            managers = canonical_pool["manager_name"].dropna().unique().tolist()
            if not managers:
                self._append_log("⚠️ هیچ مدیری در فایل استخر یافت نشد")
                QMessageBox.information(
                    self,
                    "فهرست مدیران خالی است",
                    "هیچ مدیری در ستون 'manager_name' فایل استخر یافت نشد.\n\n"
                    "از مقادیر پیش‌فرض استفاده خواهد شد.",
                )
                return self._get_default_managers()

            cleaned_managers = [str(m).strip() for m in managers if str(m).strip()]
            self._append_log(f"✅ {len(cleaned_managers)} مدیر از استخر بارگذاری شد")
            return cleaned_managers or self._get_default_managers()

        except PermissionError:
            self._append_log("❌ دسترسی به فایل استخر امکان‌پذیر نیست")
            QMessageBox.critical(
                self,
                "خطای دسترسی",
                "دسترسی به فایل استخر امکان‌پذیر نیست.\n\n"
                "لطفاً از باز نبودن فایل در برنامه‌ای دیگر اطمینان حاصل کنید.",
            )
            return self._get_default_managers()
        except Exception as exc:  # pragma: no cover - خطای پیش‌بینی‌نشده
            self._append_log(f"❌ خطای غیرمنتظره در بارگذاری مدیران: {exc}")
            QMessageBox.critical(
                self,
                "خطای بارگذاری",
                f"خطای غیرمنتظره در بارگذاری مدیران:\n{exc}\n\n"
                "از مقادیر پیش‌فرض استفاده خواهد شد.",
            )
            return self._get_default_managers()

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
        self._append_log(f"ℹ️ پیشنهاد شمارنده: {status}")

    def _launch_cli(
        self,
        argv: Sequence[str],
        action: str,
        *,
        overrides: dict[str, object] | None = None,
        on_success: Callable[[], None] | None = None,
    ) -> None:
        """اجرای فرمان CLI با Worker و رعایت قرارداد progress."""

        override_payload = overrides or {}

        def _task(*, progress: ProgressFn) -> None:
            exit_code = cli.main(
                argv,
                progress_factory=lambda: progress,
                ui_overrides=override_payload,
            )
            if exit_code != 0:
                raise RuntimeError(f"کد خروج غیرصفر: {exit_code}")

        self._launch_worker(_task, action, on_success=on_success)

    def _launch_worker(
        self,
        func: Callable[..., None],
        action: str,
        *,
        on_success: Callable[[], None] | None = None,
    ) -> None:
        """اجرای تابع در Worker با آماده‌سازی UI."""

        self._progress.setValue(0)
        self._current_action = action
        running_text = f"{action} در حال اجرا…"
        self._status.setText(running_text)
        self._set_stage(action, "در انتظار گزارش پیشرفت")
        self._update_progress_caption(0, running_text)
        self._append_log(f"<b>▶️ شروع {action}</b>")
        self._disable_controls(True)
        self._success_hook = on_success

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

    def _apply_pref_default(self, picker: FilePicker, value: str | None) -> None:
        """اگر ورودی خالی بود، مقدار پیش‌فرض را از تنظیمات اعمال می‌کند."""

        if value and not picker.text().strip():
            picker.setText(value)

    def _apply_resource_default(self, picker: FilePicker, value: str | None) -> None:
        """اعمال مسیر منابع باندل‌شده در صورت نبود ترجیح کاربر."""

        if value and not picker.text().strip():
            picker.setText(value)

    def _set_picker_button_text(self, picker: FilePicker) -> None:
        """تنظیم متن دکمهٔ انتخاب فایل برای ترجمهٔ جاری."""

        picker.set_button_text(self._t("action.browse", "انتخاب…"))

    def _save_log_to_file(self) -> None:
        """ذخیرهٔ محتوای لاگ در فایل متنی یا HTML."""

        filename, _ = QFileDialog.getSaveFileName(
            self,
            "ذخیره گزارش",
            "",
            "HTML (*.html *.htm);;Text (*.txt *.log);;All Files (*)",
        )
        if not filename:
            return
        path = Path(filename)
        suffix = path.suffix.lower()
        content = (
            self._log.toPlainText()
            if suffix in {".txt", ".log", ""}
            else self._log.toHtml()
        )
        try:
            path.write_text(content, encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(self, "ذخیره گزارش", f"امکان ذخیرهٔ فایل نبود: {exc}")
            return
        QMessageBox.information(self, "ذخیره گزارش", "گزارش با موفقیت ذخیره شد.")

    def _clear_log(self) -> None:
        """پاک کردن لاگ و بازگرداندن حالت خالی."""

        self._log.clear()
        self._sync_log_placeholder()

    def _append_log(self, text: str) -> None:
        """افزودن پیام به لاگ با برجسته کردن خطاها."""

        message = str(text or "")
        lowered = message.lower()
        if ("error" in lowered or "خطا" in message) and "<span" not in message:
            html = f'<span style="color:#c62828">{message}</span>'
        else:
            html = message
        self._log.append(html)
        self._sync_log_placeholder()

    def _sync_log_placeholder(self) -> None:
        """به‌روزرسانی وضعیت نمایش placeholder لاگ."""

        if self._log_stack is None or self._log_placeholder is None:
            return
        target = self._log if self._log.toPlainText().strip() else self._log_placeholder
        self._log_stack.setCurrentWidget(target)

    def _determine_last_output_path(self) -> str:
        """بررسی آخرین خروجی‌های ذخیره شده در تنظیمات."""

        return (
            self._prefs.last_sabt_output_allocate
            or self._prefs.last_sabt_output_rule
            or self._prefs.last_alloc_output
        )

    def _open_last_output_folder(self) -> None:
        """باز کردن پوشهٔ خروجی ذخیره شده در سیستم عامل."""

        path_text = self._determine_last_output_path()
        if not path_text:
            QMessageBox.information(self, "مسیر موجود نیست", "ابتدا یک خروجی تولید کنید.")
            return
        path = Path(path_text)
        folder = path if path.is_dir() else path.parent
        if not folder.exists():
            QMessageBox.warning(
                self, "پوشه یافت نشد", f"پوشهٔ ذخیره‌شده در دسترس نیست: {folder}"
            )
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(folder.resolve())))

    def _update_output_folder_button_state(self) -> None:
        """فعال/غیرفعال کردن دکمهٔ باز کردن پوشه بر اساس Prefs."""

        available = bool(self._determine_last_output_path())
        if self._btn_open_output_folder is not None:
            self._btn_open_output_folder.setEnabled(available)
        if self._btn_open_output_shortcut is not None:
            self._btn_open_output_shortcut.setEnabled(available)

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
        self._set_stage(self._current_action, safe_msg)
        self._update_progress_caption(self._progress.value(), safe_msg)
        self._append_log(f"{pct}% | {safe_msg}")
        self._update_status_bar_state("running")

    @Slot(bool, object)
    def _on_finished(self, success: bool, error: object | None) -> None:
        """پایان عملیات را مدیریت کرده و پیام مناسب را نمایش می‌دهد."""

        self._disable_controls(False)
        self._worker = None
        hook, self._success_hook = self._success_hook, None

        if error is not None:
            msg = str(error)
            if isinstance(error, FileNotFoundError):
                color = "#c00"
                QMessageBox.critical(self, self._t("status.error", "خطا"), msg)
            elif isinstance(error, PermissionError):
                color = "#c00"
                QMessageBox.critical(self, self._t("status.error", "خطا"), msg)
            elif isinstance(error, ValueError):
                color = "#b58900"
                QMessageBox.warning(self, self._t("status.error", "خطا"), msg)
            else:
                color = "#c00"
                QMessageBox.critical(self, self._t("status.error", "خطا"), msg)
            self._status.setText(self._t("status.error", "خطا"))
            self._set_stage(self._t("status.error", "خطا"), msg)
            self._update_progress_caption(self._progress.value(), self._t("status.error", "خطا"))
            self._append_log(f'<span style="color:{color}">❌ {msg}</span>')
            self._update_status_bar_state("error")
            return

        if not success:
            self._status.setText(self._t("status.cancelled", "لغو شد"))
            self._set_stage(
                self._t("status.cancelled", "لغو شد"),
                self._t("status.cancelled.detail", "عملیات متوقف شد"),
            )
            self._update_progress_caption(
                self._progress.value(), self._t("status.cancelled", "لغو شد")
            )
            self._append_log(f'⚠️ {self._t("status.cancelled.detail", "عملیات متوقف شد")}')
            self._update_status_bar_state("ready")
            return

        self._progress.setValue(100)
        self._status.setText(self._t("status.complete", "کامل"))
        self._set_stage(
            self._current_action, self._t("status.complete.detail", "عملیات با موفقیت پایان یافت")
        )
        self._update_progress_caption(100, self._t("status.complete", "کامل"))
        self._append_log(
            f'<span style="color:#2e7d32">✅ {self._t("status.complete.detail", "عملیات با موفقیت پایان یافت")}</span>'
        )
        self._update_status_bar_state("ready")
        if hook is not None:
            try:
                hook()
            except Exception as exc:  # pragma: no cover - unexpected UI failure
                self._append_log(f"⚠️ خطا در ذخیره تنظیمات: {exc}")

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
