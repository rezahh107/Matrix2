"""پنل لاگ با ظاهر هماهنگ و ترجمه‌پذیر."""

from __future__ import annotations

from typing import Callable

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QStackedLayout,
    QTextEdit,
    QVBoxLayout,
)

from app.ui.texts import UiTranslator
from app.ui.theme import Theme

__all__ = ["LogPanel"]


class LogPanel(QFrame):
    """ویجت ترکیبی برای نمایش و مدیریت لاگ."""

    def __init__(self, translator: UiTranslator, theme: Theme, parent: QFrame | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("logPanel")
        self._translator = translator
        self._theme = theme

        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(theme.spacing)

        stack_host = QFrame(self)
        stack_host.setObjectName("logStackHost")
        stack_layout = QStackedLayout(stack_host)
        stack_layout.setContentsMargins(0, 0, 0, 0)
        stack_layout.setStackingMode(QStackedLayout.StackingMode.StackAll)

        self._placeholder = QLabel(self._t("log.placeholder", "هنوز گزارشی ثبت نشده است."))
        self._placeholder.setAlignment(Qt.AlignCenter)
        self._placeholder.setObjectName("logPlaceholder")
        self._placeholder.setWordWrap(True)

        self._text = QTextEdit(self)
        self._text.setReadOnly(True)
        self._text.setObjectName("textLog")

        stack_layout.addWidget(self._placeholder)
        stack_layout.addWidget(self._text)
        self._stack = stack_layout
        self._text.textChanged.connect(self._sync_placeholder)
        self._sync_placeholder()

        root.addWidget(stack_host, 1)

        buttons_col = QVBoxLayout()
        buttons_col.setSpacing(theme.spacing)
        self._btn_clear = QPushButton(self._t("log.clear", "پاک کردن گزارش"), self)
        self._btn_clear.setObjectName("btnClearLog")
        self._btn_save = QPushButton(self._t("log.save", "ذخیره گزارش…"), self)
        self._btn_save.setObjectName("btnSaveLog")
        buttons_col.addWidget(self._btn_clear)
        buttons_col.addWidget(self._btn_save)
        buttons_col.addStretch(1)
        root.addLayout(buttons_col, 0)

        self.apply_theme(theme)

    # ------------------------------------------------------------------ رابط دسترسی
    @property
    def text_edit(self) -> QTextEdit:
        """دسترسی مستقیم به QTextEdit داخلی."""

        return self._text

    @property
    def clear_button(self) -> QPushButton:
        """دریافت دکمه پاک‌سازی."""

        return self._btn_clear

    @property
    def save_button(self) -> QPushButton:
        """دریافت دکمه ذخیره."""

        return self._btn_save

    # ------------------------------------------------------------------ ترجمه و تم
    def update_translator(self, translator: UiTranslator) -> None:
        """به‌روزرسانی متن‌های نمایش داده شده بر اساس مترجم جدید."""

        self._translator = translator
        self._btn_clear.setText(self._t("log.clear", "پاک کردن گزارش"))
        self._btn_save.setText(self._t("log.save", "ذخیره گزارش…"))
        self._placeholder.setText(self._t("log.placeholder", "هنوز گزارشی ثبت نشده است."))
        self._sync_placeholder()

    def apply_theme(self, theme: Theme) -> None:
        """اعمال تم روی پس‌زمینه و دکمه‌ها."""

        self._theme = theme
        self.setStyleSheet(
            f"#logPanel{{background:{theme.log_bg.name()};"
            f"border:1px solid {theme.log_border.name()};border-radius:{theme.radius}px;}}"
            f"#logPlaceholder{{color:{theme.text_muted.name()};}}"
            f"QTextEdit#textLog{{border:none;background:transparent;}}"
            f"QPushButton#btnClearLog, QPushButton#btnSaveLog{{"
            f"background:{theme.surface_alt.name()};border:1px solid {theme.border.name()};"
            f"border-radius:{theme.radius - 2}px;padding:6px 10px;}}"
            f"QPushButton#btnClearLog:hover, QPushButton#btnSaveLog:hover{{"
            f"background:{theme.surface.name()};}}"
        )

    # ------------------------------------------------------------------ داخلی
    def sync_placeholder(self) -> None:
        """همگام‌سازی وضعیت نمایش Placeholder."""

        self._sync_placeholder()

    def _sync_placeholder(self) -> None:
        target = self._text if self._text.toPlainText().strip() else self._placeholder
        self._stack.setCurrentWidget(target)

    def _t(self, key: str, fallback: str) -> str:
        return self._translator.text(key, fallback)

    def connect_clear(self, slot: Callable[[], None]) -> None:
        """اتصال سیگنال پاک کردن به تابع مورد نظر."""

        self._btn_clear.clicked.connect(slot)

    def connect_save(self, slot: Callable[[], None]) -> None:
        """اتصال سیگنال ذخیره به تابع مورد نظر."""

        self._btn_save.clicked.connect(slot)

