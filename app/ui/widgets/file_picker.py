"""ویجت انتخاب فایل عمومی برای UI."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtCore import QFileInfo, Qt
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QFileDialog,
    QFileIconProvider,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QWidget,
)

__all__ = ["FilePicker"]


class FilePicker(QWidget):
    """ویجت ساده برای انتخاب مسیر فایل ورودی یا خروجی."""

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        *,
        save: bool = False,
        placeholder: str = "",
        dialog_filter: str = "Excel/CSV (*.xlsx *.xls *.xlsm *.csv);;All Files (*.*)",
    ) -> None:
        super().__init__(parent)
        self._save = save
        self._dialog_filter = dialog_filter
        self._icon_provider = QFileIconProvider()

        self._edit = QLineEdit(self)
        self._edit.setPlaceholderText(placeholder)
        self._edit.textChanged.connect(self._sync_icon)

        self._button = QPushButton("انتخاب…", self)
        self._button.setObjectName("secondaryButton")
        self._button.clicked.connect(self._pick)

        self._icon_label = QLabel(self)
        self._icon_label.setObjectName("fileIconLabel")
        self._icon_label.setFixedWidth(20)
        self._icon_label.setAlignment(Qt.AlignCenter)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        layout.addWidget(self._icon_label)
        layout.addWidget(self._edit)
        layout.addWidget(self._button)

        self._sync_icon("")

    def set_placeholder_text(self, text: str) -> None:
        """تنظیم placeholder فیلد ورودی."""

        self._edit.setPlaceholderText(text)

    def set_button_text(self, text: str) -> None:
        """تنظیم متن دکمه انتخاب."""

        self._button.setText(text)

    def text(self) -> str:
        """بازگرداندن مقدار متنی فعلی."""

        return self._edit.text().strip()

    def path(self) -> Path:
        """بازگرداندن مسیر به صورت :class:`Path`."""

        return Path(self.text()) if self.text() else Path()

    def setText(self, value: str) -> None:
        """تنظیم مقدار متنی فیلد."""

        self._edit.setText(value)

    def line_edit(self) -> QLineEdit:
        """دسترسی مستقیم به QLineEdit داخلی برای اتصال سیگنال‌ها."""

        return self._edit

    def _pick(self) -> None:
        """باز کردن دیالوگ انتخاب فایل و مقداردهی فیلد."""

        if self._save:
            path, _ = QFileDialog.getSaveFileName(self, "ذخیره خروجی", "", self._dialog_filter)
        else:
            path, _ = QFileDialog.getOpenFileName(self, "انتخاب فایل", "", self._dialog_filter)

        if path:
            self._edit.setText(path)

    def _sync_icon(self, text: str) -> None:
        """همگام‌سازی آیکون فایل بر اساس مسیر فعلی."""

        if not text:
            self._icon_label.setText("📁")
            return
        info = QFileInfo(text)
        icon: QIcon = self._icon_provider.icon(info)
        self._icon_label.clear()
        self._icon_label.setPixmap(icon.pixmap(16, 16))
