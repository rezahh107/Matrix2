"""ویجت انتخاب فایل عمومی برای UI."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtWidgets import QFileDialog, QHBoxLayout, QLineEdit, QPushButton, QWidget

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

        self._edit = QLineEdit(self)
        self._edit.setPlaceholderText(placeholder)

        self._button = QPushButton("انتخاب…", self)
        self._button.clicked.connect(self._pick)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._edit)
        layout.addWidget(self._button)

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
