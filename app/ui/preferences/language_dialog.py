"""دیالوگ ساده برای انتخاب زبان رابط کاربری."""

from __future__ import annotations

from PySide6.QtWidgets import QComboBox, QDialog, QDialogButtonBox, QFormLayout

from app.ui.i18n import Language
from app.ui.texts import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, UiTranslator

__all__ = ["LanguageDialog"]


class LanguageDialog(QDialog):
    """دیالوگ انتخاب زبان که مقدار انتخابی را در اختیار فراخوان می‌گذارد."""

    def __init__(
        self, language: Language, translator: UiTranslator, parent: QDialog | None = None
    ) -> None:
        super().__init__(parent)
        self._translator = translator
        self._combo = QComboBox(self)
        self._combo.addItem(translator.text("language.fa", "فارسی (fa-IR)"), Language.FA.value)
        self._combo.addItem(translator.text("language.en", "English (en-US)"), Language.EN.value)
        self._combo.setCurrentIndex(self._combo.findData(language.code))

        layout = QFormLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.addRow(translator.text("dialog.language.label", "انتخاب زبان رابط کاربری"), self._combo)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.Save).setText(translator.text("action.save", "ذخیره"))
        buttons.button(QDialogButtonBox.Cancel).setText(translator.text("action.cancel", "انصراف"))
        layout.addWidget(buttons)

        self.setLayout(layout)
        self.setWindowTitle(translator.text("dialog.language.title", "تنظیمات زبان"))

    def selected_language(self) -> Language:
        """بازگرداندن زبان انتخاب‌شده توسط کاربر."""

        data = self._combo.currentData()
        if isinstance(data, Language):
            return data
        if isinstance(data, str) and data in SUPPORTED_LANGUAGES:
            return Language.from_code(data)
        return Language.from_code(DEFAULT_LANGUAGE)
