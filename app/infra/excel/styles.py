"""مدیریت استایل‌های مشترک Excel بدون انفجار تعداد Style."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

_VAZIR_KEYWORDS = ("vazir", "vazirmatn")
_DEFAULT_STYLE_NAME = "EM_DefaultCenter"

__all__ = [
    "FontConfig",
    "is_vazir_family",
    "build_font_config",
    "ensure_xlsxwriter_format",
    "ensure_openpyxl_named_style",
]


@dataclass(frozen=True)
class FontConfig:
    """پیکربندی فونت خروجی با درنظرگرفتن Override برای Vazir."""

    name: Optional[str]
    size: Optional[int]

    @property
    def has_override(self) -> bool:
        return self.size is not None or self.name is not None


def is_vazir_family(font_name: Optional[str]) -> bool:
    """تشخیص اینکه فونت متعلق به خانوادهٔ Vazir/Vazirmatn است."""

    if not font_name:
        return False
    lowered = font_name.lower()
    return any(keyword in lowered for keyword in _VAZIR_KEYWORDS)


def build_font_config(font_name: Optional[str]) -> FontConfig:
    """تولید پیکربندی فونت با اعمال اندازهٔ ۸ برای Vazir.

    مثال::

        >>> build_font_config("Vazirmatn")
        FontConfig(name='Vazirmatn', size=8)
    """

    if not font_name:
        return FontConfig(name=None, size=None)
    size = 8 if is_vazir_family(font_name) else None
    return FontConfig(name=font_name, size=size)


def ensure_xlsxwriter_format(workbook: Any, font: FontConfig, *, header: bool = False):
    """ساخت Format مشترک xlsxwriter فقط با تنظیمات فونت.

    مثال::

        >>> fmt = ensure_xlsxwriter_format(workbook, build_font_config("Vazirmatn"))
        >>> fmt.get_font_name()
        'Vazirmatn'

    پارامتر ``header`` در صورت True یک Format جداگانه برای ردیف سرستون‌ها
    (با Bold) ایجاد یا بازیابی می‌کند تا فونت هدر همسو با بدنه باشد.
    """

    if not hasattr(workbook, "_em_format_cache"):
        workbook._em_format_cache = {}  # type: ignore[attr-defined]
    cache: Dict[Tuple[Optional[str], Optional[int], bool], Any] = workbook._em_format_cache  # type: ignore[attr-defined]
    key = (font.name, font.size, header)
    if key in cache:
        return cache[key]
    options: Dict[str, Any] = {}
    if font.name:
        options["font_name"] = font.name
    if font.size:
        options["font_size"] = font.size
    if header:
        options["bold"] = True
    fmt = workbook.add_format(options)
    cache[key] = fmt
    return fmt


def ensure_openpyxl_named_style(workbook: Any, font: FontConfig):
    """ساخت NamedStyle واحد برای استفادهٔ مجدد در openpyxl.

    مثال::

        >>> style = ensure_openpyxl_named_style(workbook, build_font_config("Vazirmatn"))
        >>> style
        'EM_Vazirmatn_8'
    """

    from openpyxl.styles import Font, NamedStyle

    if not hasattr(workbook, "_em_named_styles"):
        workbook._em_named_styles = {}  # type: ignore[attr-defined]
    cache: Dict[Tuple[Optional[str], Optional[int]], str] = workbook._em_named_styles  # type: ignore[attr-defined]

    key = (font.name, font.size)
    if key in cache:
        return cache[key]

    style_name = _DEFAULT_STYLE_NAME
    if font.name:
        sanitized = font.name.replace(" ", "")[:20]
        style_name = f"EM_{sanitized}"
    if font.size:
        style_name = f"{style_name}_{font.size}"

    if style_name in workbook.named_styles:
        cache[key] = style_name
        return style_name

    named_style = NamedStyle(name=style_name)
    if font.name or font.size:
        named_style.font = Font(name=font.name, size=font.size)
    try:
        workbook.add_named_style(named_style)
    except ValueError:
        # در صورت موجود بودن استایل با همین نام، از همان نمونهٔ قبلی استفاده می‌کنیم.
        pass
    cache[key] = style_name
    return style_name
