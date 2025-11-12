"""مدیریت جدول‌های Excel با نام‌گذاری یکتا و خنثی."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List

import pandas as pd

__all__ = [
    "TableNameRegistry",
    "build_xlsxwriter_table",
    "build_openpyxl_table",
    "dedupe_headers",
]


_SLUG_CLEANER = re.compile(r"[^0-9A-Za-z_]+")
_UNDERSCORE_NORMALIZER = re.compile(r"_+")
_TABLE_PREFIX = "tbl_"
_MAX_NAME_LENGTH = 64


@dataclass
class TableNameRegistry:
    """نگه‌دارندهٔ نام‌های یکتا برای جدول‌های Excel.

    مثال::

        >>> registry = TableNameRegistry()
        >>> registry.reserve("1-گزارش")
        'tbl_t_1'
        >>> registry.reserve("1-گزارش")
        'tbl_t_1_2'
    """

    taken: set[str] = field(default_factory=set)

    def _slugify(self, sheet_name: str) -> str:
        raw = (sheet_name or "sheet").strip()
        cleaned = _SLUG_CLEANER.sub("_", raw)
        normalized = _UNDERSCORE_NORMALIZER.sub("_", cleaned).strip("_")
        if not normalized:
            normalized = "sheet"
        if not normalized[0].isalpha():
            normalized = f"t_{normalized}"
        max_slug_length = _MAX_NAME_LENGTH - len(_TABLE_PREFIX)
        return normalized[:max_slug_length]

    def reserve(self, sheet_name: str) -> str:
        """برگرداندن نام جدول یکتا با پیشوند ``tbl_``."""

        slug = self._slugify(sheet_name)
        candidate = f"{_TABLE_PREFIX}{slug}"
        index = 2
        while candidate in self.taken or len(candidate) > _MAX_NAME_LENGTH:
            suffix = f"_{index}"
            base = slug[: _MAX_NAME_LENGTH - len(_TABLE_PREFIX) - len(suffix)]
            candidate = f"{_TABLE_PREFIX}{base}{suffix}"
            index += 1
        self.taken.add(candidate)
        return candidate


def dedupe_headers(columns: Iterable[object]) -> List[str]:
    """تولید هدرهای یکتا با حفظ ترتیب ورودی."""

    seen: Dict[str, int] = {}
    deduped: List[str] = []
    for column in columns:
        text = str(column)
        base = text if text else "Column"
        count = seen.get(base, 0) + 1
        seen[base] = count
        deduped.append(base if count == 1 else f"{base}_{count}")
    return deduped


def _build_columns(df: pd.DataFrame) -> List[dict[str, str]]:
    """تولید آرایهٔ ستون‌ها با هدرهای یکتا برای جدول‌های Excel."""

    return [{"header": header} for header in dedupe_headers(df.columns)]


def build_xlsxwriter_table(df: pd.DataFrame, table_name: str) -> dict[str, object]:
    """تولید پیکربندی جدول برای xlsxwriter."""

    return {
        "name": table_name,
        "style": "Table Style Light 1",
        "columns": _build_columns(df),
    }


def build_openpyxl_table(table_name: str, ref: str, headers: List[str]):
    """ساخت نمونهٔ جدول openpyxl با استایل خنثی و هدر یکتا."""

    from openpyxl.worksheet.table import Table, TableColumn, TableStyleInfo

    table = Table(displayName=table_name, ref=ref)
    table.tableColumns = [
        TableColumn(id=idx, name=header) for idx, header in enumerate(headers, start=1)
    ]
    table.tableStyleInfo = TableStyleInfo(
        name="TableStyleLight1",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=False,
        showColumnStripes=False,
    )
    return table
