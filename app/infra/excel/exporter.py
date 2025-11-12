"""اعمال تنظیمات یکتای خروجی Excel (فونت، RTL، جدول‌ها)."""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .styles import build_font_config, ensure_openpyxl_named_style, ensure_xlsxwriter_format
from .tables import (
    TableNameRegistry,
    build_openpyxl_table,
    build_xlsxwriter_table,
    dedupe_headers,
)

__all__ = ["apply_workbook_formatting"]


_LOGGER = logging.getLogger(__name__)
_FONT_WARNING_EMITTED = False


def _warn_fonts_not_embedded() -> None:
    global _FONT_WARNING_EMITTED
    if not _FONT_WARNING_EMITTED:
        _LOGGER.warning(
            "فونت‌های سفارشی در فایل‌های Excel جاسازی نمی‌شوند؛ برای اشتراک‌گذاری امن، خروجی را به PDF تبدیل کنید."
        )
        _FONT_WARNING_EMITTED = True


def _format_xlsxwriter(
    writer: pd.ExcelWriter,
    sheet_frames: Dict[str, pd.DataFrame],
    *,
    rtl: bool,
    font_name: str | None,
) -> None:
    workbook = writer.book  # type: ignore[attr-defined]
    worksheet_map = writer.sheets  # type: ignore[attr-defined]
    font = build_font_config(font_name)
    body_fmt = ensure_xlsxwriter_format(workbook, font)
    header_fmt = ensure_xlsxwriter_format(workbook, font, header=True)
    table_names = TableNameRegistry()

    for sheet_name, df in sheet_frames.items():
        worksheet = worksheet_map[sheet_name]
        if rtl:
            worksheet.right_to_left()
        worksheet.freeze_panes(1, 0)
        column_count = len(df.columns)
        if column_count:
            worksheet.set_row(0, None, header_fmt)
            datetime_columns = {
                column
                for column, dtype in df.dtypes.items()
                if pd.api.types.is_datetime64_any_dtype(dtype)
            }
            for idx, column in enumerate(df.columns):
                if column in datetime_columns:
                    continue
                worksheet.set_column(idx, idx, None, body_fmt)
        if df.empty or column_count == 0:
            continue
        table_name = table_names.reserve(sheet_name)
        last_row = len(df)
        last_col = df.shape[1] - 1
        worksheet.add_table(0, 0, last_row, last_col, build_xlsxwriter_table(df, table_name))


def _format_openpyxl(
    writer: pd.ExcelWriter,
    sheet_frames: Dict[str, pd.DataFrame],
    *,
    rtl: bool,
    font_name: str | None,
) -> None:
    from openpyxl.utils import get_column_letter

    workbook = writer.book  # type: ignore[attr-defined]
    font = build_font_config(font_name)
    style_name = ensure_openpyxl_named_style(workbook, font)
    table_names = TableNameRegistry()

    for sheet_name, df in sheet_frames.items():
        worksheet = workbook[sheet_name]
        if rtl:
            worksheet.sheet_view.rightToLeft = True
        worksheet.freeze_panes = "A2"
        max_col = max(len(df.columns), worksheet.max_column)
        max_row = max(len(df) + 1, worksheet.max_row)
        if max_col and max_row:
            for row in worksheet.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_col):
                for cell in row:
                    cell.style = style_name
        if df.empty or df.shape[1] == 0:
            continue
        headers = dedupe_headers(df.columns)
        table_name = table_names.reserve(sheet_name)
        ref = f"A1:{get_column_letter(df.shape[1])}{len(df) + 1}"
        worksheet.add_table(build_openpyxl_table(table_name, ref, headers))
        for idx, header in enumerate(headers, start=1):
            worksheet.cell(row=1, column=idx, value=header)


def apply_workbook_formatting(
    writer: pd.ExcelWriter,
    *,
    engine: str,
    sheet_frames: Dict[str, pd.DataFrame],
    rtl: bool,
    font_name: str | None,
) -> None:
    """اعمال تنظیمات خروجی Excel پس از نوشتن DataFrameها.

    مثال::

        >>> with pd.ExcelWriter("/tmp/out.xlsx", engine="openpyxl") as writer:
        ...     df = pd.DataFrame({"A": [1, 2]})
        ...     df.to_excel(writer, sheet_name="Sheet", index=False)
        ...     apply_workbook_formatting(
        ...         writer,
        ...         engine="openpyxl",
        ...         sheet_frames={"Sheet": df},
        ...         rtl=True,
        ...         font_name="Vazirmatn",
        ...     )
    """

    _warn_fonts_not_embedded()

    if engine == "xlsxwriter":
        _format_xlsxwriter(writer, sheet_frames, rtl=rtl, font_name=font_name)
    elif engine == "openpyxl":
        _format_openpyxl(writer, sheet_frames, rtl=rtl, font_name=font_name)
