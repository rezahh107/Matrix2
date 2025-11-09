# -*- coding: utf-8 -*-
"""
ماژول io_utils: توابع ایمن I/O برای پروژه Eligibility Matrix.
این ماژول شامل توابعی برای بارگذاری و نوشتن ایمن فایل‌های Excel است.
همه توابع به گونه‌ای پیاده‌سازی شده‌اند که بدون نشت استثنا عمل کنند و مقادیر پیش‌فرض را در صورت خطا بازگردانند.
"""
from __future__ import annotations
from pathlib import Path
import os
import tempfile
from typing import Any
import pandas as pd


def load_first_sheet(path: Path) -> pd.DataFrame:
    """
    Loads the first sheet of an Excel file and returns it as a DataFrame.

    Args:
        path (Path): Path to the Excel file.

    Returns:
        pd.DataFrame: The data in the first sheet of the Excel file.
                     Returns an empty DataFrame if the file is invalid or does not exist.
    """
    try:
        if not path.exists() or not path.is_file():
            return pd.DataFrame()
        # استفاده از pandas برای خواندن اولین شیت
        df = pd.read_excel(str(path), sheet_name=0, header=0)
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


def write_xlsx_atomic(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    """
    Writes data to an Excel file atomically by using a temporary file,
    then replacing the final file with the temporary one.

    Args:
        path (Path): The path to the output Excel file.
        sheets (dict[str, pd.DataFrame]): A dictionary of sheet names and their corresponding DataFrames.
    """
    try:
        # ایجاد یک فایل موقت در همان دایرکتوری مقصد برای اطمینان از همان فایل‌سیستم
        temp_dir = path.parent
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix='.xlsx', dir=temp_dir) as tmp_file:
            tmp_path = Path(tmp_file.name)

        # نوشتن داده‌ها در فایل موقت با استفاده از xlsxwriter در یک بلاک مدیریت منابع (with)
        with pd.ExcelWriter(str(tmp_path), engine='xlsxwriter', options={'strings_to_numbers': False}) as writer:
            for sheet_name, df in sheets.items():
                # اطمینان از اینکه نام شیت معتبر است و حداکثر 31 کاراکتر دارد
                safe_sheet_name = str(sheet_name)[:31] if sheet_name else "Sheet1"
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

        # جایگزینی اتمیک فایل نهایی با فایل موقت
        os.replace(str(tmp_path), str(path))
    except Exception:
        # در صورت بروز خطا، فایل موقت را حذف می‌کنیم (اگر ایجاد شده باشد)
        try:
            os.unlink(str(tmp_path))
        except NameError:
            # متغیر tmp_path ممکن است در صورت بروز خطا قبل از ایجاد آن تعریف نشده باشد
            pass
        # تابع هیچ چیزی را پس نمی‌دهد و استثنا را نیز نمی‌فرستد بیرون


def safe_int_column(df: pd.DataFrame, col: str, default=0) -> pd.Series:
    """
    Safely converts a column of values to integers, with invalid values replaced by the default.

    Args:
        df (pd.DataFrame): The DataFrame containing the column.
        col (str): The name of the column to convert.
        default (int): The default value to use for invalid entries.
            - Invalid entries include NaN, inf, non-numeric values, etc.

    Returns:
        pd.Series: The converted column. In case of invalid input or conversion errors, a series filled with the default value is returned.
    """
    try:
        # اطمینان از وجود ستون
        if col not in df.columns:
            return pd.Series([default] * len(df), dtype='int64', name=col)

        series = df[col]

        # تبدیل به عدد، با تبدیل مقادیر غیرقابل تبدیل به NaN
        numeric_series = pd.to_numeric(series, errors='coerce')

        # جایگزینی NaN و inf با مقدار پیش‌فرض و سپس تبدیل به int
        safe_numeric_series = numeric_series.fillna(default)
        safe_numeric_series = safe_numeric_series.replace([float('inf'), float('-inf')], default)
        int_series = safe_numeric_series.astype('int64', errors='ignore')

        # در صورت عدم موفقیت در تبدیل نوع، مجدداً مقدار پیش‌فرض را قرار دهید
        if int_series.isna().any():
            int_series = int_series.fillna(default).astype('int64')

        return int_series
    except Exception:
        # در صورت بروز هرگونه خطا، یک سری جدید با مقادیر پیش‌فرض بازگردانده می‌شود
        return pd.Series([default] * len(df), dtype='int64', name=col)
