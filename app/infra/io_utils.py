"""توابع ورودی/خروجی Excel در لایهٔ زیرساخت (بدون منطق دامنه).
این ماژول فقط عملیات فایل را مدیریت می‌کند و در Core فراخوانی نمی‌شود.
"""
from __future__ import annotations
import contextlib
import os
import re
import tempfile
from os import PathLike
from pathlib import Path
from typing import Dict, Iterator, List, Tuple
import pandas as pd
from app.core.common.columns import CANON_EN_TO_FA, HeaderMode, canonicalize_headers
from app.core.policy_loader import get_policy

__all__ = [
    "write_xlsx_atomic",
    "read_excel_first_sheet",
    "read_crosswalk_workbook",
]

ALT_CODE_COLUMN = "کد جایگزین"
_INVALID_SHEET_CHARS = re.compile(r"[\\/*?:\[\]]")
_STRING_EXPORT_KEYS: Tuple[str, ...] = ("alias", "mentor_id", "postal_code")
_INT_EXPORT_KEYS: Tuple[str, ...] = ("group_code", "school_code")

def _safe_sheet_name(name: str, taken: set[str]) -> str:
    """اصلاح و یکتا‌سازی نام شیت مطابق محدودیت‌های Excel."""
    base = _INVALID_SHEET_CHARS.sub(" ", (name or "Sheet").strip()) or "Sheet"
    base = base[:31]
    candidate = base
    index = 2
    while candidate in taken or not candidate:
        suffix = f" ({index})"
        candidate = (base[: max(0, 31 - len(suffix))] + suffix).rstrip()
        index += 1
    taken.add(candidate)
    return candidate

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    ادغام ستون‌های تکراری در یک دیتافریم.
    اگر چندین ستون با نام یکسان وجود داشته باشد، فقط یک ستون نگهداری می‌شود
    (اولین ستون پر شده با مقادیر خالی از ستون‌های بعدی).
    """
    if df.empty:
        return df
    # بررسی وجود ستون تکراری
    if not any(df.columns.duplicated()):
        return df
    
    unique_columns = df.columns.unique()
    result = pd.DataFrame(index=df.index)
    
    for col in unique_columns:
        # گرفتن تمام ستون‌های با این نام
        cols_with_name = df.loc[:, df.columns == col]
        if cols_with_name.shape[1] == 1:
            result[col] = cols_with_name.iloc[:, 0]
        else:
            # استفاده از bfill برای پر کردن مقادیر خالی
            filled = cols_with_name.bfill(axis=1)
            result[col] = filled.iloc[:, 0]
    
    return result

def _ensure_series(df: pd.DataFrame, column: str) -> pd.Series:
    """
    تضمین اینکه خروجی df[column] یک Series باشد.
    اگر ستون تکراری وجود داشته باشد، اولین ستون را برگرداند.
    """
    value = df[column]
    if isinstance(value, pd.DataFrame):
        # اگر چندین ستون با نام یکسان باشد، اولین ستون را انتخاب می‌کنیم
        return value.iloc[:, 0]
    return value

def _prepare_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """تضمین نوع دادهٔ مناسب برای خروجی Excel."""
    result = df.copy()
    # اول از همه، ستون‌های تکراری را ادغام می‌کنیم
    result = _coalesce_duplicate_columns(result)
    
    for key in _STRING_EXPORT_KEYS:
        fa_name = CANON_EN_TO_FA.get(key, key)
        for column_name in (fa_name, key):
            if column_name in result.columns:
                result[column_name] = result[column_name].astype("string")
    
    for key in _INT_EXPORT_KEYS:
        fa_name = CANON_EN_TO_FA.get(key, key)
        for column_name in (fa_name, key):
            if column_name in result.columns:
                numeric = pd.to_numeric(result[column_name], errors="coerce")
                result[column_name] = numeric.astype("Int64")
    
    return result

def _apply_excel_formatting(
    writer: pd.ExcelWriter,
    *,
    engine: str,
    rtl: bool,
    font_name: str | None,
    sheet_frames: Dict[str, pd.DataFrame],
) -> None:
    """اعمال تنظیمات RTL و فونت پیش‌فرض برای خروجی Excel."""
    if not rtl and not font_name:
        return
    
    if engine == "xlsxwriter":
        workbook = writer.book  # type: ignore[attr-defined]
        fmt = workbook.add_format({"font_name": font_name}) if font_name else None
        for worksheet in writer.sheets.values():
            if rtl:
                worksheet.right_to_left()
            if fmt is not None:
                worksheet.set_column(0, 16384, None, fmt)
        return

    if engine != "openpyxl":
        return

    try:
        from openpyxl.styles import Font
    except Exception:  # pragma: no cover - dependency edge case
        Font = None  # type: ignore[assignment]

    workbook = writer.book  # type: ignore[attr-defined]
    for sheet_name, df in sheet_frames.items():
        worksheet = workbook[sheet_name]
        if rtl:
            worksheet.sheet_view.rightToLeft = True
        if not font_name or Font is None:
            continue
        
        # اضافه کردن چک ستون‌های تکراری قبل از پردازش
        df = _coalesce_duplicate_columns(df)
        
        header_font = Font(name=font_name)
        for cell in next(worksheet.iter_rows(min_row=1, max_row=1), []):
            cell.font = header_font
        if df.empty:
            continue
        
        # تغییر روش شناسایی ستون‌های متنی برای اجتناب از خطا
        text_columns = []
        for idx, column in enumerate(df.columns, start=1):
            # استفاده از تابع ایمن برای دسترسی به ستون
            column_data = _ensure_series(df, column)
            if pd.api.types.is_string_dtype(column_data) or str(column_data.dtype) in {"object", "string"}:
                text_columns.append(idx)
        
        max_rows = min(len(df) + 1, 50)
        for col_idx in text_columns:
            for row in worksheet.iter_rows(
                min_row=2,
                max_row=max_rows,
                min_col=col_idx,
                max_col=col_idx,
            ):
                for cell in row:
                    cell.font = header_font

@contextlib.contextmanager
def _temporary_file_path(*, suffix: str = "", directory: Path | str | None = None) -> Iterator[Path]:
    """مدیریت مسیر فایل موقتی با پاک‌سازی خودکار پس از اتمام کار."""
    fd, name = tempfile.mkstemp(suffix=suffix, dir=directory)
    os.close(fd)
    path = Path(name)
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)

def _pick_engine() -> str:
    """انتخاب بهترین engine نصب‌شده برای نوشتن Excel."""
    forced = os.getenv("EXCEL_ENGINE")
    if forced in {"openpyxl", "xlsxwriter"}:
        return forced

    for engine in ("openpyxl", "xlsxwriter"):
        try:
            __import__(engine)
        except Exception:
            continue
        return engine
    raise RuntimeError("هیچ‌کدام از 'openpyxl' یا 'xlsxwriter' نصب نیستند.")

def write_xlsx_atomic(
    data_dict: Dict[str, pd.DataFrame],
    filepath: Path | str | PathLike[str],
    *,
    rtl: bool | None = None,
    font_name: str | None = None,
    header_mode: HeaderMode | None = None,
) -> None:
    """نوشتن امن و اتمیک Excel با مدیریت نام شیت و انتخاب engine."""
    target_path = Path(filepath)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    policy = get_policy()
    if rtl is None:
        rtl = policy.excel.rtl
    if font_name is None:
        font_name = policy.excel.font_name
    if header_mode is None:
        header_mode = policy.excel.header_mode
    engine = _pick_engine()
    taken: set[str] = set()
    written_frames: Dict[str, pd.DataFrame] = {}
    
    # پیش‌پردازش دیتافریم‌ها
    processed_data = {}
    for sheet_name, df in data_dict.items():
        # ادغام ستون‌های تکراری قبل از هر پردازش دیگر
        df_clean = _coalesce_duplicate_columns(df.copy())
        processed_data[sheet_name] = df_clean
    
    with _temporary_file_path(suffix=".xlsx", directory=target_path.parent) as tmp_path:
        with pd.ExcelWriter(tmp_path, engine=engine) as writer:
            for sheet_name, df in processed_data.items():
                safe_name = _safe_sheet_name(str(sheet_name), taken)
                prepared = _prepare_dataframe_for_excel(df)
                if header_mode:
                    prepared = canonicalize_headers(prepared, header_mode=header_mode)
                prepared.to_excel(writer, sheet_name=safe_name, index=False)
                written_frames[safe_name] = prepared
            
            _apply_excel_formatting(
                writer,
                engine=engine,
                rtl=rtl,
                font_name=font_name,
                sheet_frames=written_frames,
            )
        os.replace(tmp_path, target_path)

def read_excel_first_sheet(path: Path | str | PathLike[str]) -> pd.DataFrame:
    """خواندن شیت اول فایل Excel به‌صورت DataFrame."""
    source = Path(path)
    try:
        with pd.ExcelFile(source) as workbook:
            if not workbook.sheet_names:
                raise ValueError(f"هیچ شیتی در فایل {source} یافت نشد.")
            return workbook.parse(
                workbook.sheet_names[0], dtype={ALT_CODE_COLUMN: str}
            )
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"فایل یافت نشد: {source}") from exc
    except Exception as exc:
        raise ValueError(f"خطا در خواندن فایل {source}: {exc}") from exc

def read_crosswalk_workbook(
    path: Path | str | PathLike[str],
) -> Tuple[pd.DataFrame, pd.DataFrame | None]:
    """خواندن شیت‌های موردنیاز Crosswalk."""
    source = Path(path)
    sheet_groups = "پایه تحصیلی (گروه آزمایشی)"
    try:
        with pd.ExcelFile(source) as workbook:
            if sheet_groups not in workbook.sheet_names:
                raise ValueError(f"شیت «{sheet_groups}» در Crosswalk یافت نشد")
            dtype_map = {ALT_CODE_COLUMN: str}
            groups_df = workbook.parse(sheet_groups, dtype=dtype_map)
            synonyms_df = None
            if "Synonyms" in workbook.sheet_names:
                synonyms_df = workbook.parse("Synonyms", dtype=dtype_map)
            return groups_df, synonyms_df
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"فایل Crosswalk یافت نشد: {source}") from exc
    except Exception as exc:
        raise ValueError(f"خطا در باز کردن Crosswalk: {exc}") from exc
