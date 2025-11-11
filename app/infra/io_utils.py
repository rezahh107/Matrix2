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

__all__ = [
    "write_xlsx_atomic",
    "read_excel_first_sheet",
    "read_crosswalk_workbook",
]

ALT_CODE_COLUMN = "کد جایگزین"

_INVALID_SHEET_CHARS = re.compile(r"[\\/*?:\[\]]")


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
        header_font = Font(name=font_name)
        for cell in next(worksheet.iter_rows(min_row=1, max_row=1), []):
            cell.font = header_font
        if df.empty:
            continue
        text_columns = [
            idx
            for idx, column in enumerate(df.columns, start=1)
            if str(df[column].dtype) in {"object", "string"}
        ]
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
    """مدیریت مسیر فایل موقتی با پاک‌سازی خودکار پس از اتمام کار.

    مثال ساده::

        >>> from pathlib import Path
        >>> from app.infra.io_utils import _temporary_file_path
        >>> with _temporary_file_path(suffix=".tmp", directory=Path("/tmp")) as tmp:  # doctest: +SKIP
        ...     _ = tmp.exists()

    Args:
        suffix: پسوند دلخواه برای فایل موقتی.
        directory: مسیر ساخت فایل موقتی (``Path`` یا ``str``؛ در صورت ``None`` از مقدار پیش‌فرض
            سیستم استفاده می‌شود).

    Yields:
        مسیر فایل موقتی که در پایان بلاک پاک می‌شود (در صورت موجود بودن).
    """

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
    rtl: bool = False,
    font_name: str | None = "Vazirmatn",
) -> None:
    """نوشتن امن و اتمیک Excel با مدیریت نام شیت و انتخاب engine.

    مثال ساده::

        >>> from pathlib import Path
        >>> import pandas as pd
        >>> from app.infra.io_utils import write_xlsx_atomic
        >>> tmp = Path("/tmp/out.xlsx")
        >>> write_xlsx_atomic({"Sheet/1": pd.DataFrame({"a": [1]})}, tmp)

    Args:
        data_dict: نگاشت نام شیت به دیتافریم.
        filepath: مسیر فایل خروجی (``str``/``Path``). در صورت نبود پوشهٔ مقصد
            ساخته می‌شود.
        rtl: در صورت True، شیت‌ها راست‌به‌چپ خواهند شد.
        font_name: نام فونت پیش‌فرض برای نوشتن (در صورت ``None`` فونت پیش‌فرض
            Excel استفاده می‌شود).
    """

    target_path = Path(filepath)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    engine = _pick_engine()
    taken: set[str] = set()
    written_frames: Dict[str, pd.DataFrame] = {}
    with _temporary_file_path(suffix=".xlsx", directory=target_path.parent) as tmp_path:
        with pd.ExcelWriter(tmp_path, engine=engine) as writer:
            for sheet_name, df in data_dict.items():
                safe_name = _safe_sheet_name(str(sheet_name), taken)
                df.to_excel(writer, sheet_name=safe_name, index=False)
                written_frames[safe_name] = df
            _apply_excel_formatting(
                writer,
                engine=engine,
                rtl=rtl,
                font_name=font_name,
                sheet_frames=written_frames,
            )
        os.replace(tmp_path, target_path)


def read_excel_first_sheet(path: Path | str | PathLike[str]) -> pd.DataFrame:
    """خواندن شیت اول فایل Excel به‌صورت DataFrame.

    مثال ساده::

        >>> from pathlib import Path
        >>> df = read_excel_first_sheet(Path("/tmp/sample.xlsx"))  # doctest: +SKIP

    Args:
        path: مسیر فایل Excel ورودی.

    Returns:
        DataFrame مربوط به اولین شیت موجود در فایل.
    """

    source = Path(path)
    try:
        with pd.ExcelFile(source) as workbook:
            if not workbook.sheet_names:
                raise ValueError(f"هیچ شیتی در فایل {source} یافت نشد.")
            return workbook.parse(
                workbook.sheet_names[0], dtype={ALT_CODE_COLUMN: str}
            )
    except FileNotFoundError as exc:  # pragma: no cover - propagate خوانا
        raise FileNotFoundError(f"فایل یافت نشد: {source}") from exc
    except Exception as exc:  # pragma: no cover - پیام خوانا
        raise ValueError(f"خطا در خواندن فایل {source}: {exc}") from exc


def read_crosswalk_workbook(
    path: Path | str | PathLike[str],
) -> Tuple[pd.DataFrame, pd.DataFrame | None]:
    """خواندن شیت‌های موردنیاز Crosswalk.

    مثال ساده::

        >>> groups_df, synonyms_df = read_crosswalk_workbook("crosswalk.xlsx")  # doctest: +SKIP

    Args:
        path: مسیر فایل Crosswalk.

    Returns:
        دوگانهٔ `(groups_df, synonyms_df)` که دومی می‌تواند ``None`` باشد.
    """

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
    except FileNotFoundError as exc:  # pragma: no cover
        raise FileNotFoundError(f"فایل Crosswalk یافت نشد: {source}") from exc
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"خطا در باز کردن Crosswalk: {exc}") from exc
