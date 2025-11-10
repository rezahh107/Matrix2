"""توابع ورودی/خروجی Excel در لایهٔ زیرساخت (بدون منطق دامنه).

این ماژول فقط عملیات فایل را مدیریت می‌کند و در Core فراخوانی نمی‌شود.
"""

from __future__ import annotations

import os
import re
import tempfile
from os import PathLike
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

__all__ = ["write_xlsx_atomic", "read_excel_first_sheet", "read_crosswalk_workbook"]

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
    """

    target_path = Path(filepath)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    engine = _pick_engine()
    taken: set[str] = set()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        with pd.ExcelWriter(tmp_path, engine=engine) as writer:
            for sheet_name, df in data_dict.items():
                safe_name = _safe_sheet_name(str(sheet_name), taken)
                df.to_excel(writer, sheet_name=safe_name, index=False)
        os.replace(tmp_path, target_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


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
            return workbook.parse(workbook.sheet_names[0])
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
            groups_df = workbook.parse(sheet_groups)
            synonyms_df = None
            if "Synonyms" in workbook.sheet_names:
                synonyms_df = workbook.parse("Synonyms")
            return groups_df, synonyms_df
    except FileNotFoundError as exc:  # pragma: no cover
        raise FileNotFoundError(f"فایل Crosswalk یافت نشد: {source}") from exc
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"خطا در باز کردن Crosswalk: {exc}") from exc
