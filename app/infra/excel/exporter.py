"""توابع نوشتن خروجی‌های اکسل با رعایت Policy و دترمینیسم."""

from __future__ import annotations

from typing import Tuple

import pandas as pd

from app.core.common.policy import load_selection_reason_policy
from app.core.policy_loader import PolicyConfig

__all__ = ["write_selection_reasons_sheet"]

_SCHEMA_HASH_NAME = "__SELECTION_REASON_SCHEMA_HASH__"


def _should_create_table(df: pd.DataFrame | None) -> bool:
    """تشخیص می‌کند آیا نیاز به ساخت Table اکسل هست یا خیر."""

    return bool(df is not None and not df.empty and df.shape[1] > 0)


def _write_schema_hash_tag(writer: pd.ExcelWriter | None, hash_value: str) -> None:
    """هش اسکیما را به‌صورت Defined Name در فایل اکسل می‌نویسد."""

    if writer is None or not hash_value:
        return
    try:
        workbook = writer.book
    except AttributeError:  # pragma: no cover - اگر writer ساختار متفاوت داشت
        return
    try:
        engine = (writer.engine or "").lower()
    except AttributeError:
        engine = ""

    if engine == "xlsxwriter":
        workbook.define_name(_SCHEMA_HASH_NAME, f'"{hash_value}"')
        return

    if engine == "openpyxl":
        try:  # import درون‌تابعی برای جلوگیری از وابستگی سخت
            from openpyxl.workbook.defined_name import DefinedName
        except Exception:  # pragma: no cover - نباید در اجرا رخ دهد
            return

        defined_names = workbook.defined_names
        if _SCHEMA_HASH_NAME in defined_names:
            defined_names[_SCHEMA_HASH_NAME].attr_text = f'"{hash_value}"'
        else:
            defined_names.add(DefinedName(_SCHEMA_HASH_NAME, attr_text=f'"{hash_value}"'))


def write_selection_reasons_sheet(
    df_reasons: pd.DataFrame | None,
    writer: pd.ExcelWriter | None,
    policy: PolicyConfig,
) -> Tuple[str, pd.DataFrame]:
    """نوشتن شیت دلایل انتخاب پشتیبان روی ExcelWriter.

    Core نباید I/O انجام دهد؛ این تابع در لایه Infra اجرا می‌شود و مسئولیت دارد
    دترمینیسم و نوع ستون‌ها را تضمین کند.
    """

    config = load_selection_reason_policy(
        policy,
        expected_version=policy.version,
        on_mismatch="warn",
    )
    columns = list(config.columns)
    if not columns:
        columns = [
            "شمارنده",
            "کدملی",
            "نام",
            "نام خانوادگی",
            "شناسه پشتیبان",
            "دلیل انتخاب پشتیبان",
        ]

    if not config.enabled:
        empty = pd.DataFrame(columns=columns)
        empty.attrs["schema_hash"] = config.schema_hash
        return config.sheet_name, empty

    if df_reasons is None or df_reasons.empty:
        sanitized = pd.DataFrame({column: pd.Series(dtype="string") for column in columns})
        if "شمارنده" in sanitized.columns:
            sanitized["شمارنده"] = sanitized["شمارنده"].astype("Int64")
    else:
        sanitized = df_reasons.copy()
        for column in columns:
            if column not in sanitized.columns:
                sanitized[column] = ""
        sanitized = sanitized.loc[:, columns]
        if "شمارنده" in columns:
            sanitized["شمارنده"] = pd.Series(
                range(1, len(sanitized) + 1), index=sanitized.index, dtype="Int64"
            )
        for column in columns:
            if column == "شمارنده":
                continue
            sanitized[column] = sanitized[column].astype("string")

    sanitized.attrs["schema_hash"] = config.schema_hash

    if writer is not None:
        sanitized.to_excel(writer, index=False, sheet_name=config.sheet_name)
        if _should_create_table(sanitized):  # فعلاً Table نمی‌سازیم؛ شرط برای توسعهٔ آینده
            pass
        _write_schema_hash_tag(writer, config.schema_hash)
    return config.sheet_name, sanitized
