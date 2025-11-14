"""زیرسیستم خروجی ImportToSabt مطابق Policy-First."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

import pandas as pd

from app.core.common.columns import canonicalize_headers

GF_FIELD_TO_COL: Mapping[str, Sequence[str]] = {
    "GF_NationalCode": (
        "student_GF_NationalCode",
        "student_national_id",
        "student_national_code",
    ),
    "GF_Mobile": (
        "student_GF_Mobile",
        "student_mobile",
    ),
    "GF_FirstName": (
        "student_GF_FirstName",
        "student_first_name",
        "student_name",
    ),
    "GF_LastName": (
        "student_GF_LastName",
        "student_last_name",
        "student_family_name",
    ),
}

_DIGIT_TRANSLATION = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")

__all__ = [
    "GF_FIELD_TO_COL",
    "load_exporter_config",
    "prepare_allocation_export_frame",
    "build_sheet2_frame",
    "apply_alias_rule",
    "build_summary_frame",
    "build_errors_frame",
    "build_optional_sheet_frame",
    "ensure_template_workbook",
    "write_import_to_sabt_excel",
    "ImportToSabtExportError",
]


class ExporterConfigError(ValueError):
    """خطای اعتبارسنجی تنظیمات Exporter."""


class ImportToSabtExportError(ValueError):
    """خطای اعتبارسنجی داده برای خروجی ImportToSabt."""


def load_exporter_config(path: str | Path) -> dict:
    """بارگذاری و اعتبارسنجی حداقلی فایل تنظیمات خروجی."""

    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Exporter config not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as handle:
        cfg = json.load(handle)
    sheets = cfg.get("sheets")
    if not isinstance(sheets, MutableMapping):
        raise ExporterConfigError("'sheets' must be a dict in exporter config")
    sheet2 = sheets.get("Sheet2")
    if not isinstance(sheet2, MutableMapping):
        raise ExporterConfigError("Exporter config must define 'Sheet2'")
    columns = sheet2.get("columns")
    if not isinstance(columns, MutableMapping) or not columns:
        raise ExporterConfigError("'Sheet2.columns' must be a non-empty dict")
    return cfg


def prepare_allocation_export_frame(
    allocations_df: pd.DataFrame,
    students_df: pd.DataFrame,
    mentors_df: pd.DataFrame,
    *,
    student_ids: pd.Series | None = None,
) -> pd.DataFrame:
    """ادغام ستون‌های دانش‌آموز و پشتیبان روی دیتافریم تخصیص."""

    alloc = canonicalize_headers(allocations_df, header_mode="en").copy()
    students = canonicalize_headers(students_df, header_mode="en").copy()
    mentors = canonicalize_headers(mentors_df, header_mode="en").copy()

    if student_ids is not None:
        aligned_ids = student_ids.reindex(students.index)
        students.loc[:, "student_id"] = aligned_ids.astype("string")
    elif "student_id" not in students.columns:
        raise ValueError("students_df must include 'student_id' column for export")

    if "mentor_id" not in mentors.columns:
        alias_series = mentors.get("alias")
        if alias_series is not None:
            mentors.loc[:, "mentor_id"] = alias_series
        else:
            raise ValueError("mentors_df must include 'mentor_id' column for export")

    def _prefix(frame: pd.DataFrame, prefix: str, preserve: Iterable[str]) -> pd.DataFrame:
        renamed: dict[str, str] = {}
        for column in frame.columns:
            name = str(column)
            if name in preserve:
                renamed[name] = name
            else:
                renamed[name] = f"{prefix}{name}"
        return frame.rename(columns=renamed)

    students_prefixed = _prefix(students, "student_", {"student_id"})
    mentors_prefixed = _prefix(mentors, "mentor_", {"mentor_id"})

    _ensure_unique_identifier(students_prefixed, "student_id", entity_name="student")
    _ensure_unique_identifier(mentors_prefixed, "mentor_id", entity_name="mentor")

    merged = _safe_merge(
        alloc,
        students_prefixed,
        how="left",
        on="student_id",
        sort=False,
        validate="many_to_one",
        context="student",
    )
    merged = _safe_merge(
        merged,
        mentors_prefixed,
        how="left",
        on="mentor_id",
        sort=False,
        validate="many_to_one",
        context="mentor",
    )

    if len(merged.index) != len(alloc.index):
        raise ImportToSabtExportError(
            "ImportToSabt export failed: expected "
            f"{len(alloc)} allocation rows after joining student/mentor details, but got "
            f"{len(merged)}. This usually means duplicate student_id or mentor_id records "
            "exist in the exporter inputs (مثلاً فایل استخر یا دانش‌آموز اشتباه). "
            "Please fix the input data and retry."
        )

    merged.index = alloc.index
    return merged


def _ensure_unique_identifier(frame: pd.DataFrame, column: str, *, entity_name: str) -> None:
    """اطمینان از یکتایی شناسه در دیتافریم ورودی برای جلوگیری از join چند به چند."""

    if column not in frame.columns:
        raise ImportToSabtExportError(
            f"ImportToSabt export failed: '{column}' column not found for {entity_name} data."
        )

    series = frame[column]
    non_na = series.dropna()
    duplicated_ids = non_na[non_na.duplicated(keep=False)]
    if not duplicated_ids.empty:
        sample = ", ".join(duplicated_ids.astype(str).unique()[:5])
        raise ImportToSabtExportError(
            "ImportToSabt export failed: detected duplicate "
            f"{entity_name}_id values ({sample}). Each {entity_name}_id must be unique "
            "in ImportToSabt exporter inputs. لطفاً داده ورودی را اصلاح کنید."
        )


def _safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    context: str,
    **kwargs: Any,
) -> pd.DataFrame:
    """اجرای merge با پیام خطای مشخص هنگام تخطی از validate."""

    try:
        return left.merge(right, **kwargs)
    except ValueError as exc:  # pragma: no cover - مسیر خطا تست دارد
        raise ImportToSabtExportError(
            "ImportToSabt export failed while joining "
            f"{context} details: {exc}"
        ) from exc


def _normalize_digits(value: Any, length: int) -> str:
    text = str(value or "").translate(_DIGIT_TRANSLATION)
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    digits = digits[-length:].zfill(length)
    return digits[:length]


def _normalize_mobile(value: Any) -> str:
    digits = _normalize_digits(value, 20)
    if not digits:
        return ""
    if digits.startswith("98") and len(digits) == 12 and digits[2] == "9":
        digits = digits[2:]
    if digits.startswith("9") and len(digits) == 10:
        digits = f"0{digits}"
    if len(digits) == 11 and digits.startswith("09"):
        return digits
    if len(digits) > 11 and digits.endswith(digits[-11:]):
        tail = digits[-11:]
        if tail.startswith("09"):
            return tail
    if len(digits) == 11:
        return digits
    return digits


def _apply_normalizers(series: pd.Series, normalizer: Any) -> pd.Series:
    if normalizer is None:
        return series
    items = normalizer if isinstance(normalizer, Sequence) and not isinstance(normalizer, str) else [normalizer]
    result = series.copy()
    for name in items:
        if name == "digits_10":
            result = result.map(lambda v: _normalize_digits(v, 10))
        elif name == "digits_16":
            result = result.map(lambda v: _normalize_digits(v, 16))
        elif name == "mobile_ir":
            result = result.map(_normalize_mobile)
    return result


def _resolve_map(map_spec: Any, cfg: Mapping[str, Any]) -> Mapping[Any, Any] | None:
    if map_spec is None:
        return None
    if isinstance(map_spec, str):
        maps = cfg.get("maps")
        if isinstance(maps, Mapping) and map_spec in maps:
            return maps[map_spec]
    if isinstance(map_spec, Mapping):
        return map_spec
    return None


def _coerce_type(series: pd.Series, type_name: str | None, precision: int | None = None) -> pd.Series:
    if type_name == "number":
        numeric = pd.to_numeric(series, errors="coerce")
        if precision is not None:
            numeric = numeric.round(precision)
        return numeric
    converted = series.astype("string")
    return converted.fillna("")


def _series_from_source(
    df: pd.DataFrame,
    source_cfg: Mapping[str, Any],
    exporter_cfg: Mapping[str, Any],
    *,
    today: datetime,
) -> pd.Series:
    source_type = source_cfg.get("source", "df")
    if source_type == "empty":
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    if source_type == "literal":
        value = source_cfg.get("value", "")
        return pd.Series([value for _ in range(len(df))], index=df.index)
    if source_type == "system":
        field = source_cfg.get("field")
        if field == "today":
            fmt = source_cfg.get("format", "%Y-%m-%d")
            return pd.Series([today.strftime(fmt) for _ in range(len(df))], index=df.index)
        return pd.Series([source_cfg.get("value", "") for _ in range(len(df))], index=df.index)
    if source_type == "gf":
        field = source_cfg.get("field")
        candidates: list[str] = []
        if isinstance(field, str):
            mapped = GF_FIELD_TO_COL.get(field)
            if mapped:
                candidates.extend(mapped)
            prefixed = f"student_{field}" if not field.startswith("student_") else field
            candidates.extend([prefixed, field])
        for column_name in dict.fromkeys(filter(None, candidates)):
            if column_name in df.columns:
                return df[column_name]
        return pd.Series(["" for _ in range(len(df))], index=df.index)
    if source_type == "lookup":
        lookup_name = source_cfg.get("lookup")
        column = source_cfg.get("field")
        lookups = exporter_cfg.get("lookups", {})
        table = lookups.get(lookup_name) if isinstance(lookups, Mapping) else None
        if isinstance(table, Mapping) and column in df.columns:
            series = df[column].map(table)
            return series.fillna("")
        return pd.Series(["" for _ in range(len(df))], index=df.index)
    column = source_cfg.get("field")
    if column in df.columns:
        return df[column]
    return pd.Series(["" for _ in range(len(df))], index=df.index)


def build_sheet2_frame(
    df_alloc: pd.DataFrame,
    exporter_cfg: Mapping[str, Any],
    today: datetime | None = None,
) -> pd.DataFrame:
    """ساخت دیتافریم Sheet2 بر اساس تنظیمات JSON."""

    if today is None:
        today = datetime.today()
    sheet_cfg = exporter_cfg["sheets"]["Sheet2"]
    columns_cfg = sheet_cfg["columns"]
    ordered_columns = list(columns_cfg.keys())
    sheet = pd.DataFrame(index=df_alloc.index)
    for column_name in ordered_columns:
        spec = columns_cfg[column_name]
        series = _series_from_source(df_alloc, spec, exporter_cfg, today=today)
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=df_alloc.index)
        series = series.reindex(df_alloc.index)
        series = _apply_normalizers(series, spec.get("normalize"))
        map_dict = _resolve_map(spec.get("map"), exporter_cfg)
        if map_dict:
            mapped = series.map(map_dict)
            if mapped.isna().any():
                mapped = mapped.fillna(series.astype("string").map(map_dict))
            series = mapped.fillna(series)
        series = _coerce_type(series, spec.get("type"), spec.get("precision"))
        sheet[column_name] = series
    sheet = sheet.loc[:, ordered_columns]
    hekmat_cfg = sheet_cfg.get("hekmat_rule")
    if isinstance(hekmat_cfg, Mapping):
        status_column = hekmat_cfg.get("status_column")
        expected = hekmat_cfg.get("expected_value")
        target_columns = hekmat_cfg.get("columns", [])
        if status_column in sheet.columns and expected is not None:
            mask = sheet[status_column] == expected
            for column in target_columns:
                if column in sheet.columns:
                    sheet.loc[~mask, column] = ""
    sheet = sheet.astype({column: "string" for column in sheet.columns})
    sheet.attrs["exporter_config"] = exporter_cfg
    return sheet


def apply_alias_rule(sheet2: pd.DataFrame, df_alloc: pd.DataFrame) -> pd.DataFrame:
    """اعمال قانون alias روی ستون کدپستی خروجی."""

    cfg = sheet2.attrs.get("exporter_config") or {}
    alias_cfg = cfg.get("alias_rule") if isinstance(cfg, Mapping) else None
    if not isinstance(alias_cfg, Mapping):
        return sheet2
    sheet_postal = alias_cfg.get("sheet_postal_column")
    sheet_alias = alias_cfg.get("sheet_alias_column")
    df_postal = alias_cfg.get("df_postal_column")
    df_alias = alias_cfg.get("df_alias_column")
    if not sheet_postal or not df_postal or sheet_postal not in sheet2.columns:
        return sheet2
    source_postal = df_alloc.get(df_postal)
    source_alias = df_alloc.get(df_alias)
    if source_postal is None:
        return sheet2
    postal_series = source_postal.astype("string").fillna("")
    alias_series: pd.Series | None = None
    if source_alias is not None:
        alias_series = source_alias.astype("string").fillna("")
    alias_values = None
    if sheet_alias and sheet_alias in sheet2.columns:
        alias_values = sheet2[sheet_alias]
    if alias_series is None or alias_series.empty:
        sheet2.loc[:, sheet_postal] = postal_series
        return sheet2
    mask = alias_series.str.strip() != ""
    sheet2.loc[:, sheet_postal] = sheet2.get(sheet_postal, postal_series)
    if alias_values is None:
        sheet2.loc[mask, sheet_postal] = alias_series[mask]
    else:
        sheet2.loc[mask, sheet_postal] = alias_values[mask]
    return sheet2


def build_summary_frame(
    exporter_cfg: Mapping[str, Any],
    *,
    total_students: int,
    allocated_count: int,
    error_count: int,
) -> pd.DataFrame:
    sheet_cfg = exporter_cfg.get("sheets", {}).get("Summary")
    if not isinstance(sheet_cfg, Mapping):
        return pd.DataFrame()
    columns = list(sheet_cfg.get("columns", []))
    if len(columns) < 2:
        return pd.DataFrame(columns=columns)
    data = [
        {columns[0]: "تعداد کل دانش‌آموز", columns[1]: total_students},
        {columns[0]: "تخصیص موفق", columns[1]: allocated_count},
        {columns[0]: "تخصیص ناموفق", columns[1]: error_count},
    ]
    return pd.DataFrame(data, columns=columns)


def build_errors_frame(logs_df: pd.DataFrame | None, exporter_cfg: Mapping[str, Any]) -> pd.DataFrame:
    sheet_cfg = exporter_cfg.get("sheets", {}).get("Errors")
    if not isinstance(sheet_cfg, Mapping):
        return pd.DataFrame()
    columns = list(sheet_cfg.get("columns", []))
    if logs_df is None or logs_df.empty:
        return pd.DataFrame(columns=columns)
    status_series = logs_df.get("allocation_status")
    if status_series is None:
        return pd.DataFrame(columns=columns)
    fail_mask = status_series.astype("string") != "success"
    failed = logs_df.loc[fail_mask.fillna(False)]
    records = []
    for _, row in failed.iterrows():
        record = {
            columns[0]: row.get("student_id", ""),
        }
        if len(columns) > 1:
            record[columns[1]] = row.get("error_type") or row.get("allocation_status")
        if len(columns) > 2:
            record[columns[2]] = row.get("detailed_reason") or row.get("selection_reason")
        records.append(record)
    return pd.DataFrame(records, columns=columns)


def build_optional_sheet_frame(exporter_cfg: Mapping[str, Any], name: str) -> pd.DataFrame | None:
    sheet_cfg = exporter_cfg.get("sheets", {}).get(name)
    if not isinstance(sheet_cfg, Mapping):
        return None
    columns = list(sheet_cfg.get("columns", []))
    if not columns:
        return None
    return pd.DataFrame(columns=columns)


def ensure_template_workbook(template_path: str | Path, exporter_cfg: Mapping[str, Any]) -> Path:
    """اگر فایل قالب موجود نباشد، نسخهٔ مینیمال ایجاد می‌کند."""

    from openpyxl import Workbook

    path = Path(template_path)
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(exporter_cfg, Mapping):
        exporter_cfg = {}
    workbook = Workbook()
    sheet2 = workbook.active
    sheet2.title = "Sheet2"
    sheets_cfg = exporter_cfg.get("sheets", {}) if isinstance(exporter_cfg, Mapping) else {}
    sheet2_cfg = sheets_cfg.get("Sheet2", {}) if isinstance(sheets_cfg, Mapping) else {}
    columns_cfg = sheet2_cfg.get("columns", {}) if isinstance(sheet2_cfg, Mapping) else {}
    for col_idx, column_name in enumerate(columns_cfg.keys(), start=1):
        sheet2.cell(row=1, column=col_idx, value=column_name)
    for sheet_name, sheet_cfg in sheets_cfg.items():
        if sheet_name == "Sheet2":
            continue
        ws = workbook.create_sheet(title=sheet_name)
        if not isinstance(sheet_cfg, Mapping):
            continue
        for col_idx, column_name in enumerate(sheet_cfg.get("columns", []), start=1):
            ws.cell(row=1, column=col_idx, value=column_name)
    workbook.save(path)
    return path


def _write_dataframe_to_sheet(ws, df: pd.DataFrame) -> None:
    if df is None:
        return
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)
    for row_idx, (_, row) in enumerate(df.iterrows(), start=2):
        for col_idx, value in enumerate(row.tolist(), start=1):
            ws.cell(row=row_idx, column=col_idx, value=value)


def _verify_headers(ws, expected: Sequence[str]) -> None:
    header_cells = next(ws.iter_rows(min_row=1, max_row=1))
    headers = [cell.value if cell.value is not None else "" for cell in header_cells]
    if len(headers) < len(expected):
        raise ValueError(f"Template sheet '{ws.title}' has fewer columns than expected")
    template_headers = headers[: len(expected)]
    if template_headers != list(expected):
        raise ValueError(
            f"Header mismatch for sheet '{ws.title}': template={template_headers} expected={list(expected)}"
        )


def write_import_to_sabt_excel(
    df_sheet2: pd.DataFrame,
    df_summary: pd.DataFrame,
    df_errors: pd.DataFrame,
    df_sheet5: pd.DataFrame | None,
    df_9394: pd.DataFrame | None,
    template_path: str | Path,
    output_path: str | Path,
) -> None:
    """نوشتن خروجی ImportToSabt روی قالب موجود بدون تغییر استایل."""

    from openpyxl import load_workbook

    exporter_cfg = df_sheet2.attrs.get("exporter_config") or {}
    template = ensure_template_workbook(template_path, exporter_cfg)
    workbook = load_workbook(template)
    sheets = {
        "Sheet2": df_sheet2,
        "Summary": df_summary,
        "Errors": df_errors,
        "Sheet5": df_sheet5,
        "9394": df_9394,
    }
    for name, df in sheets.items():
        if df is None:
            continue
        if name not in workbook.sheetnames:
            raise ValueError(f"Template is missing sheet '{name}'")
        ws = workbook[name]
        _verify_headers(ws, df.columns)
        _write_dataframe_to_sheet(ws, df)
    workbook.save(Path(output_path))
