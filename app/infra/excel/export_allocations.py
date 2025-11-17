"""خروجی تخصیص Sabt بر اساس پروفایل Policy-First."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from pathlib import Path
from typing import Iterable, Literal, Mapping, Sequence

import pandas as pd

from app.core.common.columns import CANON_EN_TO_FA, canonicalize_headers, ensure_series
from app.core.common.normalization import normalize_fa
from app.core.pipeline import enrich_student_contacts
from app.infra.excel.common import (
    attach_contact_columns,
    enforce_text_columns,
    identify_code_headers,
)
from app.infra.io_utils import write_xlsx_atomic

__all__ = [
    "AllocationExportColumn",
    "load_sabt_export_profile",
    "build_sabt_export_frame",
    "collect_trace_debug_sheets",
    "export_sabt_excel",
    "DEFAULT_SABT_PROFILE_PATH",
]


DEFAULT_SABT_PROFILE_PATH = Path("docs/Report (4).xlsx")
_PROFILE_SHEET_NAME = "Sheet1"
_HEADER_COLUMN = "عنوان ستون ها ورودی"
_VALUE_COLUMN = "مقدار برای مپ کردن از اکسل ورودی"
_ORDER_COLUMN = "اولویت و ترتیب در اکسل خروجی"
_SOURCE_COLUMN = "مقدار از کجا آورده شود"
_SOURCE_ALLOCATION = "خروجی برنامه بعد از تخصیص"
_SOURCE_STUDENT = "کپی کردن از اکسل ورودی"
_SOURCE_REMOVE = "حذف از اکسل خروجی"
_ALLOCATION_HEADER_MAP = {
    normalize_fa("پیدا کردن ردیف پشتیبان از فیلد 141"): "mentor_id",
    normalize_fa("کد ثبت نام0"): "student_id",
    normalize_fa("کپی کد جایگزین 39"): "mentor_alias_code",
}
_SPLIT_PATTERN = re.compile(r"[|،,/]+")
_ASCII_KEY_PATTERN = re.compile(r"[^0-9a-z]+")


def _clean_text(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    if text.lower() == "nan":
        return ""
    return text


def _slugify(value: str) -> str:
    normalized = normalize_fa(value)
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", normalized).strip("_")
    return slug or "column"


def _normalize_lookup_key(value: str) -> str:
    normalized = normalize_fa(value)
    normalized = "".join(ch for ch in normalized.lower() if not ch.isspace())
    if normalized:
        return normalized
    ascii_fallback = _ASCII_KEY_PATTERN.sub("", str(value).strip().lower())
    return ascii_fallback


def _iter_mapping_candidates(value: str) -> Iterable[str]:
    if not value:
        return []
    parts = [_clean_text(value)]
    parts.extend(token.strip() for token in _SPLIT_PATTERN.split(value) if token.strip())
    return parts


@dataclass(frozen=True)
class AllocationExportColumn:
    """مدل ستونی خروجی Sabt با متادیتای Policy-First.

    مثال::

        >>> AllocationExportColumn(
        ...     key="mentor_id",
        ...     header="پیدا کردن ردیف پشتیبان از فیلد 141",
        ...     source_kind="allocation",
        ...     source_field="mentor_id",
        ...     literal_value=None,
        ...     order=1,
        ... )
    """

    key: str
    header: str
    source_kind: Literal["allocation", "student", "literal"]
    source_field: str | None
    literal_value: str | int | float | None
    order: int
    mapping_hint: str | None = None


def load_sabt_export_profile(
    path: Path = DEFAULT_SABT_PROFILE_PATH,
) -> list[AllocationExportColumn]:
    """خواندن Sheet1 و تبدیل به لیست ستون‌های موردنیاز Sabt."""

    profile_path = Path(path)
    if not profile_path.exists():
        raise FileNotFoundError(f"Sabt profile not found: {profile_path}")

    df = pd.read_excel(profile_path, sheet_name=_PROFILE_SHEET_NAME)
    try:
        idx_header = df.columns.get_loc(_HEADER_COLUMN)
        idx_value = df.columns.get_loc(_VALUE_COLUMN)
        idx_order = df.columns.get_loc(_ORDER_COLUMN)
        idx_source = df.columns.get_loc(_SOURCE_COLUMN)
    except KeyError as exc:  # pragma: no cover - محافظ در برابر تغییر پروفایل
        raise ValueError(f"Sabt profile missing expected column: {exc}") from exc
    numeric_orders = pd.to_numeric(df[_ORDER_COLUMN], errors="coerce")
    numeric_count = int(numeric_orders.notna().sum())
    records: list[AllocationExportColumn] = []

    for row in df.itertuples(index=False, name=None):
        header = _clean_text(row[idx_header])
        value_map = _clean_text(row[idx_value])
        source = _clean_text(row[idx_source])
        order_raw = row[idx_order]

        if not header:
            continue
        if source == _SOURCE_REMOVE:
            continue
        try:
            order_value = float(order_raw)
        except (TypeError, ValueError):
            continue
        if math.isnan(order_value):
            continue
        order = int(order_value)

        normalized_header = normalize_fa(header)
        key = _slugify(header)
        source_field: str | None = None
        literal_value: str | int | float | None = None
        resolved_source: Literal["allocation", "student", "literal"] = "student"

        if source == _SOURCE_ALLOCATION:
            resolved_source = "allocation"
            source_field = _ALLOCATION_HEADER_MAP.get(normalized_header)
            if source_field is None:
                raise ValueError(f"Allocation source field missing for header '{header}'")
        elif source == _SOURCE_STUDENT:
            resolved_source = "student"
            source_field = value_map or header
        else:
            resolved_source = "literal"
            literal_value = value_map or header

        records.append(
            AllocationExportColumn(
                key=source_field or key,
                header=header,
                source_kind=resolved_source,
                source_field=source_field,
                literal_value=literal_value,
                order=order,
                mapping_hint=value_map or header,
            )
        )

    records.sort(key=lambda col: col.order)

    if len(records) != numeric_count:
        raise ValueError(
            "Sabt profile mismatch: numeric order rows do not equal exported columns"
        )

    order_values = [column.order for column in records]
    if len(order_values) != len(set(order_values)):
        raise ValueError("Sabt profile contains duplicate order values")

    return records


def _resolve_student_column(
    column: AllocationExportColumn,
    lookup: dict[str, str],
) -> str | None:
    candidates = list(_iter_mapping_candidates(column.source_field or ""))
    if column.header not in candidates:
        candidates.append(column.header)
    for candidate in candidates:
        key = _normalize_lookup_key(candidate)
        if key in lookup:
            return lookup[key]
    return None


def _register_lookup_key(lookup: dict[str, str], label: str, column: str) -> None:
    key = _normalize_lookup_key(label)
    if key:
        lookup.setdefault(key, column)


def _build_students_lookup(df: pd.DataFrame) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for column in df.columns:
        label = str(column).strip()
        if not label:
            continue
        _register_lookup_key(lookup, label, column)
        persian_label = CANON_EN_TO_FA.get(label)
        if persian_label:
            _register_lookup_key(lookup, persian_label, column)
    return lookup


def _resolve_fallback_student_column(
    column: AllocationExportColumn, students: pd.DataFrame
) -> str | None:
    """یافتن ستون جایگزین برای مواردی مانند «وضعیت تحصیلی» زمانی که مپ اولیه پیدا نشد."""

    normalized_candidates = {
        _normalize_lookup_key(token)
        for token in _iter_mapping_candidates(column.header)
    }
    normalized_candidates.add(_normalize_lookup_key(column.source_field or column.header))
    for candidate in normalized_candidates:
        for col in students.columns:
            if _normalize_lookup_key(str(col)) == candidate:
                return col
    if "student_educational_status" in students.columns:
        return "student_educational_status"
    return None


def _enrich_students_with_summary(
    students_en: pd.DataFrame, summary_df: pd.DataFrame | None
) -> pd.DataFrame:
    """ادغام فیلدهای هویتی از ``summary_df`` روی دیتافریم دانش‌آموزان."""

    if summary_df is None or summary_df.empty or "student_id" not in summary_df.columns:
        return students_en
    summary_en = canonicalize_headers(summary_df, header_mode="en")
    if "student_id" not in summary_en.columns:
        return students_en
    summary_en = summary_en.drop_duplicates("student_id", keep="first").copy()
    summary_indexed = summary_en.set_index("student_id", drop=False)
    students_indexed = students_en.set_index("student_id", drop=False)

    for column in (
        "student_educational_status",
        "student_registration_status",
        "student_national_code",
        "student_first_name",
        "student_last_name",
    ):
        if column in summary_indexed.columns:
            aligned = summary_indexed[column].reindex(students_indexed.index)
            base = (
                students_indexed[column]
                if column in students_indexed.columns
                else pd.Series(pd.NA, index=students_indexed.index)
            )
            students_indexed[column] = base.where(base.notna(), aligned)
    return students_indexed.reset_index(drop=True)


def build_sabt_export_frame(
    allocation_df: pd.DataFrame,
    students_df: pd.DataFrame,
    profile: Sequence[AllocationExportColumn],
    *,
    summary_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """ساخت دیتافریم Sabt با join روی student_id و مرتب‌سازی پایدار.

    ستون‌های تماس (شامل «وضعیت ثبت نام») از خروجی :func:`enrich_student_contacts`
    به دانش‌آموزان ضمیمه می‌شود تا وضعیت ثبت‌نام مستقیماً از SSOT خوانده شود.
    """

    if not profile:
        raise ValueError("Sabt export profile is empty")

    alloc_en = canonicalize_headers(allocation_df, header_mode="en").copy()
    students_contacts = enrich_student_contacts(students_df)
    students_en = canonicalize_headers(students_df, header_mode="en").copy()
    students_en = attach_contact_columns(students_en, students_contacts)
    if "student_id" in students_en.columns:
        students_en = _enrich_students_with_summary(students_en, summary_df)

    if "student_id" not in alloc_en.columns:
        raise KeyError("allocation_df must include 'student_id' column")
    if "student_id" not in students_en.columns:
        raise KeyError("students_df must include 'student_id' column")

    sort_columns = [column for column in ("student_id", "mentor_id") if column in alloc_en.columns]
    if sort_columns:
        alloc_en = alloc_en.sort_values(sort_columns, kind="mergesort")
    alloc_en = alloc_en.reset_index(drop=True)

    student_ids = ensure_series(alloc_en["student_id"]).copy()
    students_en["student_id"] = ensure_series(students_en["student_id"]).copy()
    students_unique = students_en.drop_duplicates("student_id", keep="first")
    students_indexed = students_unique.set_index("student_id", drop=False)
    lookup = _build_students_lookup(students_indexed)

    export_data: dict[str, pd.Series] = {}
    missing_columns: set[str] = set()

    for column in profile:
        if column.source_kind == "allocation":
            if not column.source_field or column.source_field not in alloc_en.columns:
                missing_columns.add(column.source_field or column.header)
                series = pd.Series(pd.NA, index=alloc_en.index, dtype="object")
            else:
                series = ensure_series(alloc_en[column.source_field]).reindex(alloc_en.index)
        elif column.source_kind == "student":
            resolved = _resolve_student_column(column, lookup)
            if resolved is None or resolved not in students_indexed.columns:
                fallback_column = _resolve_fallback_student_column(column, students_en)
                if fallback_column and fallback_column in students_indexed.columns:
                    resolved = fallback_column
            if resolved is None or resolved not in students_indexed.columns:
                missing_columns.add(column.source_field or column.header)
                series = pd.Series(pd.NA, index=alloc_en.index, dtype="object")
            else:
                aligned = students_indexed.reindex(student_ids.tolist())
                series = ensure_series(aligned[resolved]).copy()
                series.index = alloc_en.index
        else:
            literal = column.literal_value
            series = pd.Series([literal] * len(alloc_en), index=alloc_en.index)
        export_data[column.header] = series

    export_df = pd.DataFrame(export_data)
    code_headers = identify_code_headers(profile)
    export_df = enforce_text_columns(export_df, headers=code_headers)
    export_df.attrs["missing_student_columns"] = sorted(missing_columns)
    return export_df


def collect_trace_debug_sheets(trace_df: pd.DataFrame | None) -> dict[str, pd.DataFrame]:
    """ساخت شیت‌های تشخیصی از تریس برای خروجی Excel بدون تغییر رفتار اصلی.

    اگر ``trace_df.attrs`` شامل ``summary_df``، ``unallocated_summary`` یا
    ``policy_violations`` باشد، آن‌ها را در یک دیکشنری با کلیدهای ایمن برمی‌گرداند
    تا توسط :func:`write_xlsx_atomic` روی شیت‌های مجزا (summary_df،
    unallocated_summary، policy_violations، FinalStatus_counts) نوشته شوند.
    """

    if trace_df is None:
        return {}

    sheets: dict[str, pd.DataFrame] = {}
    summary_df = trace_df.attrs.get("summary_df")
    if isinstance(summary_df, pd.DataFrame) and not summary_df.empty:
        sheets["summary_df"] = summary_df.copy()
        value_counts = trace_df.attrs.get("final_status_counts")
        if hasattr(value_counts, "reset_index"):
            counts_df = value_counts.reset_index()
            counts_df.columns = ["final_status", "count"]
            sheets["FinalStatus_counts"] = counts_df

    unallocated_summary = trace_df.attrs.get("unallocated_summary")
    if isinstance(unallocated_summary, pd.DataFrame) and not unallocated_summary.empty:
        sheets["unallocated_summary"] = unallocated_summary.copy()

    policy_violations = trace_df.attrs.get("policy_violations")
    if isinstance(policy_violations, pd.DataFrame) and not policy_violations.empty:
        sheets["policy_violations"] = policy_violations.copy()

    return sheets


def export_sabt_excel(
    allocation_df: pd.DataFrame,
    students_df: pd.DataFrame,
    output_path: Path,
    profile_path: Path | None = None,
    *,
    sheet_name: str = "Sabt",
    extra_sheets: Mapping[str, pd.DataFrame] | None = None,
    summary_df: pd.DataFrame | None = None,
) -> Path:
    """نوشتن خروجی Sabt در فایل Excel مستقل با ساختار پایدار."""

    profile = load_sabt_export_profile(profile_path or DEFAULT_SABT_PROFILE_PATH)
    export_df = build_sabt_export_frame(
        allocation_df, students_df, profile, summary_df=summary_df
    )
    sheets: dict[str, pd.DataFrame] = {sheet_name: export_df}
    if extra_sheets:
        sheets.update(extra_sheets)
    write_xlsx_atomic(
        sheets,
        output_path,
        header_mode=None,
        sheet_header_modes={sheet_name: None},
        sheet_prepare_modes={sheet_name: "raw"},
    )
    return output_path

