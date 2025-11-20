"""رابط خط فرمان headless برای ماتریس و تخصیص مطابق Policy.

این ماژول کلیهٔ مسئولیت‌های I/O را بر عهده دارد و با تزریق progress به
توابع Core، اصل Policy-First و جداسازی لایه‌ها را حفظ می‌کند.

مثال::

    >>> from app.infra import cli
    >>> cli.main(["build-matrix", "--inspactor", "insp.xlsx", "--schools", "sch.xlsx",
    ...           "--crosswalk", "cross.xlsx", "--output", "out.xlsx", "--policy",
    ...           "config/policy.json"])  # doctest: +SKIP
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Literal, Mapping, Sequence
from uuid import uuid4

from dataclasses import asdict

import pandas as pd
from pandas import testing as pd_testing
from pandas.api import types as pd_types

from app.core.allocate_students import allocate_batch, build_selection_reason_rows
from app.core.allocation.engine import enrich_summary_with_history
from app.core.allocation.history_metrics import METRIC_COLUMNS, compute_history_metrics
from app.core.allocation.mentor_pool import (
    MentorPoolGovernanceConfig,
    apply_manager_mentor_governance,
    apply_mentor_pool_governance,
)
from app.core.canonical_frames import (
    canonicalize_allocation_frames,
    sanitize_pool_for_allocation as _sanitize_pool_for_allocation,
)
from app.core.build_matrix import BuildConfig, build_matrix
from app.core.policy_loader import MentorStatus, PolicyConfig, load_policy
from app.core.qa.invariants import run_all_invariants
from app.infra.excel_writer import write_selection_reasons_sheet
from app.infra.excel.export_allocations import (
    DEFAULT_SABT_PROFILE_PATH,
    build_sabt_export_frame,
    collect_trace_debug_sheets,
    load_sabt_export_profile,
)
from app.infra.excel.export_qa_validation import (
    QaValidationContext,
    export_qa_validation,
)
from app.infra.excel.import_to_sabt import (
    apply_alias_rule,
    build_errors_frame,
    build_optional_sheet_frame,
    build_sheet2_frame,
    build_summary_frame,
    load_exporter_config,
    prepare_allocation_export_frame,
    write_import_to_sabt_excel,
)
from app.infra.io_utils import (
    ALT_CODE_COLUMN,
    read_crosswalk_workbook,
    read_excel_first_sheet,
    write_xlsx_atomic,
)
from app.infra.local_database import LocalDatabase
from app.infra import history_store
from app.infra.audit_allocations import audit_allocations, summarize_report
# --- واردات اصلاح شده از app.core ---
from app.core.common.columns import (
    CANON_EN_TO_FA,
    HeaderMode,
    canonicalize_headers,
    enrich_school_columns_en,
)
from app.core.counter import (
    assert_unique_student_ids,
    assign_counters,
    build_registration_id,
    detect_academic_year_from_counters,
    find_duplicate_student_id_groups,
    infer_year_strict,
    pick_counter_sheet_name,
    year_to_yy,
)
# --- پایان واردات اصلاح شده ---

ProgressFn = Callable[[int, str], None]

_DEFAULT_POLICY_PATH = Path("config/policy.json")
_DEFAULT_EXPORTER_CONFIG_PATH = Path("config/SmartAlloc_Exporter_Config_v1.json")
_DEFAULT_SABT_TEMPLATE_PATH = Path("templates/ImportToSabt (1404) - Copy.xlsx")
_DEFAULT_ALLOC_PROFILE_PATH = DEFAULT_SABT_PROFILE_PATH
_DEFAULT_LOCAL_DB_PATH = Path("smart_alloc.db")

logger = logging.getLogger(__name__)


def _default_progress(pct: int, message: str) -> None:
    """چاپ سادهٔ وضعیت پیشرفت در حالت headless."""
    print(f"{pct:3d}% | {message}")


def _add_local_db_args(parser: argparse.ArgumentParser) -> None:
    """افزودن آرگومان‌های پایگاه دادهٔ محلی برای لاگ اجرا."""

    parser.add_argument(
        "--local-db",
        dest="local_db_path",
        default=str(_DEFAULT_LOCAL_DB_PATH),
        help="مسیر فایل SQLite جهت ثبت تاریخچهٔ اجرا",
    )
    parser.add_argument(
        "--disable-local-db",
        action="store_true",
        help="غیرفعال‌سازی ثبت تاریخچه در SQLite",
    )


def _resolve_local_db(args: argparse.Namespace) -> LocalDatabase | None:
    """تولید LocalDatabase از آرگومان‌ها یا بازگشت None در صورت غیرفعال بودن."""

    overrides = getattr(args, "_ui_overrides", {}) or {}
    if bool(overrides.get("disable_local_db")) or getattr(args, "disable_local_db", False):
        return None
    path_text = (
        overrides.get("local_db_path")
        or getattr(args, "local_db_path", None)
        or str(_DEFAULT_LOCAL_DB_PATH)
    )
    try:
        return LocalDatabase(Path(path_text))
    except Exception:  # pragma: no cover - خطاهای غیرمنتظرهٔ مسیر
        logger.exception("Failed to prepare local DB at %s", path_text)
        return None


def _print_audit_summary(report: Dict[str, Dict[str, Any]]) -> None:
    """چاپ خلاصهٔ گزارش ممیزی تخصیص."""

    print("=== Allocation Audit ===")
    for key, payload in report.items():
        count = int(payload.get("count", 0))
        print(f"{key}: {count}")
        samples = payload.get("samples") or []
        if samples:
            preview = json.dumps(samples[:3], ensure_ascii=False)
            print(f"  samples: {preview}")


def _print_metrics(report: Dict[str, Dict[str, Any]]) -> None:
    """چاپ JSON ساخت‌یافته برای سامانه‌های Observability."""

    summary = summarize_report(report)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _empty_history_metrics_df() -> pd.DataFrame:
    """دیتافریم خالی با ستون‌های KPI تاریخچه."""

    return pd.DataFrame(columns=METRIC_COLUMNS)


def _log_history_metrics(
    summary_df: pd.DataFrame | None,
    *,
    students_df: pd.DataFrame,
    history_info_df: pd.DataFrame | None,
    policy: PolicyConfig,
    history_metrics_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """ثبت خلاصهٔ KPI تاریخچه به‌صورت لاگ برای هر کانال تخصیص.

    در صورت نبود دادهٔ تاریخچه یا ستون‌های لازم، پیام واضحی چاپ می‌شود و از
    شکست جلوگیری می‌شود. دیتافریم محاسبه‌شده برای استفاده مجدد بازگردانده می‌شود.
    """

    if history_metrics_df is None:
        if summary_df is None or summary_df.empty:
            logger.info("History metrics unavailable (no summary rows).")
            return _empty_history_metrics_df()

        if history_info_df is None:
            logger.info("History metrics unavailable (no history info).")
            return _empty_history_metrics_df()

        try:
            enriched_summary = enrich_summary_with_history(
                summary_df,
                students_df=students_df,
                history_info_df=history_info_df,
                policy=policy,
            )
            history_metrics_df = compute_history_metrics(enriched_summary)
        except KeyError:
            logger.info("History metrics unavailable (missing columns).")
            return _empty_history_metrics_df()

    if history_metrics_df.empty:
        logger.info("History metrics unavailable (empty metrics).")
        return _empty_history_metrics_df()

    for _, row in history_metrics_df.iterrows():
        logger.info(
            "HistoryMetrics[channel=%s] total=%d already=%d no_match=%d missing=%d same_mentor=%d ratio=%.3f",
            row["allocation_channel"],
            int(row["students_total"]),
            int(row["history_already_allocated"]),
            int(row["history_no_history_match"]),
            int(row["history_missing_or_invalid"]),
            int(row["same_history_mentor_true"]),
            float(row["same_history_mentor_ratio"]),
        )

    return history_metrics_df


def _qa_validation_output_path(base: Path, *, stem_override: str | None = None) -> Path:
    suffix = stem_override or f"{base.stem}_validation.xlsx"
    return base.with_name(suffix)


def _export_qa_validation_workbook(
    *,
    report: "QaReport",
    base_output: Path,
    context: QaValidationContext,
    stem_override: str | None = None,
) -> Path:
    from app.core.qa.invariants import QaReport as _QaReport  # محفاظت از چرخهٔ import

    if not isinstance(report, _QaReport):
        raise TypeError("report must be QaReport")
    output_path = _qa_validation_output_path(base_output, stem_override=stem_override)
    export_qa_validation(report, output=output_path, context=context)
    return output_path
def _normalize_override_mapping(data: Mapping[object, object] | None) -> dict[str, bool]:
    if not data:
        return {}
    normalized: dict[str, bool] = {}
    for key, value in data.items():
        try:
            enabled = bool(value)
        except Exception:
            continue
        text_key = str(key).strip()
        if text_key:
            normalized[text_key] = enabled
    return normalized


def _resolve_mentor_pool_overrides(args: argparse.Namespace) -> dict[str, bool]:
    overrides: dict[str, bool] = {}
    ui_overrides = getattr(args, "_ui_overrides", {}) or {}
    ui_mapping = ui_overrides.get("mentor_pool_overrides")
    overrides.update(_normalize_override_mapping(ui_mapping if isinstance(ui_mapping, Mapping) else {}))

    raw = getattr(args, "mentor_overrides", None)
    if raw:
        payload = json.loads(raw)
        if not isinstance(payload, Mapping):
            raise ValueError("mentor-overrides must be a JSON object")
        overrides.update(_normalize_override_mapping(payload))
    return overrides


def _resolve_manager_overrides(args: argparse.Namespace) -> dict[str, bool]:
    overrides: dict[str, bool] = {}
    ui_overrides = getattr(args, "_ui_overrides", {}) or {}
    ui_mapping = ui_overrides.get("mentor_pool_manager_overrides")
    overrides.update(_normalize_override_mapping(ui_mapping if isinstance(ui_mapping, Mapping) else {}))

    raw = getattr(args, "manager_overrides", None)
    if raw:
        payload = json.loads(raw)
        if not isinstance(payload, Mapping):
            raise ValueError("manager-overrides must be a JSON object")
        overrides.update(_normalize_override_mapping(payload))
    return overrides


def _default_governance_config() -> MentorPoolGovernanceConfig:
    return MentorPoolGovernanceConfig(
        default_status=MentorStatus.ACTIVE,
        mentor_status_map={},
        allowed_statuses=(MentorStatus.ACTIVE, MentorStatus.INACTIVE),
    )


def _apply_mentor_pool_overrides(
    pool: pd.DataFrame, policy: PolicyConfig, args: argparse.Namespace
) -> pd.DataFrame:
    overrides = _resolve_mentor_pool_overrides(args)
    config: MentorPoolGovernanceConfig = getattr(
        policy, "mentor_pool_governance", _default_governance_config()
    )
    return apply_mentor_pool_governance(pool, config, overrides=overrides)


def _detect_reader(path: Path) -> Callable[[Path], pd.DataFrame]:
    """انتخاب تابع خواندن مناسب؛ برای Excel شیت 'matrix' را ترجیح بده."""
    suffix = path.suffix.lower()
    dtype_map = {ALT_CODE_COLUMN: str}
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        def _read_xlsx(p: Path) -> pd.DataFrame:
            with pd.ExcelFile(p) as xls:
                sheet = "matrix" if "matrix" in xls.sheet_names else xls.sheet_names[0]
                return xls.parse(sheet, dtype=dtype_map)
        return _read_xlsx
    return lambda p: pd.read_csv(p, dtype=dtype_map)


# --- توابع کمکی برای پارامترهای خط فرمان ---
def _normalize_min_coverage_arg(value: float | None) -> float | None:
    if value is None:
        return None
    ratio = float(value)
    if ratio > 1:
        ratio /= 100.0
    if ratio < 0 or ratio > 1:
        raise ValueError(
            "حداقل نسبت پوشش باید عددی بین 0 و 1 باشد یا به‌صورت درصد معتبر وارد شود."
        )
    return ratio


# --- توابع کمکی برای پاک‌سازی خروجی (کاملاً ایمن و جامع) ---
def _is_empty_arraylike(x) -> bool:
    """بررسی می‌کند که آیا x یک آرایه خالی است یا خیر"""
    if isinstance(x, (pd.Series, pd.DataFrame, list, tuple)):
        return len(x) == 0
    if hasattr(x, 'size') and hasattr(x, '__len__'):
        return x.size == 0
    return False

def _safe_isna(x) -> bool:
    """نسخه ایمن از pd.isna که با آرایه‌های خالی کار می‌کند"""
    try:
        if _is_empty_arraylike(x):
            return True
        result = pd.isna(x)
        if hasattr(result, "all"):
            return bool(result.all())
        if isinstance(result, (list, tuple)):
            return all(bool(item) for item in result)
        return bool(result)
    except ValueError:
        return True
    except Exception:
        return True

def _safe_json_dumps(x) -> str:
    """نسخه ایمن از json.dumps"""
    try:
        return json.dumps(x, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(x)

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ادغام امن ستون‌های تکراری حتی با نام‌های تهی/NaN."""

    if df.empty or not any(df.columns.duplicated()):
        return df.copy()

    result_df = pd.DataFrame(index=df.index)

    def _label_key(label: object) -> tuple[str, object | None]:
        try:
            if pd.isna(label):
                return ("__nan__", None)
        except TypeError:
            # برچسب‌های ناسازگار (مثل لیست) برابر None فرض می‌شوند
            return ("__nan__", None)
        return ("value", label)

    groups: dict[tuple[str, object | None], list[int]] = {}
    representatives: dict[tuple[str, object | None], object] = {}
    for idx, column in enumerate(df.columns):
        key = _label_key(column)
        if key not in groups:
            groups[key] = []
            representatives[key] = column
        groups[key].append(idx)

    for key, positions in groups.items():
        subset = df.iloc[:, positions]
        if subset.shape[1] == 1:
            result_df[representatives[key]] = subset.iloc[:, 0]
            continue
        filled = subset.bfill(axis=1)
        result_df[representatives[key]] = filled.iloc[:, 0]

    return result_df

def _is_complex_safe(x) -> bool:
    """چک می‌کند آیا یک مقدار، یک شیء پیچیده است یا نه (ایمن در برابر ndarray خالی)."""
    if isinstance(x, (dict, list, tuple, set)):
        return True
    
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.size > 0
    
    return False

def _make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    تبدیل ایمن ستون‌های object در دیتافریم برای نوشتن در Excel
    با روش‌هایی که کاملاً در برابر ستون‌های تکراری و آرایه‌های خالی مقاوم هستند.
    """
    if df.empty:
        return df.copy()
    
    # ابتدا ستون‌های تکراری را ادغام می‌کنیم
    df = _coalesce_duplicate_columns(df)
    
    out = df.copy()
    
    # پردازش هر ستون
    for col in out.columns:
        s = out[col]
        
        # اطمینان از اینکه s یک Series است (نه DataFrame)
        if isinstance(s, pd.DataFrame):
            if s.shape[1] > 0:
                # استفاده از اولین ستون
                s = s.iloc[:, 0]
            else:
                # اگر DataFrame خالی بود
                out[col] = pd.Series([""] * len(out), index=out.index)
                continue
        
        # برای ستون‌های از نوع object
        if pd_types.is_object_dtype(s.dtype):
            # تابع تبدیل ایمن برای هر مقدار
            def _safe_convert(v):
                if _safe_isna(v):
                    return ""
                if isinstance(v, (dict, list, tuple, set)):
                    return _safe_json_dumps(v)
                if isinstance(v, (pd.Series, pd.DataFrame)) and v.size == 0:
                    return ""
                return str(v)
            
            # استفاده از apply به جای map برای حساسیت کمتر به انواع داده
            out[col] = s.apply(_safe_convert)
        
        # برای ستون‌های عددی که ممکن است شامل NaN باشند
        elif pd_types.is_numeric_dtype(s.dtype):
            out[col] = s.fillna(0)
        
        # برای سایر انواع
        else:
            out[col] = s.fillna("")
    
    return out

def _ensure_valid_dataframe(df: pd.DataFrame, name: str = "") -> pd.DataFrame:
    """
    اطمینان از معتبر بودن یک دیتافریم برای نوشتن در Excel

    این تابع چک می‌کند که:
    1. ستون‌های تکراری وجود نداشته باشند
    2. هیچ سلولی حاوی شیء پیچیده نباشد
    3. هیچ سلولی NaN نباشد
    
    و در صورت لزوم، تبدیلات لازم را انجام می‌دهد.
    """
    if df.empty:
        print(f"⚠️  هشدار: دیتافریم {name} خالی است!")
        return df
    
    # 1. بررسی و ادغام ستون‌های تکراری
    duplicate_cols = df.columns[df.columns.duplicated()]
    if len(duplicate_cols) > 0:
        print(f"⚠️  هشدار: دیتافریم {name} دارای {len(duplicate_cols)} ستون تکراری است: {list(duplicate_cols.unique())}")
        df = _coalesce_duplicate_columns(df)
    
    # 2. اطمینان از اینکه هیچ ستونی DataFrame نیست
    complex_cols = []
    for col in df.columns:
        if isinstance(df[col], pd.DataFrame):
            complex_cols.append(col)
    
    if len(complex_cols) > 0:
        print(f"⚠️  هشدار: دیتافریم {name} دارای ستون‌های پیچیده است: {complex_cols}")
        df = _make_excel_safe(df)
    
    return df
# --- پایان توابع کمکی ---


def _read_optional_first_sheet(path: str | None) -> pd.DataFrame | None:
    """خواندن روستر شمارنده با تشخیص شیت مناسب و هدر EN."""

    if not path:
        return None

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Roster file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        with pd.ExcelFile(file_path) as workbook:
            sheet_name = pick_counter_sheet_name(workbook.sheet_names)
            if sheet_name is None:
                raise ValueError(
                    "هیچ شیت سازگار با شمارنده در فایل یافت نشد؛ نام‌های قابل قبول شامل 'شمارنده' و 'Counters' است."
                )
            frame = workbook.parse(sheet_name)
    else:
        frame = pd.read_csv(file_path)

    return canonicalize_headers(frame, header_mode="en")


def _resolve_optional_override(
    args: argparse.Namespace, name: str, default: str | None = None
) -> str | None:
    """اولویت‌دهی overrideهای UI نسبت به آرگومان‌های CLI."""

    overrides = getattr(args, "_ui_overrides", {}) or {}
    value = overrides.get(name)
    candidates = [value, getattr(args, name, None), default]
    for candidate in candidates:
        if isinstance(candidate, str):
            text = candidate.strip()
            if text:
                return text
    return None


def _add_center_management_args(parser: argparse.ArgumentParser) -> None:
    """افزودن گروه آرگومان‌های مرتبط با مدیریت مراکز."""

    center_group = parser.add_argument_group("مدیریت مراکز")
    center_group.add_argument(
        "--center-manager",
        action="append",
        dest="center_manager",
        metavar="CENTER_ID=MANAGER_NAME",
        help="Override مدیر یک مرکز (قابل تکرار)",
    )
    center_group.add_argument(
        "--center-priority",
        type=str,
        help="ترتیب دلخواه مراکز (لیست جداشده با ویرگول)",
    )
    center_group.add_argument(
        "--strict-manager-validation",
        action="store_true",
        help="در صورت نبود مدیر برای مراکز خطا بده",
    )


def _parse_center_managers(cli_managers: list[str] | None) -> dict[int, list[str]]:
    """تجزیهٔ آرگومان‌های --center-manager به نگاشت پایدار."""

    mapping: dict[int, list[str]] = {}
    if not cli_managers:
        return mapping
    for item in cli_managers:
        if "=" not in item:
            raise ValueError(
                f"center-manager value '{item}' must use format CENTER_ID=MANAGER_NAME"
            )
        center_text, manager_text = item.split("=", 1)
        try:
            center_id = int(center_text.strip())
        except ValueError as exc:
            raise ValueError("center id must be an integer") from exc
        manager = manager_text.strip().strip("\"'")
        if not manager:
            raise ValueError("manager name cannot be empty")
        mapping.setdefault(center_id, []).append(manager)
    return mapping


def _parse_center_priority(priority_str: str | None) -> list[int] | None:
    """تبدیل رشتهٔ اولویت مراکز به لیست اعداد."""

    if not priority_str:
        return None
    tokens = priority_str.replace("،", ",").split(",")
    result: list[int] = []
    for token in tokens:
        text = token.strip()
        if not text:
            continue
        try:
            result.append(int(text))
        except ValueError as exc:
            raise ValueError(
                f"فرمت نامعتبر برای اولویت مراکز: {priority_str}"
            ) from exc
    return result or None


def _normalize_center_override_map(source: object) -> dict[int, list[str]]:
    """تبدیل ورودی دلخواه (Mapping یا JSON) به ساختار استاندارد."""

    if not isinstance(source, Mapping):
        return {}
    normalized: dict[int, list[str]] = {}
    for key, value in source.items():
        try:
            center_id = int(key)
        except (TypeError, ValueError):
            continue
        names = value if isinstance(value, (list, tuple)) else [value]
        cleaned = [str(name).strip() for name in names if str(name).strip()]
        if cleaned:
            normalized[center_id] = cleaned
    return normalized


def _merge_center_manager_maps(
    target: dict[int, list[str]], source: Mapping[int, list[str]] | None
) -> dict[int, list[str]]:
    """ادغام نگاشت ثانویه در نگاشت اصلی با حفظ ترتیب."""

    if not source:
        return target
    for center_id, names in source.items():
        existing = target.setdefault(center_id, [])
        for name in names:
            if name not in existing:
                existing.append(name)
    return target


def _resolve_center_preferences(
    args: argparse.Namespace, policy: PolicyConfig
) -> tuple[
    Mapping[int, Sequence[str]] | None,
    Mapping[int, Sequence[str]] | None,
    list[int] | None,
    bool,
]:
    """گردآوری ورودی‌های UI و CLI برای مدیریت مراکز."""

    overrides = getattr(args, "_ui_overrides", {}) or {}
    ui_mapping = _normalize_center_override_map(overrides.get("center_managers"))

    cli_mapping = _parse_center_managers(getattr(args, "center_manager", None) or [])
    json_payload = getattr(args, "center_managers", None)
    if json_payload:
        data = json.loads(json_payload)
        if not isinstance(data, Mapping):
            raise ValueError("center-managers must be a JSON object")
        cli_mapping = _merge_center_manager_maps(
            cli_mapping, _normalize_center_override_map(data)
        )
    legacy = {"golestan_manager": 1, "sadra_manager": 2}
    for attr, center_id in legacy.items():
        text = getattr(args, attr, None)
        if isinstance(text, str) and text.strip():
            cli_mapping.setdefault(center_id, []).append(text.strip())

    priority_override = overrides.get("center_priority")
    priority_text: str | None
    if isinstance(priority_override, (list, tuple)):
        priority_text = ",".join(str(item) for item in priority_override)
    elif priority_override is not None:
        priority_text = str(priority_override)
    else:
        priority_text = getattr(args, "center_priority", None)
    try:
        center_priority = _parse_center_priority(priority_text)
    except ValueError as exc:
        raise ValueError(f"center priority override is invalid: {exc}") from exc

    strict_flag = bool(getattr(args, "strict_manager_validation", False))

    return (
        ui_mapping or None,
        cli_mapping or None,
        center_priority,
        strict_flag,
    )


def _collect_cli_center_manager_overrides(
    args: argparse.Namespace,
) -> dict[int, tuple[str, ...]]:
    """تابع سازگار با تست‌های قدیمی برای جمع‌آوری override های CLI."""

    cli_mapping = _parse_center_managers(getattr(args, "center_manager", None) or [])
    json_payload = getattr(args, "center_managers", None)
    if json_payload:
        data = json.loads(json_payload)
        if not isinstance(data, Mapping):
            raise ValueError("center-managers must be a JSON object")
        cli_mapping = _merge_center_manager_maps(
            cli_mapping, _normalize_center_override_map(data)
        )
    legacy = {"golestan_manager": 1, "sadra_manager": 2}
    for attr, center_id in legacy.items():
        text = getattr(args, attr, None)
        if isinstance(text, str) and text.strip():
            cli_mapping.setdefault(center_id, []).append(text.strip())
    return {center_id: tuple(names) for center_id, names in cli_mapping.items() if names}


def _maybe_export_import_to_sabt(
    *,
    args: argparse.Namespace,
    allocations_df: pd.DataFrame,
    students_df: pd.DataFrame,
    mentors_df: pd.DataFrame,
    logs_df: pd.DataFrame,
    student_ids: pd.Series,
) -> None:
    """تولید فایل ImportToSabt در صورت مشخص شدن مسیر خروجی."""

    sabt_output = _resolve_optional_override(args, "sabt_output")
    if not sabt_output:
        return
    cfg_path = _resolve_optional_override(
        args, "sabt_config", str(_DEFAULT_EXPORTER_CONFIG_PATH)
    ) or str(_DEFAULT_EXPORTER_CONFIG_PATH)
    template_path = _resolve_optional_override(
        args, "sabt_template", str(_DEFAULT_SABT_TEMPLATE_PATH)
    ) or str(_DEFAULT_SABT_TEMPLATE_PATH)
    exporter_cfg = load_exporter_config(cfg_path)
    export_df = prepare_allocation_export_frame(
        allocations_df,
        students_df,
        mentors_df,
        student_ids=student_ids,
    )
    df_sheet2 = build_sheet2_frame(export_df, exporter_cfg)
    df_sheet2 = apply_alias_rule(df_sheet2, export_df)
    status_series = logs_df.get("allocation_status")
    error_count = 0
    if isinstance(status_series, pd.Series):
        error_count = int((status_series.astype("string") != "success").sum())
    dedupe_logs = export_df.attrs.get("dedupe_logs")
    df_summary = build_summary_frame(
        exporter_cfg,
        total_students=len(students_df),
        allocated_count=len(df_sheet2),
        error_count=error_count,
        dedupe_logs=dedupe_logs,
    )
    df_errors = build_errors_frame(logs_df, exporter_cfg)
    df_sheet5 = build_optional_sheet_frame(exporter_cfg, "Sheet5")
    df_9394 = build_optional_sheet_frame(exporter_cfg, "9394")
    write_import_to_sabt_excel(
        df_sheet2,
        df_summary,
        df_errors,
        df_sheet5,
        df_9394,
        template_path,
        sabt_output,
    )


def _compose_duplicate_display_name(row: pd.Series) -> str:
    """تولید نام قابل‌خواندن برای گزارش ردیف تکراری."""

    if row is None:
        return ""
    candidates = [
        row.get("full_name"),
        row.get("student_name"),
        row.get("name"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    first = str(row.get("first_name", "")).strip()
    last = str(row.get("last_name", "")).strip()
    return " ".join(part for part in (first, last) if part).strip()


def _build_duplicate_row_report(
    students_df: pd.DataFrame,
    students_en: pd.DataFrame,
    duplicate_groups: dict[str, list[object]],
) -> list[dict[str, object]]:
    """ساخت ساختار قابل‌نمایش برای شناسه‌های تکراری."""

    position_map = {index: pos for pos, index in enumerate(students_df.index)}
    report: list[dict[str, object]] = []
    for student_id, index_list in duplicate_groups.items():
        rows: list[dict[str, object]] = []
        ordered = sorted(index_list, key=lambda idx: position_map.get(idx, 10**9))
        for index_label in ordered:
            position = position_map.get(index_label)
            row = students_en.loc[index_label] if index_label in students_en.index else None
            if row is None:
                national_id = ""
            else:
                raw_national_id = row.get("national_id", "")
                if pd.isna(raw_national_id):  # type: ignore[arg-type]
                    national_id = ""
                else:
                    national_id = str(raw_national_id).strip()
            rows.append(
                {
                    "index": index_label,
                    "position": None if position is None else position + 1,
                    "national_id": national_id,
                    "name": _compose_duplicate_display_name(row),
                }
            )
        report.append({"student_id": student_id, "rows": rows})
    return report


def _format_duplicate_report(report: list[dict[str, object]]) -> str:
    """تبدیل ساختار تکراری‌ها به متن فشرده برای چاپ."""

    lines: list[str] = []
    for payload in report:
        student_id = payload.get("student_id")
        row_items: list[str] = []
        for row in payload.get("rows", []):
            name = row.get("name") or "-"
            national_id = row.get("national_id") or "-"
            position = row.get("position")
            index_label = row.get("index")
            label_parts = []
            if position is not None:
                label_parts.append(f"ردیف داده {position}")
            label_parts.append(f"index={index_label}")
            label_parts.append(f"کدملی={national_id}")
            if name and name != "-":
                label_parts.append(f"نام={name}")
            row_items.append("، ".join(label_parts))
        joined = " | ".join(row_items)
        lines.append(f"student_id {student_id} در ردیف‌های زیر تکراری است → {joined}")
    return "\n".join(lines)


def _prompt_duplicate_resolution(report_text: str) -> str:
    """نمایش گزارش و دریافت تصمیم کاربر برای رفع تکرار."""

    print("❌ student_id تکراری در خروجی شمارنده یافت شد:")
    print(report_text)
    print("گزینه‌ها:")
    print("  [R] تخصیص شمارندهٔ جدید به ردیف‌های تکراری")
    print("  [D] حذف ردیف‌های تکراری و نگهداشت اولین رخداد")
    print("  [A] انصراف و اصلاح دستی (خروج با خطا)")
    while True:
        choice = input("گزینهٔ موردنظر (R/D/A): ").strip().lower()
        mapping = {"r": "assign-new", "d": "drop", "a": "abort"}
        if choice in mapping:
            return mapping[choice]
        print("گزینهٔ نامعتبر است؛ یکی از R/D/A را انتخاب کنید.")


def _assign_new_counters_for_duplicates(
    counters: pd.Series,
    duplicate_groups: dict[str, list[object]],
    students_en: pd.DataFrame,
    policy: PolicyConfig,
    academic_year: int,
) -> tuple[pd.Series, int]:
    """تخصیص شمارندهٔ جدید برای ردیف‌های تکراری بدون حذف داده."""

    gender_codes = policy.gender_codes
    male_value = int(gender_codes.male.value)
    female_value = int(gender_codes.female.value)
    male_mid3 = str(gender_codes.male.counter_code).zfill(3)
    female_mid3 = str(gender_codes.female.counter_code).zfill(3)
    summary = {
        "reused_count": 0,
        "new_male_count": 0,
        "new_female_count": 0,
        "next_male_start": 1,
        "next_female_start": 1,
    }
    summary.update(counters.attrs.get("counter_summary", {}))
    next_male = int(summary.get("next_male_start", 1))
    next_female = int(summary.get("next_female_start", 1))
    yy = year_to_yy(academic_year)
    resolved_rows = 0

    position_map = {index: pos for pos, index in enumerate(students_en.index)}

    for index_list in duplicate_groups.values():
        ordered = sorted(index_list, key=lambda idx: position_map.get(idx, 10**9))
        for index_label in ordered[1:]:
            if index_label not in students_en.index:
                continue
            row = students_en.loc[index_label]
            gender_value = pd.to_numeric(row.get("gender"), errors="coerce")
            if pd.isna(gender_value):
                raise ValueError(
                    "gender نامعتبر برای ردیف تکراری یافت شد؛ امکان تخصیص شمارندهٔ جدید نیست."
                )
            gender_value = int(gender_value)
            if gender_value == male_value:
                sequence = next_male
                next_male += 1
                summary["new_male_count"] = int(summary.get("new_male_count", 0)) + 1
                mid3 = male_mid3
            elif gender_value == female_value:
                sequence = next_female
                next_female += 1
                summary["new_female_count"] = int(summary.get("new_female_count", 0)) + 1
                mid3 = female_mid3
            else:
                raise ValueError(
                    "مقدار gender برای ردیف تکراری با policy هم‌خوانی ندارد؛ شمارندهٔ جدید قابل تولید نیست."
                )
            counters.at[index_label] = build_registration_id(yy, mid3, sequence)
            resolved_rows += 1

    if resolved_rows:
        summary["next_male_start"] = next_male
        summary["next_female_start"] = next_female
        summary["duplicate_resolution_mode"] = "assign-new"
        summary["duplicate_resolution_count"] = int(
            summary.get("duplicate_resolution_count", 0)
        ) + resolved_rows
        counters.attrs["counter_summary"] = summary
    return counters, resolved_rows


def _apply_counter_duplicate_strategy(
    *,
    counters: pd.Series,
    duplicate_groups: dict[str, list[object]],
    students_df: pd.DataFrame,
    students_en: pd.DataFrame,
    policy: PolicyConfig,
    academic_year: int,
    strategy: str,
    interactive: bool,
) -> tuple[pd.Series, bool, tuple[object, ...]]:
    """اجرای استراتژی انتخاب‌شده برای رفع تکرار شناسه‌ها."""

    report = _build_duplicate_row_report(students_df, students_en, duplicate_groups)
    report_text = _format_duplicate_report(report)

    normalized_strategy = (strategy or "prompt").strip().lower()
    valid_strategies = {"prompt", "abort", "drop", "assign-new"}
    if normalized_strategy not in valid_strategies:
        normalized_strategy = "prompt"

    if normalized_strategy == "prompt":
        if not interactive:
            raise ValueError(
                "student_id تکراری یافت شد و ورودی تعاملی در دسترس نیست؛ "
                "یکی از گزینه‌های --counter-duplicate-strategy={drop|assign-new|abort} را مشخص کنید."
            )
        normalized_strategy = _prompt_duplicate_resolution(report_text)
    elif normalized_strategy == "abort":
        print("❌ student_id تکراری در خروجی شمارنده یافت شد:")
        print(report_text)

    if normalized_strategy == "abort":
        raise ValueError(
            "student_id تکراری یافت شد؛ اجرای شمارنده متوقف شد تا ورودی اصلاح شود."
        )

    if normalized_strategy == "drop":
        drop_indexes: list[object] = []
        for payload in report:
            rows = payload.get("rows", [])
            drop_indexes.extend(row.get("index") for row in rows[1:])
        drop_indexes = [idx for idx in drop_indexes if idx in students_df.index]
        if not drop_indexes:
            return counters, False, tuple()
        print(
            f"ℹ️  {len(drop_indexes)} ردیف تکراری حذف می‌شود؛ شمارنده برای ردیف‌های باقی‌مانده بازتولید خواهد شد."
        )
        return counters, True, tuple(drop_indexes)

    updated, resolved_rows = _assign_new_counters_for_duplicates(
        counters,
        duplicate_groups,
        students_en,
        policy,
        academic_year,
    )
    print(f"ℹ️  شمارندهٔ جدید برای {resolved_rows} ردیف تکراری ساخته شد.")
    return updated, False, tuple()


def _inject_student_ids(
    students_df: pd.DataFrame,
    args: argparse.Namespace,
    policy: PolicyConfig,
) -> tuple[pd.Series, Dict[str, int], pd.DataFrame]:
    """ساخت ستون student_id با رعایت Policy و ورودی‌های UI/CLI."""

    overrides = getattr(args, "_ui_overrides", {}) or {}
    is_ui_mode = bool(getattr(args, "_ui_mode", False))

    def _resolve_path(name: str) -> str | None:
        value = overrides.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        cli_value = getattr(args, name, None)
        if isinstance(cli_value, str) and cli_value.strip():
            return cli_value.strip()
        return None

    prior_path = _resolve_path("prior_roster")
    current_path = _resolve_path("current_roster")

    prior_df = _read_optional_first_sheet(prior_path)
    current_df = _read_optional_first_sheet(current_path)

    strategy_override = overrides.get("counter_duplicate_strategy")
    if strategy_override in (None, "") and is_ui_mode:
        strategy_override = "assign-new"
    strategy_value = None
    if isinstance(strategy_override, str) and strategy_override.strip():
        strategy_value = strategy_override.strip()
    elif isinstance(getattr(args, "counter_duplicate_strategy", None), str):
        strategy_value = getattr(args, "counter_duplicate_strategy").strip()
    strategy_value = (strategy_value or "prompt").strip().lower()

    academic_year = overrides.get("academic_year") or getattr(args, "academic_year", None)
    if academic_year in ("", None):
        academic_year = infer_year_strict(current_df)
    if academic_year in ("", None):
        raise ValueError(
            "سال تحصیلی مشخص نشده یا در روستر جاری یکتا نیست؛ مقدار --academic-year الزامی است."
        )

    try:
        year_value = int(academic_year)
    except (TypeError, ValueError) as exc:  # pragma: no cover - نگهبان مهاجرت
        raise ValueError(f"سال تحصیلی نامعتبر است: {academic_year}") from exc

    final_students_en: pd.DataFrame | None = None

    while True:
        students_en = canonicalize_headers(students_df, header_mode="en")
        students_en = enrich_school_columns_en(
            students_en, empty_as_zero=policy.school_code_empty_as_zero
        )
        final_students_en = students_en

        required = {"national_id", "gender"}
        missing = sorted(required - set(students_en.columns))
        if missing:
            raise ValueError(
                "ستون‌های لازم برای شمارنده یافت نشدند؛ ستون‌های مورد انتظار: 'national_id' و 'gender'."
            )

        counters = assign_counters(
            students_en,
            prior_roster_df=prior_df,
            current_roster_df=current_df,
            academic_year=year_value,
        )

        duplicate_groups = find_duplicate_student_id_groups(counters)
        if duplicate_groups:
            counters, retry_required, drop_indexes = _apply_counter_duplicate_strategy(
                counters=counters,
                duplicate_groups=duplicate_groups,
                students_df=students_df,
                students_en=students_en,
                policy=policy,
                academic_year=year_value,
                strategy=strategy_value,
                interactive=(sys.stdin.isatty() and not is_ui_mode),
            )
            if retry_required:
                if drop_indexes:
                    students_df = students_df.drop(index=list(drop_indexes))
                continue
        break

    students_en = final_students_en if final_students_en is not None else canonicalize_headers(
        students_df, header_mode="en"
    )
    students_fa = canonicalize_headers(students_en, header_mode="fa")
    school_fa = CANON_EN_TO_FA.get("school_code", "کد مدرسه")
    if school_fa in students_fa.columns:
        school_series = students_fa[school_fa]
        if isinstance(school_series, pd.DataFrame):
            school_series = school_series.iloc[:, 0]
        students_df[school_fa] = school_series
    for column_name in ("school_code_raw", "school_code_norm", "school_status_resolved"):
        if column_name in students_en.columns:
            students_df[column_name] = students_en[column_name]

    assert_unique_student_ids(counters)

    summary = {
        "reused_count": 0,
        "new_male_count": 0,
        "new_female_count": 0,
        "next_male_start": 1,
        "next_female_start": 1,
    }
    summary.update(counters.attrs.get("counter_summary", {}))

    print(
        "[Counter] reused={reused_count} new_male={new_male_count} "
        "new_female={new_female_count} next_male_start={next_male_start} "
        "next_female_start={next_female_start}".format(**summary)
    )

    return counters, summary, students_df



def _run_build_matrix(args: argparse.Namespace, policy: PolicyConfig, progress: ProgressFn) -> int:
    """اجرای فرمان ساخت ماتریس با چاپ پیشرفت و خروجی Excel."""

    inspactor = Path(args.inspactor)
    schools = Path(args.schools)
    crosswalk = Path(args.crosswalk)
    output = Path(args.output)

    progress(0, f"policy {policy.version} loaded")
    insp_df = read_excel_first_sheet(inspactor)
    schools_df = read_excel_first_sheet(schools)
    crosswalk_groups_df, crosswalk_synonyms_df = read_crosswalk_workbook(crosswalk)

    governance_cfg: MentorPoolGovernanceConfig = getattr(
        policy, "mentor_pool_governance", _default_governance_config()
    )
    mentor_overrides = _resolve_mentor_pool_overrides(args)
    manager_overrides = _resolve_manager_overrides(args)
    if mentor_overrides or manager_overrides:
        insp_df = apply_manager_mentor_governance(
            insp_df,
            governance_cfg,
            mentor_overrides=mentor_overrides,
            manager_overrides=manager_overrides,
        )

    inputs = {
        "inspactor": str(inspactor),
        "schools": str(schools),
        "crosswalk": str(crosswalk),
    }
    inputs_mtime = {
        "inspactor": inspactor.stat().st_mtime,
        "schools": schools.stat().st_mtime,
        "crosswalk": crosswalk.stat().st_mtime,
    }

    min_coverage = _normalize_min_coverage_arg(getattr(args, "min_coverage", None))
    expected_policy_version = getattr(args, "policy_version", None)
    if isinstance(expected_policy_version, str):
        expected_policy_version = expected_policy_version.strip() or None
    cfg = BuildConfig(
        policy=policy,
        min_coverage_ratio=min_coverage,
        expected_policy_version=expected_policy_version,
    )

    if cfg.expected_policy_version and cfg.policy_version != cfg.expected_policy_version:
        raise ValueError(
            "policy version mismatch: "
            f"loaded='{cfg.policy_version}' expected='{cfg.expected_policy_version}'"
        )

    (
        matrix,
        validation,
        removed,
        unmatched_schools,
        unseen_groups,
        invalid_mentors,
        join_key_duplicates,
        progress_log,
    ) = build_matrix(
        insp_df,
        schools_df,
        crosswalk_groups_df,
        crosswalk_synonyms_df=crosswalk_synonyms_df,
        cfg=cfg,
        progress=progress,
    )

    duplicate_threshold = int(getattr(cfg, "join_key_duplicate_threshold", 0) or 0)
    duplicate_rows = int(len(join_key_duplicates))
    if duplicate_rows > duplicate_threshold >= 0:
        preview = ""
        if "warning_type" in validation.columns and "warning_message" in validation.columns:
            warning_mask = validation["warning_type"].notna()
            if bool(warning_mask.any()):
                preview = str(validation.loc[warning_mask, "warning_message"].iloc[0])
        progress(
            65,
            (
                "❌ join-key duplicates exceed threshold: "
                f"rows={duplicate_rows} threshold={duplicate_threshold}"
            ),
        )
        message = (
            "تعداد ردیف‌های دارای کلید تکراری ({rows}) از آستانهٔ مجاز "
            "({threshold}) بیشتر است."
        ).format(rows=duplicate_rows, threshold=duplicate_threshold)
        if preview:
            message += f" نمونه: {preview}"
        error = ValueError(message)
        setattr(error, "is_join_key_duplicate_threshold_error", True)
        raise error

    progress(70, "building sheets")
    meta = {
        "policy_version": policy.version,
        "ssot_version": "1.0.2",
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "build_host": platform.node(),
        "inputs": inputs,
        "inputs_mtime": inputs_mtime,
        "rowcounts": {
            "inspactor": int(len(insp_df)),
            "schools": int(len(schools_df)),
        },
    }
    group_coverage_summary = progress_log.attrs.get("group_coverage_summary")
    if group_coverage_summary:
        meta["group_coverage_summary"] = group_coverage_summary
    coverage_metrics = progress_log.attrs.get("coverage_metrics")
    if coverage_metrics:
        meta["coverage_metrics"] = asdict(coverage_metrics)
    normalization_reports = progress_log.attrs.get("column_normalization_reports")
    if normalization_reports:
        meta["column_normalization_reports"] = normalization_reports

    progress(72, "qa invariants")
    qa_report = run_all_invariants(
        policy=policy,
        matrix=matrix,
        inspactor=insp_df,
        invalid_mentors=invalid_mentors,
    )
    qa_context = QaValidationContext(
        matrix=matrix,
        inspactor=insp_df,
        invalid_mentors=invalid_mentors,
        meta=meta,
    )
    _export_qa_validation_workbook(
        report=qa_report,
        base_output=output,
        context=qa_context,
        stem_override="matrix_vs_students_validation.xlsx",
    )
    if not qa_report.passed:
        failed_rules = {violation.rule_id for violation in qa_report.violations}
        detail = "; ".join(f"{v.rule_id}: {v.message}" for v in qa_report.violations)
        raise ValueError(
            "QA invariants failed: "
            f"rules={sorted(failed_rules)} details={detail or 'n/a'}"
        )
    sheets = {
        "matrix": matrix,
        "validation": validation,
        "removed": removed,
        "unmatched_schools": unmatched_schools,
        "unseen_groups": unseen_groups,
        "invalid_mentors": invalid_mentors,
        "join_key_duplicates": join_key_duplicates,
        "progress_log": progress_log,
        "meta": pd.json_normalize([meta]),
    }
    if isinstance(unseen_groups, pd.DataFrame) and not unseen_groups.empty:
        sheets["invalid_group_tokens"] = unseen_groups
    group_coverage_df = progress_log.attrs.get("group_coverage")
    if isinstance(group_coverage_df, pd.DataFrame):
        sheets["group_coverage_debug"] = group_coverage_df
        if "is_unseen_viable" in group_coverage_df.columns:
            unseen_slice = group_coverage_df[
                group_coverage_df["is_unseen_viable"] == True
            ]
        else:
            unseen_slice = group_coverage_df[
                group_coverage_df["status"].isin(["candidate_only", "blocked_candidate"])
            ]
        sheets["group_coverage_unseen"] = unseen_slice
    header_internal = policy.excel.header_mode_internal
    prepared_sheets = {
        name: canonicalize_headers(df, header_mode=header_internal)
        for name, df in sheets.items()
    }
    write_xlsx_atomic(
        prepared_sheets,
        output,
        rtl=policy.excel.rtl,
        font_name=policy.excel.font_name,
        font_size=policy.excel.font_size,
        header_mode=policy.excel.header_mode_write,
    )
    progress(100, "done")
    return 0


def _load_matrix_candidate_pool(matrix_path: Path, policy: PolicyConfig) -> pd.DataFrame:
    """خواندن شیت ماتریس و آماده‌سازی آن به‌عنوان استخر منتورها.

    مثال::

        >>> from pathlib import Path
        >>> import pandas as pd
        >>> sample = Path("matrix.xlsx")
        >>> _ = pd.DataFrame({
        ...     "mentor_name": ["مجازی", "علی"],
        ...     "alias": [7501, 102],
        ...     "remaining_capacity": [0, 3],
        ... }).to_excel(sample, sheet_name="matrix", index=False)  # doctest: +SKIP
        >>> policy = load_policy()  # doctest: +SKIP
        >>> sanitized = _load_matrix_candidate_pool(sample, policy)  # doctest: +SKIP
        >>> int(sanitized.loc[0, "remaining_capacity"])  # doctest: +SKIP
        3

    Args:
        matrix_path: مسیر فایل ماتریس ساخته‌شده توسط build-matrix.
        policy: سیاست فعال برای نرمال‌سازی و اعمال فیلترهای مجازی.

    Returns:
        DataFrame سازگار با allocate_batch که منتورهای مجازی را حذف کرده است.
    """

    if not matrix_path.exists():
        raise FileNotFoundError(f"Matrix file not found: {matrix_path}")

    try:
        with pd.ExcelFile(matrix_path) as workbook:
            if "matrix" not in workbook.sheet_names:
                raise ValueError(
                    "شیت 'matrix' در فایل ماتریس یافت نشد؛ build-matrix باید اجرا شده باشد."
                )
            frame = workbook.parse("matrix")
    except FileNotFoundError:
        raise
    except Exception as exc:  # pragma: no cover - خطای خواندن پیش‌بینی‌نشده
        raise ValueError(f"خطا در خواندن ماتریس {matrix_path}: {exc}") from exc

    return canonicalize_headers(
        frame, header_mode=policy.excel.header_mode_internal
    )


def _prepare_allocation_frames(
    students_df: pd.DataFrame,
    pool_df: pd.DataFrame,
    *,
    policy: PolicyConfig,
    sanitize_pool: bool = True,
    pool_source: str = "inspactor",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """نرمال‌سازی ستون‌های ورودی برای اجرای تخصیص."""

    return canonicalize_allocation_frames(
        students_df,
        pool_df,
        policy=policy,
        sanitize_pool=sanitize_pool,
        pool_source=pool_source,
    )

def _allocate_and_write(
    students_base: pd.DataFrame,
    pool_base: pd.DataFrame,
    *,
    args: argparse.Namespace,
    policy: PolicyConfig,
    progress: ProgressFn,
    output: Path,
    capacity_column: str,
    db: LocalDatabase | None,
    command_name: str,
    input_students_path: Path | None,
    input_pool_path: Path | None,
    policy_path: Path,
) -> int:
    """اجرای تخصیص، الصاق شناسه‌ها و نوشتن خروجی‌های Excel."""
    run_uuid = uuid4().hex
    started_at = datetime.now(timezone.utc)
    cli_args_text = " ".join(getattr(args, "_raw_argv", [])).strip() or None
    qa_report: object | None = None
    history_metrics_df: pd.DataFrame | None = None
    success = False
    status_message = "success"

    student_ids, counter_summary, students_base = _inject_student_ids(
        students_base, args, policy
    )
    setattr(args, "_counter_summary", counter_summary)

    ui_center_map, cli_center_map, center_priority, strict_validation = _resolve_center_preferences(
        args, policy
    )

    allocations_df: pd.DataFrame | None = None
    updated_pool_df: pd.DataFrame | None = None
    logs_df: pd.DataFrame | None = None
    trace_df: pd.DataFrame | None = None
    sabt_allocations_df: pd.DataFrame | None = None

    try:
        allocations_df, updated_pool_df, logs_df, trace_df = allocate_batch(
            students_base.copy(deep=True),
            pool_base.copy(deep=True),
            policy=policy,
            progress=progress,
            capacity_column=capacity_column,
            frames_already_canonical=True,
            center_manager_map=cli_center_map,
            ui_center_manager_map=ui_center_map,
            center_priority=center_priority,
            strict_center_validation=strict_validation,
        )

        header_internal: HeaderMode = policy.excel.header_mode_internal  # type: ignore[assignment]

        def _attach_student_id(frame: pd.DataFrame, ensure_existing: bool = False) -> pd.DataFrame:
            en_frame = canonicalize_headers(frame, header_mode="en")
            aligned = student_ids.reindex(en_frame.index)
            aligned_string = aligned.astype("string")
            if ensure_existing and "student_id" in en_frame.columns:
                existing = en_frame["student_id"].astype("string")
                en_frame["student_id"] = existing.fillna(aligned_string)
            else:
                en_frame["student_id"] = aligned_string
            return canonicalize_headers(en_frame, header_mode=header_internal)

        allocations_df = _attach_student_id(allocations_df)
        logs_df = _attach_student_id(logs_df, ensure_existing=True)
        trace_df = _attach_student_id(trace_df, ensure_existing=True)

        export_profile_choice = _resolve_optional_override(args, "export_profile", "sabt") or "sabt"
        export_profile_path = _resolve_optional_override(
            args, "export_profile_path", str(_DEFAULT_ALLOC_PROFILE_PATH)
        ) or str(_DEFAULT_ALLOC_PROFILE_PATH)
        students_for_export = canonicalize_headers(students_base, header_mode=header_internal)
        students_for_export["student_id"] = (
            student_ids.reindex(students_for_export.index).astype("string")
        )
        if export_profile_choice == "sabt":
            sabt_profile = load_sabt_export_profile(Path(export_profile_path))
            sabt_allocations_df = build_sabt_export_frame(
                allocations_df,
                students_for_export,
                profile=sabt_profile,
                summary_df=trace_df.attrs.get("summary_df"),
            )

        # --- پاک‌سازی جامع خروجی قبل از نوشتن ---
        # اطمینان از معتبر بودن همه دیتافریم‌ها
        allocations_df = _ensure_valid_dataframe(allocations_df, "allocations")
        updated_pool_df = _ensure_valid_dataframe(updated_pool_df, "updated_pool")
        logs_df = _ensure_valid_dataframe(logs_df, "logs")
        trace_df = _ensure_valid_dataframe(trace_df, "trace")
        selection_reasons_df = build_selection_reason_rows(
            allocations_df,
            students_base,
            pool_base,
            policy=policy,
            logs=logs_df,
            trace=trace_df,
        )
        selection_reasons_df = _ensure_valid_dataframe(selection_reasons_df, "selection_reasons")
        sheet_name, selection_reasons_df = write_selection_reasons_sheet(
            selection_reasons_df,
            writer=None,
            policy=policy,
        )

        _maybe_export_import_to_sabt(
            args=args,
            allocations_df=allocations_df,
            students_df=students_base,
            mentors_df=pool_base,
            logs_df=logs_df,
            student_ids=student_ids,
        )

        if sabt_allocations_df is not None:
            sabt_allocations_df = _ensure_valid_dataframe(sabt_allocations_df, "allocations_sabt")

        qa_report = run_all_invariants(
            policy=policy,
            allocation=allocations_df,
            allocation_summary=updated_pool_df,
            student_report=None,
        )
        qa_context = QaValidationContext(
            allocation=allocations_df,
            allocation_summary=updated_pool_df,
            meta={
                "policy_version": policy.version,
                "ssot_version": "1.0.2",
                "source_output": str(output),
            },
        )
        _export_qa_validation_workbook(
            report=qa_report,
            base_output=output,
            context=qa_context,
        )
        if not qa_report.passed:
            failed_rules = {violation.rule_id for violation in qa_report.violations}
            detail = "; ".join(f"{v.rule_id}: {v.message}" for v in qa_report.violations)
            raise ValueError(
                "QA invariants failed: "
                f"rules={sorted(failed_rules)} details={detail or 'n/a'}"
            )

        # تبدیل نهایی به فرمت‌های قابل نوشتن در Excel
        allocations_df = _make_excel_safe(allocations_df)
        updated_pool_df = _make_excel_safe(updated_pool_df)
        logs_df = _make_excel_safe(logs_df)
        trace_df = _make_excel_safe(trace_df)
        selection_reasons_df = _make_excel_safe(selection_reasons_df)
        # sabt_allocations_df با هدر اصلی حفظ می‌شود اما از مسیر آماده‌سازی پیش‌فرض
        # عبور می‌کند تا ستون‌های موبایل/رهگیری به‌صورت متن و با صفر پیشتاز ذخیره شوند.
        # --- پایان پاک‌سازی ---

        progress(90, "writing outputs")
        sheets: dict[str, pd.DataFrame] = {}
        header_overrides: dict[str, HeaderMode | None] = {}
        prepare_overrides: dict[str, Literal["default", "raw"]] = {}
        if sabt_allocations_df is not None:
            sheets["allocations"] = allocations_df
            sheets["allocations_sabt"] = sabt_allocations_df
            header_overrides["allocations_sabt"] = None
        else:
            sheets["allocations"] = allocations_df
        sheets["updated_pool"] = updated_pool_df
        sheets["logs"] = logs_df
        sheets["trace"] = trace_df
        sheets[sheet_name] = selection_reasons_df

        summary_df_attr = trace_df.attrs.get("summary_df")
        history_info_df = trace_df.attrs.get("history_info_df")
        ui_overrides = getattr(args, "_ui_overrides", {}) or {}
        history_metrics_df = _empty_history_metrics_df()
        if (
            isinstance(summary_df_attr, pd.DataFrame)
            and not summary_df_attr.empty
            and history_info_df is not None
        ):
            try:
                enriched_summary = enrich_summary_with_history(
                    summary_df_attr,
                    students_df=students_base,
                    history_info_df=history_info_df,
                    policy=policy,
                )
                history_metrics_df = compute_history_metrics(enriched_summary)
            except KeyError:
                history_metrics_df = _empty_history_metrics_df()

        history_metrics_df = _log_history_metrics(
            summary_df_attr,
            students_df=students_base,
            history_info_df=history_info_df,
            policy=policy,
            history_metrics_df=history_metrics_df,
        )

        metrics_callback = ui_overrides.get("history_metrics_callback")
        if callable(metrics_callback):
            try:
                metrics_callback(history_metrics_df.copy())
            except Exception:  # pragma: no cover - UI callback safety
                logger.exception("Failed to deliver history metrics to UI")

        debug_sheets = collect_trace_debug_sheets(
            trace_df,
            students_df=students_base,
            history_info_df=history_info_df,
            policy=policy,
        )
        for name, df in debug_sheets.items():
            sheets[name] = _make_excel_safe(df)
            header_overrides[name] = None

        header_internal = policy.excel.header_mode_internal
        prepared_sheets: dict[str, pd.DataFrame] = {}
        for name, df in sheets.items():
            if header_overrides.get(name) is None:
                prepared_sheets[name] = df
            else:
                prepared_sheets[name] = canonicalize_headers(df, header_mode=header_internal)
        write_xlsx_atomic(
            prepared_sheets,
            output,
            rtl=policy.excel.rtl,
            font_name=policy.excel.font_name,
            font_size=policy.excel.font_size,
            header_mode=policy.excel.header_mode_write,
            sheet_header_modes=header_overrides,
            sheet_prepare_modes=prepare_overrides,
        )

        if getattr(args, "determinism_check", False):
            progress(92, "determinism check")
            allocations_check, pool_check, logs_check, trace_check = allocate_batch(
                students_base.copy(deep=True),
                pool_base.copy(deep=True),
                policy=policy,
                progress=lambda *_: None,
                capacity_column=capacity_column,
                frames_already_canonical=True,
                center_manager_map=cli_center_map,
                ui_center_manager_map=ui_center_map,
                center_priority=center_priority,
                strict_center_validation=strict_validation,
            )

            header_internal = policy.excel.header_mode_internal

            def _canon(df: pd.DataFrame) -> pd.DataFrame:
                return canonicalize_headers(df, header_mode=header_internal).reset_index(drop=True)

            try:
                pd_testing.assert_frame_equal(_canon(allocations_df), _canon(allocations_check))
                pd_testing.assert_frame_equal(_canon(updated_pool_df), _canon(pool_check))
                pd_testing.assert_frame_equal(_canon(logs_df), _canon(logs_check))
                pd_testing.assert_frame_equal(_canon(trace_df), _canon(trace_check))
            except AssertionError as exc:  # pragma: no cover - determinism failure path
                raise RuntimeError("Determinism check failed: outputs differ between runs") from exc

        if getattr(args, "audit", False) or getattr(args, "metrics", False):
            progress(95, "auditing allocations")
            report = audit_allocations(output)
            if getattr(args, "audit", False):
                _print_audit_summary(report)
            if getattr(args, "metrics", False):
                _print_metrics(report)

        progress(100, "done")
        success = True
        return 0
    except Exception as exc:
        status_message = str(exc)
        raise
    finally:
        completed_at = datetime.now(timezone.utc)
        total_students = len(students_base)
        allocated_students = (
            allocations_df.shape[0] if isinstance(allocations_df, pd.DataFrame) else None
        )
        unallocated_students = (
            total_students - allocated_students
            if allocated_students is not None
            else None
        )
        qa_outcome = history_store.summarize_qa(qa_report)
        run_ctx = history_store.build_run_context(
            command=command_name,
            cli_args=cli_args_text,
            policy_version=policy.version,
            ssot_version="1.0.2",
            started_at=started_at,
            completed_at=completed_at,
            success=success,
            message=status_message,
            input_students=input_students_path,
            input_pool=input_pool_path,
            output=output,
            policy_path=policy_path,
            total_students=total_students,
            allocated_students=allocated_students,
            unallocated_students=unallocated_students,
        )
        history_store.log_allocation_run(
            run_uuid=run_uuid,
            ctx=run_ctx,
            history_metrics=history_metrics_df if success else None,
            qa_outcome=qa_outcome,
            db=db,
        )


def _run_allocate(args: argparse.Namespace, policy: PolicyConfig, progress: ProgressFn) -> int:
    """اجرای فرمان تخصیص دانش‌آموزان با خروجی Excel."""

    students_path = Path(args.students)
    pool_path = Path(args.pool)
    output = Path(args.output)
    policy_path = Path(args.policy)
    capacity_column = args.capacity_column or policy.columns.remaining_capacity

    reader_students = _detect_reader(students_path)
    reader_pool = _detect_reader(pool_path)

    progress(0, "loading inputs")
    students_df = reader_students(students_path)
    pool_df = reader_pool(pool_path)

    students_base, pool_base = _prepare_allocation_frames(
        students_df,
        pool_df,
        policy=policy,
        sanitize_pool=True,
        pool_source="inspactor",
    )

    pool_base = _apply_mentor_pool_overrides(pool_base, policy, args)

    db = _resolve_local_db(args)
    return _allocate_and_write(
        students_base,
        pool_base,
        args=args,
        policy=policy,
        progress=progress,
        output=output,
        capacity_column=capacity_column,
        db=db,
        command_name="allocate",
        input_students_path=students_path,
        input_pool_path=pool_path,
        policy_path=policy_path,
    )


def _run_rule_engine(
    args: argparse.Namespace, policy: PolicyConfig, progress: ProgressFn
) -> int:
    """اجرای موتور قواعد روی ماتریس ساخته‌شده بدون نیاز به استخر جداگانه."""

    students_path = Path(args.students)
    matrix_path = Path(args.matrix)
    output = Path(args.output)
    policy_path = Path(args.policy)
    capacity_column = args.capacity_column or policy.columns.remaining_capacity

    reader_students = _detect_reader(students_path)

    progress(0, "loading inputs")
    students_df = reader_students(students_path)
    pool_df = _load_matrix_candidate_pool(matrix_path, policy)

    students_base, pool_base = _prepare_allocation_frames(
        students_df,
        pool_df,
        policy=policy,
        sanitize_pool=True,
        pool_source="matrix",
    )

    pool_base = _apply_mentor_pool_overrides(pool_base, policy, args)

    db = _resolve_local_db(args)
    return _allocate_and_write(
        students_base,
        pool_base,
        args=args,
        policy=policy,
        progress=progress,
        output=output,
        capacity_column=capacity_column,
        db=db,
        command_name="rule-engine",
        input_students_path=students_path,
        input_pool_path=matrix_path,
        policy_path=policy_path,
    )


def _build_parser() -> argparse.ArgumentParser:
    """ایجاد پارسر دستورات با زیرفرمان‌های build، allocate و rule-engine."""
    parser = argparse.ArgumentParser(description="Eligibility Matrix CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    build_cmd = sub.add_parser(
        "build-matrix",
        help="ساخت ماتریس اهلیت",
        description=(
            "ساخت ماتریس مطابق policy؛ شامل گیت صحت کد/نام مدرسه که "
            "در صورت عبور از آستانهٔ policy اجرا را متوقف می‌کند و ردیف‌های خطا را "
            "در شیت invalid_mentors ثبت می‌کند."
        ),
    )
    build_cmd.add_argument("--inspactor", required=True, help="مسیر فایل inspactor")
    build_cmd.add_argument("--schools", required=True, help="مسیر فایل schools")
    build_cmd.add_argument("--crosswalk", required=True, help="مسیر فایل crosswalk")
    build_cmd.add_argument("--output", required=True, help="مسیر Excel خروجی")
    build_cmd.add_argument(
        "--policy",
        default=str(_DEFAULT_POLICY_PATH),
        help="مسیر فایل policy.json",
    )
    build_cmd.add_argument(
        "--min-coverage",
        type=float,
        default=None,
        help="حداقل نسبت پوشش (0-1 یا درصد؛ پیش‌فرض از policy)",
    )
    build_cmd.add_argument(
        "--policy-version",
        default=None,
        help="نسخه یا هش policy مورد انتظار برای تطبیق قبل از ساخت",
    )
    build_cmd.add_argument(
        "--manager-overrides",
        default=None,
        help="JSON object نگاشت manager→enabled برای اجرای جاری ماتریس",
    )
    build_cmd.add_argument(
        "--mentor-overrides",
        default=None,
        help="JSON object نگاشت mentor_id→enabled برای اجرای جاری ماتریس",
    )

    alloc_cmd = sub.add_parser("allocate", help="تخصیص دانش‌آموزان به منتورها")
    alloc_cmd.add_argument("--students", required=True, help="مسیر فایل دانش‌آموزان")
    alloc_cmd.add_argument("--pool", required=True, help="مسیر استخر منتورها")
    alloc_cmd.add_argument("--output", required=True, help="مسیر Excel خروجی تخصیص")
    alloc_cmd.add_argument(
        "--capacity-column",
        default=None,
        help="نام ستون ظرفیت باقی‌مانده در استخر (پیش‌فرض از policy)",
    )
    alloc_cmd.add_argument(
        "--academic-year",
        type=int,
        required=False,
        help="سال تحصیلی شروع (مثلاً 1404)",
    )
    alloc_cmd.add_argument(
        "--prior-roster",
        default=None,
        help="مسیر روستر سال قبل برای بازیابی شمارنده",
    )
    alloc_cmd.add_argument(
        "--current-roster",
        default=None,
        help="مسیر روستر سال جاری برای ادامه شمارنده",
    )
    alloc_cmd.add_argument(
        "--policy",
        default=str(_DEFAULT_POLICY_PATH),
        help="مسیر فایل policy.json",
    )
    _add_center_management_args(alloc_cmd)
    alloc_cmd.add_argument(
        "--golestan-manager",
        default=None,
        help="(Legacy) نام مدیر مرکز گلستان (شناسه مرکز ۱)",
    )
    alloc_cmd.add_argument(
        "--sadra-manager",
        default=None,
        help="(Legacy) نام مدیر مرکز صدرا (شناسه مرکز ۲)",
    )
    alloc_cmd.add_argument(
        "--center-managers",
        default=None,
        help="نگاشت JSON مرکز→لیست مدیران برای override گروهی",
    )
    alloc_cmd.add_argument(
        "--mentor-overrides",
        default=None,
        help="JSON object نگاشت mentor_id→enabled برای اجرای جاری",
    )
    alloc_cmd.add_argument(
        "--audit",
        action="store_true",
        help="پس از تولید خروجی، ممیزی خودکار را اجرا کن",
    )
    alloc_cmd.add_argument(
        "--metrics",
        action="store_true",
        help="پس از اجرا، خلاصهٔ JSON ممیزی را چاپ کن",
    )
    alloc_cmd.add_argument(
        "--sabt-output",
        default=None,
        help="در صورت تعیین، خروجی ImportToSabt را در این مسیر بنویس",
    )
    alloc_cmd.add_argument(
        "--sabt-config",
        default=str(_DEFAULT_EXPORTER_CONFIG_PATH),
        help="مسیر فایل SmartAlloc Exporter Config",
    )
    alloc_cmd.add_argument(
        "--sabt-template",
        default=str(_DEFAULT_SABT_TEMPLATE_PATH),
        help="مسیر فایل قالب ImportToSabt",
    )
    alloc_cmd.add_argument(
        "--export-profile",
        choices=("basic", "sabt"),
        default="sabt",
        help="نوع خروجی شیت allocations (basic=ساختار قبلی، sabt=پروفایل 45 ستونی)",
    )
    alloc_cmd.add_argument(
        "--export-profile-path",
        default=str(_DEFAULT_ALLOC_PROFILE_PATH),
        help="مسیر فایل پروفایل Sabt (Sheet1) برای خروجی تخصیص",
    )
    alloc_cmd.add_argument(
        "--determinism-check",
        action="store_true",
        help="اجرای دوباره تخصیص برای تضمین دترمینیسم",
    )
    alloc_cmd.add_argument(
        "--counter-duplicate-strategy",
        choices=("prompt", "abort", "drop", "assign-new"),
        default="prompt",
        help="نحوهٔ مدیریت student_id تکراری: prompt=سوال تعاملی، drop=حذف، assign-new=شمارندهٔ جدید",
    )
    _add_local_db_args(alloc_cmd)

    rule_cmd = sub.add_parser(
        "rule-engine",
        help="اجرای موتور قواعد روی ماتریس ساخته‌شده بدون استخر مجزا",
    )
    rule_cmd.add_argument("--matrix", required=True, help="مسیر فایل ماتریس")
    rule_cmd.add_argument("--students", required=True, help="مسیر فایل دانش‌آموزان")
    rule_cmd.add_argument("--output", required=True, help="مسیر خروجی تخصیص")
    rule_cmd.add_argument(
        "--capacity-column",
        default=None,
        help="نام ستون ظرفیت باقی‌مانده (پیش‌فرض policy)",
    )
    rule_cmd.add_argument(
        "--golestan-manager",
        default="شهدخت کشاورز",
        help="نام مدیر مرکز گلستان (شناسه مرکز ۱)",
    )
    rule_cmd.add_argument(
        "--sadra-manager",
        default="آیناز هوشمند",
        help="نام مدیر مرکز صدرا (شناسه مرکز ۲)",
    )
    rule_cmd.add_argument(
        "--center-priority",
        default="1,2,0",
        help="ترتیب پردازش مراکز هنگام اجرای موتور قواعد",
    )
    rule_cmd.add_argument(
        "--academic-year",
        type=int,
        required=False,
        help="سال تحصیلی شروع (مثلاً 1404)",
    )
    rule_cmd.add_argument(
        "--prior-roster",
        default=None,
        help="مسیر روستر سال قبل برای بازیابی شمارنده",
    )
    rule_cmd.add_argument(
        "--current-roster",
        default=None,
        help="مسیر روستر سال جاری برای ادامه شمارنده",
    )
    rule_cmd.add_argument(
        "--policy",
        default=str(_DEFAULT_POLICY_PATH),
        help="مسیر فایل policy.json",
    )
    rule_cmd.add_argument(
        "--mentor-overrides",
        default=None,
        help="JSON object نگاشت mentor_id→enabled برای اجرای جاری",
    )
    rule_cmd.add_argument(
        "--audit",
        action="store_true",
        help="پس از تولید خروجی، ممیزی خودکار را اجرا کن",
    )
    rule_cmd.add_argument(
        "--metrics",
        action="store_true",
        help="پس از اجرا، خلاصهٔ JSON ممیزی را چاپ کن",
    )
    rule_cmd.add_argument(
        "--determinism-check",
        action="store_true",
        help="اجرای دوباره تخصیص برای تضمین دترمینیسم",
    )
    rule_cmd.add_argument(
        "--sabt-output",
        default=None,
        help="در صورت تعیین، خروجی ImportToSabt را در این مسیر بنویس",
    )
    rule_cmd.add_argument(
        "--sabt-config",
        default=str(_DEFAULT_EXPORTER_CONFIG_PATH),
        help="مسیر فایل SmartAlloc Exporter Config",
    )
    rule_cmd.add_argument(
        "--sabt-template",
        default=str(_DEFAULT_SABT_TEMPLATE_PATH),
        help="مسیر فایل قالب ImportToSabt",
    )
    rule_cmd.add_argument(
        "--export-profile",
        choices=("basic", "sabt"),
        default="sabt",
        help="نوع خروجی شیت allocations هنگام اجرای rule-engine",
    )
    rule_cmd.add_argument(
        "--export-profile-path",
        default=str(_DEFAULT_ALLOC_PROFILE_PATH),
        help="مسیر فایل پروفایل Sabt برای خروجی rule-engine",
    )
    rule_cmd.add_argument(
        "--counter-duplicate-strategy",
        choices=("prompt", "abort", "drop", "assign-new"),
        default="prompt",
        help="نحوهٔ مدیریت student_id تکراری هنگام تولید شمارنده",
    )
    _add_local_db_args(rule_cmd)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    progress_factory: Callable[[], ProgressFn] | None = None,
    build_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
    allocate_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
    rule_engine_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int]
    | None = None,
    ui_overrides: dict[str, object] | None = None,
) -> int:
    """نقطهٔ ورود CLI؛ خروجی ۰ به معنای موفقیت است."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    args._raw_argv = list(argv) if argv is not None else sys.argv[1:]

    args._ui_overrides = ui_overrides or {}
    args._ui_mode = ui_overrides is not None

    policy_path = Path(args.policy)
    policy = load_policy(policy_path)

    progress = progress_factory() if progress_factory is not None else _default_progress

    try:
        if args.command == "build-matrix":
            runner = build_runner or _run_build_matrix
            return runner(args, policy, progress)

        if args.command == "allocate":
            runner = allocate_runner or _run_allocate
            return runner(args, policy, progress)

        if args.command == "rule-engine":
            runner = rule_engine_runner or _run_rule_engine
            return runner(args, policy, progress)

        raise RuntimeError(f"Unsupported command: {args.command}")
    except ValueError as exc:
        if ui_overrides is not None:
            raise
        is_coverage_error = getattr(exc, "is_coverage_threshold_error", False)
        is_dedup_error = getattr(exc, "is_dedup_removed_threshold_error", False)
        is_duplicate_error = getattr(exc, "is_join_key_duplicate_threshold_error", False)
        is_school_lookup_error = getattr(
            exc, "is_school_lookup_threshold_error", False
        )
        if not (
            is_coverage_error
            or is_dedup_error
            or is_duplicate_error
            or is_school_lookup_error
        ):
            raise
        print(f"❌ {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
