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
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Sequence

import pandas as pd
from pandas import testing as pd_testing
from pandas.api import types as pd_types

from app.core.allocate_students import allocate_batch, build_selection_reason_rows
from app.core.canonical_frames import (
    canonicalize_allocation_frames,
    sanitize_pool_for_allocation as _sanitize_pool_for_allocation,
)
from app.core.build_matrix import BuildConfig, build_matrix
from app.core.policy_loader import PolicyConfig, load_policy
from app.infra.excel_writer import write_selection_reasons_sheet
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
    detect_academic_year_from_counters,
    infer_year_strict,
    pick_counter_sheet_name,
)
# --- پایان واردات اصلاح شده ---

ProgressFn = Callable[[int, str], None]

_DEFAULT_POLICY_PATH = Path("config/policy.json")
_DEFAULT_EXPORTER_CONFIG_PATH = Path("config/SmartAlloc_Exporter_Config_v1.json")
_DEFAULT_SABT_TEMPLATE_PATH = Path("templates/ImportToSabt (1404) - Copy.xlsx")


def _default_progress(pct: int, message: str) -> None:
    """چاپ سادهٔ وضعیت پیشرفت در حالت headless."""
    print(f"{pct:3d}% | {message}")


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


def _parse_center_priority_arg(value: object) -> tuple[int, ...]:
    """تبدیل ورودی دلخواه به توالی پایدار از مقادیر عددی مرکز."""

    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        text = str(value).replace("،", ",").strip()
        if not text:
            return ()
        items = [token.strip() for token in text.split(",") if token.strip()]
    priority: list[int] = []
    for token in items:
        try:
            priority.append(int(token))
        except (TypeError, ValueError) as exc:
            raise ValueError("center priority must be a comma-separated list of integers") from exc
    return tuple(priority)


def _resolve_center_preferences(
    args: argparse.Namespace,
) -> tuple[dict[int, tuple[str, ...]], tuple[int, ...]]:
    """خواندن تنظیمات مدیر مرکز و ترتیب اولویت با اولویت‌دهی به UI."""

    overrides = getattr(args, "_ui_overrides", {}) or {}

    def _resolve_text(key: str, default: str) -> str:
        value = overrides.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        cli_value = getattr(args, key, None)
        if isinstance(cli_value, str) and cli_value.strip():
            return cli_value.strip()
        return default

    golestan = _resolve_text("golestan_manager", "شهدخت کشاورز")
    sadra = _resolve_text("sadra_manager", "آیناز هوشمند")
    manager_map: dict[int, tuple[str, ...]] = {}
    if golestan:
        manager_map[1] = (golestan,)
    if sadra:
        manager_map[2] = (sadra,)

    priority_source = overrides.get("center_priority") or getattr(args, "center_priority", None)
    try:
        priority = _parse_center_priority_arg(priority_source)
    except ValueError as exc:
        raise ValueError(f"center priority override is invalid: {exc}") from exc
    if not priority:
        priority = (1, 2, 0)

    return manager_map, priority


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


def _inject_student_ids(
    students_df: pd.DataFrame,
    args: argparse.Namespace,
    policy: PolicyConfig,
) -> tuple[pd.Series, Dict[str, int]]:
    """ساخت ستون student_id با رعایت Policy و ورودی‌های UI/CLI."""

    overrides = getattr(args, "_ui_overrides", {}) or {}

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

    students_en = canonicalize_headers(students_df, header_mode="en")
    students_en = enrich_school_columns_en(
        students_en, empty_as_zero=policy.school_code_empty_as_zero
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
    required = {"national_id", "gender"}
    missing = sorted(required - set(students_en.columns))
    if missing:
        raise ValueError(
            "ستون‌های لازم برای شمارنده یافت نشدند؛ ستون‌های مورد انتظار: 'national_id' و 'gender'."
        )

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

    counters = assign_counters(
        students_en,
        prior_roster_df=prior_df,
        current_roster_df=current_df,
        academic_year=year_value,
    )

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

    return counters, summary



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
    normalization_reports = progress_log.attrs.get("column_normalization_reports")
    if normalization_reports:
        meta["column_normalization_reports"] = normalization_reports
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
) -> int:
    """اجرای تخصیص، الصاق شناسه‌ها و نوشتن خروجی‌های Excel."""

    student_ids, counter_summary = _inject_student_ids(students_base, args, policy)
    setattr(args, "_counter_summary", counter_summary)

    center_manager_map, center_priority = _resolve_center_preferences(args)

    allocations_df, updated_pool_df, logs_df, trace_df = allocate_batch(
        students_base.copy(deep=True),
        pool_base.copy(deep=True),
        policy=policy,
        progress=progress,
        capacity_column=capacity_column,
        frames_already_canonical=True,
        center_manager_map=center_manager_map,
        center_priority=center_priority,
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

    # تبدیل نهایی به فرمت‌های قابل نوشتن در Excel
    allocations_df = _make_excel_safe(allocations_df)
    updated_pool_df = _make_excel_safe(updated_pool_df)
    logs_df = _make_excel_safe(logs_df)
    trace_df = _make_excel_safe(trace_df)
    selection_reasons_df = _make_excel_safe(selection_reasons_df)
    # --- پایان پاک‌سازی ---

    progress(90, "writing outputs")
    sheets = {
        "allocations": allocations_df,
        "updated_pool": updated_pool_df,
        "logs": logs_df,
        "trace": trace_df,
        sheet_name: selection_reasons_df,
    }
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

    if getattr(args, "determinism_check", False):
        progress(92, "determinism check")
        allocations_check, pool_check, logs_check, trace_check = allocate_batch(
            students_base.copy(deep=True),
            pool_base.copy(deep=True),
            policy=policy,
            progress=lambda *_: None,
            capacity_column=capacity_column,
            frames_already_canonical=True,
            center_manager_map=center_manager_map,
            center_priority=center_priority,
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
    return 0


def _run_allocate(args: argparse.Namespace, policy: PolicyConfig, progress: ProgressFn) -> int:
    """اجرای فرمان تخصیص دانش‌آموزان با خروجی Excel."""

    students_path = Path(args.students)
    pool_path = Path(args.pool)
    output = Path(args.output)
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

    return _allocate_and_write(
        students_base,
        pool_base,
        args=args,
        policy=policy,
        progress=progress,
        output=output,
        capacity_column=capacity_column,
    )


def _run_rule_engine(
    args: argparse.Namespace, policy: PolicyConfig, progress: ProgressFn
) -> int:
    """اجرای موتور قواعد روی ماتریس ساخته‌شده بدون نیاز به استخر جداگانه."""

    students_path = Path(args.students)
    matrix_path = Path(args.matrix)
    output = Path(args.output)
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

    return _allocate_and_write(
        students_base,
        pool_base,
        args=args,
        policy=policy,
        progress=progress,
        output=output,
        capacity_column=capacity_column,
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
    alloc_cmd.add_argument(
        "--golestan-manager",
        default="شهدخت کشاورز",
        help="نام مدیر مرکز گلستان (شناسه مرکز ۱)",
    )
    alloc_cmd.add_argument(
        "--sadra-manager",
        default="آیناز هوشمند",
        help="نام مدیر مرکز صدرا (شناسه مرکز ۲)",
    )
    alloc_cmd.add_argument(
        "--center-priority",
        default="1,2,0",
        help="ترتیب پردازش مراکز هنگام تخصیص (لیست جداشده با ویرگول)",
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
        "--determinism-check",
        action="store_true",
        help="اجرای دوباره تخصیص برای تضمین دترمینیسم",
    )

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

    args._ui_overrides = ui_overrides or {}

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
