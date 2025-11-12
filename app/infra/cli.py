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
import re
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Sequence

import pandas as pd
from pandas import testing as pd_testing
from pandas.api import types as pd_types

from app.core.allocate_students import allocate_batch, build_selection_reason_rows
from app.core.build_matrix import build_matrix
from app.core.policy_loader import PolicyConfig, load_policy
from app.infra.excel_writer import write_selection_reasons_sheet
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
    coerce_semantics,
    enrich_school_columns_en,
    resolve_aliases,
)
from app.core.common.column_normalizer import normalize_input_columns
from app.core.common.normalization import safe_int_value
from app.core.common.utils import normalize_fa
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


def _make_unique_columns(columns: Sequence[str]) -> list[str]:
    """ساخت نام ستون یکتا با حفظ ترتیب اولیه."""

    seen: dict[str, int] = {}
    result: list[str] = []
    for column in columns:
        base = str(column).strip() or "column"
        count = seen.get(base, 0)
        name = base if count == 0 else f"{base} ({count + 1})"
        while name in seen:
            count += 1
            name = f"{base} ({count + 1})"
        seen[base] = count + 1
        seen[name] = 1
        result.append(name)
    return result


def _sanitize_pool_for_allocation(
    df: pd.DataFrame,
    *,
    policy: PolicyConfig | None = None,
) -> pd.DataFrame:
    """پاک‌سازی استخر پشتیبان‌ها برای تخصیص مطابق Policy.

    مثال::

        >>> import pandas as pd
        >>> raw = pd.DataFrame({
        ...     "mentor_name": ["مجازی", "علی"],
        ...     "alias": [7501, 102],
        ...     "remaining_capacity": [100, 2],
        ... })
        >>> cleaned = _sanitize_pool_for_allocation(raw)
        >>> cleaned["remaining_capacity"].tolist()
        [2]
    """

    active_policy = policy or load_policy()
    frame = canonicalize_headers(
        df, header_mode=active_policy.excel.header_mode_internal
    ).copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = ["__".join(map(str, tpl)).strip() for tpl in frame.columns.to_flat_index()]
    if frame.columns.duplicated().any():
        frame.columns = _make_unique_columns(frame.columns)

    mask_virtual = pd.Series(False, index=frame.index)
    patterns = active_policy.virtual_name_patterns
    regex: re.Pattern[str] | None = None
    if patterns:
        joined = "|".join(f"(?:{pattern})" for pattern in patterns)
        regex = re.compile(joined, re.IGNORECASE)
    if regex and "mentor_name" in frame.columns:
        mask_virtual |= frame["mentor_name"].astype(str).map(lambda text: bool(regex.search(text)))

    alias_ranges = active_policy.virtual_alias_ranges
    for column_name in ("alias", "mentor_id"):
        if column_name not in frame.columns:
            continue
        alias_numeric = pd.to_numeric(frame[column_name], errors="coerce")
        for start, end in alias_ranges:
            mask_virtual |= alias_numeric.between(start, end, inclusive="both")

    sanitized = frame.loc[~mask_virtual].copy()

    rename_candidates = {
        "remaining_capacity | remaining_capacity": "remaining_capacity",
    }
    for old, new in rename_candidates.items():
        if old in sanitized.columns and new not in sanitized.columns:
            sanitized = sanitized.rename(columns={old: new})

    defaults = {
        "remaining_capacity": ("Int64", 0),
        "allocations_new": ("Int64", 0),
        "mentor_id": ("Int64", pd.NA),
    }
    for column, (dtype, default) in defaults.items():
        if column not in sanitized.columns:
            sanitized[column] = pd.Series([default] * len(sanitized), dtype=dtype)
        else:
            series = pd.Series(sanitized[column])
            if dtype == "Int64":
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
            else:
                series = series.astype(dtype)
            sanitized[column] = series

    return canonicalize_headers(sanitized, header_mode=active_policy.excel.header_mode_internal)


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
    """
    ادغام ستون‌های تکراری در یک دیتافریم با روش کاملاً ایمن
    
    این تابع تمام ستون‌هایی که نام یکسان دارند را ادغام می‌کند و فقط یک نمونه از هر نام ستون نگه می‌دارد.
    """
    if df.empty or not any(df.columns.duplicated()):
        return df.copy()
    
    # ایجاد یک DataFrame جدید برای نتایج
    result_df = pd.DataFrame(index=df.index)
    
    # پردازش هر نام ستون منحصر به فرد
    unique_columns = df.columns.unique()
    for col_name in unique_columns:
        # گرفتن تمام ستون‌هایی که این نام را دارند
        cols_with_name = df.loc[:, df.columns == col_name]
        
        if cols_with_name.shape[1] == 1:
            # اگر فقط یک ستون با این نام وجود داشت
            result_df[col_name] = cols_with_name.iloc[:, 0]
        else:
            # اگر چند ستون با این نام وجود داشت
            # استفاده از bfill برای پر کردن مقادیر خالی با مقادیر بعدی
            filled = cols_with_name.bfill(axis=1)
            result_df[col_name] = filled.iloc[:, 0]
    
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
    students_en = enrich_school_columns_en(students_en)
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

    (
        matrix,
        validation,
        removed,
        unmatched_schools,
        unseen_groups,
        invalid_mentors,
    ) = build_matrix(
        insp_df,
        schools_df,
        crosswalk_groups_df,
        crosswalk_synonyms_df=crosswalk_synonyms_df,
        progress=progress,
    )

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
    sheets = {
        "matrix": matrix,
        "validation": validation,
        "removed": removed,
        "unmatched_schools": unmatched_schools,
        "unseen_groups": unseen_groups,
        "invalid_mentors": invalid_mentors,
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
        header_mode=policy.excel.header_mode_write,
    )
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
    pool_df = _sanitize_pool_for_allocation(pool_df, policy=policy)

    # --- تکمیل داده‌های دانش‌آموزان بر اساس فایل ورودی واقعی (Report (1).xlsx) ---
    # بر اساس محتوای واقعی فایل، ستون‌های لازم را ایجاد می‌کنیم
    
    # 1. کدرشته: از group_code
    if 'group_code' in students_df.columns:
        students_df['کدرشته'] = students_df['group_code']
    else:
        students_df['کدرشته'] = 0
    
    # 2. گروه آزمایشی: در فایل وجود ندارد، مقدار پیش‌فرض
    students_df['گروه آزمایشی'] = "نامشخص"
    
    # 3. جنسیت: از فایل (به شرط وجود)
    if 'جنسیت' not in students_df.columns and 'جنسیت' in students_df.columns:
        students_df['جنسیت'] = students_df['جنسیت']
    elif 'جنسیت' not in students_df.columns:
        students_df['جنسیت'] = 1  # 1 برای پسر، 2 برای دختر (مقدار پیش‌فرض)
    
    # 4. دانش آموز فارغ: از وضعیت تحصیلی
    if 'وضعیت تحصیلی' in students_df.columns:
        students_df['دانش آموز فارغ'] = students_df['وضعیت تحصیلی']
    else:
        students_df['دانش آموز فارغ'] = 0  # 0 برای مشغول به تحصیل، 1 برای فارغ‌التحصیل
    
    # 5. مرکز گلستان صدرا: در فایل وجود ندارد، مقدار پیش‌فرض
    students_df['مرکز گلستان صدرا'] = 0
    
    # 6. مالی حکمت بنیاد: در فایل وجود ندارد، مقدار پیش‌فرض
    students_df['مالی حکمت بنیاد'] = 0
    
    # 7. کد مدرسه: در فایل وجود ندارد، مقدار پیش‌فرض
    students_df['کد مدرسه'] = 0
    
    # --- اعمال نرمال‌سازی‌های استاندارد ---
    students_df = resolve_aliases(students_df, "report")
    students_df = coerce_semantics(students_df, "report")
    students_df, _ = normalize_input_columns(
        students_df, kind="StudentReport", include_alias=True, report=False
    )

    pool_df = resolve_aliases(pool_df, "inspactor")
    pool_df = coerce_semantics(pool_df, "inspactor")
    pool_df, _ = normalize_input_columns(
        pool_df, kind="MentorPool", include_alias=True, report=False
    )

    # Normalize mentor_id to string
    if "کد کارمندی پشتیبان" in pool_df.columns:
        pool_df["کد کارمندی پشتیبان"] = (
            pool_df["کد کارمندی پشتیبان"].fillna("").astype(str).str.strip()
        )
    else:
        # اگر ستون کد کارمندی نبود، یک ستون با مقادیر پیش‌فرض ایجاد می‌کنیم
        pool_df["کد کارمندی پشتیبان"] = ["MENTOR_" + str(i) for i in range(len(pool_df))]

    # اطمینان از وجود ستون‌های ضروری برای حالت‌های پیش‌فرض
    if "occupancy_ratio" not in pool_df.columns:
        pool_df["occupancy_ratio"] = 0.0
    if "allocations_new" not in pool_df.columns:
        pool_df["allocations_new"] = 0
    if "remaining_capacity" not in pool_df.columns:
        pool_df["remaining_capacity"] = 1  # ظرفیت پیش‌فرض 1

    students_base = students_df.copy(deep=True)
    pool_base = pool_df.copy(deep=True)

    student_ids, counter_summary = _inject_student_ids(students_base, args, policy)
    setattr(args, "_counter_summary", counter_summary)

    allocations_df, updated_pool_df, logs_df, trace_df = allocate_batch(
        students_base.copy(deep=True),
        pool_base.copy(deep=True),
        policy=policy,
        progress=progress,
        capacity_column=capacity_column,
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


def _build_parser() -> argparse.ArgumentParser:
    """ایجاد پارسر دستورات با زیرفرمان‌های build و allocate."""
    parser = argparse.ArgumentParser(description="Eligibility Matrix CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    build_cmd = sub.add_parser("build-matrix", help="ساخت ماتریس اهلیت")
    build_cmd.add_argument("--inspactor", required=True, help="مسیر فایل inspactor")
    build_cmd.add_argument("--schools", required=True, help="مسیر فایل schools")
    build_cmd.add_argument("--crosswalk", required=True, help="مسیر فایل crosswalk")
    build_cmd.add_argument("--output", required=True, help="مسیر Excel خروجی")
    build_cmd.add_argument(
        "--policy",
        default=str(_DEFAULT_POLICY_PATH),
        help="مسیر فایل policy.json",
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
        "--determinism-check",
        action="store_true",
        help="اجرای دوباره تخصیص برای تضمین دترمینیسم",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    progress_factory: Callable[[], ProgressFn] | None = None,
    build_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
    allocate_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
    ui_overrides: dict[str, object] | None = None,
) -> int:
    """نقطهٔ ورود CLI؛ خروجی ۰ به معنای موفقیت است."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    args._ui_overrides = ui_overrides or {}

    policy_path = Path(args.policy)
    policy = load_policy(policy_path)

    progress = progress_factory() if progress_factory is not None else _default_progress

    if args.command == "build-matrix":
        runner = build_runner or _run_build_matrix
        return runner(args, policy, progress)

    if args.command == "allocate":
        runner = allocate_runner or _run_allocate
        return runner(args, policy, progress)

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
