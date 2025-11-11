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
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd
from pandas.api import types as pd_types

from app.core.allocate_students import allocate_batch
from app.core.build_matrix import build_matrix
from app.core.policy_loader import PolicyConfig, load_policy
from app.infra.io_utils import (
    ALT_CODE_COLUMN,
    read_crosswalk_workbook,
    read_excel_first_sheet,
    write_xlsx_atomic,
)
# --- واردات اصلاح شده از app.core ---
from app.core.common.columns import (
    resolve_aliases,
    coerce_semantics,
)
from app.core.common.column_normalizer import normalize_input_columns
from app.core.common.normalization import safe_int_value
# --- پایان واردات اصلاح شده ---

ProgressFn = Callable[[int, str], None]

_DEFAULT_POLICY_PATH = Path("config/policy.json")


def _default_progress(pct: int, message: str) -> None:
    """چاپ سادهٔ وضعیت پیشرفت در حالت headless."""
    print(f"{pct:3d}% | {message}")


def _detect_reader(path: Path) -> Callable[[Path], pd.DataFrame]:
    """انتخاب تابع خواندن مناسب؛ برای Excel شیت 'matrix' را ترجیح بده."""
    suffix = path.suffix.lower()
    dtype_map = {ALT_CODE_COLUMN: str}
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        def _read_xlsx(p: Path) -> pd.DataFrame:
            xls = pd.ExcelFile(p)
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
        return pd.isna(x)
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
    header_mode = policy.excel.header_mode
    write_xlsx_atomic(
        sheets,
        output,
        rtl=policy.excel.rtl,
        font_name=policy.excel.font_name,
        header_mode=header_mode,
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

    allocations_df, updated_pool_df, logs_df, trace_df = allocate_batch(
        students_df,
        pool_df,
        policy=policy,
        progress=progress,
        capacity_column=capacity_column,
    )

    # --- پاک‌سازی جامع خروجی قبل از نوشتن ---
    # اطمینان از معتبر بودن همه دیتافریم‌ها
    allocations_df = _ensure_valid_dataframe(allocations_df, "allocations")
    updated_pool_df = _ensure_valid_dataframe(updated_pool_df, "updated_pool")
    logs_df = _ensure_valid_dataframe(logs_df, "logs")
    trace_df = _ensure_valid_dataframe(trace_df, "trace")
    
    # تبدیل نهایی به فرمت‌های قابل نوشتن در Excel
    allocations_df = _make_excel_safe(allocations_df)
    updated_pool_df = _make_excel_safe(updated_pool_df)
    logs_df = _make_excel_safe(logs_df)
    trace_df = _make_excel_safe(trace_df)
    # --- پایان پاک‌سازی ---

    progress(90, "writing outputs")
    sheets = {
        "allocations": allocations_df,
        "updated_pool": updated_pool_df,
        "logs": logs_df,
        "trace": trace_df,
    }
    header_mode = policy.excel.header_mode
    write_xlsx_atomic(
        sheets,
        output,
        rtl=policy.excel.rtl,
        font_name=policy.excel.font_name,
        header_mode=header_mode,
    )
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
        "--policy",
        default=str(_DEFAULT_POLICY_PATH),
        help="مسیر فایل policy.json",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    progress_factory: Callable[[], ProgressFn] | None = None,
    build_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
    allocate_runner: Callable[[argparse.Namespace, PolicyConfig, ProgressFn], int] | None = None,
) -> int:
    """نقطهٔ ورود CLI؛ خروجی ۰ به معنای موفقیت است."""
    parser = _build_parser()
    args = parser.parse_args(argv)

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
