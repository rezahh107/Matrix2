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
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.build_matrix import build_matrix
from app.core.policy_loader import PolicyConfig, load_policy
from app.infra.io_utils import (
    ALT_CODE_COLUMN,
    read_crosswalk_workbook,
    read_excel_first_sheet,
    write_xlsx_atomic,
)

ProgressFn = Callable[[int, str], None]

_DEFAULT_POLICY_PATH = Path("config/policy.json")


def _default_progress(pct: int, message: str) -> None:
    """چاپ سادهٔ وضعیت پیشرفت در حالت headless."""

    print(f"{pct:3d}% | {message}")


def _detect_reader(path: Path) -> Callable[[Path], pd.DataFrame]:
    """انتخاب تابع خواندن مناسب بر اساس پسوند فایل."""

    suffix = path.suffix.lower()
    dtype_map = {ALT_CODE_COLUMN: str}
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return lambda p: pd.read_excel(p, dtype=dtype_map)
    return lambda p: pd.read_csv(p, dtype=dtype_map)


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
    # Safety net برای هدرهای fa_en اگر کاربر policy را هنوز به‌روز نکرده باشد
    _alias_map = {
        "کدرشته | group_code": "کدرشته",
        "جنسیت | gender": "جنسیت",
        "دانش آموز فارغ | graduation_status": "دانش آموز فارغ",
        "مرکز گلستان صدرا | center": "مرکز گلستان صدرا",
        "مالی حکمت بنیاد | finance": "مالی حکمت بنیاد",
        "کد مدرسه | school_code": "کد مدرسه",
        "کد کارمندی پشتیبان | mentor_id": "کد کارمندی پشتیبان",
    }
    rename_cols = {c: _alias_map[c] for c in list(pool_df.columns) if c in _alias_map}
    if rename_cols:
        pool_df = pool_df.rename(columns=rename_cols)
    # Normalize mentor_id to string (حفظ صفرهای پیشرو/آلفانامبر)
    if "کد کارمندی پشتیبان" in pool_df.columns:
        pool_df["کد کارمندی پشتیبان"] = (
            pool_df["کد کارمندی پشتیبان"].fillna("").astype(str).str.strip()
        )

    # Optional: پیش‌فرض‌های رتبه‌بندی اگر بخواهیم در Infra هم مطمئن باشیم
    if "occupancy_ratio" not in pool_df.columns:
        pool_df["occupancy_ratio"] = 0.0
    if "allocations_new" not in pool_df.columns:
        pool_df["allocations_new"] = 0

    allocations_df, updated_pool_df, logs_df, trace_df = allocate_batch(
        students_df,
        pool_df,
        policy=policy,
        progress=progress,
        capacity_column=capacity_column,
    )

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
