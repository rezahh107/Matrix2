"""ابزار ممیزی خروجی تخصیص مطابق Policy-First."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pandas as pd

from app.core.common.columns import canonicalize_headers, ensure_series
from app.core.policy_loader import PolicyConfig, get_policy

__all__ = ["audit_allocations", "audit_allocations_cli", "summarize_report"]


def _duplicate_student_ids(frame: pd.DataFrame) -> tuple[int, List[str]]:
    """تشخیص شناسه‌های دانش‌آموز تکراری و نمونه‌برداری."""

    if frame.empty or "student_id" not in frame.columns:
        return 0, []

    series = frame["student_id"].astype("string")
    duplicates = series[series.duplicated(keep=False)].dropna()
    if duplicates.empty:
        return 0, []

    unique_ids = duplicates.drop_duplicates().head(5).tolist()
    return len(unique_ids), unique_ids


def _counter_overflow_hits(frame: pd.DataFrame) -> tuple[int, List[str]]:
    """بررسی شناسه‌هایی که به مرز 9999 رسیده‌اند."""

    if frame.empty or "student_id" not in frame.columns:
        return 0, []

    series = frame["student_id"].astype("string")
    mask = series.str.fullmatch(r"\d{9}") & series.str.endswith("9999")
    hits = series[mask]
    if hits.empty:
        return 0, []

    return len(hits), hits.head(5).tolist()


def _year_ambiguity(frame: pd.DataFrame) -> tuple[int, List[str]]:
    """بررسی اختلاف سال تحصیلی بر اساس پیشوند YY."""

    if frame.empty or "student_id" not in frame.columns:
        return 0, []

    series = frame["student_id"].astype("string")
    prefixes = {
        value[:2]
        for value in series
        if value and isinstance(value, str) and value.isdigit() and len(value) == 9
    }
    if len(prefixes) <= 1:
        return 0, sorted(prefixes)
    return len(prefixes), sorted(prefixes)


def _load_sheet(
    workbook: pd.ExcelFile,
    sheet_name: str,
    *,
    header_mode_internal: str,
) -> pd.DataFrame:
    """خواندن شیت و کاننیکال‌سازی ستون‌ها مطابق حالت داخلی."""

    if sheet_name not in workbook.sheet_names:
        return pd.DataFrame()
    frame = workbook.parse(sheet_name)
    return canonicalize_headers(frame, header_mode=header_mode_internal)


def _compile_virtual_pattern(policy: PolicyConfig) -> re.Pattern[str] | None:
    """تولید regex ترکیبی برای تشخیص منتورهای مجازی."""

    if not policy.virtual_name_patterns:
        return None
    joined = "|".join(f"(?:{pattern})" for pattern in policy.virtual_name_patterns)
    return re.compile(joined, re.IGNORECASE)


def _virtual_hits(
    allocations: pd.DataFrame,
    policy: PolicyConfig,
    regex: re.Pattern[str] | None,
) -> tuple[int, List[Mapping[str, Any]]]:
    """شمارش تخصیص‌های منتور مجازی بر اساس Policy."""

    if allocations.empty:
        return 0, []

    mask = pd.Series(False, index=allocations.index)
    if regex and "mentor_name" in allocations.columns:
        mask |= allocations["mentor_name"].astype(str).map(lambda value: bool(regex.search(value)))

    range_pairs = policy.virtual_alias_ranges
    for column_name in ("alias", "mentor_id"):
        if column_name not in allocations.columns:
            continue
        alias_numeric = pd.to_numeric(ensure_series(allocations[column_name]), errors="coerce")
        for start, end in range_pairs:
            mask |= alias_numeric.between(start, end, inclusive="both")

    rows = allocations.loc[mask]
    sample_columns = [
        column
        for column in rows.columns
        if column in {"student_id", "mentor_name", "mentor_id", "alias"}
    ]
    samples = rows.head(10)[sample_columns]
    return int(mask.sum()), samples.to_dict("records")


def _capacity_stuck(
    logs: pd.DataFrame,
) -> tuple[int, List[Mapping[str, Any]]]:
    """پیدا کردن تخصیص‌هایی که ظرفیت قبل/بعد ثابت مانده است."""

    if logs.empty:
        return 0, []
    required = {"capacity_before", "capacity_after"}
    if not required.issubset(logs.columns):
        return 0, []
    before = pd.to_numeric(ensure_series(logs["capacity_before"]), errors="coerce")
    after = pd.to_numeric(ensure_series(logs["capacity_after"]), errors="coerce")
    mask = before.eq(after)
    stuck = logs.loc[mask]
    columns = [
        column
        for column in stuck.columns
        if column in {"student_id", "mentor_id", "capacity_before", "capacity_after"}
    ]
    return int(mask.sum()), stuck.head(10)[columns].to_dict("records")


def _trace_mismatch(
    trace: pd.DataFrame,
    *,
    expected_stage_count: int,
) -> tuple[int, List[Mapping[str, Any]]]:
    """شناسایی دانش‌آموزانی که Trace کامل یا True ندارند."""

    if trace.empty or "student_id" not in trace.columns:
        return 0, []

    df = trace.copy()
    if "matched" in df.columns:
        matched_series = df["matched"]
        if matched_series.dtype != bool:
            df["matched"] = matched_series.astype(str).str.lower().isin({"true", "1", "yes"})

    grouped = df.groupby("student_id")
    failing_ids: List[Any] = []
    for student_id, group in grouped:
        stage_count = group["stage"].nunique(dropna=True) if "stage" in group else 0
        all_matched = bool(group.get("matched", pd.Series([], dtype=bool)).all()) if "matched" in group else True
        if stage_count < expected_stage_count or not all_matched:
            failing_ids.append(student_id)

    samples: List[Mapping[str, Any]] = []
    for student_id in failing_ids[:10]:
        subset = df.loc[df["student_id"] == student_id]
        stages = subset.get("stage", pd.Series([], dtype=object)).tolist()
        matched = subset.get("matched", pd.Series([], dtype=object)).tolist()
        samples.append(
            {
                "student_id": student_id,
                "stages": stages,
                "matched": matched,
            }
        )
    return len(failing_ids), samples


def audit_allocations(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """اجرای ممیزی روی خروجی Excel تخصیص و بازگشت گزارش ساخت‌یافته."""

    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"Allocation output not found: {target}")

    policy = get_policy()
    regex = _compile_virtual_pattern(policy)

    with pd.ExcelFile(target) as workbook:
        allocations = _load_sheet(
            workbook,
            "allocations",
            header_mode_internal=policy.excel.header_mode_internal,
        )
        logs = _load_sheet(
            workbook,
            "logs",
            header_mode_internal=policy.excel.header_mode_internal,
        )
        trace = _load_sheet(
            workbook,
            "trace",
            header_mode_internal=policy.excel.header_mode_internal,
        )

    virtual_count, virtual_samples = _virtual_hits(allocations, policy, regex)
    stuck_count, stuck_samples = _capacity_stuck(logs)
    trace_count, trace_samples = _trace_mismatch(
        trace,
        expected_stage_count=len(policy.trace_stage_names),
    )
    duplicate_count, duplicate_samples = _duplicate_student_ids(allocations)
    overflow_count, overflow_samples = _counter_overflow_hits(allocations)
    ambiguity_count, ambiguity_samples = _year_ambiguity(allocations)

    return {
        "VirtualMentorHits": {"count": virtual_count, "samples": virtual_samples},
        "CapacityStuck": {"count": stuck_count, "samples": stuck_samples},
        "TraceMismatch": {"count": trace_count, "samples": trace_samples},
        "duplicate_student_ids": {"count": duplicate_count, "samples": duplicate_samples},
        "counter_overflow_hits": {"count": overflow_count, "samples": overflow_samples},
        "year_ambiguity": {"count": ambiguity_count, "samples": ambiguity_samples},
    }


def summarize_report(report: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    """خلاصهٔ شمارشی ممیزی با حفظ ترتیب کلیدها."""

    summary: Dict[str, Any] = {}
    for key, payload in report.items():
        count = int(payload.get("count", 0))
        samples = payload.get("samples")
        summary[key] = {
            "count": count,
            "samples": list(samples)[:3] if isinstance(samples, Iterable) else [],
        }
    return summary


def audit_allocations_cli(path: str | Path) -> int:
    """اجرای ممیزی از خط فرمان و چاپ JSON؛ بازگشت کد وضعیت مناسب."""

    report = audit_allocations(path)
    summary = summarize_report(report)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    exit_code = 0
    for payload in summary.values():
        if int(payload.get("count", 0)) > 0:
            exit_code = 1
    return exit_code


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit allocation outputs")
    parser.add_argument("path", help="مسیر فایل Excel تخصیص")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    """نقطهٔ ورود CLI برای ممیزی خروجی تخصیص."""

    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return audit_allocations_cli(args.path)


if __name__ == "__main__":
    raise SystemExit(main())
