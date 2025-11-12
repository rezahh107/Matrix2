"""ابزار ممیزی خروجی تخصیص برای تضمین معیارهای پذیرش."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Mapping

import pandas as pd

from app.core.common.columns import canonicalize_headers
from app.core.policy_loader import PolicyConfig, get_policy

__all__ = ["audit_allocations"]


def _load_sheet(
    workbook: pd.ExcelFile,
    sheet_name: str,
    *,
    header_mode_internal: str,
) -> pd.DataFrame:
    if sheet_name not in workbook.sheet_names:
        return pd.DataFrame()
    frame = workbook.parse(sheet_name)
    return canonicalize_headers(frame, header_mode=header_mode_internal)


def _compile_virtual_pattern(policy: PolicyConfig) -> re.Pattern[str] | None:
    if not policy.virtual_name_patterns:
        return None
    joined = "|".join(f"(?:{pattern})" for pattern in policy.virtual_name_patterns)
    return re.compile(joined, re.IGNORECASE)


def _virtual_hits(
    allocations: pd.DataFrame,
    policy: PolicyConfig,
    regex: re.Pattern[str] | None,
) -> tuple[int, List[Mapping[str, Any]]]:
    if allocations.empty:
        return 0, []

    mask = pd.Series(False, index=allocations.index)
    if regex and "mentor_name" in allocations.columns:
        mask |= allocations["mentor_name"].astype(str).map(lambda value: bool(regex.search(value)))

    range_pairs = policy.virtual_alias_ranges
    for column_name in ("alias", "mentor_id"):
        if column_name not in allocations.columns:
            continue
        alias_numeric = pd.to_numeric(allocations[column_name], errors="coerce")
        for start, end in range_pairs:
            mask |= alias_numeric.between(start, end, inclusive="both")

    rows = allocations.loc[mask]
    samples = rows.head(10)[[col for col in rows.columns if col in {"student_id", "mentor_name", "mentor_id", "alias"}]]
    return int(mask.sum()), samples.to_dict("records")


def _capacity_stuck(
    logs: pd.DataFrame,
) -> tuple[int, List[Mapping[str, Any]]]:
    if logs.empty:
        return 0, []
    required = {"capacity_before", "capacity_after"}
    if not required.issubset(logs.columns):
        return 0, []
    before = pd.to_numeric(logs["capacity_before"], errors="coerce")
    after = pd.to_numeric(logs["capacity_after"], errors="coerce")
    mask = before.eq(after)
    stuck = logs.loc[mask]
    columns = [col for col in stuck.columns if col in {"student_id", "mentor_id", "capacity_before", "capacity_after"}]
    return int(mask.sum()), stuck.head(10)[columns].to_dict("records")


def _trace_mismatch(
    trace: pd.DataFrame,
    *,
    expected_stage_count: int,
) -> tuple[int, List[Mapping[str, Any]]]:
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
    """اجرای ممیزی روی خروجی Excel تخصیص و برگرداندن خلاصهٔ متریک‌ها.

    مثال::

        >>> from pathlib import Path
        >>> report = audit_allocations(Path("allocations.xlsx"))  # doctest: +SKIP
        >>> report["VirtualMentorHits"]["count"]
        0
    """

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

    return {
        "VirtualMentorHits": {"count": virtual_count, "samples": virtual_samples},
        "CapacityStuck": {"count": stuck_count, "samples": stuck_samples},
        "TraceMismatch": {"count": trace_count, "samples": trace_samples},
    }
