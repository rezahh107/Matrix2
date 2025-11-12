"""توابع Explainability برای ساخت متن دلایل انتخاب پشتیبان."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import pandas as pd
import re

from .columns import canonicalize_headers

__all__ = ["ReasonContext", "summarize_trace_steps", "render_selection_reason"]


@dataclass(frozen=True)
class ReasonContext:
    """کانتکست داده‌ای لازم برای توضیح انتخاب پشتیبان.

    مثال::

        >>> ctx = ReasonContext(
        ...     gender_label="دختر",
        ...     school_name="دبیرستان نمونه",
        ...     is_after_school=False,
        ...     track_label="ریاضی",
        ...     mentor_id="101",
        ...     mentor_name="خانم الف",
        ...     ranking_chain="occupancy→allocations_new→mentor_id",
        ... )
    """

    gender_label: str
    school_name: str
    is_after_school: bool
    track_label: str
    mentor_id: str
    mentor_name: str
    ranking_chain: str
    trace_summary: str | None = None


_SANITIZE_RE = re.compile(r"[\n\r\t]+")


class _DefaultDict(dict[str, str]):
    """نگهبان برای format که کلیدهای ناشناخته را خالی برمی‌گرداند."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - مسیر محافظتی
        return ""


def _sanitize(value: object) -> str:
    """حذف کاراکترهای کنترلی و trim دوطرفه."""

    text = str(value or "").strip()
    return _SANITIZE_RE.sub(" — ", text)


def summarize_trace_steps(
    trace_df: pd.DataFrame | None,
    student_id: str,
    *,
    stage_order: Sequence[str],
    labels: Mapping[str, str],
) -> str:
    """ساخت خلاصهٔ تریس هشت‌مرحله‌ای برای درج در دلیل."""

    if trace_df is None or trace_df.empty:
        return ""

    canonical = canonicalize_headers(trace_df, header_mode="en")
    if "student_id" not in canonical.columns or "stage" not in canonical.columns:
        return ""

    subset = canonical[canonical["student_id"].astype("string") == str(student_id)]
    if subset.empty:
        return ""

    parts: list[str] = []
    for stage in stage_order:
        stage_rows = subset[subset["stage"].astype("string") == str(stage)]
        if stage_rows.empty:
            continue
        last_row = stage_rows.iloc[-1]
        after_value = last_row.get("total_after")
        try:
            after_int = int(float(after_value))
        except (TypeError, ValueError):
            continue
        label = labels.get(stage, stage)
        parts.append(f"{label}={after_int}")
    return "→".join(parts)


def render_selection_reason(
    template: str,
    ctx: ReasonContext,
    *,
    context_labels: Sequence[str],
) -> str:
    """رندر متن دلیل به‌صورت دترمینیستیک و بدون I/O."""

    payload = _DefaultDict(
        gender_label=_sanitize(ctx.gender_label),
        school_name=_sanitize(ctx.school_name),
        is_after_school=str(bool(ctx.is_after_school)).lower(),
        track_label=_sanitize(ctx.track_label),
        mentor_id=_sanitize(ctx.mentor_id),
        mentor_name=_sanitize(ctx.mentor_name),
        ranking_chain=_sanitize(ctx.ranking_chain),
        trace_summary=_sanitize(ctx.trace_summary) if ctx.trace_summary else "",
    )
    rendered = _sanitize(template.format_map(payload))

    summary_values = (
        _sanitize(ctx.gender_label),
        _sanitize(ctx.school_name),
        _sanitize(ctx.track_label),
        _sanitize(ctx.ranking_chain),
    )
    labelled: list[str] = []
    for label, value in zip(context_labels, summary_values):
        clean_label = _sanitize(label)
        if value:
            labelled.append(f"{clean_label or 'برچسب'}: {value}")
    parts = [part for part in (rendered, " | ".join(labelled) if labelled else "") if part]
    if ctx.trace_summary:
        parts.append(f"مراحل: {_sanitize(ctx.trace_summary)}")
    text = " — ".join(parts)
    return text if len(text) <= 512 else f"{text[:509]}…"
