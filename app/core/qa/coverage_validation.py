from __future__ import annotations

from typing import Dict

from app.core.matrix.coverage import CoverageMetrics

__all__ = ["build_coverage_validation_fields"]


def build_coverage_validation_fields(
    *,
    metrics: CoverageMetrics,
    coverage_threshold: float,
    total_rows: int,
) -> Dict[str, object]:
    """ساخت فیلدهای پوشش برای شیت validation.

    اعداد کلیدی پوشش از روی :class:`CoverageMetrics` و تعداد سطرهای ماتریس
    استخراج می‌شود تا متادیتا و گزارش‌ها هم‌خوان باقی بمانند. مقدار
    ``total_candidates`` با تقسیم تعداد سطرهای ماتریس بر نسبت پوشش به‌صورت
    پایدار محاسبه می‌شود تا با ستون‌های خروجی هماهنگ باشد.
    """

    total_candidates = (
        int(round(total_rows / metrics.coverage_ratio)) if metrics.coverage_ratio else 0
    )

    return {
        "coverage_ratio": float(metrics.coverage_ratio),
        "unseen_group_count": int(metrics.unseen_viable_groups),
        "invalid_group_token_count": int(metrics.invalid_group_token_count),
        "coverage_denominator_groups": int(metrics.total_groups),
        "total_candidates": int(total_candidates),
        "covered_groups": int(metrics.covered_groups),
        "unmatched_school_count": int(metrics.unmatched_school_count),
        "coverage_threshold": float(coverage_threshold),
    }
