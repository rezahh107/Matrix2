from __future__ import annotations

from typing import Dict

from app.core.matrix.coverage import CoverageMetrics

__all__ = ["build_coverage_validation_fields"]


def build_coverage_validation_fields(
    *,
    metrics: CoverageMetrics,
    coverage_threshold: float,
) -> Dict[str, object]:
    """ساخت فیلدهای مرتبط با پوشش برای شیت validation.

    این تابع اعداد کلیدی پوشش (مخرج، گروه‌های دیده‌شده/ندیده، توکن‌های نامعتبر و
    نسبت پوشش) را از روی :class:`CoverageMetrics` استخراج می‌کند تا در متادیتا و
    گزارش‌ها یکسان باقی بمانند.
    """

    return {
        "coverage_ratio": float(metrics.coverage_ratio),
        "unseen_group_count": int(metrics.unseen_viable_groups),
        "invalid_group_token_count": int(metrics.invalid_group_token_count),
        "coverage_denominator_groups": int(metrics.total_groups),
        "covered_groups": int(metrics.covered_groups),
        "unmatched_school_count": int(metrics.unmatched_school_count),
        "coverage_threshold": float(coverage_threshold),
    }
