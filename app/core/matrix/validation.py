from __future__ import annotations

from dataclasses import asdict
from typing import Dict

from app.core.matrix.coverage import CoverageMetrics

__all__ = ["build_coverage_validation_fields"]


def build_coverage_validation_fields(
    *,
    metrics: CoverageMetrics,
    invalid_group_token_count: int,
    coverage_threshold: float,
) -> Dict[str, object]:
    """ساخت فیلدهای مرتبط با پوشش برای شیت validation.

    این تابع اعداد کلیدی پوشش (مخرج، گروه‌های دیده‌شده/ندیده، توکن‌های نامعتبر و
    نسبت پوشش) را از روی :class:`CoverageMetrics` استخراج می‌کند تا در متادیتا و
    گزارش‌ها یکسان باقی بمانند.
    """

    data = asdict(metrics)
    data.update(
        {
            "unseen_group_count": metrics.unseen_viable_groups,
            "invalid_group_token_count": int(invalid_group_token_count),
            "coverage_denominator_groups": metrics.total_groups,
            "coverage_threshold": float(coverage_threshold),
        }
    )
    return data
