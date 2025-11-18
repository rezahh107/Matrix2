"""ابزارهای دیباگ و گروهبندی ماتریس اهلیت."""

from .coverage import compute_group_coverage_debug
from .grouping import build_candidate_group_keys

__all__ = ["compute_group_coverage_debug", "build_candidate_group_keys"]
