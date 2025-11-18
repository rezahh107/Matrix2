from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence, Tuple

import pandas as pd

from app.core.common.columns import enforce_join_key_types
from app.core.matrix.grouping import build_candidate_group_keys

__all__ = [
    "CoverageMetrics",
    "CoveragePolicyConfig",
    "compute_coverage_metrics",
    "compute_group_coverage_debug",
]


CoverageSummary = Mapping[str, int | float]


@dataclass(frozen=True)
class CoveragePolicyConfig:
    """تنظیمات سیاست پوشش ماتریس.

    - ``denominator_mode``: فضای گروه برای سنجش پوشش چگونه انتخاب شود.
      * ``"mentors"``: فقط گروه‌های منتور (پس از فیلتر alias/capacity).
      * ``"mentors_students_intersection"``: اشتراک گروه‌های منتور و دانش‌آموز.
      * ``"mentors_students_union"``: اجتماع گروه‌های منتور و دانش‌آموز.
    - ``require_student_presence``: آیا حضور دانش‌آموز برای ورود به مخرج الزامی است.
    - ``include_blocked_candidates_in_denominator``: آیا گروه‌های مسدودشده به‌دلیل
      alias/capacity در مخرج پوشش لحاظ شوند یا فقط برای دیباگ گزارش شوند.
    """

    denominator_mode: str = "mentors"
    require_student_presence: bool = False
    include_blocked_candidates_in_denominator: bool = False


@dataclass(frozen=True)
class CoverageMetrics:
    """خروجی محاسبهٔ پوشش ماتریس بر اساس سیاست پوشش."""

    total_groups: int
    covered_groups: int
    unseen_viable_groups: int
    blocked_groups: int
    candidate_groups: int
    matrix_only_groups: int
    invalid_group_tokens: int
    unmatched_school_count: int
    coverage_ratio: float


def _aggregate_group_status(row: pd.Series) -> Tuple[bool, str]:
    has_candidate = bool(row["candidate_row_count"] > 0)
    has_matrix = bool(row["matrix_row_count"] > 0)
    can_generate = bool(row.get("candidate_can_generate", False))

    if has_matrix and has_candidate:
        return has_candidate, "covered"
    if has_matrix:
        return has_candidate, "matrix_only"
    if has_candidate:
        if not can_generate:
            return has_candidate, "blocked_candidate"
        return has_candidate, "candidate_only"
    return has_candidate, "matrix_only"


def compute_group_coverage_debug(
    matrix_df: pd.DataFrame,
    base_df: pd.DataFrame,
    *,
    join_keys: Sequence[str],
    center_column: str,
    finance_column: str,
    school_code_column: str,
) -> tuple[pd.DataFrame, CoverageSummary]:
    """محاسبهٔ پوشش گروهی ماتریس و خلاصهٔ دیباگ آن.

    این تابع فضای گروهی منتورها (پس از فیلترهای پایه) را از `base_df` استخراج کرده و با
    سطرهای نهایی `matrix_df` بر اساس کلیدهای join شش‌گانه مقایسه می‌کند. خروجی شامل
    DataFrame با ستون‌های کلیدی زیر است:

    - کلیدهای join (کدرشته، جنسیت، دانش‌آموز فارغ، مرکز، مالی، کد مدرسه)
    - ``candidate_row_count``: تعداد ترکیب‌های بالقوه از سطرهای پایه
    - ``candidate_mentor_count``: تعداد منتورهای یکتا در آن گروه
    - ``candidate_can_generate``: آیا حداقل یک ترکیب قابل تبدیل به سطر ماتریس بوده است؟
    - ``matrix_row_count``: تعداد سطرهای واقعی ماتریس در همان گروه
    - ``status``: یکی از «covered» | «candidate_only» | «blocked_candidate» | «matrix_only»

    خلاصهٔ عددی نیز برای لاگ و شیت متادیتا بازگردانده می‌شود.
    """

    candidate_keys = build_candidate_group_keys(
        base_df,
        join_keys=join_keys,
        center_column=center_column,
        finance_column=finance_column,
        school_code_column=school_code_column,
    )
    matrix_keys = enforce_join_key_types(matrix_df, join_keys)

    candidate_grouped = pd.DataFrame(columns=list(join_keys))
    if not candidate_keys.empty:
        candidate_grouped = (
            candidate_keys.groupby(list(join_keys), dropna=False)
            .agg(
                candidate_row_count=("mentor_id", "size"),
                candidate_mentor_count=("mentor_id", pd.Series.nunique),
                candidate_can_generate=("can_generate", "max"),
                candidate_has_alias=("has_alias", "max"),
                variant_set=("variant", lambda vals: tuple(dict.fromkeys(vals))),
            )
            .reset_index()
        )

    matrix_grouped = pd.DataFrame(columns=list(join_keys))
    if not matrix_keys.empty:
        matrix_grouped = (
            matrix_keys.groupby(list(join_keys), dropna=False)
            .agg(
                matrix_row_count=(matrix_keys.columns[0], "size"),
                matrix_mentor_count=("کد کارمندی پشتیبان", pd.Series.nunique)
                if "کد کارمندی پشتیبان" in matrix_keys.columns
                else (matrix_keys.columns[0], "size"),
            )
            .reset_index()
        )

    merged = candidate_grouped.merge(
        matrix_grouped,
        on=list(join_keys),
        how="outer",
        sort=True,
        suffixes=("_candidate", "_matrix"),
    )
    for column in ("candidate_row_count", "candidate_mentor_count", "matrix_row_count", "matrix_mentor_count"):
        if column not in merged.columns:
            merged[column] = 0
    if "candidate_can_generate" in merged.columns:
        merged["candidate_can_generate"] = merged["candidate_can_generate"].where(
            pd.notna(merged["candidate_can_generate"]), False
        ).astype(bool, copy=False)
    else:
        merged["candidate_can_generate"] = False
    if "candidate_has_alias" in merged.columns:
        merged["candidate_has_alias"] = merged["candidate_has_alias"].where(
            pd.notna(merged["candidate_has_alias"]), False
        ).astype(bool, copy=False)
    else:
        merged["candidate_has_alias"] = False
    if "variant_set" in merged.columns:
        merged["variant_set"] = merged["variant_set"].apply(
            lambda vals: tuple(dict.fromkeys(vals)) if isinstance(vals, (list, tuple)) else tuple()
        )
    else:
        merged["variant_set"] = pd.Series([tuple()] * len(merged))
    merged[["candidate_row_count", "candidate_mentor_count", "matrix_row_count", "matrix_mentor_count"]] = merged[
        ["candidate_row_count", "candidate_mentor_count", "matrix_row_count", "matrix_mentor_count"]
    ].fillna(0)

    merged[["candidate_row_count", "candidate_mentor_count", "matrix_row_count", "matrix_mentor_count"]] = merged[
        ["candidate_row_count", "candidate_mentor_count", "matrix_row_count", "matrix_mentor_count"]
    ].astype(int)

    status_labels: list[str] = []
    has_candidate_flags: list[bool] = []
    is_blocked_flags: list[bool] = []
    is_viable_flags: list[bool] = []
    for _, row in merged.iterrows():
        has_candidate, label = _aggregate_group_status(row)
        status_labels.append(label)
        has_candidate_flags.append(has_candidate)
        blocked = has_candidate and not bool(row.get("candidate_can_generate", False))
        is_blocked_flags.append(blocked)
        is_viable_flags.append(has_candidate and bool(row.get("candidate_can_generate", False)))
    merged["status"] = status_labels
    merged["has_candidate"] = has_candidate_flags
    merged["is_blocked_candidate"] = is_blocked_flags
    merged["is_candidate_viable"] = is_viable_flags

    summary = {
        "total_groups": int(len(merged)),
        "covered_groups": int((merged["status"] == "covered").sum()),
        "candidate_only_groups": int((merged["status"] == "candidate_only").sum()),
        "blocked_candidate_groups": int((merged["status"] == "blocked_candidate").sum()),
        "matrix_only_groups": int((merged["status"] == "matrix_only").sum()),
        "candidate_groups": int((merged["has_candidate"] == True).sum()),
    }

    ordered_columns = list(join_keys) + [
        "candidate_row_count",
        "candidate_mentor_count",
        "candidate_can_generate",
        "candidate_has_alias",
        "matrix_row_count",
        "matrix_mentor_count",
        "status",
        "has_candidate",
        "is_candidate_viable",
        "is_blocked_candidate",
        "variant_set",
    ]
    for column in ordered_columns:
        if column not in merged.columns:
            merged[column] = pd.NA
    merged = merged.loc[:, ordered_columns].sort_values(list(join_keys), kind="stable")

    return merged.reset_index(drop=True), summary


def _student_group_keys(students_df: pd.DataFrame, join_keys: Sequence[str]) -> pd.DataFrame:
    if students_df is None or students_df.empty:
        return pd.DataFrame(columns=list(join_keys))
    projected = enforce_join_key_types(students_df, join_keys)
    return projected.loc[:, list(join_keys)].drop_duplicates().reset_index(drop=True)


def _denominator_mask(
    coverage_df: pd.DataFrame,
    *,
    policy: CoveragePolicyConfig,
    student_groups: pd.DataFrame,
) -> pd.Series:
    if coverage_df.empty:
        return pd.Series([], dtype=bool)

    mask = coverage_df["is_candidate_viable"].copy()
    if policy.include_blocked_candidates_in_denominator:
        mask |= coverage_df["is_blocked_candidate"]

    if not student_groups.empty:
        merge_keys = list(student_groups.columns)
        student_flags_raw = coverage_df.merge(
            student_groups.assign(_student_present=True),
            on=merge_keys,
            how="left",
            sort=False,
        )["_student_present"]
        student_flags = pd.Series(
            student_flags_raw.to_numpy(dtype=bool, na_value=False),
            index=coverage_df.index,
        )
        if policy.denominator_mode == "mentors_students_intersection" or policy.require_student_presence:
            mask &= student_flags
        elif policy.denominator_mode == "mentors_students_union":
            mask |= student_flags
    return mask


def compute_coverage_metrics(
    *,
    matrix_df: pd.DataFrame,
    base_df: pd.DataFrame,
    students_df: pd.DataFrame | None,
    join_keys: Sequence[str],
    policy: CoveragePolicyConfig,
    unmatched_school_count: int,
    invalid_group_tokens: int,
    center_column: str,
    finance_column: str,
    school_code_column: str,
) -> tuple[CoverageMetrics, pd.DataFrame, CoverageSummary]:
    """محاسبهٔ شاخص‌های پوشش ماتریس بر اساس سیاست پوشش.

    خروجی شامل دیتافریم پوشش (با ستون‌های بولی ``is_unseen_viable``،
    ``in_coverage_denominator`` و ...) و خلاصهٔ عددی برای لاگ است.
    """

    coverage_df, summary = compute_group_coverage_debug(
        matrix_df,
        base_df,
        join_keys=join_keys,
        center_column=center_column,
        finance_column=finance_column,
        school_code_column=school_code_column,
    )

    student_groups = _student_group_keys(
        students_df if students_df is not None else pd.DataFrame(), join_keys
    )
    denominator_mask = _denominator_mask(
        coverage_df, policy=policy, student_groups=student_groups
    )
    coverage_df = coverage_df.copy()
    coverage_df["in_coverage_denominator"] = denominator_mask
    coverage_df["is_unseen_viable"] = denominator_mask & (
        coverage_df["matrix_row_count"] <= 0
    )
    coverage_df["is_covered"] = denominator_mask & (
        coverage_df["matrix_row_count"] > 0
    )

    total_groups = int(denominator_mask.sum())
    covered_groups = int(coverage_df["is_covered"].sum())
    unseen_viable_groups = int(coverage_df["is_unseen_viable"].sum())
    blocked_groups = int(coverage_df["is_blocked_candidate"].sum())
    candidate_groups = int(coverage_df["has_candidate"].sum())
    matrix_only_groups = int((coverage_df["status"] == "matrix_only").sum())
    coverage_ratio = covered_groups / total_groups if total_groups else 1.0

    metrics = CoverageMetrics(
        total_groups=total_groups,
        covered_groups=covered_groups,
        unseen_viable_groups=unseen_viable_groups,
        blocked_groups=blocked_groups,
        candidate_groups=candidate_groups,
        matrix_only_groups=matrix_only_groups,
        invalid_group_tokens=int(invalid_group_tokens),
        unmatched_school_count=int(unmatched_school_count),
        coverage_ratio=float(coverage_ratio),
    )

    return metrics, coverage_df, summary
