from __future__ import annotations

"""لایهٔ مرکزی QA برای اینورینت‌های ماتریس و تخصیص."""

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import pandas as pd
from pandas.api import types as ptypes

from app.core.policy_loader import PolicyConfig

RuleId = str

__all__ = [
    "QaViolation",
    "QaRuleResult",
    "QaReport",
    "run_all_invariants",
    "check_STU_01",
    "check_STU_02",
    "check_JOIN_01",
    "check_SCHOOL_01",
    "check_ALLOC_01",
]


@dataclass(frozen=True)
class QaViolation:
    """نمایش یک تخطی از قانون QA.

    Attributes
    ----------
    rule_id:
        شناسهٔ پایدار قانون (مثلاً ``"QA_RULE_STU_01"``).
    level:
        سطح تخطی؛ در این نسخه فقط ``"error"`` پشتیبانی می‌شود.
    message:
        توضیح خوانا از علت تخطی.
    details:
        دادهٔ ساخت‌یافتهٔ اختیاری برای گزارش‌های اکسل/لاگ.
    """

    rule_id: RuleId
    level: str
    message: str
    details: Mapping[str, object] | None = None


@dataclass(frozen=True)
class QaRuleResult:
    """نتیجهٔ اجرای یک قانون واحد QA."""

    rule_id: RuleId
    passed: bool
    violations: list[QaViolation]


@dataclass(frozen=True)
class QaReport:
    """گزارش نهایی QA برای یک نوبت ساخت/تخصیص."""

    results: list[QaRuleResult]

    @property
    def violations(self) -> list[QaViolation]:
        """تمام تخطی‌ها را در یک لیست مسطح برمی‌گرداند."""

        merged: list[QaViolation] = []
        for result in self.results:
            merged.extend(result.violations)
        return merged

    @property
    def passed(self) -> bool:
        """آیا تمام قوانین بدون تخطی عبور کرده‌اند؟"""

        return all(result.passed for result in self.results)

    def violations_by_rule(self, rule_id: RuleId) -> list[QaViolation]:
        """لیست تخطی‌های مربوط به یک قانون مشخص را برمی‌گرداند."""

        return [
            violation
            for result in self.results
            if result.rule_id == rule_id
            for violation in result.violations
        ]

    def to_summary_frame(self, *, descriptions: Mapping[str, str] | None = None) -> pd.DataFrame:
        """خلاصهٔ قوانین را به‌صورت DataFrame برمی‌گرداند.

        Parameters
        ----------
        descriptions:
            نقشهٔ اختیاری ``rule_id`` به توضیح خوانا برای نمایش در گزارش.
        """

        descriptions = descriptions or {}
        rows = []
        for result in sorted(self.results, key=lambda item: item.rule_id):
            rows.append(
                {
                    "rule_id": result.rule_id,
                    "description": descriptions.get(result.rule_id, ""),
                    "status": "PASS" if result.passed else "FAIL",
                    "violations_count": len(result.violations),
                }
            )
        return pd.DataFrame(rows)

    def to_details_frame(self, rule_id: RuleId) -> pd.DataFrame:
        """تبدیل تخطی‌های یک قانون به DataFrame ساخت‌یافته."""

        violations = self.violations_by_rule(rule_id)
        base_columns = ["rule_id", "level", "message"]
        rows: list[dict[str, object]] = []
        detail_keys: set[str] = set()
        for violation in violations:
            detail_map = violation.details or {}
            detail_keys.update(detail_map.keys())
            row = {
                "rule_id": violation.rule_id,
                "level": violation.level,
                "message": violation.message,
            }
            row.update(detail_map)
            rows.append(row)

        ordered_columns = base_columns + sorted(detail_keys)
        frame = pd.DataFrame(rows, columns=ordered_columns)
        if not frame.empty:
            sort_keys = [col for col in ordered_columns if col in frame.columns and col not in {"message"}]
            if sort_keys:
                frame = frame.sort_values(by=sort_keys, kind="stable").reset_index(drop=True)
        return frame


def run_all_invariants(
    *,
    policy: PolicyConfig,
    matrix: pd.DataFrame | None = None,
    allocation: pd.DataFrame | None = None,
    student_report: pd.DataFrame | None = None,
    inspactor: pd.DataFrame | None = None,
    invalid_mentors: pd.DataFrame | None = None,
    allocation_summary: pd.DataFrame | None = None,
) -> QaReport:
    """اجرای همهٔ قوانین QA و تولید گزارش تجمیعی.

    مثال ساده
    ---------
    >>> import pandas as pd
    >>> from app.core.policy_loader import load_policy
    >>> policy = load_policy()
    >>> matrix = pd.DataFrame({"کدرشته": [1201], "جنسیت": [1], "دانش آموز فارغ": [0],
    ... "مرکز گلستان صدرا": [0], "مالی حکمت بنیاد": [0], "کد مدرسه": [1010],
    ... "has_school_constraint": [False]})
    >>> report = run_all_invariants(policy=policy, matrix=matrix)
    >>> report.passed
    True
    """

    checks = [
        check_STU_01(
            matrix=matrix,
            allocation=allocation,
            student_report=student_report,
        ),
        check_STU_02(allocation=allocation, inspactor=inspactor),
        check_JOIN_01(matrix=matrix, policy=policy),
        check_SCHOOL_01(matrix=matrix, invalid_mentors=invalid_mentors, policy=policy),
        check_ALLOC_01(
            allocation=allocation,
            allocation_summary=allocation_summary,
            policy=policy,
        ),
    ]
    return QaReport(results=checks)


def _resolve_student_count(frame: pd.DataFrame | None) -> int | None:
    if frame is None:
        return None
    if "student_id" in frame.columns:
        return int(frame["student_id"].notna().sum())
    return int(len(frame))


def _resolve_mentor_column(frame: pd.DataFrame | None) -> str | None:
    if frame is None:
        return None
    candidates = (
        "mentor_id",
        "کد کارمندی پشتیبان",
        "mentor_code",
    )
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def check_STU_01(
    *,
    matrix: pd.DataFrame | None,
    allocation: pd.DataFrame | None,
    student_report: pd.DataFrame | None,
) -> QaRuleResult:
    """QA_RULE_STU_01 — هم‌خوانی تعداد دانش‌آموز در همهٔ خروجی‌ها."""

    counts = {
        "student_report": _resolve_student_count(student_report),
        "matrix": _resolve_student_count(matrix),
        "allocation": _resolve_student_count(allocation),
    }
    known_counts = {k: v for k, v in counts.items() if v is not None}

    violations: list[QaViolation] = []
    if len(set(known_counts.values())) > 1:
        violations.append(
            QaViolation(
                rule_id="QA_RULE_STU_01",
                level="error",
                message="عدم تطابق تعداد دانش‌آموز بین خروجی‌ها",
                details=known_counts,
            )
        )

    return QaRuleResult(
        rule_id="QA_RULE_STU_01",
        passed=not violations,
        violations=violations,
    )


def check_STU_02(
    *,
    allocation: pd.DataFrame | None,
    inspactor: pd.DataFrame | None,
) -> QaRuleResult:
    """QA_RULE_STU_02 — شمار دانش‌آموز به ازای هر منتور مطابق Inspactor/Allocation."""

    mentor_col = _resolve_mentor_column(inspactor) or _resolve_mentor_column(allocation)
    if mentor_col is None or allocation is None or inspactor is None:
        return QaRuleResult("QA_RULE_STU_02", True, [])

    expected_col_candidates: Sequence[str] = (
        "expected_student_count",
        "student_count",
        "students_count",
    )
    expected_col = next((c for c in expected_col_candidates if c in inspactor.columns), None)
    if expected_col is None:
        return QaRuleResult("QA_RULE_STU_02", True, [])

    expected_counts = (
        inspactor[[mentor_col, expected_col]]
        .rename(columns={mentor_col: "mentor_id", expected_col: "expected"})
        .dropna(subset=["mentor_id"])
        .assign(mentor_id=lambda df: df["mentor_id"].astype(str))
    )
    expected_counts["expected"] = pd.to_numeric(expected_counts["expected"], errors="coerce")
    expected_counts = expected_counts.dropna(subset=["expected"])
    expected_counts = expected_counts.groupby("mentor_id", as_index=False)["expected"].sum()

    alloc_counts = (
        allocation[[mentor_col]]
        .rename(columns={mentor_col: "mentor_id"})
        .dropna(subset=["mentor_id"])
        .assign(mentor_id=lambda df: df["mentor_id"].astype(str))
        .groupby("mentor_id", as_index=False)
        .size()
        .rename(columns={"size": "assigned"})
    )

    merged = expected_counts.merge(alloc_counts, on="mentor_id", how="left").fillna({"assigned": 0})
    mismatches = merged[merged["expected"] != merged["assigned"]]

    violations: list[QaViolation] = []
    for _, row in mismatches.iterrows():
        violations.append(
                QaViolation(
                    rule_id="QA_RULE_STU_02",
                    level="error",
                    message="اختلاف شمارش دانش‌آموز برای منتور",
                    details={
                        "mentor_id": row["mentor_id"],
                        "expected": int(row["expected"]),
                        "assigned": int(row["assigned"]),
                    },
                )
            )

    return QaRuleResult(
        rule_id="QA_RULE_STU_02",
        passed=not violations,
        violations=violations,
    )


def check_JOIN_01(*, matrix: pd.DataFrame | None, policy: PolicyConfig) -> QaRuleResult:
    """QA_RULE_JOIN_01 — سلامت ۶ کلید join در ماتریس."""

    violations: list[QaViolation] = []
    if matrix is None:
        return QaRuleResult("QA_RULE_JOIN_01", True, violations)

    missing = [key for key in policy.join_keys if key not in matrix.columns]
    if missing:
        violations.append(
            QaViolation(
                rule_id="QA_RULE_JOIN_01",
                level="error",
                message="ستون‌های join در ماتریس ناقص است",
                details={"missing_columns": tuple(missing)},
            )
        )
        return QaRuleResult("QA_RULE_JOIN_01", False, violations)

    for key in policy.join_keys:
        series = matrix[key]
        if series.isna().any():
            violations.append(
                QaViolation(
                    rule_id="QA_RULE_JOIN_01",
                    level="error",
                    message=f"مقدار خالی در ستون join '{key}'",
                    details={"null_rows": int(series.isna().sum())},
                )
            )
        if not ptypes.is_integer_dtype(series):
            violations.append(
                QaViolation(
                    rule_id="QA_RULE_JOIN_01",
                    level="error",
                    message=f"ستون join '{key}' باید نوع عددی صحیح داشته باشد",
                    details={"dtype": str(series.dtype)},
                )
            )

    return QaRuleResult(
        rule_id="QA_RULE_JOIN_01",
        passed=not violations,
        violations=violations,
    )


def check_SCHOOL_01(
    *,
    matrix: pd.DataFrame | None,
    invalid_mentors: pd.DataFrame | None,
    policy: PolicyConfig,
) -> QaRuleResult:
    """QA_RULE_SCHOOL_01 — تمایز منتورهای آزاد و مقید به مدرسه."""

    violations: list[QaViolation] = []
    if matrix is None:
        return QaRuleResult("QA_RULE_SCHOOL_01", True, violations)

    if "has_school_constraint" not in matrix.columns:
        violations.append(
            QaViolation(
                rule_id="QA_RULE_SCHOOL_01",
                level="error",
                message="ستون has_school_constraint در ماتریس موجود نیست",
            )
        )
        return QaRuleResult("QA_RULE_SCHOOL_01", False, violations)

    mentor_col = _resolve_mentor_column(matrix)
    invalid_ids: set[int] = set()
    if invalid_mentors is not None:
        invalid_col = _resolve_mentor_column(invalid_mentors)
        if invalid_col and invalid_col in invalid_mentors.columns:
            invalid_ids = set(
                pd.to_numeric(invalid_mentors[invalid_col], errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )

    unrestricted_mask = matrix["has_school_constraint"] == False  # noqa: E712
    if mentor_col and invalid_ids:
        unrestricted_ids = set(
            pd.to_numeric(matrix.loc[unrestricted_mask, mentor_col], errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        leaked = sorted(unrestricted_ids.intersection(invalid_ids))
        if leaked:
            violations.append(
                QaViolation(
                    rule_id="QA_RULE_SCHOOL_01",
                    level="error",
                    message="منتور آزاد در لیست خطای مدرسه دیده شده است",
                    details={"mentor_ids": tuple(leaked)},
                )
            )

    restricted_mask = matrix["has_school_constraint"] == True  # noqa: E712
    school_col = policy.columns.school_code
    if restricted_mask.any():
        restricted_rows = matrix.loc[restricted_mask]
        missing_school = restricted_rows[school_col].isna() | (
            pd.to_numeric(restricted_rows[school_col], errors="coerce").fillna(0).eq(0)
        )
        if missing_school.any():
            offenders: Iterable[int] = ()
            if mentor_col:
                offenders = (
                    pd.to_numeric(
                        restricted_rows.loc[missing_school, mentor_col], errors="coerce"
                    )
                    .dropna()
                    .astype(int)
                    .tolist()
                )
            violations.append(
                QaViolation(
                    rule_id="QA_RULE_SCHOOL_01",
                    level="error",
                    message="منتور مقید مدرسه بدون کد مدرسه معتبر",
                    details={"mentor_ids": tuple(offenders)},
                )
            )

    return QaRuleResult(
        rule_id="QA_RULE_SCHOOL_01",
        passed=not violations,
        violations=violations,
    )


def check_ALLOC_01(
    *,
    allocation: pd.DataFrame | None,
    allocation_summary: pd.DataFrame | None,
    policy: PolicyConfig,
) -> QaRuleResult:
    """QA_RULE_ALLOC_01 — ظرفیت و نسبت اشغال منتورها در تخصیص."""

    violations: list[QaViolation] = []
    if allocation is None or allocation_summary is None:
        return QaRuleResult("QA_RULE_ALLOC_01", True, violations)

    mentor_col = _resolve_mentor_column(allocation_summary) or _resolve_mentor_column(allocation)
    if mentor_col is None:
        return QaRuleResult("QA_RULE_ALLOC_01", True, violations)

    assigned = (
        pd.to_numeric(allocation[mentor_col], errors="coerce")
        .dropna()
        .astype(int)
        .value_counts()
    )

    summary = allocation_summary.copy()
    summary["__mentor"] = pd.to_numeric(summary[mentor_col], errors="coerce")
    summary = summary.dropna(subset=["__mentor"])
    summary["__mentor"] = summary["__mentor"].astype(int)

    remaining_col = policy.columns.remaining_capacity
    occupancy_col = "occupancy_ratio"
    alloc_new_col = "allocations_new"

    for _, row in summary.iterrows():
        mentor_id = int(row["__mentor"])
        assigned_count = int(assigned.get(mentor_id, 0))
        remaining = float(pd.to_numeric(row.get(remaining_col, 0), errors="coerce"))
        alloc_new = float(pd.to_numeric(row.get(alloc_new_col, assigned_count), errors="coerce"))

        if assigned_count > remaining + alloc_new + 1e-9:
            violations.append(
                QaViolation(
                    rule_id="QA_RULE_ALLOC_01",
                    level="error",
                    message="تخصیص بیش از ظرفیت منتور",
                    details={
                        "mentor_id": mentor_id,
                        "assigned": assigned_count,
                        "remaining": remaining,
                        "allocations_new": alloc_new,
                    },
                )
            )

        if occupancy_col in summary.columns:
            denominator = remaining + alloc_new
            expected_ratio = 0.0 if denominator <= 0 else alloc_new / denominator
            actual_ratio = float(pd.to_numeric(row.get(occupancy_col, 0), errors="coerce"))
            if abs(actual_ratio - expected_ratio) > 1e-6:
                violations.append(
                    QaViolation(
                        rule_id="QA_RULE_ALLOC_01",
                        level="error",
                        message="نسبت اشغال با فرمول ظرفیت هم‌خوان نیست",
                        details={
                            "mentor_id": mentor_id,
                            "expected_ratio": expected_ratio,
                            "actual_ratio": actual_ratio,
                        },
                    )
                )

    return QaRuleResult(
        rule_id="QA_RULE_ALLOC_01",
        passed=not violations,
        violations=violations,
    )
