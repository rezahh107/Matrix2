from __future__ import annotations

import pandas as pd

from app.core.qa.invariants import check_STU_01, check_STU_02


def test_stu_01_passes_when_counts_match() -> None:
    students = pd.DataFrame({"student_id": ["S1", "S2", "S3"]})
    matrix = students.copy()
    allocation = students.copy()

    result = check_STU_01(matrix=matrix, allocation=allocation, student_report=students)

    assert result.passed
    assert not result.violations


def test_stu_01_flags_mismatch() -> None:
    students = pd.DataFrame({"student_id": ["S1", "S2", "S3"]})
    matrix = students.iloc[:2]
    allocation = students.iloc[:2]

    result = check_STU_01(matrix=matrix, allocation=allocation, student_report=students)

    assert not result.passed
    assert result.violations[0].rule_id == "QA_RULE_STU_01"


def test_stu_02_detects_per_mentor_delta() -> None:
    allocation = pd.DataFrame({"mentor_id": [1, 1, 2], "student_id": ["S1", "S2", "S3"]})
    inspactor = pd.DataFrame(
        {"mentor_id": [1, 2], "expected_student_count": [3, 2]}
    )

    result = check_STU_02(allocation=allocation, inspactor=inspactor)

    assert not result.passed
    assert any(v.details == {"mentor_id": 2, "expected": 2, "assigned": 1} for v in result.violations)


def test_stu_02_handles_non_numeric_mentor_ids() -> None:
    allocation = pd.DataFrame(
        {"mentor_id": ["EMP-1", "EMP-1", "EMP-2"], "student_id": ["S1", "S2", "S3"]}
    )
    inspactor = pd.DataFrame(
        {"mentor_id": ["EMP-1", "EMP-2"], "expected_student_count": [3, 2]}
    )

    result = check_STU_02(allocation=allocation, inspactor=inspactor)

    assert not result.passed
    assert any(
        v.details == {"mentor_id": "EMP-2", "expected": 2, "assigned": 1}
        for v in result.violations
    )

