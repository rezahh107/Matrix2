from pathlib import Path

import pandas as pd

from app.core.qa.invariants import QaReport, QaRuleResult, QaViolation
from app.infra.excel.export_qa_validation import (
    QaValidationContext,
    export_qa_validation,
)


def _sample_report() -> QaReport:
    violations = [
        QaViolation(
            rule_id="QA_RULE_STU_01",
            level="error",
            message="mismatch",
            details={"student_report": 2, "matrix": 1, "allocation": 1},
        ),
        QaViolation(
            rule_id="QA_RULE_STU_02",
            level="error",
            message="mentor mismatch",
            details={"mentor_id": 11, "expected": 2, "assigned": 1},
        ),
        QaViolation(
            rule_id="QA_RULE_STU_02",
            level="error",
            message="mentor mismatch",
            details={"mentor_id": 12, "expected": 1, "assigned": 3},
        ),
        QaViolation(
            rule_id="QA_RULE_SCHOOL_01",
            level="error",
            message="school constraint",
            details={"mentor_ids": (21,)},
        ),
        QaViolation(
            rule_id="QA_RULE_ALLOC_01",
            level="error",
            message="over capacity",
            details={
                "mentor_id": 31,
                "assigned": 4,
                "remaining": 1,
                "allocations_new": 1,
                "expected_ratio": 0.5,
                "actual_ratio": 0.8,
            },
        ),
    ]
    return QaReport(
        results=[
            QaRuleResult("QA_RULE_STU_01", False, [violations[0]]),
            QaRuleResult("QA_RULE_STU_02", False, violations[1:3]),
            QaRuleResult("QA_RULE_JOIN_01", True, []),
            QaRuleResult("QA_RULE_SCHOOL_01", False, [violations[3]]),
            QaRuleResult("QA_RULE_ALLOC_01", False, [violations[4]]),
        ]
    )


def test_export_qa_validation_builds_expected_sheets(tmp_path: Path) -> None:
    report = _sample_report()
    output = tmp_path / "qa_validation.xlsx"
    context = QaValidationContext(meta={"policy_version": "1.0.3", "ssot_version": "1.0.2"})

    export_qa_validation(report, output=output, context=context)

    assert output.exists()
    with pd.ExcelFile(output) as workbook:
        sheet_names = workbook.sheet_names
        sheets = {name: workbook.parse(name) for name in sheet_names}

    expected_sheets = {
        "summary",
        "students_per_mentor",
        "school_binding_issues",
        "allocation_capacity",
        "join_keys",
        "student_counts",
        "meta",
    }
    assert expected_sheets.issubset(set(sheet_names))

    summary = sheets["summary"]
    assert int(summary.loc[summary["rule_id"] == "QA_RULE_STU_02", "violations_count"].iloc[0]) == 2

    students = sheets["students_per_mentor"]
    assert {11, 12} == set(int(value) for value in students["mentor_id"].tolist())

    meta = sheets["meta"].iloc[0].to_dict()
    assert meta.get("policy_version") == "1.0.3"

