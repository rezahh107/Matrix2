from pathlib import Path

from app.core.qa.invariants import QaReport, QaRuleResult
from app.infra.cli import _export_qa_validation_workbook
from app.infra.excel.export_qa_validation import QaValidationContext


def test_cli_exports_validation_workbook(tmp_path: Path) -> None:
    base_output = tmp_path / "allocations.xlsx"
    report = QaReport(results=[QaRuleResult("QA_RULE_STU_01", True, [])])
    context = QaValidationContext(meta={"policy_version": "1.0.3"})

    output_path = _export_qa_validation_workbook(
        report=report,
        base_output=base_output,
        context=context,
    )

    assert output_path.name == "allocations_validation.xlsx"
    assert output_path.exists()
