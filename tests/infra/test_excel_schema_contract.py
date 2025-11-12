from __future__ import annotations

import importlib.util
from io import BytesIO

import pandas as pd
import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.policy_loader import load_policy
from app.infra.excel.exporter import write_selection_reasons_sheet

openpyxl = pytest.importorskip("openpyxl")


def _defined_name_map(workbook: "openpyxl.Workbook") -> dict[str, str]:
    container = workbook.defined_names
    if hasattr(container, "definedName"):
        entries = container.definedName or []
        return {entry.name: entry.attr_text for entry in entries}
    return {name: container[name].attr_text for name in container.keys()}


@pytest.mark.parametrize("engine", ["openpyxl", "xlsxwriter"])
def test_no_table_on_empty_df_both_engines(engine: str) -> None:
    if importlib.util.find_spec(engine) is None:
        pytest.skip(f"excel engine '{engine}' not available")

    policy = load_policy()
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine=engine) as writer:
        sheet_name, sanitized = write_selection_reasons_sheet(pd.DataFrame(), writer, policy)
        assert sanitized.empty
        assert sanitized.attrs["schema_hash"] == policy.emission.selection_reasons.schema_hash
        assert list(sanitized.columns) == list(policy.emission.selection_reasons.columns)

    buffer.seek(0)
    workbook = openpyxl.load_workbook(buffer, data_only=True)
    worksheet = workbook[sheet_name]
    assert len(worksheet.tables) == 0
    headers = [cell.value for cell in next(worksheet.iter_rows(min_row=1, max_row=1))]
    assert headers == list(policy.emission.selection_reasons.columns)
    defined_names = _defined_name_map(workbook)
    expected_hash = f'"{policy.emission.selection_reasons.schema_hash}"'
    if defined_names:
        assert defined_names.get("__SELECTION_REASON_SCHEMA_HASH__") == expected_hash


def test_headers_parity_openpyxl_vs_xlsxwriter_equals_policy_columns() -> None:
    policy = load_policy()
    sample = pd.DataFrame(
        [
            {
                "شمارنده": 5,
                "کدملی": "0012345678",
                "نام": "زهرا",
                "نام خانوادگی": "رضایی",
                "شناسه پشتیبان": "3201",
                "دلیل انتخاب پشتیبان": "نمونه متن",
            }
        ]
    )

    sanitized_outputs: dict[str, pd.DataFrame] = {}
    for engine in ("openpyxl", "xlsxwriter"):
        if importlib.util.find_spec(engine) is None:
            continue
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine=engine) as writer:
            sheet_name, sanitized = write_selection_reasons_sheet(sample, writer, policy)
        sanitized_outputs[engine] = sanitized
        buffer.seek(0)
        workbook = openpyxl.load_workbook(buffer, data_only=True)
        worksheet = workbook[sheet_name]
        headers = [cell.value for cell in next(worksheet.iter_rows(min_row=1, max_row=1))]
        assert headers == list(policy.emission.selection_reasons.columns)
        assert sanitized.attrs["schema_hash"] == policy.emission.selection_reasons.schema_hash

    assert sanitized_outputs, "at least one excel engine must be available"
    for sanitized in sanitized_outputs.values():
        assert list(sanitized.columns) == list(policy.emission.selection_reasons.columns)

    if len(sanitized_outputs) == 2:
        pd.testing.assert_frame_equal(
            sanitized_outputs["openpyxl"].reset_index(drop=True),
            sanitized_outputs["xlsxwriter"].reset_index(drop=True),
            check_dtype=False,
        )


def test_schema_hash_roundtrip_defined_name() -> None:
    policy = load_policy()
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sheet_name, _ = write_selection_reasons_sheet(pd.DataFrame(), writer, policy)
    buffer.seek(0)
    workbook = openpyxl.load_workbook(buffer, data_only=True)
    defined_names = _defined_name_map(workbook)
    assert defined_names.get("__SELECTION_REASON_SCHEMA_HASH__") == f'"{policy.emission.selection_reasons.schema_hash}"'
    assert sheet_name in workbook.sheetnames
