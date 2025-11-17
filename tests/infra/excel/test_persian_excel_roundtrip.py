from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from app.core.common.normalization import normalize_persian_text
from app.infra.excel.exporter import apply_workbook_formatting


def test_excel_rtl_alignment_and_values(tmp_path: Path):
    frame = pd.DataFrame({"نام": ["ياسر"], "Count": [2]})
    target = tmp_path / "persian.xlsx"

    with pd.ExcelWriter(target, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="Allocation", index=False)
        apply_workbook_formatting(
            writer,
            engine="openpyxl",
            sheet_frames={"Allocation": frame},
            rtl=True,
            font_name="Vazirmatn",
            font_size=10,
        )

    book = load_workbook(target)
    sheet = book["Allocation"]

    assert sheet.sheet_view.rightToLeft is True
    assert sheet["A2"].alignment.horizontal == "right"
    assert sheet["B2"].alignment.horizontal in {None, "general"}
    assert normalize_persian_text(sheet["A2"].value) == normalize_persian_text("ياسر")
