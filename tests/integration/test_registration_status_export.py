from __future__ import annotations

from collections import OrderedDict
from datetime import datetime

import pandas as pd

from app.infra.excel.export_allocations import (
    AllocationExportColumn,
    build_sabt_export_frame,
)
from app.infra.excel.import_to_sabt import build_sheet2_frame


def test_registration_status_shared_between_sheet2_and_allocations_sabt() -> None:
    allocations_df = pd.DataFrame(
        [
            {"student_id": "STD-1", "mentor_id": "EMP-1"},
            {"student_id": "STD-2", "mentor_id": "EMP-2"},
            {"student_id": "STD-3", "mentor_id": "EMP-3"},
        ]
    )
    students_df = pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "student_registration_status": 0,
                "student_finance": 3,
            },
            {
                "student_id": "STD-2",
                "student_registration_status": 1,
                "student_finance": 0,
            },
            {
                "student_id": "STD-3",
                "student_registration_status": 3,
                "student_finance": 0,
            },
        ]
    )

    df_alloc = allocations_df.merge(students_df, on="student_id", how="left")

    exporter_cfg = {
        "maps": {"registration_status": {"0": "عادی", "1": "بنیاد", "3": "حکمت"}},
        "sheets": {
            "Sheet2": {
                "columns": OrderedDict(
                    [
                        ("شناسه دانش آموز", {"source": "df", "field": "student_id", "type": "text"}),
                        (
                            "وضعیت ثبت نام",
                            {
                                "source": "df",
                                "field": "student_registration_status",
                                "map": "registration_status",
                                "type": "text",
                            },
                        ),
                    ]
                ),
                "landline_column": "تلفن ثابت",
            }
        },
    }

    sabt_profile = [
        AllocationExportColumn(
            key="student_id",
            header="کد ثبت نام",
            source_kind="allocation",
            source_field="student_id",
            literal_value=None,
            order=1,
        ),
        AllocationExportColumn(
            key="student_registration_status",
            header="وضعیت ثبت نام",
            source_kind="student",
            source_field="student_registration_status",
            literal_value=None,
            order=2,
        ),
    ]

    sheet2 = build_sheet2_frame(df_alloc, exporter_cfg, today=datetime(2024, 1, 1))
    sabt = build_sabt_export_frame(allocations_df, students_df, sabt_profile)

    assert sheet2["وضعیت ثبت نام"].tolist() == ["عادی", "بنیاد", "حکمت"]
    assert sabt["وضعیت ثبت نام"].tolist() == [0, 1, 3]
