from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

_HERE = Path(__file__).resolve()
for candidate in _HERE.parents:
    if (candidate / "pyproject.toml").exists():
        sys.path.insert(0, str(candidate))
        break

from app.infra.excel.export_allocations import (
    AllocationExportColumn,
    build_sabt_export_frame,
)
from app.infra.excel.import_to_sabt import build_sheet2_frame


def test_allocation_exports_share_phone_policy() -> None:
    allocations_df = pd.DataFrame(
        [
            {"student_id": "STD-1", "mentor_id": "EMP-1"},
            {"student_id": "STD-2", "mentor_id": "EMP-2"},
        ]
    )
    students_df = pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "student_mobile": "9357174851",
                "contact1_mobile": "۰۹۱۲۳۴۵۶۷۸۰",
                "contact2_mobile": "09123456780",
                "student_landline": "3512345678",
                "student_registration_status": "0",
                "hekmat_tracking": "",
            },
            {
                "student_id": "STD-2",
                "student_mobile": "9123456789",
                "contact1_mobile": "",
                "contact2_mobile": "۰۹۳۵۱۱۱۲۲۳۳",
                "student_landline": "7123456",
                "student_registration_status": "3",
                "hekmat_tracking": "",
            },
        ]
    )
    df_alloc = allocations_df.merge(students_df, on="student_id", how="left")

    exporter_cfg = {
        "maps": {"registration_status": {"0": "عادی", "3": "حکمت"}},
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
                        (
                            "تلفن همراه",
                            {"source": "df", "field": "student_mobile", "type": "text"},
                        ),
                        (
                            "تلفن رابط 1",
                            {"source": "df", "field": "contact1_mobile", "type": "text"},
                        ),
                        (
                            "تلفن رابط 2",
                            {"source": "df", "field": "contact2_mobile", "type": "text"},
                        ),
                        ("تلفن ثابت", {"source": "df", "field": "student_landline", "type": "text"}),
                        (
                            "کد رهگیری حکمت",
                            {"source": "df", "field": "hekmat_tracking", "type": "text"},
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
            header="student_id",
            source_kind="allocation",
            source_field="student_id",
            literal_value=None,
            order=1,
        ),
        AllocationExportColumn(
            key="student_mobile",
            header="تلفن همراه",
            source_kind="student",
            source_field="تلفن همراه",
            literal_value=None,
            order=2,
        ),
        AllocationExportColumn(
            key="contact1_mobile",
            header="تلفن رابط 1",
            source_kind="student",
            source_field="تلفن رابط 1",
            literal_value=None,
            order=3,
        ),
        AllocationExportColumn(
            key="contact2_mobile",
            header="تلفن رابط 2",
            source_kind="student",
            source_field="تلفن رابط 2",
            literal_value=None,
            order=4,
        ),
        AllocationExportColumn(
            key="student_landline",
            header="تلفن ثابت",
            source_kind="student",
            source_field="تلفن ثابت",
            literal_value=None,
            order=5,
        ),
        AllocationExportColumn(
            key="hekmat_tracking",
            header="کد رهگیری حکمت",
            source_kind="student",
            source_field="کد رهگیری حکمت",
            literal_value=None,
            order=6,
        ),
    ]

    sheet = build_sheet2_frame(df_alloc, exporter_cfg, today=datetime(2024, 1, 1))
    sabt = build_sabt_export_frame(allocations_df, students_df, sabt_profile)

    expected_mobile = ["09357174851", "09123456789"]
    assert sheet["تلفن همراه"].tolist() == expected_mobile
    assert sabt["تلفن همراه"].tolist() == expected_mobile

    expected_guardian = ["09123456780", "09351112233"]
    assert sheet["تلفن رابط 1"].tolist() == expected_guardian
    assert sheet["تلفن رابط 2"].fillna("").tolist() == ["", ""]
    assert sabt["تلفن رابط 1"].tolist() == expected_guardian
    assert sabt["تلفن رابط 2"].fillna("").tolist() == ["", ""]

    expected_landline = ["3512345678", "00000000000"]
    assert sheet["تلفن ثابت"].tolist() == expected_landline
    assert sabt["تلفن ثابت"].tolist() == expected_landline

    expected_tracking = ["", "1111111111111111"]
    assert sheet["کد رهگیری حکمت"].tolist() == expected_tracking
    assert sabt["کد رهگیری حکمت"].tolist() == expected_tracking
