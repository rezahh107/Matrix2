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

from app.core.pipeline import enrich_student_contacts
from app.infra.excel.import_to_sabt import build_sheet2_frame

_EXPORTER_CFG = {
    "maps": {
        "registration_status": {"0": "عادی", "3": "حکمت"},
    },
    "sheets": {
        "Sheet2": {
            "columns": OrderedDict(
                [
                    ("شناسه دانش آموز", {"source": "df", "field": "student_id", "type": "text"}),
                    (
                        "وضعیت ثبت نام",
                        {
                            "source": "df",
                            "field": "registration_status",
                            "map": "registration_status",
                            "type": "text",
                        },
                    ),
                    (
                        "تلفن همراه",
                        {
                            "source": "df",
                            "field": "student_mobile",
                            "normalize": "mobile_ir",
                            "type": "text",
                        },
                    ),
                    (
                        "تلفن رابط 1",
                        {
                            "source": "df",
                            "field": "contact1_mobile",
                            "normalize": "mobile_ir",
                            "type": "text",
                        },
                    ),
                    (
                        "تلفن رابط 2",
                        {
                            "source": "df",
                            "field": "contact2_mobile",
                            "normalize": "mobile_ir",
                            "type": "text",
                        },
                    ),
                    ("تلفن ثابت", {"source": "df", "field": "student_landline", "type": "text"}),
                    (
                        "کد رهگیری حکمت",
                        {"source": "df", "field": "hekmat_tracking", "normalize": "digits_16", "type": "text"},
                    ),
                ]
            ),
            "landline_column": "تلفن ثابت",
            "hekmat_rule": {
                "status_column": "وضعیت ثبت نام",
                "expected_value": "حکمت",
                "columns": ["کد رهگیری حکمت"],
            },
        }
    },
}


def test_phone_rules_in_sheet2_output() -> None:
    df_alloc = pd.DataFrame(
        [
            {
                "student_id": "S1",
                "registration_status": "0",
                "student_mobile": "9357174851",
                "contact1_mobile": "۰۹۱۲۳۴۵۶۷۸۰",
                "contact2_mobile": "09123456780",
                "student_landline": "3512345678",
                "hekmat_tracking": "",
            },
            {
                "student_id": "S2",
                "registration_status": "0",
                "student_mobile": "",
                "contact1_mobile": "",
                "contact2_mobile": "",
                "student_landline": "6123456789",
                "hekmat_tracking": "",
            },
            {
                "student_id": "S3",
                "registration_status": "3",
                "student_mobile": "9123456789",
                "contact1_mobile": "",
                "contact2_mobile": "۰۹۳۵-۱۱۱ ۲۲۳۳",
                "student_landline": "",
                "hekmat_tracking": "",
            },
            {
                "student_id": "S4",
                "registration_status": "3",
                "student_mobile": "۰۹۳۵۱۲۳۴",
                "contact1_mobile": "۰۹۱۲۰۰۰۰۰۰۰",
                "contact2_mobile": "",
                "student_landline": "2123456789",
                "hekmat_tracking": "",
            },
        ]
    )

    enriched = enrich_student_contacts(df_alloc)
    sheet = build_sheet2_frame(enriched, _EXPORTER_CFG, today=datetime(2024, 1, 1))

    assert sheet.loc[0, "تلفن همراه"] == "09357174851"
    assert sheet.loc[0, "تلفن رابط 1"] == "09123456780"
    assert sheet.loc[0, "تلفن رابط 2"] == ""
    assert sheet.loc[0, "تلفن ثابت"] == "3512345678"

    assert sheet.loc[1, "تلفن ثابت"] == ""

    assert sheet.loc[2, "تلفن همراه"] == "09123456789"
    assert sheet.loc[2, "تلفن رابط 1"] == "09351112233"
    assert sheet.loc[2, "تلفن رابط 2"] == ""
    assert sheet.loc[2, "تلفن ثابت"] == "00000000000"
    assert sheet.loc[2, "کد رهگیری حکمت"] == "1111111111111111"

    assert sheet.loc[3, "تلفن همراه"] == ""
    assert sheet.loc[3, "تلفن ثابت"] == "00000000000"
    assert sheet.loc[3, "کد رهگیری حکمت"] == "1111111111111111"

    assert sheet["کد رهگیری حکمت"].dtype == "string"
