from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.infra.excel.import_to_sabt import (  # noqa: E402
    apply_alias_rule,
    build_errors_frame,
    build_optional_sheet_frame,
    build_sheet2_frame,
    build_summary_frame,
    ensure_template_workbook,
    ImportToSabtExportError,
    load_exporter_config,
    prepare_allocation_export_frame,
    write_import_to_sabt_excel,
)


def _sample_alloc_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "student_id": "STD-1",
                "student_GF_FirstName": "سارا",
                "student_GF_LastName": "محمدی",
                "student_GF_Mobile": "۹۱۲۳۴۵۶۷۸۹",
                "student_GF_NationalCode": "۰۰۱۲۳۴۵۶۷۸",
                "student_gender": 0,
                "student_graduation_status": 0,
                "student_center": 1,
                "student_finance": 3,
                "student_group_code": 1201,
                "student_exam_group": "ریاضی",
                "student_school_code": 10101,
                "student_school_name": "دبیرستان بعثت",
                "student_school_code_1": 10101,
                "student_school_name_1": "دبیرستان بعثت",
                "student_school_code_2": pd.NA,
                "student_school_name_2": pd.NA,
                "student_school_code_3": pd.NA,
                "student_school_name_3": pd.NA,
                "student_school_code_4": pd.NA,
                "student_school_name_4": pd.NA,
                "registration_status": "3",
                "hekmat_tracking": "۱۲۳۴۵۶۷۸۹۰۱۲۳۴۵۶",
                "hekmat_package": "طلایی",
                "mentor_postal_code": "۱۲۳۴۵",
                "mentor_alias_postal_code": "54321",
                "mentor_mentor_name": "پشتیبان حکمت",
                "mentor_mentor_id": "EMP-1",
                "mentor_manager_name": "مدیر حکمت",
                "allocation_status": "success",
            },
            {
                "student_id": "STD-2",
                "student_GF_FirstName": "علی",
                "student_GF_LastName": "حسینی",
                "student_GF_Mobile": "09123456789",
                "student_GF_NationalCode": "1234567890",
                "student_gender": 1,
                "student_graduation_status": 1,
                "student_center": 2,
                "student_finance": 0,
                "student_group_code": 1502,
                "student_exam_group": "تجربی",
                "student_school_code": 20202,
                "student_school_name": "دبیرستان اندیشه",
                "student_school_code_1": 20202,
                "student_school_name_1": "دبیرستان اندیشه",
                "student_school_code_2": pd.NA,
                "student_school_name_2": pd.NA,
                "student_school_code_3": pd.NA,
                "student_school_name_3": pd.NA,
                "student_school_code_4": pd.NA,
                "student_school_name_4": pd.NA,
                "registration_status": "0",
                "hekmat_tracking": "654321",
                "hekmat_package": "نقره‌ای",
                "mentor_postal_code": "67890",
                "mentor_alias_postal_code": "",
                "mentor_mentor_name": "پشتیبان عادی",
                "mentor_mentor_id": "EMP-2",
                "mentor_manager_name": "مدیر عادی",
                "allocation_status": "failed",
            },
        ]
    )


def test_sheet2_structure_matches_template(tmp_path: Path) -> None:
    cfg = load_exporter_config("config/SmartAlloc_Exporter_Config_v1.json")
    df_alloc = _sample_alloc_frame()
    df_sheet2 = build_sheet2_frame(df_alloc, cfg, today=datetime(2024, 3, 20))
    df_sheet2 = apply_alias_rule(df_sheet2, df_alloc)
    logs_df = pd.DataFrame(
        [
            {"student_id": "STD-1", "allocation_status": "success"},
            {
                "student_id": "STD-2",
                "allocation_status": "failed",
                "error_type": "CAPACITY",
                "detailed_reason": "Full",
            },
        ]
    )
    df_summary = build_summary_frame(
        cfg,
        total_students=5,
        allocated_count=len(df_sheet2),
        error_count=1,
    )
    df_errors = build_errors_frame(logs_df, cfg)
    df_sheet5 = build_optional_sheet_frame(cfg, "Sheet5")
    df_9394 = build_optional_sheet_frame(cfg, "9394")
    template = tmp_path / "template.xlsx"
    ensure_template_workbook(template, cfg)
    output = tmp_path / "sabt.xlsx"
    write_import_to_sabt_excel(
        df_sheet2,
        df_summary,
        df_errors,
        df_sheet5,
        df_9394,
        template,
        output,
    )
    assert output.exists()
    df_result = pd.read_excel(output, sheet_name="Sheet2")
    df_template = pd.read_excel(template, sheet_name="Sheet2")
    assert df_result.columns.tolist() == df_template.columns.tolist()
    assert len(df_result.columns) == 46
    assert df_result.loc[0, "شناسه دانش آموز"] == "STD-1"
    assert str(df_result.iloc[0]["کد ملی"]).zfill(10) == "0012345678"
    assert str(df_result.iloc[0]["تلفن همراه"]).zfill(11) == "09123456789"
    assert str(df_result.iloc[0]["کد پستی"]).zfill(10) == "0000054321"


def test_hekmat_rule_resets_non_matching_rows() -> None:
    cfg = load_exporter_config("config/SmartAlloc_Exporter_Config_v1.json")
    df_alloc = _sample_alloc_frame()
    df_sheet2 = build_sheet2_frame(df_alloc, cfg)
    assert df_sheet2.loc[0, "کد رهگیری حکمت"] == "1234567890123456"
    assert df_sheet2.loc[1, "کد رهگیری حکمت"] == ""
    assert df_sheet2.loc[1, "نوع بسته حکمت"] == ""


def test_mobile_normalizer_handles_various_inputs() -> None:
    cfg = load_exporter_config("config/SmartAlloc_Exporter_Config_v1.json")
    df_alloc = _sample_alloc_frame().copy()
    df_alloc.loc[0, "student_GF_Mobile"] = "۹۱۲۳۴۵۶۷۸۹"
    df_alloc.loc[1, "student_GF_Mobile"] = "9123456789"
    df_sheet2 = build_sheet2_frame(df_alloc, cfg)
    assert df_sheet2.loc[0, "تلفن همراه"] == "09123456789"
    assert df_sheet2.loc[1, "تلفن همراه"] == "09123456789"


def test_alias_rule_prefers_alias_when_present() -> None:
    cfg = load_exporter_config("config/SmartAlloc_Exporter_Config_v1.json")
    df_alloc = _sample_alloc_frame()
    df_sheet2 = build_sheet2_frame(df_alloc, cfg)
    df_sheet2 = apply_alias_rule(df_sheet2, df_alloc)
    assert df_sheet2.loc[0, "کد پستی"] == "0000054321"
    assert df_sheet2.loc[1, "کد پستی"] == "0000067890"


def test_prepare_allocation_export_frame_preserves_length_and_index() -> None:
    alloc = pd.DataFrame(
        [
            {"student_id": "STD-1", "mentor_id": "EMP-1", "allocation_status": "success"},
            {"student_id": "STD-2", "mentor_id": "EMP-2", "allocation_status": "success"},
        ]
    )
    students = pd.DataFrame(
        [
            {"student_id": "STD-1", "GF_FirstName": "سارا"},
            {"student_id": "STD-2", "GF_FirstName": "علی"},
        ]
    )
    mentors = pd.DataFrame(
        [
            {"mentor_id": "EMP-1", "mentor_name": "پشتیبان ۱"},
            {"mentor_id": "EMP-2", "mentor_name": "پشتیبان ۲"},
        ]
    )

    merged = prepare_allocation_export_frame(alloc, students, mentors)

    assert len(merged) == len(alloc)
    assert list(merged.index) == list(alloc.index)


def test_prepare_allocation_export_frame_rejects_duplicate_students() -> None:
    alloc = pd.DataFrame([
        {"student_id": "STD-1", "mentor_id": "EMP-1"},
    ])
    students = pd.DataFrame(
        [
            {"student_id": "STD-1", "GF_FirstName": "سارا"},
            {"student_id": "STD-1", "GF_FirstName": "زهرا"},
        ]
    )
    mentors = pd.DataFrame([
        {"mentor_id": "EMP-1", "mentor_name": "پشتیبان ۱"},
    ])

    with pytest.raises(ImportToSabtExportError) as excinfo:
        prepare_allocation_export_frame(alloc, students, mentors)

    message = str(excinfo.value)
    assert "duplicate" in message
    assert "STD-1" in message


def test_prepare_allocation_export_frame_rejects_duplicate_mentors() -> None:
    alloc = pd.DataFrame([
        {"student_id": "STD-1", "mentor_id": "EMP-1"},
    ])
    students = pd.DataFrame([
        {"student_id": "STD-1", "GF_FirstName": "سارا"},
    ])
    mentors = pd.DataFrame(
        [
            {"mentor_id": "EMP-1", "mentor_name": "پشتیبان ۱"},
            {"mentor_id": "EMP-1", "mentor_name": "پشتیبان ۲"},
        ]
    )

    with pytest.raises(ImportToSabtExportError) as excinfo:
        prepare_allocation_export_frame(alloc, students, mentors)

    message = str(excinfo.value)
    assert "duplicate" in message
    assert "EMP-1" in message
