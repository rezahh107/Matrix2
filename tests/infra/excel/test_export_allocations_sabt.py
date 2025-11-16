"""تست‌های خروجی Sabt برای اکسل تخصیص."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from pandas import testing as pd_testing
from pandas.api import types as pd_types
import pytest

from app.infra.excel.common import enforce_text_columns, identify_code_headers
from app.infra.excel.export_allocations import (
    AllocationExportColumn,
    build_sabt_export_frame,
    export_sabt_excel,
    load_sabt_export_profile,
)

_PROFILE_PATH = Path("docs/Report (4).xlsx")
_SNAPSHOT_PATH = Path("tests/infra/excel/data/sabt_expected.csv")
_NUMERIC_FIELDS = {"معدل", "معدل نیم سال"}
_DATE_FIELDS = {"تاریخ تولد", "تاریخ ثبت نام", "تاریخ اولین آزمون"}


@pytest.fixture(scope="module")
def sabt_profile() -> list[AllocationExportColumn]:
    if not _PROFILE_PATH.exists():
        pytest.skip("Sabt profile file is not available in the repository")
    return load_sabt_export_profile(_PROFILE_PATH)


@pytest.fixture(scope="module")
def sample_allocations_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": [5003, 5001, 5002, 5005, 5004],
            "mentor_id": [801, 800, 802, 804, 803],
            "mentor_alias_code": [1903, 1901, 1902, 1905, 1904],
        }
    )


@pytest.fixture(scope="module")
def sample_students_df(sabt_profile: list[AllocationExportColumn]) -> pd.DataFrame:
    return _build_sample_students_frame(sabt_profile)


def _build_sample_students_frame(profile: list[AllocationExportColumn]) -> pd.DataFrame:
    student_ids = [5003, 5001, 5002, 5005, 5004]
    data: dict[str, list] = {"student_id": student_ids}
    for column in profile:
        if column.source_kind != "student":
            continue
        field = column.source_field
        if not field or field in data:
            continue
        if field in _DATE_FIELDS:
            start = pd.Timestamp("2024-01-01")
            data[field] = [start + pd.Timedelta(days=idx) for idx in range(len(student_ids))]
        elif field in _NUMERIC_FIELDS:
            base_value = 18.0 if field == "معدل" else 17.0
            data[field] = [base_value + idx for idx in range(len(student_ids))]
        else:
            data[field] = [f"{field}-{idx + 1}" for idx in range(len(student_ids))]
    return pd.DataFrame(data)


def _load_snapshot_dataframe(
    profile: list[AllocationExportColumn],
) -> pd.DataFrame:
    """خواندن Snapshot متنی Sabt و همسان‌سازی انواع داده با خروجی واقعی."""

    if not _SNAPSHOT_PATH.exists():
        pytest.fail(
            "Sabt golden snapshot CSV is missing; please regenerate it before running tests"
        )

    snapshot = pd.read_csv(_SNAPSHOT_PATH)
    snapshot = snapshot.where(pd.notna(snapshot), pd.NA)

    def _maybe_convert_datetime(series: pd.Series) -> pd.Series:
        converted = pd.to_datetime(series, errors="coerce", format="ISO8601")
        return converted if converted.notna().sum() == series.notna().sum() else series

    def _maybe_convert_numeric(series: pd.Series) -> pd.Series:
        converted = pd.to_numeric(series, errors="coerce")
        return converted if converted.notna().sum() == series.notna().sum() else series

    for column in _DATE_FIELDS:
        if column in snapshot.columns:
            snapshot[column] = _maybe_convert_datetime(snapshot[column])
    for column in _NUMERIC_FIELDS:
        if column in snapshot.columns:
            snapshot[column] = _maybe_convert_numeric(snapshot[column])

    snapshot = snapshot.convert_dtypes()
    for column in snapshot.columns:
        series = snapshot[column]
        if series.isna().all():
            snapshot[column] = pd.Series([pd.NA] * len(series), dtype="object")
    snapshot = enforce_text_columns(snapshot, headers=identify_code_headers(profile))
    snapshot = snapshot.reset_index(drop=True)
    return snapshot


def test_load_sabt_export_profile_matches_sheet1_row_count() -> None:
    if not _PROFILE_PATH.exists():
        pytest.skip("Sabt profile file is not available in the repository")
    profile = load_sabt_export_profile(_PROFILE_PATH)
    sheet = pd.read_excel(_PROFILE_PATH, sheet_name="Sheet1")
    numeric_orders = pd.to_numeric(sheet["اولویت و ترتیب در اکسل خروجی"], errors="coerce")
    numeric_count = int(numeric_orders.notna().sum())
    assert len(profile) == numeric_count == 45
    assert profile[0].order == 1
    assert profile[-1].order == numeric_count
    allocation_keys = [column.key for column in profile if column.source_kind == "allocation"]
    assert allocation_keys == ["mentor_id", "student_id", "mentor_alias_code"]


def test_build_sabt_export_frame_sources_allocation_and_student_correctly() -> None:
    allocations_df = pd.DataFrame(
        {
            "student_id": [2, 1],
            "mentor_id": ["M-2", "M-1"],
            "mentor_alias_code": ["A2", "A1"],
        }
    )
    students_df = pd.DataFrame(
        {
            "student_id": [1, 2],
            "کدملی": ["001", "002"],
            "نام": ["الف", "ب"],
            "معدل": [18.5, 19.0],
        }
    )
    profile = [
        AllocationExportColumn(
            key="mentor_id",
            header="پشتیبان",
            source_kind="allocation",
            source_field="mentor_id",
            literal_value=None,
            order=1,
        ),
        AllocationExportColumn(
            key="student_id",
            header="کد ثبت نام",
            source_kind="allocation",
            source_field="student_id",
            literal_value=None,
            order=2,
        ),
        AllocationExportColumn(
            key="national_id",
            header="کدملی",
            source_kind="student",
            source_field="کدملی",
            literal_value=None,
            order=3,
        ),
        AllocationExportColumn(
            key="name",
            header="نام",
            source_kind="student",
            source_field="نام",
            literal_value=None,
            order=4,
        ),
        AllocationExportColumn(
            key="gpa",
            header="معدل",
            source_kind="student",
            source_field="معدل",
            literal_value=None,
            order=5,
        ),
    ]
    export_df = build_sabt_export_frame(allocations_df, students_df, profile)
    assert list(export_df.columns) == ["پشتیبان", "کد ثبت نام", "کدملی", "نام", "معدل"]
    assert export_df.iloc[0]["کد ثبت نام"] == "1"
    assert export_df.iloc[1]["کدملی"] == "002"
    assert pd_types.is_string_dtype(export_df["کد ثبت نام"])
    assert pd_types.is_float_dtype(export_df["معدل"])


def test_build_sabt_export_frame_matches_profile_against_english_headers() -> None:
    allocations_df = pd.DataFrame(
        {
            "student_id": [101],
            "mentor_id": ["M-1"],
            "mentor_alias_code": ["A-1"],
        }
    )
    students_df = pd.DataFrame(
        {
            "student_id": [101],
            "group_code": ["3001"],
            "gender": ["1"],
        }
    )
    profile = [
        AllocationExportColumn(
            key="mentor_id",
            header="پشتیبان",
            source_kind="allocation",
            source_field="mentor_id",
            literal_value=None,
            order=1,
        ),
        AllocationExportColumn(
            key="student_id",
            header="کد ثبت نام",
            source_kind="allocation",
            source_field="student_id",
            literal_value=None,
            order=2,
        ),
        AllocationExportColumn(
            key="group_code",
            header="گروه آزمایشی نهایی",
            source_kind="student",
            source_field="کد رشته",
            literal_value=None,
            order=3,
        ),
        AllocationExportColumn(
            key="gender",
            header="جنسیت",
            source_kind="student",
            source_field="جنسیت (0 یا 1)",
            literal_value=None,
            order=4,
        ),
    ]
    export_df = build_sabt_export_frame(allocations_df, students_df, profile)
    assert export_df.loc[0, "گروه آزمایشی نهایی"] == "3001"
    assert export_df.loc[0, "جنسیت"] == "1"


def test_export_sabt_excel_headers_and_types(tmp_path: Path) -> None:
    allocations_df = pd.DataFrame(
        {
            "student_id": [10],
            "mentor_id": [111],
            "mentor_alias_code": [700],
        }
    )
    students_df = pd.DataFrame(
        {
            "student_id": [10],
            "نام": ["آراد"],
            "معدل": [19.25],
        }
    )
    profile_df = pd.DataFrame(
        {
            "عنوان ستون ها ورودی": [
                "پیدا کردن ردیف پشتیبان از فیلد 141",
                "کد ثبت نام0",
                "نام",
                "معدل",
            ],
            "مقدار برای مپ کردن از اکسل ورودی": [
                "mentor_id",
                "student_id",
                "نام",
                "معدل",
            ],
            "اولویت و ترتیب در اکسل خروجی": [1, 2, 3, 4],
            "مقدار از کجا آورده شود": [
                "خروجی برنامه بعد از تخصیص",
                "خروجی برنامه بعد از تخصیص",
                "کپی کردن از اکسل ورودی",
                "کپی کردن از اکسل ورودی",
            ],
            "عنوان ستون در خروجی اکسل": ["", "", "", ""],
        }
    )
    profile_path = tmp_path / "profile.xlsx"
    profile_df.to_excel(profile_path, sheet_name="Sheet1", index=False)
    output_path = tmp_path / "sabt.xlsx"
    export_sabt_excel(
        allocations_df,
        students_df,
        output_path,
        profile_path=profile_path,
        sheet_name="Sabt",
    )
    exported = pd.read_excel(output_path, sheet_name="Sabt")
    assert list(exported.columns) == [
        "پیدا کردن ردیف پشتیبان از فیلد 141",
        "کد ثبت نام0",
        "نام",
        "معدل",
    ]
    assert exported.iloc[0]["معدل"] == pytest.approx(19.25)
    workbook = load_workbook(output_path)
    sheet = workbook["Sabt"]
    assert sheet["A2"].data_type == "s"
    assert sheet["A2"].value == "111"
    assert sheet["B2"].data_type == "s"
    assert sheet["B2"].value == "10"


def test_sabt_export_golden_snapshot(
    sabt_profile: list[AllocationExportColumn],
    sample_students_df: pd.DataFrame,
    sample_allocations_df: pd.DataFrame,
) -> None:
    export_df = build_sabt_export_frame(sample_allocations_df, sample_students_df, sabt_profile)
    expected = _load_snapshot_dataframe(sabt_profile)
    pd_testing.assert_frame_equal(
        export_df.reset_index(drop=True),
        expected,
        check_dtype=False,
    )
