from __future__ import annotations

import logging
from typing import Dict

import pandas as pd
import pytest

from app.core.build_matrix import BuildConfig, validate_with_students
from app.core.common.normalization import normalize_header, resolve_group_code


@pytest.fixture()
def _group_map() -> Dict[str, int]:
    return {"تجربی": 2, "انسانی": 1}


def test_normalize_header_handles_zwnj_and_arabic_variants() -> None:
    assert normalize_header("كُد‌ رشته") == "کد رشته"


def test_resolve_group_code_prefers_major_code(_group_map: Dict[str, int]) -> None:
    row = pd.Series({"کد رشته": "3", "گروه آزمایشی": "تجربی", "student_id": "A"})
    stats: Dict[str, int] = {}

    code = resolve_group_code(
        row,
        _group_map,
        major_column="کد رشته",
        group_column="گروه آزمایشی",
        stats=stats,
    )

    assert code == 3
    assert stats.get("resolved_by_major_code") == 1
    assert stats.get("resolved_by_crosswalk", 0) == 0


def test_resolve_group_code_supports_persian_digits(_group_map: Dict[str, int]) -> None:
    row = pd.Series({"کد رشته": "۳", "student_id": "B"})

    code = resolve_group_code(
        row,
        _group_map,
        major_column="کد رشته",
        group_column="گروه آزمایشی",
    )

    assert code == 3


def test_resolve_group_code_trims_zero_padding(_group_map: Dict[str, int]) -> None:
    row = pd.Series({"کد رشته": "003", "student_id": "C"})

    code = resolve_group_code(
        row,
        _group_map,
        major_column="کد رشته",
        group_column="گروه آزمایشی",
    )

    assert code == 3


def test_resolve_group_code_falls_back_to_crosswalk(_group_map: Dict[str, int]) -> None:
    row = pd.Series({"کد رشته": "", "گروه آزمایشی": "تجربی", "student_id": "D"})

    code = resolve_group_code(
        row,
        _group_map,
        major_column="کد رشته",
        group_column="گروه آزمایشی",
    )

    assert code == 2


def test_resolve_group_code_logs_mismatch_warning(caplog: pytest.LogCaptureFixture, _group_map: Dict[str, int]) -> None:
    row = pd.Series({"کد رشته": 3, "گروه آزمایشی": "انسانی", "student_id": "E"})

    with caplog.at_level(logging.WARNING):
        code = resolve_group_code(
            row,
            _group_map,
            major_column="کد رشته",
            group_column="گروه آزمایشی",
            stats={},
        )

    assert code == 3
    assert any("mismatch" in record.message for record in caplog.records)


def test_validate_with_students_marks_unresolved_group_code() -> None:
    students_df = pd.DataFrame(
        {
            "کد پستی": ["0010"],
            "نام پشتیبان": ["پشتیبان الف"],
            "مدیر": ["شهدخت کشاورز"],
            "نام مدرسه 1": ["نمونه"],
            "کد رشته": [""],
            "گروه آزمایشی": [""],
            "جنسیت": [1],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "جایگزین": ["0010"],
            "عادی مدرسه": ["مدرسه‌ای"],
            "کدرشته": [3],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [1],
            "مرکز گلستان صدرا": [1],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [1001],
            "remaining_capacity": [1],
        }
    )
    schools_df = pd.DataFrame({"کد مدرسه": [1001], "نام مدرسه": ["نمونه"]})
    crosswalk_df = pd.DataFrame(
        {
            "گروه آزمایشی": ["تجربی"],
            "کد گروه": [2],
            "مقطع تحصیلی": ["دوازدهم"],
        }
    )

    stud_df, breakdown, _ = validate_with_students(
        students_df,
        matrix_df,
        schools_df,
        crosswalk_df,
        cfg=BuildConfig(),
    )

    assert pd.isna(stud_df.loc[0, "group_code"])
    assert stud_df.loc[0, "reason"] == "دانش‌آموز فاقد «کد رشته» و «گروه آزمایشی» معتبر است"
    assert breakdown.loc[0, "reason"] == "دانش‌آموز فاقد «کد رشته» و «گروه آزمایشی» معتبر است"
