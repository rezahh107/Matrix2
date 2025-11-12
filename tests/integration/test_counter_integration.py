"""تست ادغامی برای اتصال شمارنده به خروجی‌های تخصیص."""

from __future__ import annotations

import pandas as pd
import pytest

from app.core.common.columns import HeaderMode, canonicalize_headers
from app.core.counter import assert_unique_student_ids, assign_counters


def test_counter_end_to_end_with_persian_headers() -> None:
    students = pd.DataFrame(
        {
            "کد ملی": ["0000000001", "0000000002", "0000000003", "0000000004"],
            "جنسیت": [1, 1, 0, 0],
        }
    )
    prior = pd.DataFrame(
        {
            "کد ملی": ["0000000002", "0000000004"],
            "student_id": ["533570123", "533730007"],
        }
    )
    current = pd.DataFrame({"student_id": ["543570050", "543730020"]})

    students_en = canonicalize_headers(students, header_mode="en")
    counters = assign_counters(
        students_en,
        prior_roster_df=prior,
        current_roster_df=current,
        academic_year=1404,
    )
    assert_unique_student_ids(counters)

    allocations = pd.DataFrame({"mentor": list("ABCD")})
    logs = pd.DataFrame({"event": ["alloc"] * 4})
    trace = pd.DataFrame({"stage": ["type"] * 4})

    header_internal: HeaderMode = "en"

    allocations_en = canonicalize_headers(allocations, header_mode="en")
    allocations_en["student_id"] = counters.values
    logs_en = canonicalize_headers(logs, header_mode="en")
    logs_en["student_id"] = counters.reindex(logs_en.index).values
    trace_en = canonicalize_headers(trace, header_mode="en")
    trace_en["student_id"] = counters.reindex(trace_en.index).values

    allocations_final = canonicalize_headers(allocations_en, header_mode=header_internal)
    logs_final = canonicalize_headers(logs_en, header_mode=header_internal)
    trace_final = canonicalize_headers(trace_en, header_mode=header_internal)

    expected = ["543570051", "533570123", "543730021", "533730007"]
    assert allocations_final["student_id"].tolist() == expected
    assert logs_final["student_id"].tolist() == expected
    assert trace_final["student_id"].tolist() == expected
    assert allocations_final["student_id"].is_unique


def test_counter_duplicate_rows_trigger_validation() -> None:
    students = pd.DataFrame({"کد ملی": ["0000000001", "0000000001"], "جنسیت": [1, 1]})
    students_en = canonicalize_headers(students, header_mode="en")

    counters = assign_counters(
        students_en,
        prior_roster_df=None,
        current_roster_df=None,
        academic_year=1404,
    )

    assert counters.iloc[0] == counters.iloc[1]
    with pytest.raises(ValueError):
        assert_unique_student_ids(counters)
