"""آزمون‌های واحد برای نرمال‌سازی ستون مدرسهٔ دانش‌آموز."""

from __future__ import annotations

import pandas as pd

from app.core.allocate_students import _normalize_students
from app.core.common.columns import CANON_EN_TO_FA
from app.core.policy_loader import load_policy


def _base_student_frame(values: list[str | None]) -> pd.DataFrame:
    """ساخت دیتافریم پایه با ستون‌های اجباری سیاست."""

    count = len(values)
    return pd.DataFrame(
        {
            "مدرسه نهايی": values,
            "کدرشته": [101] * count,
            "گروه آزمایشی": ["تجربی"] * count,
            "جنسیت": [1] * count,
            "دانش آموز فارغ": [0] * count,
            "مرکز گلستان صدرا": [0] * count,
            "مالی حکمت بنیاد": [0] * count,
        }
    )


def test_school_alias_and_numeric_normalization() -> None:
    policy = load_policy()
    frame = _base_student_frame(["۶۶۳"])

    normalized = _normalize_students(frame, policy)

    school_col = CANON_EN_TO_FA["school_code"]
    assert normalized.at[0, school_col] == 663
    assert int(normalized.at[0, "school_code_norm"]) == 663
    assert normalized.at[0, "school_code_raw"] == "۶۶۳"
    assert normalized.at[0, "school_status_resolved"] == 1
    assert str(normalized["school_status_resolved"].dtype) == "Int64"


def test_school_status_false_for_zero_or_empty() -> None:
    policy = load_policy()
    frame = _base_student_frame(["۰", None])

    normalized = _normalize_students(frame, policy)

    statuses = normalized["school_status_resolved"].tolist()
    assert statuses == [0, 0]
    norm_values = normalized["school_code_norm"].tolist()
    assert norm_values[0] == 0
    if policy.school_code_empty_as_zero:
        assert norm_values[1] == 0
    else:
        assert pd.isna(norm_values[1])
