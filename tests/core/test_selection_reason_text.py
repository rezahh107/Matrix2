from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.policy_loader import load_policy
from app.core.reason.selection_reason import (
    ReasonContext,
    build_selection_reason_rows,
    render_reason,
)
from app.core.common.normalization import fa_digitize, safe_truncate
from app.core.common.policy import SelectionReasonLabels, SelectionReasonPolicy


def _policy_stub() -> SelectionReasonPolicy:
    return SelectionReasonPolicy(
        enabled=True,
        sheet_name="دلایل انتخاب پشتیبان",
        template=(
            "{gender_segment} — {school_segment} — {track_segment} — "
            "{capacity_segment} — {result_segment} — {tiebreak_segment}"
        ),
        trace_stage_labels=("جنسیت", "مدرسه", "رشته", "سیاست"),
        version="1.0.3",
        locale="fa",
        labels=SelectionReasonLabels(
            gender="جنسیت",
            school="مدرسه",
            track="رشته/گروه",
            capacity="ظرفیت",
            result="نتیجه",
            tiebreak="سیاست رتبه‌بندی",
        ),
        columns=(
            "شمارنده",
            "کدملی",
            "نام",
            "نام خانوادگی",
            "شناسه پشتیبان",
            "دلیل انتخاب پشتیبان",
        ),
        schema_hash="stub",
    )


def test_reason_chain_order_locale() -> None:
    policy = _policy_stub()
    context = ReasonContext(
        gender_value="دختر",
        school_value="دبیرستان نمونه",
        track_value="ریاضی",
        capacity_value="occupancy=۱۲٫۵",
        mentor_id="۱۰۱",
        mentor_name="منتور الف",
        after_school_label="پس‌مدرسه‌ای: بله",
        occupancy_ratio="12.50",
        allocations_new="1",
        remaining_capacity="3",
        tiebreak_text="۱) نسبت اشغال کمتر → ۲) تخصیص جدید کمتر → ۳) شناسه پشتیبان (مرتب‌سازی طبیعی)",
    )
    text = render_reason(context, policy)
    assert text.startswith("جنسیت: دختر"), text
    assert "مدرسه: دبیرستان نمونه" in text
    assert "رشته/گروه: ریاضی" in text
    assert text.count("—") >= 5


def test_tiebreak_explanation_reflects_policy() -> None:
    policy = load_policy()
    allocations = pd.DataFrame(
        [
            {
                "student_id": "S-1",
                "mentor_id": "201",
                "occupancy_ratio": 0.25,
                "allocations_new": 2,
                policy.capacity_column: 7,
                "counter": 10,
            }
        ]
    )
    students = pd.DataFrame(
        [
            {
                "student_id": "S-1",
                "کدملی": "0012345678",
                "نام": "زهرا",
                "نام خانوادگی": "محمدی",
                "کدرشته": 1201,
                "گروه آزمایشی": "تجربی",
                "جنسیت": policy.gender_codes.female.value,
                "دانش آموز فارغ": 0,
                "مرکز گلستان صدرا": 1,
                "مالی حکمت بنیاد": 0,
                "کد مدرسه": 2020,
            }
        ]
    )
    mentors = pd.DataFrame(
        [
            {
                "mentor_id": "201",
                "mentor_name": "منتور تست",
            }
        ]
    )

    reasons = build_selection_reason_rows(
        allocations,
        students,
        mentors,
        policy=policy,
        logs=None,
        trace=None,
    )
    reason_text = reasons.iloc[0]["دلیل انتخاب پشتیبان"]
    assert "۱) نسبت اشغال کمتر" in reason_text
    assert "۲) تخصیص جدید کمتر" in reason_text
    assert "۳) شناسه پشتیبان" in reason_text


def test_safe_truncate_unicode_boundary() -> None:
    text = "عبارت با ایموجی 😊 و ترکیب‌ها"
    truncated = safe_truncate(text, 12)
    assert truncated.endswith("…")
    assert "😊" not in truncated  # emoji removed safely


def test_fa_digitize_display_only() -> None:
    sample = "شناسه 123 و ظرفیت 45"
    assert fa_digitize(sample) == "شناسه ۱۲۳ و ظرفیت ۴۵"
