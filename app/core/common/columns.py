"""ماژول نگاشت نام ستون‌ها بر اساس منبع داده (Policy-First)."""

from __future__ import annotations

from typing import Dict, Mapping

import pandas as pd

from app.core.policy_loader import get_policy

__all__ = ["CANON", "ALIASES_DEFAULT", "resolve_aliases", "collect_aliases_for"]


# نام‌های واحد (canonical)
CANON: Mapping[str, str] = {
    "group_code": "کدرشته",
    "gender": "جنسیت",
    "graduation_status": "دانش آموز فارغ",
    "center": "مرکز گلستان صدرا",
    "finance": "مالی حکمت بنیاد",
    "school_code": "کد مدرسه",
    "school_name": "نام مدرسه",
    "school_code_1": "نام مدرسه 1",
    "school_code_2": "نام مدرسه 2",
    "school_code_3": "نام مدرسه 3",
    "school_code_4": "نام مدرسه 4",
    "postal": "کدپستی",
    "mentor_id": "کد کارمندی پشتیبان",
    "schools_covered_count": "تعداد مدارس تحت پوشش",
    "covered_students_count": "تعداد داوطلبان تحت پوشش",
}


# Synonyms per source
ALIASES_DEFAULT: Dict[str, Dict[str, str]] = {
    "inspactor": {
        "کد گروه آزمایشی": CANON["group_code"],
        "کد رشته": CANON["group_code"],
        "نام مدرسه 1": CANON["school_code_1"],
        "نام مدرسه 2": CANON["school_code_2"],
        "نام مدرسه 3": CANON["school_code_3"],
        "نام مدرسه 4": CANON["school_code_4"],
        "کدپستی": CANON["postal"],
        "تعداد مدارس تحت پوشش": CANON["schools_covered_count"],
        "تعداد داوطلبان تحت پوشش": CANON["covered_students_count"],
        "کد کارمندی پشتیبان": CANON["mentor_id"],
    },
    "school": {
        "کد مدرسه": CANON["school_code"],
        "نام مدرسه": CANON["school_name"],
        "کد کامل مدرسه": "کد کامل مدرسه",
        "کد آموزش و پرورش": "کد آموزش و پرورش",
    },
    "report": {
        "کد مدرسه": CANON["school_name"],
        "کد_مدرسه": CANON["school_code"],
        "کد رشته": CANON["group_code"],
        "کد کارمندی پشتیبان": CANON["mentor_id"],
        "جنسیت": CANON["gender"],
        "دانش آموز فارغ": CANON["graduation_status"],
        "دانش_آموز_فارغ": CANON["graduation_status"],
        "مرکز_گلستان_صدرا": CANON["center"],
        "مالی_حکمت_بنیاد": CANON["finance"],
    },
}


def collect_aliases_for(source: str) -> Mapping[str, str]:
    """دریافت نگاشت نام ستون‌ها برای منبع داده."""

    policy = get_policy()
    policy_aliases = policy.column_aliases.get(source, {})
    merged: Dict[str, str] = {**ALIASES_DEFAULT.get(source, {}), **policy_aliases}
    return merged


def resolve_aliases(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """نام ستون‌ها را به استاندارد Policy تبدیل می‌کند."""

    aliases = collect_aliases_for(source)
    new_cols = [aliases.get(column, column) for column in df.columns]
    result = df.copy()
    result.columns = new_cols
    return result

