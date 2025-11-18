from __future__ import annotations

from typing import List, Mapping, Sequence

import pandas as pd

from app.core.common.columns import enforce_join_key_types

__all__ = ["build_candidate_group_keys"]


def _safe_int(value: object) -> int:
    """تبدیل امن مقدار ورودی به int با پیش‌فرض صفر."""

    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return 0
    except Exception:
        pass
    try:
        if isinstance(value, bool):
            return int(value)
        text = str(value).strip()
            
        if text.isdigit():
            return int(text)
    except Exception:
        return 0
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return 0


def _ensure_iterable(values: object) -> list:
    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return [values]


def build_candidate_group_keys(
    base_df: pd.DataFrame,
    *,
    join_keys: Sequence[str],
    center_column: str,
    finance_column: str,
    school_code_column: str,
) -> pd.DataFrame:
    """استخراج فضای گروه‌های بالقوه از `base_df` بر اساس کلیدهای join.

    این تابع همان ترکیب کلیدهای شش‌گانهٔ Policy را از روی سطرهای پایهٔ منتورها
    می‌سازد تا مشخص شود هر منتور برای چه گروه‌هایی (کدرشته، جنسیت، وضعیت فارغ،
    مرکز، مالی، کد مدرسه) پتانسیل تولید سطر ماتریس دارد. علاوه‌بر کلیدها، ستون‌های
    کمکی زیر نیز بازگردانده می‌شوند:

    - ``variant``: نوع سناریو ("normal" یا "school")
    - ``has_alias``: آیا alias متناظر وجود دارد؟
    - ``can_generate``: آیا این سطر می‌تواند وارد ماتریس شود؟ (alias و امکان نوع)
    - ``mentor_id``: شناسهٔ پشتیبان برای شمارش یکتا

    خروجی صرفاً یک DataFrame کمکی است و هیچ I/O انجام نمی‌دهد.
    """

    if base_df.empty:
        columns = list(join_keys) + ["variant", "has_alias", "can_generate", "mentor_id"]
        return pd.DataFrame(columns=columns)

    records: List[Mapping[str, object]] = []

    track_key = next((key for key in join_keys if "رشته" in key or "گروه" in key), "کدرشته")
    gender_key = next((key for key in join_keys if "جنسیت" in key), "جنسیت")
    status_key = next((key for key in join_keys if "دانش" in key), "دانش آموز فارغ")

    for row in base_df.to_dict(orient="records"):
        mentor_id = row.get("mentor_id", "")
        group_pairs = _ensure_iterable(row.get("group_pairs") or [])
        genders = _ensure_iterable(row.get("genders") or [])
        statuses_normal = _ensure_iterable(row.get("statuses_normal") or [])
        statuses_school = _ensure_iterable(row.get("statuses_school") or [])
        finance_variants = _ensure_iterable(row.get("finance") or [])
        schools_normal = _ensure_iterable(row.get("schools_normal") or [""])
        school_codes = _ensure_iterable(row.get("school_codes") or [])
        center_code = _safe_int(row.get("center_code", 0))

        alias_normal = row.get("alias_normal")
        alias_school = row.get("alias_school")
        alias_normal_present = not pd.isna(alias_normal) and str(alias_normal).strip() != ""
        alias_school_present = not pd.isna(alias_school) and str(alias_school).strip() != ""
        can_normal = bool(row.get("can_normal", False)) and alias_normal_present
        can_school = bool(row.get("can_school", False)) and alias_school_present

        for group_pair in group_pairs:
            if not isinstance(group_pair, (list, tuple)) or len(group_pair) != 2:
                continue
            _name, code = group_pair
            group_code = _safe_int(code)
            for gender in genders or [""]:
                gender_code = _safe_int(gender)
                for status in statuses_normal or [""]:
                    status_code = _safe_int(status)
                    for finance in finance_variants or [0]:
                        finance_code = _safe_int(finance)
                        for school in schools_normal or [""]:
                            school_code = _safe_int(school)
                            record: dict[str, object] = {
                                track_key: group_code,
                                gender_key: gender_code,
                                status_key: status_code,
                                center_column: center_code,
                                finance_column: finance_code,
                                school_code_column: school_code,
                                "variant": "normal",
                                "has_alias": alias_normal_present,
                                "can_generate": can_normal,
                                "mentor_id": mentor_id,
                            }
                            records.append(record)

                for status in statuses_school or [""]:
                    status_code = _safe_int(status)
                    for finance in finance_variants or [0]:
                        finance_code = _safe_int(finance)
                        for school in school_codes or [0]:
                            school_code = _safe_int(school)
                            record = {
                                track_key: group_code,
                                gender_key: gender_code,
                                status_key: status_code,
                                center_column: center_code,
                                finance_column: finance_code,
                                school_code_column: school_code,
                                "variant": "school",
                                "has_alias": alias_school_present,
                                "can_generate": can_school,
                                "mentor_id": mentor_id,
                            }
                            records.append(record)

    frame = pd.DataFrame.from_records(records)
    present_keys = [key for key in join_keys if key in frame.columns]
    if present_keys:
        frame = enforce_join_key_types(frame, present_keys)
    ordered_columns: List[str] = list(join_keys) + ["variant", "has_alias", "can_generate", "mentor_id"]
    for column in ordered_columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, ordered_columns].copy()
