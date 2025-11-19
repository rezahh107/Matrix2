from __future__ import annotations

import enum
from typing import Iterable

import pandas as pd

from app.core.common.phone_rules import normalize_digits
from app.core.common.columns import CANON_EN_TO_FA

__all__ = [
    "HistoryStatus",
    "HISTORY_SNAPSHOT_COLUMNS",
    "dedupe_by_national_id",
    "build_history_snapshot_from_df",
]

_MISSING_OR_INVALID = "missing_or_invalid_national_code"
_NO_HISTORY_MATCH = "no_history_match"
_HISTORY_MATCH = "history_match"
def _candidate_columns(*names: str | None) -> tuple[str, ...]:
    return tuple(name for name in names if name)


_MENTOR_COLUMN_CANDIDATES: tuple[str, ...] = _candidate_columns(
    "mentor_id",
    CANON_EN_TO_FA.get("mentor_id"),
)
_CENTER_COLUMN_CANDIDATES: tuple[str, ...] = _candidate_columns(
    CANON_EN_TO_FA.get("center"),
    "center_code",
    "center_id",
)

HISTORY_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "history_mentor_id",
    "history_center_code",
)


class HistoryStatus(str, enum.Enum):
    """برچسب‌های وضعیت تطبیق سوابق تاریخی دانش‌آموز."""

    ALREADY_ALLOCATED = "already_allocated"
    NO_HISTORY_MATCH = "no_history_match"
    MISSING_OR_INVALID_NATIONAL_ID = "missing_or_invalid_national_code"


def _normalize_national_code(value: object) -> str:
    """تبدیل مقدار ورودی به رشتهٔ ده‌رقمی کد ملی یا رشتهٔ خالی.

    - مقادیر None یا NaN و هر مقدار غیر ده‌رقمی به رشتهٔ خالی تبدیل می‌شوند.
    - تنها رقم‌ها پس از نرمال‌سازی ارقام فارسی/عربی نگه داشته می‌شوند.
    """

    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except TypeError:
        # برای مقادیری که isna پشتیبانی نمی‌کند
        pass

    digits_only = normalize_digits(value) or ""
    return digits_only if len(digits_only) == 10 else ""


def _normalize_series(series: pd.Series | None, index: pd.Index | None) -> pd.Series:
    base_index = series.index if series is not None else index
    if base_index is None:
        return pd.Series([], dtype="string")
    if series is None:
        return pd.Series([""] * len(base_index), index=base_index, dtype="string")
    normalized = series.map(_normalize_national_code)
    return normalized.astype("string")


def _first_present_column(df: pd.DataFrame, candidates: Iterable[str]) -> pd.Series | None:
    for column in candidates:
        if column in df.columns:
            return df[column]
    return None


def _empty_snapshot_frame() -> pd.DataFrame:
    index = pd.Index([], name="normalized_national_code", dtype="string")
    return pd.DataFrame(index=index, columns=list(HISTORY_SNAPSHOT_COLUMNS))


def build_history_snapshot_from_df(history_df: pd.DataFrame) -> pd.DataFrame:
    """ساخت نمای خلاصهٔ سوابق تاریخی بر اساس کد ملی نرمال‌شده.

    این تابع حداقل ستون «history_mentor_id» را استخراج می‌کند و در صورت وجود ستون‌های
    مربوط به مرکز، ستون «history_center_code» را نیز تکمیل می‌کند. در صورت نبودن
    ستون‌های موردنیاز، دیتافریمی خالی اما با همان اسکیمای خروجی برمی‌گردد تا الحاق
    بعدی در تریس ساده بماند.
    """

    if history_df is None or history_df.empty:
        return _empty_snapshot_frame()

    national_series = _first_present_column(history_df, ("national_code", "کد ملی"))
    mentor_series = _first_present_column(history_df, _MENTOR_COLUMN_CANDIDATES)
    if national_series is None or mentor_series is None:
        return _empty_snapshot_frame()

    normalized_codes = _normalize_series(national_series, history_df.index)

    snapshot = pd.DataFrame({"normalized_national_code": normalized_codes})
    snapshot["history_mentor_id"] = mentor_series.astype("object")

    center_series = _first_present_column(history_df, _CENTER_COLUMN_CANDIDATES)
    if center_series is None:
        snapshot["history_center_code"] = pd.Series(
            pd.NA, index=history_df.index, dtype="object"
        )
    else:
        snapshot["history_center_code"] = center_series.astype("object")

    snapshot = snapshot[snapshot["normalized_national_code"].ne("")]
    if snapshot.empty:
        return _empty_snapshot_frame()

    snapshot = snapshot.drop_duplicates(
        subset=["normalized_national_code"], keep="last"
    )
    snapshot = snapshot.set_index("normalized_national_code")
    snapshot.index = snapshot.index.astype("string")
    # اطمینان از ترتیب ثابت ستون‌ها
    snapshot = snapshot.reindex(columns=list(HISTORY_SNAPSHOT_COLUMNS))
    return snapshot


def dedupe_by_national_id(
    students_df: pd.DataFrame, history_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """جداسازی دانش‌آموزان بر اساس وجود کد ملی در سوابق قبلی.

    خروجی‌ها همیشه شامل ستون‌های ``history_status`` و ``dedupe_reason`` هستند تا وضعیت دانش‌آموز
    در تریس/خلاصهٔ نهایی نیز قابل مشاهده باشد.

    - ``already_allocated_df``: دانش‌آموزانی که کد ملی نرمال‌شدهٔ آن‌ها در سوابق وجود دارد با برچسب
      ``HistoryStatus.ALREADY_ALLOCATED``، دلیل ``history_match`` و ستون‌های اسنپ‌شات مانند
      ``history_mentor_id`` و ``history_center_code`` که از آخرین رکورد قابل‌دسترس استخراج شده‌اند.
    - ``new_candidates_df``: سایر دانش‌آموزان؛ در صورت داشتن کد ملی معتبر ولی بدون تطبیق، برچسب
      ``HistoryStatus.NO_HISTORY_MATCH`` گرفته و اگر کد ملی مفقود یا نامعتبر باشد، برچسب
      ``HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID`` می‌گیرد. ستون‌های اسنپ‌شات برای این دسته
      مقدار خنثی (``pd.NA``) دارند تا اسکیمای خروجی ثابت بماند.

    مثال::

        >>> students = pd.DataFrame({"نام": ["الف", "ب", "ج"], "کد ملی": ["0012345678", "1234567890", "123"]})
        >>> history = pd.DataFrame({"national_code": ["0012345678"]})
        >>> allocated, new = dedupe_by_national_id(students, history)
        >>> allocated[["نام", "history_status"]].values.tolist()
        [['الف', 'already_allocated']]
        >>> new[["نام", "history_status", "dedupe_reason"]].values.tolist()
        [['ب', 'no_history_match', 'no_history_match'], ['ج', 'missing_or_invalid_national_code', 'missing_or_invalid_national_code']]

    :param students_df: دیتافریم دانش‌آموزان.
    :param history_df: دیتافریم سوابق قبلی که توسط لایهٔ Infra بارگذاری شده است.
    :return: ``(already_allocated_df, new_candidates_df)`` با ترتیب و شاخص اصلی حفظ شده.
    """

    if students_df is None or history_df is None:
        raise ValueError("students_df و history_df نباید None باشند")

    student_series = _first_present_column(
        students_df, ("national_code", "کد ملی")
    )
    history_series = _first_present_column(history_df, ("national_code", "کد ملی"))

    student_norm = _normalize_series(student_series, students_df.index)
    history_norm = _normalize_series(history_series, history_df.index)
    history_snapshot = build_history_snapshot_from_df(history_df)

    history_codes = set(history_norm[history_norm != ""].unique())
    already_mask = student_norm.ne("") & student_norm.isin(history_codes)

    invalid_mask = student_norm.eq("")

    history_status = pd.Series(index=students_df.index, dtype="string")
    history_status.loc[already_mask] = HistoryStatus.ALREADY_ALLOCATED.value
    history_status.loc[invalid_mask] = HistoryStatus.MISSING_OR_INVALID_NATIONAL_ID.value
    history_status.loc[~already_mask & ~invalid_mask] = (
        HistoryStatus.NO_HISTORY_MATCH.value
    )

    reasons = pd.Series(index=students_df.index, dtype="string")
    reasons.loc[already_mask] = _HISTORY_MATCH
    reasons.loc[invalid_mask] = _MISSING_OR_INVALID
    reasons.loc[~already_mask & ~invalid_mask] = _NO_HISTORY_MATCH

    def _attach_snapshot(
        frame: pd.DataFrame, normalized_codes: pd.Series
    ) -> None:
        for column in HISTORY_SNAPSHOT_COLUMNS:
            if history_snapshot.empty:
                mapped = pd.Series(pd.NA, index=frame.index, dtype="object")
            else:
                lookup = history_snapshot[column]
                mapped = normalized_codes.map(lookup)
                mapped = mapped.where(pd.notna(mapped), pd.NA)
            frame[column] = mapped

    enriched_df = students_df.copy()
    enriched_df["history_status"] = history_status.astype("string")
    enriched_df["dedupe_reason"] = reasons.astype("string")
    _attach_snapshot(enriched_df, student_norm)

    already_allocated_df = enriched_df.loc[already_mask].copy()
    new_candidates_df = enriched_df.loc[~already_mask].copy()

    return already_allocated_df, new_candidates_df
