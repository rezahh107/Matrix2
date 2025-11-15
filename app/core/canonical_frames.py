"""توابع کاننیکال‌ساز فریم‌های دانش‌آموز و استخر منتور (SSoT)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd

from .common.column_normalizer import normalize_input_columns
from .common.columns import (
    CANON_EN_TO_FA,
    CANON_FA_TO_EN,
    HeaderMode,
    canonicalize_headers,
    coerce_semantics,
    enrich_school_columns_en,
    ensure_series,
    enforce_join_key_types,
    resolve_aliases,
)
from .common.ids import build_mentor_alias_map, extract_alias_code_series
from .common.normalization import normalize_fa
from .policy_loader import PolicyConfig

__all__ = [
    "PoolCanonicalizationStats",
    "POOL_DUPLICATE_SUMMARY_ATTR",
    "POOL_JOIN_KEY_DUPLICATES_ATTR",
    "canonicalize_students_frame",
    "canonicalize_pool_frame",
    "canonicalize_allocation_frames",
    "sanitize_pool_for_allocation",
]

_POOL_STATS_ATTR = "pool_canonicalization_stats"
POOL_JOIN_KEY_DUPLICATES_ATTR = "pool_join_key_duplicates"
POOL_DUPLICATE_SUMMARY_ATTR = "pool_duplicate_summary"


@dataclass(slots=True)
class PoolCanonicalizationStats:
    """شمارندهٔ اصلاحات استخر منتور برای گزارش QA.

    مثال::

        >>> stats = PoolCanonicalizationStats()
        >>> stats.virtual_filtered += 2
        >>> stats.as_dict()["virtual_filtered"]
        2
    """

    virtual_filtered: int = 0
    mentor_id_autofill: int = 0
    alias_autofill: int = 0
    alias_unmatched: int = 0
    alias_pairs: int = 0
    capacity_coerced: int = 0
    columns_renamed: int = 0
    default_columns_injected: int = 0
    join_key_duplicates: int = 0

    def as_dict(self) -> dict[str, int]:
        """برگرداندن کپی دیکشنری برای درج در گزارش QA."""

        return {
            "virtual_filtered": self.virtual_filtered,
            "mentor_id_autofill": self.mentor_id_autofill,
            "alias_autofill": self.alias_autofill,
            "alias_unmatched": self.alias_unmatched,
            "alias_pairs": self.alias_pairs,
            "capacity_coerced": self.capacity_coerced,
            "columns_renamed": self.columns_renamed,
            "default_columns_injected": self.default_columns_injected,
            "join_key_duplicates": self.join_key_duplicates,
        }


def _attach_pool_stats(frame: pd.DataFrame, stats: PoolCanonicalizationStats) -> pd.DataFrame:
    """ذخیرهٔ آمار اصلاح در attrs دیتافریم و بازگرداندن همان دیتافریم."""

    frame.attrs[_POOL_STATS_ATTR] = stats
    return frame


def _coerce_capacity_series(
    series: pd.Series, stats: PoolCanonicalizationStats
) -> pd.Series:
    """تبدیل مقادیر ظرفیت به Int64 و افزایش شمارندهٔ اصلاحات."""

    numeric = pd.to_numeric(series, errors="coerce")
    invalid_mask = numeric.isna() & series.notna()
    stats.capacity_coerced += int(invalid_mask.sum())
    filled = numeric.fillna(0).astype("Int64")
    return filled


def _make_unique_columns(columns: Sequence[str]) -> list[str]:
    """ساخت نام ستون یکتا با حفظ ترتیب اولیه برای جلوگیری از برخورد."""

    seen: dict[str, int] = {}
    result: list[str] = []
    for column in columns:
        base = str(column).strip() or "column"
        count = seen.get(base, 0)
        name = base if count == 0 else f"{base} ({count + 1})"
        while name in seen:
            count += 1
            name = f"{base} ({count + 1})"
        seen[base] = count + 1
        seen[name] = 1
        result.append(name)
    return result


def _empty_join_key_report(
    join_keys: Sequence[str], mentor_column: str
) -> pd.DataFrame:
    """ساخت دیتافریم تهی با ستون‌های کلید شش‌تایی و شناسهٔ پشتیبان."""

    data: dict[str, pd.Series] = {
        column: pd.Series(dtype="Int64") for column in join_keys
    }
    data[mentor_column] = pd.Series(dtype="string")
    data["duplicate_group_size"] = pd.Series(dtype="Int64")
    return pd.DataFrame(data)


def _build_join_key_duplicate_report(
    frame: pd.DataFrame, join_keys: Sequence[str], mentor_column: str
) -> pd.DataFrame:
    """گزارش ردیف‌های تکراری بر اساس کلید شش‌تایی Policy.

    مثال::

        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "کدرشته": [1201, 1201],
        ...     "جنسیت": [1, 1],
        ...     "دانش آموز فارغ": [0, 0],
        ...     "مرکز گلستان صدرا": [1, 1],
        ...     "مالی حکمت بنیاد": [0, 0],
        ...     "کد مدرسه": [3581, 3581],
        ...     "کد کارمندی پشتیبان": ["EMP-1", "EMP-2"],
        ... })
        >>> report = _build_join_key_duplicate_report(
        ...     df,
        ...     [
        ...         "کدرشته",
        ...         "جنسیت",
        ...         "دانش آموز فارغ",
        ...         "مرکز گلستان صدرا",
        ...         "مالی حکمت بنیاد",
        ...         "کد مدرسه",
        ...     ],
        ...     "کد کارمندی پشتیبان",
        ... )
        >>> int(report["duplicate_group_size"].iat[0])
        2

    """

    if any(column not in frame.columns for column in join_keys):
        return _empty_join_key_report(join_keys, mentor_column)
    if mentor_column not in frame.columns:
        return _empty_join_key_report(join_keys, mentor_column)
    mask_duplicates = frame.duplicated(subset=list(join_keys), keep=False)
    if not bool(mask_duplicates.any()):
        return _empty_join_key_report(join_keys, mentor_column)
    subset_columns = list(join_keys) + [mentor_column]
    report = frame.loc[mask_duplicates, subset_columns].copy()
    report = report.sort_values(subset_columns, kind="stable")
    group_sizes = (
        report.groupby(list(join_keys), sort=False)[mentor_column]
        .transform("size")
        .astype("Int64")
    )
    report["duplicate_group_size"] = group_sizes
    return report.reset_index(drop=True)


def _json_safe_value(value: object) -> object:
    """تبدیل مقادیر pandas/numpy به انواع قابل‌سریال JSON."""

    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, (bool, int, float, str)):
        return value
    item_getter = getattr(value, "item", None)
    if callable(item_getter):  # pragma: no branch - رفتار numpy scalar
        try:
            return item_getter()
        except Exception:  # pragma: no cover - نگهبان دفاعی
            return str(value)
    iso_getter = getattr(value, "isoformat", None)
    if callable(iso_getter):
        try:
            return iso_getter()
        except Exception:  # pragma: no cover - نگهبان دفاعی
            return str(value)
    return str(value)


def _build_duplicate_summary(report: pd.DataFrame, *, sample_size: int = 5) -> dict[str, Any]:
    """تولید خلاصهٔ JSON-safe از تکرارهای کلید شش‌تایی برای درج در attrs."""

    if report is None or report.empty:
        return {"total": 0, "sample": []}
    sample_rows = report.head(sample_size).to_dict(orient="records")
    safe_rows = [
        {key: _json_safe_value(value) for key, value in row.items()}
        for row in sample_rows
    ]
    return {"total": int(len(report)), "sample": safe_rows}


def sanitize_pool_for_allocation(
    df: pd.DataFrame,
    *,
    policy: PolicyConfig,
    output_header_mode: HeaderMode | None = None,
) -> pd.DataFrame:
    """حذف منتورهای مجازی و نرمال‌سازی اولیهٔ استخر برای تخصیص.

    مثال::

        >>> import pandas as pd
        >>> from app.core.policy_loader import load_policy
        >>> policy = load_policy()  # doctest: +SKIP
        >>> raw = pd.DataFrame({
        ...     "mentor_name": ["مجازی", "علی"],
        ...     "alias": [7501, 102],
        ...     "remaining_capacity": [100, 2],
        ... })
        >>> clean = sanitize_pool_for_allocation(raw, policy=policy)  # doctest: +SKIP
        >>> int(clean["remaining_capacity"].sum())  # doctest: +SKIP
        2

    Args:
        df: دیتافریم خام استخر منتورها (گزارش Inspactor یا ماتریس).
        policy: سیاست جاری برای تشخیص الگوهای مجازی و حالت Excel.

    Returns:
        دیتافریم پاک‌سازی‌شده با نام ستون‌های یکدست و آمار اصلاح
        در ``df.attrs["pool_canonicalization_stats"]``.
    """

    stats = PoolCanonicalizationStats()
    frame = canonicalize_headers(df, header_mode=policy.excel.header_mode_internal).copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = [
            "__".join(map(str, tpl)).strip() for tpl in frame.columns.to_flat_index()
        ]
    if frame.columns.duplicated().any():
        frame.columns = _make_unique_columns(list(map(str, frame.columns)))

    mask_virtual = pd.Series(False, index=frame.index)
    patterns = policy.virtual_name_patterns
    regex = None
    if patterns:
        joined = "|".join(f"(?:{pattern})" for pattern in patterns)
        regex = re.compile(joined, re.IGNORECASE)
    if regex and "mentor_name" in frame.columns:
        mask_virtual |= frame["mentor_name"].astype(str).map(lambda text: bool(regex.search(text)))

    alias_ranges = policy.virtual_alias_ranges
    for column_name in ("alias", "mentor_id"):
        if column_name not in frame.columns:
            continue
        alias_numeric = pd.to_numeric(ensure_series(frame[column_name]), errors="coerce")
        for start, end in alias_ranges:
            mask_virtual |= alias_numeric.between(start, end, inclusive="both")

    sanitized = frame.loc[~mask_virtual].copy()
    stats.virtual_filtered += int(mask_virtual.sum())

    rename_candidates = {
        "remaining_capacity | remaining_capacity": "remaining_capacity",
    }
    for old, new in rename_candidates.items():
        if old in sanitized.columns and new not in sanitized.columns:
            sanitized = sanitized.rename(columns={old: new})
            stats.columns_renamed += 1

    defaults = {
        "remaining_capacity": ("Int64", 0),
        "allocations_new": ("Int64", 0),
        "mentor_id": ("Int64", pd.NA),
    }
    for column, (dtype, default) in defaults.items():
        if column not in sanitized.columns:
            sanitized[column] = pd.Series([default] * len(sanitized), dtype=dtype)
            stats.default_columns_injected += 1
        else:
            series = ensure_series(sanitized[column])
            if column == "remaining_capacity":
                series = _coerce_capacity_series(series, stats)
            elif dtype == "Int64":
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
            else:
                series = series.astype(dtype)
            sanitized[column] = series

    header_mode = output_header_mode or policy.excel.header_mode_internal
    result = canonicalize_headers(sanitized, header_mode=header_mode)
    return _attach_pool_stats(result, stats)


def _append_bilingual_alias_columns(
    frame: pd.DataFrame, policy: PolicyConfig
) -> pd.DataFrame:
    """افزودن ستون‌های دوزبانه برای کلیدهای Join جهت خوانایی."""

    alias_targets = list(dict.fromkeys(policy.join_keys))
    result = frame.copy()
    for fa_name in alias_targets:
        if fa_name not in result.columns:
            continue
        normalized_key = normalize_fa(fa_name)
        en_name = CANON_FA_TO_EN.get(normalized_key)
        if not en_name or en_name == fa_name:
            continue
        bilingual = f"{fa_name} | {en_name}"
        if bilingual in result.columns:
            continue
        insert_at = result.columns.get_loc(fa_name) + 1
        result.insert(insert_at, bilingual, result[fa_name].copy())
    return result


def _ensure_student_defaults(frame: pd.DataFrame, policy: PolicyConfig) -> pd.DataFrame:
    """تزریق ستون‌های حیاتی با مقادیر پیش‌فرض برای دانش‌آموزان."""

    fallback_map: dict[str | None, tuple[tuple[str, ...], object]] = {
        "group_code": (("group_code", "major_code"), 0),
        "exam_group": ((), "نامشخص"),
        "gender": (("gender",), 1),
        "graduation_status": (("وضعیت تحصیلی",), 0),
        "center": ((), 0),
        "finance": ((), 0),
        "school_code": ((), 0),
    }
    ensured = frame.copy()
    for join_key in policy.join_keys:
        if join_key in ensured.columns:
            continue
        normalized = normalize_fa(join_key)
        english = CANON_FA_TO_EN.get(normalized)
        fallbacks, default = fallback_map.get(english, ((), 0))
        for fallback in fallbacks:
            if fallback in ensured.columns:
                ensured[join_key] = ensured[fallback]
                break
        else:
            ensured[join_key] = default
    return ensured


def _ensure_exam_group_column(frame: pd.DataFrame) -> pd.DataFrame:
    """افزودن ستون «گروه آزمایشی» در صورت نبود، با مقادیر تهی."""

    exam_en = CANON_FA_TO_EN.get(normalize_fa(CANON_EN_TO_FA["exam_group"]), "exam_group")
    if exam_en in frame.columns:
        return frame
    ensured = frame.copy()
    ensured[exam_en] = pd.Series([pd.NA] * len(ensured), dtype="string", index=ensured.index)
    return ensured


def canonicalize_students_frame(
    students_df: pd.DataFrame,
    *,
    policy: PolicyConfig,
) -> pd.DataFrame:
    """کاننیکال‌سازی کامل دیتافریم دانش‌آموز برای تخصیص (SSoT)."""

    students = resolve_aliases(students_df.copy(deep=True), "report")
    if isinstance(students.columns, pd.MultiIndex):
        students.columns = [
            next((str(part).strip() for part in tpl if str(part).strip()), "column")
            for tpl in students.columns.to_flat_index()
        ]
    if students.columns.duplicated().any():
        students.columns = _make_unique_columns(list(map(str, students.columns)))
    school_fa = CANON_EN_TO_FA["school_code"]
    if school_fa in students.columns:
        # استفاده از ensure_series باعث می‌شود در صورت وجود ستون‌های تکراری، فقط
        # نخستین ستون انتخاب شده و از خطای «DataFrame.str» جلوگیری گردد.
        pre_normal_raw = ensure_series(students[school_fa]).astype("string").str.strip()
    else:
        pre_normal_raw = pd.Series(
            [pd.NA] * len(students), dtype="string", index=students.index
        )
    students = _ensure_student_defaults(students, policy)
    students = coerce_semantics(students, "report")
    students, _ = normalize_input_columns(
        students, kind="StudentReport", include_alias=True, report=False
    )
    students_en = canonicalize_headers(students, header_mode="en")
    if "school_code_raw" not in students_en.columns:
        students_en["school_code_raw"] = pre_normal_raw.reindex(students_en.index)
    students_en = enrich_school_columns_en(
        students_en, empty_as_zero=policy.school_code_empty_as_zero
    )
    students_en = _ensure_exam_group_column(students_en)
    students = canonicalize_headers(students_en, header_mode="fa")
    default_index = students_en.index
    school_code_raw = students_en.get(
        "school_code_raw",
        pd.Series([pd.NA] * len(default_index), dtype="string", index=default_index),
    )
    students["school_code_raw"] = school_code_raw
    school_code_norm = students_en.get(
        "school_code_norm",
        pd.Series([pd.NA] * len(default_index), dtype="Int64", index=default_index),
    )
    if policy.school_code_empty_as_zero:
        school_code_norm = school_code_norm.fillna(0)
    students["school_code_norm"] = school_code_norm.astype("Int64")
    students["school_status_resolved"] = students_en.get(
        "school_status_resolved",
        pd.Series([0] * len(default_index), dtype="Int64", index=default_index),
    )
    if school_fa in students.columns:
        students[school_fa] = students["school_code_norm"]
    missing = [column for column in policy.join_keys if column not in students.columns]
    if missing:
        raise KeyError(f"Student data missing columns: {missing}")
    required_fields = set(policy.required_student_fields)
    missing_required = []
    for field in required_fields:
        canonical = CANON_EN_TO_FA.get(field, field)
        if field not in students.columns and canonical not in students.columns:
            missing_required.append(field)
    if missing_required:
        raise ValueError(f"Missing columns: {missing_required}")
    students = enforce_join_key_types(students, policy.join_keys)
    return students


def canonicalize_pool_frame(
    pool_df: pd.DataFrame,
    *,
    policy: PolicyConfig,
    sanitize_pool: bool = True,
    pool_source: str = "inspactor",
    require_join_keys: bool = True,
    preserve_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """کاننیکال‌سازی استخر منتورها از هر منبع (inspactor/matrix).

    آمار اصلاحات در ``df.attrs["pool_canonicalization_stats"]`` قرار می‌گیرد.
    """

    source = pool_source if pool_source in {"inspactor", "matrix"} else "inspactor"
    preserved: dict[str, pd.Series] = {}
    if preserve_columns:
        for column in preserve_columns:
            if column in pool_df.columns:
                preserved[column] = pool_df[column].copy()

    pool = pool_df.copy(deep=True)
    stats = PoolCanonicalizationStats()
    if sanitize_pool:
        pool = sanitize_pool_for_allocation(pool, policy=policy)
        stats = pool.attrs.get(_POOL_STATS_ATTR, stats)
    else:
        pool = canonicalize_headers(pool, header_mode=policy.excel.header_mode_internal)
    pool = resolve_aliases(pool, source)  # type: ignore[arg-type]
    pool = coerce_semantics(pool, source)  # type: ignore[arg-type]
    pool, _ = normalize_input_columns(
        pool,
        kind="MentorPool" if source == "inspactor" else "MentorMatrix",
        include_alias=True,
        report=False,
    )

    mentor_column = "کد کارمندی پشتیبان"
    if mentor_column in pool.columns:
        mentor_source = ensure_series(pool[mentor_column])
        mentor_normalized = mentor_source.astype("string").fillna("").str.strip()
    else:
        mentor_normalized = pd.Series(
            [f"MENTOR_{i}" for i in range(len(pool))],
            index=pool.index,
            dtype="string",
        )
    pool[mentor_column] = mentor_normalized

    alias_series = pd.Series([""] * len(pool), dtype="string", index=pool.index)
    alias_map: dict[str, str] = {}
    if source == "inspactor":
        alias_series = extract_alias_code_series(pool)
        alias_map, alias_stats = build_mentor_alias_map(
            pool, mentor_column=mentor_column, alias_series=alias_series
        )
        stats.alias_pairs = alias_stats.unique_aliases
    else:
        stats.alias_pairs = 0

    capacity_alias = policy.columns.remaining_capacity
    if capacity_alias in pool.columns and "remaining_capacity" not in pool.columns:
        pool["remaining_capacity"] = ensure_series(pool[capacity_alias])
    for column_name in {capacity_alias, "remaining_capacity"}:
        if column_name in pool.columns:
            series = ensure_series(pool[column_name])
            pool[column_name] = _coerce_capacity_series(series, stats)

    for column, default in {
        "allocations_new": 0,
        "occupancy_ratio": 0.0,
        "remaining_capacity": 0,
    }.items():
        if column not in pool.columns:
            pool[column] = default

    mentor_id_series = ensure_series(pool.get("mentor_id")) if "mentor_id" in pool.columns else None
    if mentor_id_series is None:
        mentor_id_series = pd.Series([pd.NA] * len(pool), dtype="string", index=pool.index)
    mentor_id_series = mentor_id_series.astype("string").str.strip()
    missing_mask = mentor_id_series.eq("") | mentor_id_series.isna()
    if missing_mask.any():
        alias_target_mask = missing_mask & mentor_normalized.eq("")
        if source == "inspactor" and alias_target_mask.any():
            alias_candidates = alias_series.astype("string").fillna("")
            alias_missing = alias_candidates[alias_target_mask]
            alias_non_empty = alias_missing[alias_missing.ne("")]
            alias_unmatched_count = 0
            matched_alias = pd.Series(dtype="string")
            if not alias_non_empty.empty:
                alias_lookup = alias_non_empty.map(alias_map.get).fillna("")
                alias_lookup = alias_lookup.astype("string").str.strip()
                matched_alias = alias_lookup[alias_lookup.ne("")]
                if not matched_alias.empty:
                    mentor_id_series.loc[matched_alias.index] = matched_alias
                    pool.loc[matched_alias.index, mentor_column] = matched_alias
                    stats.alias_autofill += int(len(matched_alias))
                alias_unmatched_count = int(len(alias_non_empty) - len(matched_alias))
            if alias_unmatched_count > 0:
                stats.alias_unmatched += alias_unmatched_count
        post_alias_missing = mentor_id_series.eq("") | mentor_id_series.isna()
        mentor_id_series = mentor_id_series.mask(post_alias_missing, mentor_normalized)
        stats.mentor_id_autofill += int(missing_mask.sum())
    pool["mentor_id"] = mentor_id_series

    required = set(policy.join_keys) | {"کد کارمندی پشتیبان"}
    missing = [column for column in required if column not in pool.columns]
    if missing and require_join_keys:
        raise KeyError(f"Pool data missing columns: {missing}")

    present_join_keys = [column for column in policy.join_keys if column in pool.columns]
    if present_join_keys:
        pool = enforce_join_key_types(pool, present_join_keys)
    duplicate_report = _build_join_key_duplicate_report(
        pool, policy.join_keys, mentor_column
    )
    stats.join_key_duplicates = int(len(duplicate_report))
    pool.attrs[POOL_JOIN_KEY_DUPLICATES_ATTR] = duplicate_report
    pool.attrs[POOL_DUPLICATE_SUMMARY_ATTR] = _build_duplicate_summary(duplicate_report)
    pool = _append_bilingual_alias_columns(pool, policy)
    if preserved:
        for column, original in preserved.items():
            if column not in pool.columns:
                pool[column] = original.reindex(pool.index)
    return _attach_pool_stats(pool, stats)


def canonicalize_allocation_frames(
    students_df: pd.DataFrame,
    pool_df: pd.DataFrame,
    *,
    policy: PolicyConfig,
    sanitize_pool: bool = True,
    pool_source: str = "inspactor",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """تولید نسخهٔ کاننیکال دانش‌آموز/استخر برای مصرف allocate_batch."""

    students = canonicalize_students_frame(students_df, policy=policy)
    pool = canonicalize_pool_frame(
        pool_df,
        policy=policy,
        sanitize_pool=sanitize_pool,
        pool_source=pool_source,
    )
    return students, pool

