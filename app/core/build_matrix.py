#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_matrix.py — Eligibility Matrix Builder (SSoT v1.0.2, script v1.0.4)

Summary
-------
- Normal mentors: build BOTH statuses → student=1 AND graduate=0
- School mentors: build ONLY student=1
- Everything else as per SSoT v1.0.2 (capacity gate, crosswalk+synonyms, finance 0/1/3, center mapping, atomic writes)
- Patch: When school code is empty, set "کد مدرسه" to numeric 0 in output rows.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import platform  # ← added
import re
import sys
import unicodedata
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import IntEnum, auto
from functools import lru_cache, wraps
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

# =============================================================================
# CONSTANTS
# =============================================================================
__version__ = "1.0.4"  # bumped
# Postal code range for NORMAL mentors
MIN_POSTAL_CODE = 1000
MAX_POSTAL_CODE = 9999
# Sort fallbacks for null values
SCHOOL_CODE_NULL_SORT = 999999
ALIAS_FALLBACK_SORT = 10**12  # push non-numeric aliases last

# Column headers (Persian per SSoT)
CAPACITY_CURRENT_COL = "تعداد داوطلبان تحت پوشش"
CAPACITY_SPECIAL_COL = "تعداد تحت پوشش خاص"
COL_MENTOR_NAME = "نام پشتیبان"
COL_MANAGER_NAME = "نام مدیر"
COL_MENTOR_ID = "کد کارمندی پشتیبان"  # mandatory; only trusted employee code
COL_MENTOR_ROWID = "ردیف پشتیبان"
COL_GROUP = "گروه آزمایشی"
COL_SCHOOL1 = "نام مدرسه 1"
COL_SCHOOL2 = "نام مدرسه 2"
COL_SCHOOL3 = "نام مدرسه 3"
COL_SCHOOL4 = "نام مدرسه 4"
COL_SCHOOL_CODE = "کد مدرسه"
COL_SCHOOL_COUNT = "تعداد مدارس تحت پوشش"
COL_POSTAL = "کدپستی"
COL_CAN_ALLOC = "امکان جذب دانش آموز"
COL_GENDER = "جنسیت"
COL_STATUS_A = "وضعیت تحصیلی"
COL_STATUS_B = "نوع دانش آموز"
# Optional
COL_GROUP_INCLUDED = "شامل گروه های آزمایشی"

# Regex / normalization
_RE_BIDI = re.compile("[\u200c-\u200f\u202a-\u202e]")
_RE_NONWORD = re.compile(r"[^\w\u0600-\u06FF0-9\s]+", flags=re.UNICODE)
_RE_WHITESPACE = re.compile(r"\s+")
_TRANS_PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
_RE_SPLIT_ITEMS = re.compile(r"[,\u060C]\s*")
_RE_RANGE = re.compile(r"^\s*(\d+)\s*[:\-–]\s*(\d+)\s*$")

# Built-in synonyms (keys normalized later)
BUILTIN_SYNONYMS = {
    "دهم علوم انسانی": "دهم انسانی",
    "یازدهم شبکه و نرم‌افزار رایانه": "یازدهم شبکه و نرم افزار رایانه",
    "کل متوسطه 1": "__BUCKET__متوسطه اول",
    "کل متوسطه 2": "__BUCKET__متوسطه دوم",
    "کل دبستان": "__BUCKET__دبستان",
    "کل هنرستان": "__BUCKET__هنرستان",
    "کل کنکوری": "__BUCKET__کنکوری",
    "کنکوریها": "__BUCKET__کنکوری",
}

# =============================================================================
# LOGGING
# =============================================================================
logger = logging.getLogger("eligibility_matrix")

# =============================================================================
# ENUMS
# =============================================================================
class Gender(IntEnum):
    FEMALE = 0
    MALE = auto()


class Status(IntEnum):
    GRADUATE = 0
    STUDENT = auto()


class Center(IntEnum):
    MARKAZ = 0
    GOLESTAN = auto()
    SADRA = auto()


class Finance(IntEnum):
    NORMAL = 0
    BONYAD = auto()
    HEKMAT = 3  # non-sequential on purpose

# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass(slots=True)
class BuildConfig:
    version: str = __version__
    finance_variants: tuple[int, ...] = (Finance.NORMAL, Finance.BONYAD, Finance.HEKMAT)
    default_status: int = Status.STUDENT
    enable_capacity_gate: bool = True
    center_manager_map: dict[str, int] = field(
        default_factory=lambda: {
            "شهدخت کشاورز": Center.GOLESTAN,
            "آیناز هوشمند": Center.SADRA,
        }
    )
    can_allocate_truthy: tuple[str, ...] = ("بلی", "بله", "Yes", "yes", "1", "true", "True")

# =============================================================================
# NORMALIZATION
# =============================================================================
@lru_cache(maxsize=1024)
def normalize_fa(text: Any) -> str:
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    s = str(text).replace("ي", "ی").replace("ك", "ک")
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = _RE_BIDI.sub("", s)
    s = _RE_NONWORD.sub(" ", s)
    s = s.translate(_TRANS_PERSIAN_DIGITS)
    return _RE_WHITESPACE.sub(" ", s).strip().lower()


def to_numlike_str(value: Any) -> str:
    s = str(value).strip()
    try:
        f = float(s)
        return str(int(f)) if f.is_integer() else s
    except ValueError:
        return s


def ensure_list(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for v in values:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        t = str(v).strip()
        if not t or t == "0":
            continue
        t = t.replace("،", ",").replace("|", ",")
        result.extend(p.strip() for p in t.split(",") if p.strip())
    seen: set[str] = set()
    uniq: list[str] = []
    for x in result:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

# =============================================================================
# DOMAIN HELPERS
# =============================================================================
def norm_gender(value: Any) -> int | None:
    s = normalize_fa(value)
    match s:
        case "1" | "پسر" | "male" | "boy" | "پسرانه":
            return Gender.MALE
        case "0" | "دختر" | "female" | "girl" | "دخترانه":
            return Gender.FEMALE
        case _:
            return None


def gender_text(code: int | None) -> str:
    return {Gender.FEMALE: "دختر", Gender.MALE: "پسر"}.get(int(code), "") if code is not None else ""


def norm_status(value: Any) -> int | None:
    s = normalize_fa(value)
    match s:
        case "1" | "دانش آموز" | "دانش‌آموز" | "student":
            return Status.STUDENT
        case "0" | "فارغ" | "فارغ التحصیل" | "فارغ‌التحصیل" | "graduate":
            return Status.GRADUATE
        case _:
            return None


def status_text(code: int | None) -> str:
    return {Status.STUDENT: "دانش‌آموز", Status.GRADUATE: "فارغ‌التحصیل"}.get(int(code), "") if code is not None else ""


def center_from_manager(manager_name: str, cfg: BuildConfig) -> int:
    name = str(manager_name or "")
    for needle, center_code in cfg.center_manager_map.items():
        if needle and needle in name:
            return int(center_code)
    return Center.MARKAZ


def center_text(code: int) -> str:
    return {Center.MARKAZ: "مرکز", Center.GOLESTAN: "گلستان", Center.SADRA: "صدرا"}.get(int(code), "")

# =============================================================================
# CROSSWALK
# =============================================================================
def validate_dataframe_columns(*required_cols: str):
    def decorator(func):
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns: {missing}")
            return func(df, *args, **kwargs)

        return wrapper

    return decorator


def load_crosswalk_groups(
    crosswalk_path: Path,
) -> tuple[dict[str, int], dict[int, str], dict[str, list[tuple[str, int]]], dict[str, str]]:
    try:
        with pd.ExcelFile(crosswalk_path) as xls:
            sheet_name = "پایه تحصیلی (گروه آزمایشی)"
            if sheet_name not in xls.sheet_names:
                raise ValueError(f"شیت «{sheet_name}» در Crosswalk یافت نشد")
            df = xls.parse(sheet_name)
            name_to_code: dict[str, int] = {}
            code_to_name: dict[int, str] = {}
            buckets: dict[str, list[tuple[str, int]]] = {}
            for _, row in df.iterrows():
                gname = str(row["گروه آزمایشی"])
                gcode = int(row["کد گروه"])
                level = str(row["مقطع تحصیلی"])
                name_to_code[normalize_fa(gname)] = gcode
                code_to_name[gcode] = gname  # display name
                buckets.setdefault(level, []).append((gname, gcode))

            synonyms = {normalize_fa(k): v for k, v in BUILTIN_SYNONYMS.items()}
            if "Synonyms" in xls.sheet_names:
                syn_df = xls.parse("Synonyms")
                src_col = next(
                    (c for c in syn_df.columns if "from" in normalize_fa(c) or "alias" in normalize_fa(c)),
                    syn_df.columns[0],
                )
                dst_col = next(
                    (c for c in syn_df.columns if "to" in normalize_fa(c) or "target" in normalize_fa(c)),
                    syn_df.columns[1] if len(syn_df.columns) > 1 else syn_df.columns[0],
                )
                for _, r in syn_df.iterrows():
                    src = normalize_fa(r.get(src_col, ""))
                    dst = str(r.get(dst_col, "")).strip()
                    if src and dst:
                        synonyms[src] = dst
            return name_to_code, code_to_name, buckets, synonyms
    except FileNotFoundError:
        raise FileNotFoundError(f"فایل Crosswalk یافت نشد: {crosswalk_path}")
    except Exception as exc:
        raise ValueError(f"خطا در باز کردن Crosswalk: {exc}")

# -----------------------------------------------------------------------------
def expand_group_token(
    token: str,
    name_to_code: dict[str, int],
    buckets: dict[str, list[tuple[str, int]]],
    synonyms: dict[str, str],
) -> list[tuple[str, int]]:
    t = normalize_fa(token)
    if not t:
        return []
    out: list[tuple[str, int]] = []
    # 1) Synonym
    if t in synonyms:
        syn = synonyms[t]
        if syn.startswith("__BUCKET__"):
            out.extend(buckets.get(syn.replace("__BUCKET__", ""), []))
        else:
            key = normalize_fa(syn)
            if key in name_to_code:
                out.append((syn, name_to_code[key]))
    # 2) Buckets
    checks = [
        (["متوسطه1", "متوسطه اول"], "متوسطه اول"),
        (["متوسطه2", "متوسطه دوم"], "متوسطه دوم"),
        (["دبستان"], "دبستان"),
        (["هنرستان"], "هنرستان"),
        (["کنکوری"], "کنکوری"),
    ]
    for kws, bucket in checks:
        if any(k in t for k in kws):
            out.extend(buckets.get(bucket, []))
    # 3) grade × major
    for num, name in [("10", "دهم"), ("11", "یازدهم"), ("12", "دوازدهم")]:
        if name in t or num in t:
            for kw, pretty in [
                ("تجربی", "تجربی"),
                ("ریاضی", "ریاضی"),
                ("انسانی", "انسانی"),
                ("علوم و معارف اسلامی", "علوم و معارف اسلامی"),
            ]:
                if normalize_fa(kw) in t:
                    title = f"{name} {pretty}".strip()
                    key = normalize_fa(title)
                    if key in name_to_code:
                        out.append((title, name_to_code[key]))
    # 4) direct
    if not out and t in name_to_code:
        out.append((token, name_to_code[t]))

    # dedup by code
    seen = set()
    uniq: list[tuple[str, int]] = []
    for n, c in out:
        if c not in seen:
            uniq.append((n, c))
            seen.add(c)
    return uniq

# =============================================================================
# I/O
# =============================================================================
def load_first_sheet(path: Path) -> pd.DataFrame:
    try:
        with pd.ExcelFile(path) as xls:
            return xls.parse(xls.sheet_names[0])
    except FileNotFoundError:
        raise FileNotFoundError(f"فایل یافت نشد: {path}")
    except Exception as exc:
        raise ValueError(f"خطا در خواندن فایل {path}: {exc}")


@validate_dataframe_columns(COL_SCHOOL_CODE)
def build_school_maps(schools_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    name_cols = [c for c in schools_df.columns if "نام مدرسه" in c]
    if not name_cols:
        raise ValueError("ستون نام مدرسه در SchoolReport یافت نشد")
    code_to_name = dict(zip(schools_df[COL_SCHOOL_CODE].astype(str), schools_df[name_cols[0]].astype(str)))
    name_to_code: dict[str, str] = {}
    for _, r in schools_df.iterrows():
        code = str(r[COL_SCHOOL_CODE])
        for col in name_cols:
            nm = normalize_fa(r[col])
            if nm:
                name_to_code.setdefault(nm, code)
    return code_to_name, name_to_code


def write_xlsx_atomic(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    tmp = path.with_suffix(path.suffix + ".part")
    with pd.ExcelWriter(tmp, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name)
    os.replace(tmp, path)


def safe_int_column(df: pd.DataFrame, col: str, default: int = 0) -> pd.Series:
    return pd.to_numeric(df.get(col), errors="coerce").fillna(default).astype(int)

# =============================================================================
# CAPACITY GATE (R0)
# =============================================================================
def capacity_gate(insp: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    if CAPACITY_CURRENT_COL not in insp.columns or CAPACITY_SPECIAL_COL not in insp.columns:
        logger.warning("Capacity columns not found; skipping capacity gate")
        return insp.copy(), pd.DataFrame(), True

    df = insp.copy()
    df["_cap_cur"] = safe_int_column(df, CAPACITY_CURRENT_COL, default=0)
    df["_cap_spec"] = safe_int_column(df, CAPACITY_SPECIAL_COL, default=0)

    removed_mask = ~(df["_cap_cur"] < df["_cap_spec"])

    keep_cols = [COL_MENTOR_NAME, COL_MANAGER_NAME, CAPACITY_CURRENT_COL, CAPACITY_SPECIAL_COL]
    if COL_SCHOOL1 in df.columns:
        keep_cols.append(COL_SCHOOL1)
    if COL_GROUP in df.columns:
        keep_cols.append(COL_GROUP)

    removed = df.loc[removed_mask, keep_cols].copy()
    removed = removed.rename(
        columns={
            CAPACITY_CURRENT_COL: "تحت پوشش فعلی",
            CAPACITY_SPECIAL_COL: "ظرفیت خاص",
            COL_MENTOR_NAME: "پشتیبان",
            COL_MANAGER_NAME: "مدیر",
            COL_SCHOOL1: "مدرسه (خام)",
            COL_GROUP: "گروه آزمایشی (خام)",
        }
    )
    removed["دلیل حذف"] = "تعداد داوطلبان تحت پوشش ≥ تعداد تحت پوشش خاص"

    kept = df.loc[~removed_mask].copy()
    drop_cols = ["_cap_cur", "_cap_spec"]
    kept = kept.drop(columns=drop_cols, errors="ignore")
    removed = removed.drop(columns=drop_cols, errors="ignore")

    logger.info("Capacity gate: kept=%d, removed=%d", len(kept), len(removed))
    return kept, removed, False

# =============================================================================
# SCHOOL CODE EXTRACTION
# =============================================================================
def to_int_str_or_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    t = str(value).strip()
    if not t:
        return None
    try:
        iv = int(float(t))
        return None if iv == 0 else str(iv)
    except ValueError:
        return None


def collect_school_codes_from_row(r: pd.Series, name_to_code: dict[str, str], school_cols: list[str]) -> list[str]:
    codes = [
        code
        for col in school_cols
        if (raw := r.get(col)) and (code := to_int_str_or_none(raw) or name_to_code.get(normalize_fa(raw)))
    ]
    seen: set[str] = set()
    out: list[str] = []
    for c in codes:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out

# =============================================================================
# PROGRESS (optional)
# =============================================================================
try:
    from tqdm import tqdm  # type: ignore

    HAS_TQDM = True
except Exception:
    HAS_TQDM = False


def progress(it, total: int | None = None):
    return tqdm(it, total=total) if HAS_TQDM else it

# =============================================================================
# GROUP CODE PARSING
# =============================================================================
def parse_group_code_spec(spec: Any) -> list[int]:
    if spec is None or (isinstance(spec, float) and math.isnan(spec)):
        return []
    s = str(spec).strip()
    if not s:
        return []
    s = s.translate(_TRANS_PERSIAN_DIGITS)
    parts = _RE_SPLIT_ITEMS.split(s)
    out: list[int] = []
    seen: set[int] = set()
    for tok in parts:
        tok = tok.strip()
        if not tok:
            continue
        m = _RE_RANGE.match(tok)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a > b:
                a, b = b, a
            for v in range(a, b + 1):
                if v not in seen:
                    out.append(v)
                    seen.add(v)
            continue
        if tok.isdigit():
            v = int(tok)
            if v not in seen:
                out.append(v)
                seen.add(v)
    return out

# =============================================================================
# ROW GENERATION
# =============================================================================
def generate_row_variants(
    base: dict,
    group_pairs: List[tuple[str, int]],
    genders: List[Any],
    statuses: List[Any],
    schools_raw: List[Any],
    finance_variants: Iterable[int],
    code_to_name_school: Dict[str, str],
) -> List[dict]:
    rows: List[dict] = []

    def _school_lookup(sc_val: Any) -> tuple[str, str, int]:
        """returns (school_code, school_name, is_school_flag)
        Patch: empty -> ("0", "", 0) so output column «کد مدرسه» becomes 0.
        """
        # If empty → set school code to 0 (string to keep type consistent across DataFrame)
        if sc_val is None or str(sc_val).strip() == "":
            return "0", "", 0
        s = str(sc_val).strip()
        if s.isdigit():
            return s, code_to_name_school.get(s, ""), 1
        return "", str(sc_val), 0

    iter_genders = genders or [""]
    iter_statuses = statuses or [""]
    iter_schools = schools_raw if any(schools_raw) else [""]

    for (g_name, g_code), ge, st, fin, sc in product(
        group_pairs, iter_genders, iter_statuses, finance_variants, iter_schools
    ):
        gcode = norm_gender(ge) if ge != "" else None
        stcode = norm_status(st) if st != "" else None
        school_code, school_name, is_school = _school_lookup(sc)

        # Alias rule per SSoT
        alias_val = base["alias"] if is_school == 0 else base["mentor_id"]

        rows.append(
            {
                "جایگزین": int(alias_val) if str(alias_val).strip().isdigit() else str(alias_val),
                "پشتیبان": base["supporter"],
                "مدیر": base["manager"],
                "ردیف پشتیبان": int(base["row_id"]) if str(base["row_id"]).strip().isdigit() else "",
                "نام رشته": g_name,
                "کدرشته": int(g_code),
                "جنسیت": int(gcode) if gcode is not None else "",
                "دانش آموز فارغ": int(stcode) if stcode is not None else "",
                "مرکز گلستان صدرا": int(base["center_code"]),
                "مالی حکمت بنیاد": int(fin),
                "کد مدرسه": school_code,
                "عادی مدرسه": "مدرسه‌ای" if is_school else "عادی",
                "نام مدرسه": school_name,
                "جنسیت2": gender_text(gcode),
                "دانش آموز فارغ2": status_text(stcode),
                "مرکز گلستان صدرا3": base["center_text"],
            }
        )
    return rows

# =============================================================================
# BUILD MATRIX HELPERS
# =============================================================================
def extract_base_data(r: pd.Series, cfg: BuildConfig, alias_int: int | None, center_code: int) -> dict:
    supporter = str(r.get(COL_MENTOR_NAME, "")).strip()
    manager = str(r.get(COL_MANAGER_NAME, "")).strip()
    mentor_id = r.get(COL_MENTOR_ID, "")
    row_id = r.get(COL_MENTOR_ROWID, r.name + 1)
    return {
        "supporter": supporter,
        "manager": manager,
        "mentor_id": mentor_id,
        "row_id": row_id,
        "alias": alias_int if alias_int is not None else "",
        "center_code": center_code,
        "center_text": center_text(center_code),
    }


def process_row(
    r: pd.Series,
    cfg: BuildConfig,
    name_to_code: dict[str, int],
    code_to_name: dict[int, str],
    buckets: dict[str, list[tuple[str, int]]],
    synonyms: dict[str, str],
    school_name_to_code: dict[str, str],
    code_to_name_school: dict[str, str],
    group_cols: list[str],
    school_cols: list[str],
    gender_col: str | None,
    included_col: str | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    rows: list[dict] = []
    unseen_groups: list[dict] = []
    unmatched_schools: list[dict] = []

    # Optional "can allocate"
    if COL_CAN_ALLOC in r and str(r.get(COL_CAN_ALLOC, "")).strip() not in cfg.can_allocate_truthy:
        return rows, unseen_groups, unmatched_schools

    # Must have mentor employee code
    if pd.isna(r[COL_MENTOR_ID]) or str(r[COL_MENTOR_ID]).strip() == "":
        return rows, unseen_groups, unmatched_schools

    # ----- groups
    group_pairs: list[tuple[str, int]] = []
    used_included = False
    if included_col:
        raw_spec = r.get(included_col)
        codes = parse_group_code_spec(raw_spec)
        if codes:
            used_included = True
            for gc in codes:
                if gc in code_to_name:
                    group_pairs.append((code_to_name[gc], gc))
                else:
                    unseen_groups.append(
                        {"group_token": f"code:{gc}", "supporter": r[COL_MENTOR_NAME], "manager": r[COL_MANAGER_NAME]}
                    )

    if not used_included:
        raw_groups = ensure_list([r[c] for c in group_cols]) if group_cols else []
        expanded: list[tuple[str, int]] = []
        for tok in raw_groups or []:
            ex = expand_group_token(tok, name_to_code, buckets, synonyms)
            if not ex:
                unseen_groups.append(
                    {"group_token": str(tok), "supporter": r[COL_MENTOR_NAME], "manager": r[COL_MANAGER_NAME]}
                )
            expanded.extend(ex)
        seen_codes = set()
        for name, code in expanded:
            if code not in seen_codes:
                group_pairs.append((name, code))
                seen_codes.add(code)

    if not group_pairs:
        return rows, unseen_groups, unmatched_schools

    # ----- gender
    genders = ensure_list([r.get(gender_col)]) if gender_col else [""]

    # ----- schools & alias
    school_codes = collect_school_codes_from_row(r, school_name_to_code, school_cols)

    alias_val = r.get(COL_POSTAL, "")
    alias_int = None
    try:
        ai = int(float(str(alias_val).strip()))
        if MIN_POSTAL_CODE <= ai <= MAX_POSTAL_CODE:
            alias_int = ai
    except Exception:
        pass

    # detect capabilities
    school_count = 0
    if COL_SCHOOL_COUNT in r:
        try:
            school_count = int(float(str(r.get(COL_SCHOOL_COUNT, "0")).strip() or "0"))
        except Exception:
            pass
    can_accept_school = (school_count > 0) or (len(school_codes) > 0)
    can_accept_normal = alias_int is not None

    center_code = center_from_manager(r[COL_MANAGER_NAME], cfg)
    base = extract_base_data(r, cfg, alias_int, center_code)

    # *******************************
    # STATUS POLICY (SSoT §8.3)
    # Normal rows => BOTH statuses [1,0]
    # School rows => ONLY [1]
    statuses_normal: List[Any] = [1, 0]
    statuses_school: List[Any] = [1]
    # *******************************

    if can_accept_normal:
        rows.extend(
            generate_row_variants(
                base=base,
                group_pairs=group_pairs,
                genders=genders,
                statuses=statuses_normal,   # ← force both statuses
                schools_raw=[""],
                finance_variants=cfg.finance_variants,
                code_to_name_school=code_to_name_school,
            )
        )

    if can_accept_school and school_codes:
        rows.extend(
            generate_row_variants(
                base=base,
                group_pairs=group_pairs,
                genders=genders,
                statuses=statuses_school,   # ← force student-only
                schools_raw=school_codes,
                finance_variants=cfg.finance_variants,
                code_to_name_school=code_to_name_school,
            )
        )

    for sc in (school_codes or []):
        if sc not in code_to_name_school:
            unmatched_schools.append(
                {"raw_school": str(sc), "supporter": r[COL_MENTOR_NAME], "manager": r[COL_MANAGER_NAME]}
            )

    return rows, unseen_groups, unmatched_schools

# =============================================================================
# BUILD MATRIX
# =============================================================================
def build_matrix(
    inspactor_xlsx: Path,
    schools_xlsx: Path,
    crosswalk_xlsx: Path,
    cfg: BuildConfig = BuildConfig(),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    logger.info("Loading inputs...")
    insp = load_first_sheet(inspactor_xlsx)
    schools = load_first_sheet(schools_xlsx)
    name_to_code, code_to_name, buckets, synonyms = load_crosswalk_groups(crosswalk_xlsx)
    code_to_name_school, school_name_to_code = build_school_maps(schools)

    if cfg.enable_capacity_gate:
        insp, removed_mentors, r0_skipped = capacity_gate(insp)
    else:
        removed_mentors = pd.DataFrame()
        r0_skipped = True
        logger.warning("Capacity gate disabled by config.")

    # detect columns
    gender_col = COL_GENDER if COL_GENDER in insp.columns else None
    included_col = next(
        (c for c in insp.columns if normalize_fa(c) == normalize_fa(COL_GROUP_INCLUDED)),
        next((c for c in insp.columns if all(k in normalize_fa(c) for k in ("شامل", "گروه", "آزمایشی"))), None),
    )
    group_cols = [c for c in insp.columns if ("گروه آزمایشی" in str(c)) and (c != included_col)]
    school_cols = [c for c in [COL_SCHOOL1, COL_SCHOOL2, COL_SCHOOL3, COL_SCHOOL4] if c in insp.columns]

    # generate rows
    logger.info("Generating matrix rows...")
    results = insp.apply(
        lambda r: process_row(
            r,
            cfg,
            name_to_code,
            code_to_name,
            buckets,
            synonyms,
            school_name_to_code,
            code_to_name_school,
            group_cols,
            school_cols,
            gender_col,
            included_col,
        ),
        axis=1,
    )

    all_rows: list[dict] = []
    all_unseen_groups: list[dict] = []
    all_unmatched_schools: list[dict] = []
    invalid_mentors = insp[insp[COL_MENTOR_ID].isna() | (insp[COL_MENTOR_ID].astype(str).str.strip() == "")]
    invalid_mentors_df = pd.DataFrame(
        {
            "row_index": invalid_mentors.index + 1,
            "پشتیبان": invalid_mentors[COL_MENTOR_NAME],
            "مدیر": invalid_mentors[COL_MANAGER_NAME],
            "reason": "missing mentor employee code",
        }
    )

    for rows, unseen, unmatched in results:
        all_rows.extend(rows)
        all_unseen_groups.extend(unseen)
        all_unmatched_schools.extend(unmatched)

    matrix = pd.DataFrame(all_rows)

    # stable ordering + counter
    if not matrix.empty:

        def _to_int_or(val: Any, default: int) -> int:
            try:
                return int(val)
            except Exception:
                return default

        matrix = matrix.assign(
            _school_sort=matrix["کد مدرسه"].apply(lambda v: _to_int_or(v, SCHOOL_CODE_NULL_SORT)),
            _alias_sort=matrix["جایگزین"].apply(lambda v: _to_int_or(v, ALIAS_FALLBACK_SORT)),
        )
        matrix = matrix.sort_values(
            by=["مرکز گلستان صدرا", "کدرشته", "_school_sort", "_alias_sort"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        matrix = matrix.drop(columns=["_school_sort", "_alias_sort"])
        matrix.insert(0, "counter", range(1, len(matrix) + 1))

    validation = pd.DataFrame(
        [
            {
                "total_rows": len(matrix),
                "distinct_supporters": matrix["پشتیبان"].nunique() if not matrix.empty else 0,
                "school_based_rows": int((matrix["عادی مدرسه"] == "مدرسه‌ای").sum()) if not matrix.empty else 0,
                "finance_0_rows": int((matrix["مالی حکمت بنیاد"] == Finance.NORMAL).sum()) if not matrix.empty else 0,
                "finance_1_rows": int((matrix["مالی حکمت بنیاد"] == Finance.BONYAD).sum()) if not matrix.empty else 0,
                "finance_3_rows": int((matrix["مالی حکمت بنیاد"] == Finance.HEKMAT).sum()) if not matrix.empty else 0,
                "removed_mentors": 0 if removed_mentors is None else len(removed_mentors),
                "r0_skipped": 1 if r0_skipped else 0,
            }
        ]
    )

    removed_df = removed_mentors
    unmatched_schools_df = pd.DataFrame(all_unmatched_schools).drop_duplicates()
    unseen_groups_df = pd.DataFrame(all_unseen_groups).drop_duplicates()

    meta = {
        "ssot_version": "1.0.2",
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "build_host": platform.node(),
        "inputs": {
            "inspactor": str(inspactor_xlsx),
            "schools": str(schools_xlsx),
            "crosswalk": str(crosswalk_xlsx),
        },
        "inputs_mtime": {
            "inspactor": inspactor_xlsx.stat().st_mtime,
            "schools": schools_xlsx.stat().st_mtime,
            "crosswalk": crosswalk_xlsx.stat().st_mtime,
        },
        "rowcounts": {"inspactor": int(len(insp)), "schools": int(len(schools))},
        "config": asdict(cfg),
    }
    meta_df = pd.DataFrame([meta])  # kept for potential downstream use

    return matrix, validation, removed_df, unmatched_schools_df, unseen_groups_df, invalid_mentors_df, meta


# =============================================================================
# VALIDATION vs StudentReport (optional)
# =============================================================================
def infer_students_gender_from_path(path: Path) -> int | None:
    s = str(path)
    if "3570" in s:
        return Gender.MALE
    if "3730" in s:
        return Gender.FEMALE
    return None


def resolve_students_gender_series(stud_df: pd.DataFrame, students_xlsx: Path, mode: str) -> pd.Series:
    mode = (mode or "auto").lower()
    n = len(stud_df)
    if mode == "male":
        return pd.Series([Gender.MALE] * n, index=stud_df.index)
    if mode == "female":
        return pd.Series([Gender.FEMALE] * n, index=stud_df.index)
    hint = infer_students_gender_from_path(students_xlsx)
    if hint is not None:
        return pd.Series([hint] * n, index=stud_df.index)
    if "جنسیت" in stud_df.columns:
        return stud_df["جنسیت"].apply(norm_gender).fillna(Gender.MALE)
    return pd.Series([Gender.MALE] * n, index=stud_df.index)


def validate_with_students(
    students_xlsx: Path,
    matrix_df: pd.DataFrame,
    schools_xlsx: Path,
    crosswalk_xlsx: Path,
    students_gender_mode: str = "auto",
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    stud_raw = load_first_sheet(students_xlsx)
    schools = load_first_sheet(schools_xlsx)
    with pd.ExcelFile(crosswalk_xlsx) as xls:
        cross = xls.parse("پایه تحصیلی (گروه آزمایشی)")
    group_map = {
        normalize_fa(n): int(c)
        for n, c in zip(cross["گروه آزمایشی"], cross["کد گروه"])
        if pd.notna(n) and pd.notna(c)
    }

    # School name → code
    name_cols = [c for c in schools.columns if "نام مدرسه" in c]
    name_to_code: Dict[str, str] = {}
    for _, row in schools.iterrows():
        code = str(row[COL_SCHOOL_CODE])
        for col in name_cols:
            nm = normalize_fa(row[col])
            if nm:
                name_to_code.setdefault(nm, code)

    # Support "کد پستی" OR "کد جایگزین"
    std_alias_col = "کد پستی" if "کد پستی" in stud_raw.columns else ("کد جایگزین" if "کد جایگزین" in stud_raw.columns else None)
    if std_alias_col is None:
        raise ValueError("در StudentReport ستونی با عنوان «کد پستی» یا «کد جایگزین» یافت نشد.")

    stud = pd.DataFrame(
        {
            "student_postal": stud_raw[std_alias_col].apply(to_numlike_str),
            "alias_norm": stud_raw[std_alias_col].apply(to_numlike_str),
            "mentor_name": stud_raw["نام پشتیبان"].astype(str).str.strip(),
            "manager": stud_raw["مدیر"].astype(str).str.strip(),
            "group_code": stud_raw["گروه آزمایشی"].apply(lambda x: group_map.get(normalize_fa(x), None)),
            "school_code": stud_raw[COL_SCHOOL1].apply(
                lambda x: name_to_code.get(normalize_fa(x), "")
            )
            if COL_SCHOOL1 in stud_raw.columns
            else "",
        }
    )
    stud["status_code"] = Status.STUDENT
    stud["gender_code"] = resolve_students_gender_series(stud_raw, students_xlsx, students_gender_mode).values

    mat = matrix_df.copy()
    mat["alias_norm"] = mat["جایگزین"].apply(to_numlike_str)
    mat["school_code"] = mat["کد مدرسه"].astype(str).str.strip()

    def _student_type_from_postal(v: str) -> str:
        if not v:
            return "normal_by_alias"
        try:
            iv = int(v)
            if iv < MIN_POSTAL_CODE:
                return "school_by_schoolcode"
            if MIN_POSTAL_CODE <= iv <= MAX_POSTAL_CODE:
                return "normal_by_alias"
            return "school_by_mentorid"
        except Exception:
            return "school_by_mentorid"

    def _sub_by_type(row: pd.Series) -> tuple[pd.DataFrame, str | None]:
        stype = _student_type_from_postal(row["student_postal"])
        if stype == "normal_by_alias":
            sub = mat[(mat["عادی مدرسه"] == "عادی") & (mat["alias_norm"] == row["alias_norm"])]
            return (sub, None if not sub.empty else "no normal alias match")
        if stype == "school_by_mentorid":
            sub = mat[(mat["عادی مدرسه"] == "مدرسه‌ای") & (mat["alias_norm"] == row["alias_norm"])]
            return (sub, None if not sub.empty else "no mentor-id school match")
        sub = mat[(mat["عادی مدرسه"] == "مدرسه‌ای")]
        if row["school_code"]:
            sub = sub[sub["school_code"].astype(str) == str(row["school_code"])]
        return (sub, None if not sub.empty else "no school-code match")

    def _check_gender(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        sub2 = sub[sub["جنسیت"] == row["gender_code"]]
        return (sub2, None if not sub2.empty else "gender mismatch")

    def _check_status(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        sub2 = sub[sub["دانش آموز فارغ"] == row["status_code"]]
        return (sub2, None if not sub2.empty else "status mismatch")

    def _check_center(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        def _center_from_manager(n: str) -> int:
            if "شهدخت" in str(n) and "کشاورز" in str(n):
                return Center.GOLESTAN
            if "آیناز" in str(n) and "هوشمند" in str(n):
                return Center.SADRA
            return Center.MARKAZ

        expected = _center_from_manager(row["manager"])
        sub2 = sub[sub["مرکز گلستان صدرا"] == expected]
        return (sub2, None if not sub2.empty else "center mismatch (manager-based)")

    def _check_group(row: pd.Series, sub: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
        if row["group_code"] is None:
            return (sub, None)
        sub2 = sub[sub["کدرشته"] == row["group_code"]]
        return (sub2, None if not sub2.empty else "group_code mismatch")

    def first_fail_reason(row: pd.Series) -> str:
        sub, err = _sub_by_type(row)
        if err:
            return err
        for checker in (_check_gender, _check_status, _check_center, _check_group):
            sub, err = checker(row, sub)
            if err:
                return err
        return "MATCHED"

    stud["reason"] = stud.apply(first_fail_reason, axis=1)
    stud["match"] = stud["reason"].eq("MATCHED")

    breakdown = stud["reason"].value_counts().reset_index()
    breakdown.columns = ["reason", "count"]
    summary = {"total": int(len(stud)), "matched": int(stud["match"].sum()), "unmatched": int((~stud["match"]).sum())}

    logger.info("Validation summary: %s", summary)
    return stud, breakdown, summary

# =============================================================================
# CLI
# =============================================================================
def parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build Eligibility Matrix (+ optional StudentReport validation)")
    ap.add_argument("--inspactor", required=True, type=Path, help="Path to InspactorReport.xlsx")
    ap.add_argument("--schools", required=True, type=Path, help="Path to SchoolReport.xlsx")
    ap.add_argument("--crosswalk", required=True, type=Path, help="Path to Crosswalk.xlsx")
    ap.add_argument("--outdir", required=True, type=Path, help="Output directory")
    ap.add_argument("--students", required=False, type=Path, help="Optional StudentReport.xlsx for validation")
    ap.add_argument(
        "--students-gender",
        choices=["auto", "male", "female"],
        default="auto",
        help="Gender policy for validation. 'auto' infers 3570=male, 3730=female, else column 'جنسیت' or male.",
    )
    ap.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR|CRITICAL")
    ap.add_argument("--no-capacity-gate", action="store_true", help="Disable capacity gate (for debugging)")
    return ap.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = BuildConfig(enable_capacity_gate=(not args.no_capacity_gate))
    args.outdir.mkdir(parents=True, exist_ok=True)

    matrix, validation, removed, unmatched_schools, unseen_groups, invalid_mentors, meta_dict = build_matrix(
        args.inspactor, args.schools, args.crosswalk, cfg
    )

    # Write matrix (CSV + XLSX atomic)
    out_csv = args.outdir / "eligibility_matrix.csv"
    out_xlsx = args.outdir / "eligibility_matrix.xlsx"
    matrix.to_csv(out_csv, index=False, encoding="utf-8-sig")

    sheets: Dict[str, pd.DataFrame] = {"matrix": matrix, "validation": validation}
    if not removed.empty:
        sheets["removed_mentors"] = removed.sort_values(["مدیر", "پشتیبان"])
    if not unmatched_schools.empty:
        sheets["unmatched_schools"] = unmatched_schools
    if not unseen_groups.empty:
        sheets["unseen_groups"] = unseen_groups
    if not invalid_mentors.empty:
        sheets["invalid_mentors"] = invalid_mentors
    sheets["meta"] = pd.DataFrame([meta_dict])

    write_xlsx_atomic(out_xlsx, sheets)
    logger.info("Matrix written: %s (and CSV: %s)", out_xlsx, out_csv)

    outputs = {"matrix_csv": str(out_csv), "matrix_xlsx": str(out_xlsx)}

    # Optional validation vs StudentReport
    if args.students and args.students.exists():
        checks, breakdown, summary = validate_with_students(
            args.students, matrix, args.schools, args.crosswalk, students_gender_mode=args.students_gender
        )
        out_v = args.outdir / "matrix_vs_students_validation.xlsx"
        write_xlsx_atomic(
            out_v,
            {
                "checks": checks,
                "breakdown": breakdown,
                "summary": pd.DataFrame([summary]),
                "meta": pd.DataFrame([{"students_file": str(args.students), "gender_mode": args.students_gender}]),
            },
        )
        outputs["validation_xlsx"] = str(out_v)
        if summary["unmatched"] > 0:
            cols = ["alias_norm", "mentor_name", "manager", "group_code", "school_code", "reason"]
            unmatched_csv = args.outdir / "unmatched_students.csv"
            checks.loc[~checks["match"], cols].to_csv(unmatched_csv, index=False, encoding="utf-8-sig")
            outputs["unmatched_csv"] = str(unmatched_csv)

    print(
        json.dumps(
            {
                "version": __version__,
                "outputs": outputs,
                "matrix_rows": int(len(matrix)),
                "supporters": int(matrix["پشتیبان"].nunique()) if not matrix.empty else 0,
                "school_based_rows": int((matrix["عادی مدرسه"] == "مدرسه‌ای").sum()) if not matrix.empty else 0,
                "finance_rows": {
                    "0": int((matrix["مالی حکمت بنیاد"] == Finance.NORMAL).sum()) if not matrix.empty else 0,
                    "1": int((matrix["مالی حکمت بنیاد"] == Finance.BONYAD).sum()) if not matrix.empty else 0,
                    "3": int((matrix["مالی حکمت بنیاد"] == Finance.HEKMAT).sum()) if not matrix.empty else 0,
                },
                "removed_mentors": int(len(removed)),
                "r0_skipped": int(validation.iloc[0]["r0_skipped"]),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
