"""تست‌های پذیرش برای پشتیبانی از کد عددی در expand_group_token."""

from __future__ import annotations

import pandas as pd

from app.core.build_matrix import (
    BuildConfig,
    _as_domain_config,
    _prepare_base_rows,
    COL_MANAGER_NAME,
    COL_MENTOR_ID,
    COL_MENTOR_NAME,
    expand_group_token,
    prepare_crosswalk_mappings,
)


def _sample_crosswalk() -> tuple[dict[str, int], dict[int, str], dict[str, list[tuple[str, int]]], dict[str, str]]:
    crosswalk = pd.DataFrame(
        {"گروه آزمایشی": ["یازدهم ریاضی"], "کد گروه": [27], "مقطع تحصیلی": ["متوسطه دوم"]}
    )
    return prepare_crosswalk_mappings(crosswalk, None)


def test_expand_group_token_supports_numeric_code() -> None:
    """ورودی عددی باید مستقیماً به نام/کد معتبر نگاشت شود."""

    name_to_code, code_to_name, buckets, synonyms = _sample_crosswalk()

    result = expand_group_token("27", name_to_code, code_to_name, buckets, synonyms)

    assert result == [("یازدهم ریاضی", 27)]


def test_prepare_base_rows_accepts_numeric_group_code() -> None:
    """کد عددی در ستون گروه آزمایشی نباید به unseen_groups اضافه شود."""

    name_to_code, code_to_name, buckets, synonyms = _sample_crosswalk()
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    insp = pd.DataFrame(
        {
            COL_MENTOR_ID: ["EMP-1"],
            COL_MENTOR_NAME: ["پشتیبان الف"],
            COL_MANAGER_NAME: ["مدیر الف"],
            "گروه آزمایشی": ["27"],
        }
    )

    base_df, unseen_groups, unmatched_schools = _prepare_base_rows(
        insp,
        cfg=cfg,
        domain_cfg=domain_cfg,
        name_to_code=name_to_code,
        code_to_name=code_to_name,
        buckets=buckets,
        synonyms=synonyms,
        school_name_to_code={},
        code_to_name_school={},
        group_cols=["گروه آزمایشی"],
        school_cols=[],
        gender_col=None,
        included_col=None,
    )

    assert unseen_groups == []
    assert unmatched_schools == []
    assert base_df.iloc[0]["group_pairs"] == [("یازدهم ریاضی", 27)]
