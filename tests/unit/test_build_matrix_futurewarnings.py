import warnings

import pandas as pd

from app.core.build_matrix import (
    CAPACITY_CURRENT_COL,
    CAPACITY_SPECIAL_COL,
    BuildConfig,
    _as_domain_config,
    _explode_rows,
)


def test_explode_rows_school_code_fillna_futurewarning_free() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    base_df = pd.DataFrame(
        {
            "alias_school": ["A"],
            "statuses_school": [[1]],
            "school_codes": [[None, "4002"]],
            "finance": [[0]],
            "group_pairs": [[("گروه", 3001)]],
            "genders": [["1"]],
            "supporter": ["پشتیبان"],
            "mentor_id": ["10"],
            "manager": ["مدیر"],
            "mentor_row_id": [1],
            "center_code": [101],
            "capacity_current": [5],
            "capacity_special": [0],
            "capacity_remaining": [5],
            "center_text": ["مرکز"],
        }
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        result = _explode_rows(
            base_df,
            alias_col="alias_school",
            status_col="statuses_school",
            school_col="school_codes",
            type_label="مدرسه‌ای",
            code_to_name_school={0: "", 4002: "مدرسه"},
            cfg=cfg,
            domain_cfg=domain_cfg,
            cap_current_col=CAPACITY_CURRENT_COL,
            cap_special_col=CAPACITY_SPECIAL_COL,
            remaining_col="remaining_capacity",
            school_code_col="کد مدرسه",
        )

    assert list(result["کد مدرسه"].astype("Int64")) == [0, 4002]
