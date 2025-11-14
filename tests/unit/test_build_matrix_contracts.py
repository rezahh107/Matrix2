from __future__ import annotations

import pandas as pd

from app.core.build_matrix import BuildConfig, _validate_alias_contract, _validate_school_code_contract


def test_validate_alias_contract_handles_duplicate_columns() -> None:
    cfg = BuildConfig()
    matrix = pd.DataFrame(
        {
            "جایگزین": ["1234", "EMP-2"],
            "کد کارمندی پشتیبان": ["EMP-1", "EMP-2"],
            "عادی مدرسه": ["عادی", "مدرسه‌ای"],
        }
    )
    matrix.insert(0, "alias_copy", matrix["جایگزین"])
    matrix.columns = ["جایگزین", "جایگزین", "کد کارمندی پشتیبان", "عادی مدرسه"]

    _validate_alias_contract(matrix, cfg=cfg)


def test_validate_school_code_contract_handles_duplicate_row_types() -> None:
    matrix = pd.DataFrame(
        {
            "عادی مدرسه": ["مدرسه‌ای", "عادی"],
            "کد مدرسه": [5678, 0],
        }
    )
    matrix.insert(0, "row_type_copy", matrix["عادی مدرسه"])
    matrix.columns = ["عادی مدرسه", "عادی مدرسه", "کد مدرسه"]

    _validate_school_code_contract(matrix, school_code_col="کد مدرسه")
