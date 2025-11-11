from __future__ import annotations

from pathlib import Path
import sys
from typing import Mapping

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common import columns


class _DummyPolicy:
    def __init__(self, alias_map: Mapping[str, Mapping[str, str]] | None = None):
        self.column_aliases = alias_map or {}


def test_resolve_aliases_uses_policy_overrides(monkeypatch) -> None:
    df = pd.DataFrame({"alias": [1]})
    policy = _DummyPolicy({"inspactor": {"alias": "کد کارمندی پشتیبان"}})
    monkeypatch.setattr(columns, "get_policy", lambda: policy)

    resolved = columns.resolve_aliases(df, "inspactor")

    assert list(resolved.columns) == ["کد کارمندی پشتیبان"]


def test_coerce_semantics_report_builds_required_columns(monkeypatch) -> None:
    monkeypatch.setattr(columns, "get_policy", lambda: _DummyPolicy())
    df = pd.DataFrame(
        {
            "وضعیت تحصیلی": ["فارغ التحصیل"],
            "مرکز ثبت نام": [" مرکز 12 "],
            "کد کارمندی پشتیبان": [4001.0],
            "کد رشته": ["1201"],
            "کد مدرسه": ["5001"],
            "جایگزین": [pd.NA],
        }
    )

    resolved = columns.resolve_aliases(df, "report")
    coerced = columns.coerce_semantics(resolved, "report")

    grad_col = columns.CANON_EN_TO_FA["graduation_status"]
    center_col = columns.CANON_EN_TO_FA["center"]
    mentor_col = columns.CANON_EN_TO_FA["mentor_id"]
    school_col = columns.CANON_EN_TO_FA["school_code"]

    assert str(coerced[grad_col].dtype) in {"Int64", "int64"}
    assert int(coerced.loc[0, grad_col]) == 1
    assert str(coerced[center_col].dtype) in {"Int64", "int64"}
    assert int(coerced.loc[0, center_col]) == 12
    assert coerced.loc[0, mentor_col] == "4001"
    assert coerced[mentor_col].dtype.name in {"object", "string"}
    assert str(coerced[school_col].dtype) in {"Int64", "int64"}


def test_canonicalize_headers_supports_bilingual(monkeypatch) -> None:
    monkeypatch.setattr(columns, "get_policy", lambda: _DummyPolicy())
    df = pd.DataFrame({"کدرشته": [101], "نام مدرسه": ["Sample"]})

    bilingual = columns.canonicalize_headers(df, header_mode="fa_en")
    english = columns.canonicalize_headers(df, header_mode="en")

    assert list(bilingual.columns) == [
        "کدرشته | group_code",
        "نام مدرسه | school_name",
    ]
    assert list(english.columns) == ["group_code", "school_name"]


def test_identifier_columns_do_not_gain_decimal_suffix(monkeypatch) -> None:
    monkeypatch.setattr(columns, "get_policy", lambda: _DummyPolicy())
    df = pd.DataFrame({"کد کارمندی پشتیبان": ["4002.0"]})
    coerced = columns.coerce_semantics(df, "inspactor")

    mentor_col = columns.CANON_EN_TO_FA["mentor_id"]
    assert coerced.loc[0, mentor_col] == "4002"
    assert coerced[mentor_col].dtype.name in {"object", "string"}
