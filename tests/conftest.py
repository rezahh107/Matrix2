from __future__ import annotations

from typing import Sequence

import pandas as pd
import pytest

JOIN_KEYS_6: list[str] = [
    "کدرشته",
    "جنسیت",
    "دانش آموز فارغ",
    "مرکز گلستان صدرا",
    "مالی حکمت بنیاد",
    "کد مدرسه",
]


def make_empty_pool_with_join_keys(join_keys: Sequence[str] = JOIN_KEYS_6) -> pd.DataFrame:
    """ایجاد دیتافریم خالی استخر پشتیبان‌ها با کلیدهای اتصال و ستون کد کارمندی.

    مثال ساده:
        >>> df = make_empty_pool_with_join_keys()
        >>> list(df.columns)[-1]
        'کد کارمندی پشتیبان'
    """

    payload = {key: pd.Series([], dtype="Int64") for key in join_keys}
    payload["کد کارمندی پشتیبان"] = pd.Series([], dtype="string")
    return pd.DataFrame(payload)


@pytest.fixture
def mentor_pool_empty() -> pd.DataFrame:
    """فیکسچر دیتافریم خالی استخر پشتیبان‌ها با ۶ کلید اتصال."""

    return make_empty_pool_with_join_keys()


@pytest.fixture
def mentor_pool_with_duplicates() -> pd.DataFrame:
    """فیکسچر استخر پشتیبان با دو ردیف تکراری روی ۶ کلید اتصال.

    مثال:
        >>> df = mentor_pool_with_duplicates()
        >>> df.shape[0]
        2
    """

    data = {key: pd.Series([1, 1], dtype="Int64") for key in JOIN_KEYS_6}
    data["کد کارمندی پشتیبان"] = pd.Series(["E1", "E1"], dtype="string")
    return pd.DataFrame(data)
