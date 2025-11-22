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
    """ایجاد دیتافریم خالی استخر پشتیبان‌ها با ۶ کلید اتصال و ستون کد کارمندی.

    در این دیتافریم همهٔ کلیدهای اتصال از نوع ``Int64`` هستند و ستون «کد کارمندی پشتیبان» از نوع
    ``string`` تعریف می‌شود تا بررسی آستانهٔ تکرار کلیدها بدون هشدار dtype انجام شود.

    مثال ساده:
        >>> df = make_empty_pool_with_join_keys()
        >>> list(df.columns)
        ['کدرشته', 'جنسیت', 'دانش آموز فارغ', 'مرکز گلستان صدرا', 'مالی حکمت بنیاد', 'کد مدرسه', 'کد کارمندی پشتیبان']
    """

    payload = {key: pd.Series([], dtype="Int64") for key in join_keys}
    payload["کد کارمندی پشتیبان"] = pd.Series([], dtype="string")
    return pd.DataFrame(payload)


@pytest.fixture
def mentor_pool_empty() -> pd.DataFrame:
    """فیکسچر دیتافریم خالی استخر پشتیبان با کلیدهای اتصال و ستون کد کارمندی.

    این فیکسچر برای مسیرهای خطا/ورودی خالی در CLI استفاده می‌شود و dtype کلیدها ``Int64`` است.
    """

    return make_empty_pool_with_join_keys()


@pytest.fixture
def mentor_pool_with_duplicates() -> pd.DataFrame:
    """فیکسچر استخر پشتیبان با ردیف‌های تکراری روی ۶ کلید اتصال.

    هر شش کلید join با dtype ``Int64`` مقدار برابر دارند و ستون «کد کارمندی پشتیبان» رشته‌ای است.

    مثال:
        >>> df = mentor_pool_with_duplicates()
        >>> df["کد کارمندی پشتیبان"].tolist()
        ['E1', 'E1', 'E2']
    """

    data = {key: pd.Series([1, 1, 1], dtype="Int64") for key in JOIN_KEYS_6}
    data["کد کارمندی پشتیبان"] = pd.Series(["E1", "E1", "E2"], dtype="string")
    return pd.DataFrame(data)
