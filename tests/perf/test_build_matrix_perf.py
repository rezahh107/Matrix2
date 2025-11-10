from __future__ import annotations

import os
import time

import numpy as np
import pandas as pd
import pytest

from app.core.build_matrix import BuildConfig, build_matrix


def _synthetic_inputs(size: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(42)
    mentors = pd.DataFrame(
        {
            "نام پشتیبان": [f"پشتیبان {i}" for i in range(size)],
            "نام مدیر": rng.choice(["شهدخت کشاورز", "آینا هوشمند"], size=size),
            "کد کارمندی پشتیبان": [f"EMP-{i:05d}" for i in range(size)],
            "ردیف پشتیبان": np.arange(1, size + 1),
            "گروه آزمایشی": rng.choice(["تجربی", "ریاضی", "انسانی"], size=size),
            "جنسیت": rng.choice(["دختر", "پسر"], size=size),
            "دانش آموز فارغ": rng.integers(0, 2, size=size),
            "کدپستی": rng.choice(["12345", "67890", ""], size=size, p=[0.45, 0.45, 0.10]),
            "تعداد داوطلبان تحت پوشش": rng.integers(0, 10, size=size),
            "تعداد تحت پوشش خاص": rng.integers(10, 20, size=size),
            "نام مدرسه 1": rng.choice(["", "مدرسه نمونه 1", "مدرسه نمونه 2"], size=size, p=[0.6, 0.2, 0.2]),
            "تعداد مدارس تحت پوشش": rng.integers(0, 2, size=size),
            "امکان جذب دانش آموز": ["بلی"] * size,
            "مالی حکمت بنیاد": rng.choice([0, 1, 3], size=size),
            "مرکز گلستان صدرا": [0] * size,
        }
    )

    schools = pd.DataFrame(
        {
            "کد مدرسه": ["5001", "5002"],
            "نام مدرسه 1": ["مدرسه نمونه 1", "مدرسه نمونه 2"],
        }
    )

    crosswalk = pd.DataFrame(
        {
            "گروه آزمایشی": ["تجربی", "ریاضی", "انسانی"],
            "کد گروه": [1201, 2201, 3201],
            "مقطع تحصیلی": ["دهم", "دهم", "دهم"],
        }
    )

    return mentors, schools, crosswalk


@pytest.mark.slow
def test_build_matrix_performance_10k() -> None:
    if os.getenv("PERF") != "1":
        pytest.skip("PERF environment variable not set")

    insp_df, schools_df, crosswalk_df = _synthetic_inputs(10_000)
    cfg = BuildConfig()

    start = time.perf_counter()
    build_matrix(insp_df, schools_df, crosswalk_df, cfg=cfg, progress=lambda *_: None)
    duration = time.perf_counter() - start

    assert duration <= 60.0
