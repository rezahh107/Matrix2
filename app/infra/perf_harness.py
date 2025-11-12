"""هارنس ساده برای سنجش کارایی تخصیص مطابق سقف بودجه."""

from __future__ import annotations

import argparse
import random
import time
import tracemalloc
from typing import Sequence

import numpy as np
import pandas as pd

from app.core.allocate_students import allocate_batch
from app.core.policy_loader import load_policy


def _generate_students(size: int) -> pd.DataFrame:
    majors = np.random.choice([1201, 1202, 1300], size=size)
    student_ids = [f"STD-{i:05d}" for i in range(size)]
    payload = {
        "student_id": student_ids,
        "کدرشته": majors,
        "گروه آزمایشی": np.where(majors == 1300, "انسانی", "تجربی"),
        "جنسیت": np.random.choice([0, 1], size=size),
        "دانش آموز فارغ": np.random.choice([0, 1], size=size),
        "مرکز گلستان صدرا": np.random.choice([0, 1], size=size),
        "مالی حکمت بنیاد": np.random.choice([0, 1], size=size),
        "کد مدرسه": np.random.randint(1000, 9999, size=size),
    }
    return pd.DataFrame(payload)


def _generate_pool(size: int, *, policy_capacity: int) -> pd.DataFrame:
    mentor_count = max(1, size // 2)
    rows = []
    for idx in range(mentor_count):
        remaining = random.randint(1, policy_capacity)
        rows.append(
            {
                "mentor_name": f"منتور {idx}",
                "alias": 1000 + idx,
                "remaining_capacity": remaining,
                "کدرشته": random.choice([1201, 1202, 1300]),
                "گروه آزمایشی": random.choice(["تجربی", "ریاضی", "انسانی"]),
                "جنسیت": random.choice([0, 1]),
                "دانش آموز فارغ": random.choice([0, 1]),
                "مرکز گلستان صدرا": random.choice([0, 1]),
                "مالی حکمت بنیاد": random.choice([0, 1]),
                "کد مدرسه": random.randint(1000, 9999),
                "کد کارمندی پشتیبان": 1000 + idx,
            }
        )
    return pd.DataFrame(rows)


def _run_once(size: int) -> tuple[float, float]:
    policy = load_policy()
    students = _generate_students(size)
    pool = _generate_pool(size, policy_capacity=5)

    tracemalloc.start()
    start = time.perf_counter()
    allocate_batch(students, pool, policy=policy)
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / (1024 * 1024)
    return elapsed, peak_mb


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Performance harness for allocator")
    parser.add_argument("--size", type=int, default=10000, help="تعداد دانش‌آموزان نمونه")
    parser.add_argument("--budget-seconds", type=float, default=60.0, help="حداکثر زمان مجاز")
    parser.add_argument(
        "--budget-ram-mb",
        type=float,
        default=2048.0,
        help="حداکثر حافظهٔ مجاز (مگابایت)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    elapsed, peak_mb = _run_once(args.size)
    print(f"students={args.size} elapsed={elapsed:.2f}s peak_ram={peak_mb:.1f}MB")

    exit_code = 0
    if elapsed > args.budget_seconds:
        print(f"⚠️  elapsed time {elapsed:.2f}s exceeded budget {args.budget_seconds:.2f}s")
        exit_code = 1
    if peak_mb > args.budget_ram_mb:
        print(f"⚠️  peak memory {peak_mb:.1f}MB exceeded budget {args.budget_ram_mb:.1f}MB")
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
