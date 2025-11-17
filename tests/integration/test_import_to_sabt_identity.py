from __future__ import annotations

import pandas as pd

from app.core.allocate_students import allocate_batch, build_selection_reason_rows
from app.core.policy_loader import load_policy
from app.infra.excel.export_allocations import AllocationExportColumn, build_sabt_export_frame


def test_allocations_sabt_and_reasons_preserve_identity() -> None:
    policy = load_policy()
    students = pd.DataFrame(
        [
            {
                "student_id": 101,
                policy.stage_column("group"): 10,
                policy.stage_column("gender"): 0,
                policy.stage_column("graduation_status"): 0,
                policy.stage_column("center"): 1,
                policy.stage_column("finance"): 1,
                policy.stage_column("school"): 1112,
                "student_national_code": "1234567890",
                "student_educational_status": 1,
                "student_registration_status": 3,
                "first_name": "Sara",
                "family_name": "Ahmadi",
            }
        ]
    )
    pool = pd.DataFrame(
        {
            policy.stage_column("group"): [10],
            policy.stage_column("gender"): [0],
            policy.stage_column("graduation_status"): [0],
            policy.stage_column("center"): [1],
            policy.stage_column("finance"): [1],
            policy.stage_column("school"): [1112],
            policy.columns.remaining_capacity: [1],
            "mentor_id": ["EMP-7"],
            "پشتیبان": ["Mentor"],
        }
    )

    allocations, _, logs, trace = allocate_batch(students, pool, policy=policy)
    summary_df = trace.attrs.get("summary_df")

    profile = [
        AllocationExportColumn(
            key="student_id",
            header="کد ثبت نام",
            source_kind="allocation",
            source_field="student_id",
            literal_value=None,
            order=1,
        ),
        AllocationExportColumn(
            key="student_educational_status",
            header="وضعیت تحصیلی",
            source_kind="student",
            source_field="student_educational_status",
            literal_value=None,
            order=2,
        ),
    ]

    sabt_df = build_sabt_export_frame(
        allocations,
        students,
        profile,
        summary_df=summary_df,
    )

    assert sabt_df.shape[0] == allocations.shape[0]
    assert sabt_df["وضعیت تحصیلی"].notna().all()

    reasons = build_selection_reason_rows(
        allocations,
        students,
        pool,
        policy=policy,
        logs=logs,
        trace=trace,
    )
    assert {"student_id", "کدملی", "نام", "نام خانوادگی"}.issubset(reasons.columns)
    joined = sabt_df.merge(
        reasons,
        left_on="کد ثبت نام",
        right_on="student_id",
        how="left",
        validate="one_to_one",
    )
    assert joined["کدملی"].notna().all()
    assert joined["نام"].iloc[0] == "Sara"
    assert joined["نام خانوادگی"].iloc[0] == "Ahmadi"
