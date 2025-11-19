from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.core.allocation.channels import (
    AllocationChannel,
    derive_allocation_channel,
    derive_channels_for_students,
)
from app.core.allocation.trace import attach_allocation_channel
from app.core.policy.config import AllocationChannelConfig
from app.core.policy_loader import parse_policy_dict


@dataclass(frozen=True)
class _Columns:
    school_code: str


class _FakePolicy:
    """پیکربندی حداقلی برای تست منطق کانال."""

    def __init__(self) -> None:
        self.columns = _Columns(school_code="کد مدرسه")
        self._stage_columns = {"center": "مرکز گلستان صدرا"}
        self.allocation_channels = AllocationChannelConfig(
            school_codes=(10,),
            center_channels={"GOLESTAN": (1,), "SADRA": (2,)},
            registration_center_column="registration_center",
            educational_status_column="student_educational_status",
            active_status_values=(0,),
        )

    def stage_column(self, stage: str) -> str:
        return self._stage_columns[stage]


def _policy_payload_with_channels() -> dict[str, object]:
    payload = {
        "version": "1.0.3",
        "normal_statuses": [0, 1],
        "school_statuses": [0],
        "postal_valid_range": [1000, 9999],
        "finance_variants": [0, 1, 3],
        "center_map": {"شهدخت کشاورز": 1, "آیناز هوشمند": 2, "*": 0},
        "school_code_empty_as_zero": True,
        "prefer_major_code": True,
        "coverage_threshold": 0.95,
        "dedup_removed_ratio_threshold": 0.05,
        "school_lookup_mismatch_threshold": 0.0,
        "alias_rule": {
            "normal": "postal_or_fallback_mentor_id",
            "school": "mentor_id",
        },
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "gender_codes": {
            "male": {"value": 1, "counter_code": "357"},
            "female": {"value": 0, "counter_code": "373"},
        },
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio"},
            {
                "name": "max_remaining_capacity",
                "column": "remaining_capacity_desc",
            },
            {"name": "min_allocations_new", "column": "allocations_new"},
            {"name": "min_mentor_id", "column": "mentor_sort_key"},
        ],
        "trace_stages": [
            {"stage": "type", "column": "کدرشته"},
            {"stage": "group", "column": "گروه آزمایشی"},
            {"stage": "gender", "column": "جنسیت"},
            {"stage": "graduation_status", "column": "دانش آموز فارغ"},
            {"stage": "center", "column": "مرکز گلستان صدرا"},
            {"stage": "finance", "column": "مالی حکمت بنیاد"},
            {"stage": "school", "column": "کد مدرسه"},
            {"stage": "capacity_gate", "column": "remaining_capacity"},
        ],
        "columns": {
            "postal_code": "کدپستی",
            "school_count": "تعداد مدارس تحت پوشش",
            "school_code": "کد مدرسه",
            "capacity_current": "تعداد داوطلبان تحت پوشش",
            "capacity_special": "تعداد تحت پوشش خاص",
            "remaining_capacity": "remaining_capacity",
        },
        "virtual_alias_ranges": [[7000, 7999]],
        "virtual_name_patterns": ["در\\s+انتظار\\s+تخصیص"],
        "excel": {
            "rtl": True,
            "font_name": "Tahoma",
            "font_size": 8,
            "header_mode_internal": "en",
            "header_mode_write": "fa_en",
        },
        "allocation_channels": {
            "school_codes": [10],
            "center_channels": {"GOLESTAN": [1], "SADRA": [2]},
            "registration_center_column": "registration_center",
            "educational_status_column": "student_educational_status",
            "active_status_values": [0],
        },
    }
    return payload


def test_channel_derivation_vectorized_matches_rowwise() -> None:
    policy = _FakePolicy()
    students = pd.DataFrame(
        [
            {
                "student_id": 1,
                policy.columns.school_code: 10,
                policy.stage_column("center"): 0,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 2,
                policy.columns.school_code: 0,
                policy.stage_column("center"): 1,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 3,
                policy.columns.school_code: 0,
                policy.stage_column("center"): 2,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 4,
                policy.columns.school_code: 0,
                policy.stage_column("center"): 0,
                "registration_center": 99,
                "student_educational_status": 1,
            },
        ]
    )

    vectorized = derive_channels_for_students(students, policy)
    assert list(vectorized) == [
        AllocationChannel.SCHOOL,
        AllocationChannel.GOLESTAN,
        AllocationChannel.SADRA,
        AllocationChannel.GENERIC,
    ]
    rowwise = [derive_allocation_channel(row, policy) for _, row in students.iterrows()]
    assert rowwise == list(vectorized)
    again = derive_channels_for_students(students, policy)
    pd.testing.assert_series_equal(vectorized, again)


def test_attach_allocation_channel_with_real_policy() -> None:
    policy = parse_policy_dict(_policy_payload_with_channels())
    center_column = policy.stage_column("center")
    students = pd.DataFrame(
        [
            {
                "student_id": 1,
                policy.columns.school_code: 10,
                center_column: 0,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 2,
                policy.columns.school_code: 0,
                center_column: 1,
                "registration_center": 0,
                "student_educational_status": 0,
            },
            {
                "student_id": 3,
                policy.columns.school_code: 0,
                center_column: 0,
                "registration_center": 2,
                "student_educational_status": 0,
            },
        ]
    )
    summary = pd.DataFrame(
        [
            {"student_id": 1, "final_status": "ALLOCATED"},
            {"student_id": 2, "final_status": "ALLOCATED"},
            {"student_id": 3, "final_status": "ALLOCATED"},
        ]
    )

    enriched = attach_allocation_channel(summary, students, policy=policy)
    assert "allocation_channel" in enriched.columns
    assert enriched["allocation_channel"].tolist() == [
        AllocationChannel.SCHOOL.value,
        AllocationChannel.GOLESTAN.value,
        AllocationChannel.SADRA.value,
    ]
