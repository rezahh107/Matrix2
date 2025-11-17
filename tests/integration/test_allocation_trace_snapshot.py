from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd
import pytest

from app.core.common.reasons import ReasonCode
from app.core.common.trace import build_allocation_trace
from app.core.policy_loader import PolicyConfig, load_policy

SNAPSHOT_PATH = Path(__file__).resolve().parents[1] / "snapshots" / "allocation_trace_snapshot.json"
CANONICAL_TRACE_ORDER: tuple[str, ...] = (
    "type",
    "group",
    "gender",
    "graduation_status",
    "center",
    "finance",
    "school",
    "capacity_gate",
)


def _build_focused_fixture() -> tuple[PolicyConfig, pd.DataFrame, dict[str, dict[str, object]]]:
    policy = load_policy()
    pool = pd.DataFrame(
        {
            "پشتیبان": ["مهسا", "علیرضا", "طاها"],
            "کدرشته": [1201, 1201, 5000],
            "گروه آزمایشی": ["تجربی", "تجربی", "ریاضی"],
            "جنسیت": [
                policy.gender_codes.female.value,
                policy.gender_codes.female.value,
                policy.gender_codes.male.value,
            ],
            "دانش آموز فارغ": [0, 0, 0],
            "مرکز گلستان صدرا": [1, 2, 1],
            "مالی حکمت بنیاد": [0, 1, 0],
            "کد مدرسه": [2001, 2001, 2001],
            "remaining_capacity": [2, 1, 0],
        }
    )
    students = {
        "pass": {
            "student_id": "STD-PASS",
            "کدرشته": 1201,
            "گروه آزمایشی": "تجربی",
            "جنسیت": policy.gender_codes.female.value,
            "دانش آموز فارغ": 0,
            "مرکز گلستان صدرا": 1,
            "مالی حکمت بنیاد": 0,
            "کد مدرسه": 2001,
            "school_code_raw": "2001",
        },
        "fail": {
            "student_id": "STD-FAIL",
            "کدرشته": 9999,
            "گروه آزمایشی": "ریاضی",
            "جنسیت": policy.gender_codes.male.value,
            "دانش آموز فارغ": 1,
            "مرکز گلستان صدرا": 3,
            "مالی حکمت بنیاد": 3,
            "کد مدرسه": 9999,
            "school_code_raw": "9999",
        },
    }
    return policy, pool, students


@pytest.fixture()
def allocation_trace_fixture() -> tuple[PolicyConfig, pd.DataFrame, dict[str, dict[str, object]]]:
    return _build_focused_fixture()


def _ordered_trace(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    order = {stage: idx for idx, stage in enumerate(CANONICAL_TRACE_ORDER)}
    return sorted(records, key=lambda item: order[item["stage"]])


def _normalize_extras(extras: dict[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in sorted(extras.items()):
        if isinstance(value, dict):
            normalized[key] = {inner_key: value[inner_key] for inner_key in sorted(value)}
        else:
            normalized[key] = value
    return normalized


def _record_trace(student: dict[str, object], pool: pd.DataFrame, policy: PolicyConfig) -> list[dict[str, object]]:
    trace = build_allocation_trace(student, pool, policy=policy)
    serialized: list[dict[str, object]] = []
    for record in trace:
        extras = dict(record.get("extras") or {})
        serialized.append(
            {
                "stage": record["stage"],
                "column": record["column"],
                "expected_value": record.get("expected_value"),
                "total_before": int(record["total_before"]),
                "total_after": int(record["total_after"]),
                "matched": bool(record["matched"]),
                "expected_op": record.get("expected_op"),
                "expected_threshold": record.get("expected_threshold"),
                "extras": _normalize_extras(extras),
            }
        )
    return _ordered_trace(serialized)


def _build_snapshot_payload(policy: PolicyConfig, pool: pd.DataFrame, students: dict[str, dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    return {
        "pass": _record_trace(students["pass"], pool, policy),
        "fail": _record_trace(students["fail"], pool, policy),
    }


def test_allocation_trace_snapshot(allocation_trace_fixture: tuple[PolicyConfig, pd.DataFrame, dict[str, dict[str, object]]]) -> None:
    policy, pool, students = allocation_trace_fixture
    snapshot = _build_snapshot_payload(policy, pool, students)

    pass_codes = [
        ReasonCode(entry["extras"]["rule_reason_code"]) for entry in snapshot["pass"]
    ]
    assert pass_codes == [ReasonCode.OK] * len(CANONICAL_TRACE_ORDER)

    fail_codes = [
        ReasonCode(entry["extras"]["rule_reason_code"]) for entry in snapshot["fail"]
    ]
    assert fail_codes == [
        ReasonCode.TYPE_MISMATCH,
        ReasonCode.GROUP_MISMATCH,
        ReasonCode.GENDER_MISMATCH,
        ReasonCode.GRADUATION_STATUS_MISMATCH,
        ReasonCode.CENTER_MISMATCH,
        ReasonCode.FINANCE_MISMATCH,
        ReasonCode.SCHOOL_STATUS_MISMATCH,
        ReasonCode.CAPACITY_FULL,
    ]

    recorded = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    assert snapshot == recorded


def _write_snapshot(path: Path = SNAPSHOT_PATH) -> None:
    policy, pool, students = _build_focused_fixture()
    payload = _build_snapshot_payload(policy, pool, students)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover - ابزار به‌روزرسانی snapshot
    _write_snapshot()
