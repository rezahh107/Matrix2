"""تست واحد برای اطمینان از اعتبارسنجی و بارگذاری سیاست."""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.policy_loader import (
    PolicyConfig,
    load_policy,
    parse_policy_dict,
)


@pytest.fixture(autouse=True)
def _clear_policy_cache() -> None:
    """پیش و پس از هر تست کش لود سیاست پاک شود."""

    load_policy.cache_clear()
    yield
    load_policy.cache_clear()


def _valid_payload() -> dict[str, object]:
    return {
        "version": "1.0.3",
        "normal_statuses": [1, 0],
        "school_statuses": [1],
        "join_keys": [
            "کدرشته",
            "جنسیت",
            "دانش آموز فارغ",
            "مرکز گلستان صدرا",
            "مالی حکمت بنیاد",
            "کد مدرسه",
        ],
        "ranking_rules": [
            {"name": "min_occupancy_ratio", "column": "occupancy_ratio", "ascending": True},
            {"name": "min_allocations_new", "column": "allocations_new", "ascending": True},
            {"name": "min_mentor_id", "column": "mentor_sort_key", "ascending": True},
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
    }


def test_missing_keys() -> None:
    payload = _valid_payload()
    payload.pop("ranking_rules")
    with pytest.raises(ValueError, match="Policy keys missing"):
        parse_policy_dict(payload)


def test_join_keys_constraints() -> None:
    payload = _valid_payload()
    payload["join_keys"] = ["a", "a", "b", "c", "d", "e"]
    with pytest.raises(ValueError, match="join_keys must be unique"):
        parse_policy_dict(payload)


def test_ranking_constraints() -> None:
    payload = _valid_payload()
    payload["ranking_rules"] = [
        {"name": "dup", "column": "occupancy_ratio", "ascending": True},
        {"name": "dup", "column": "allocations_new", "ascending": True},
        {"name": "ok", "column": "mentor_sort_key", "ascending": True},
    ]
    with pytest.raises(ValueError, match="ranking items must be unique"):
        parse_policy_dict(payload)


def test_ranking_must_have_three_items() -> None:
    payload = _valid_payload()
    payload["ranking_rules"].append(
        {"name": "extra", "column": "mentor_extra", "ascending": True}
    )
    with pytest.raises(ValueError, match="ranking must contain exactly 3 items"):
        parse_policy_dict(payload)


def test_trace_stage_order_enforced() -> None:
    payload = _valid_payload()
    payload["trace_stages"][1]["stage"] = "gender"
    with pytest.raises(ValueError, match="Trace stage order mismatch"):
        parse_policy_dict(payload)


def test_trace_stage_missing_stage_raises() -> None:
    payload = _valid_payload()
    payload["trace_stages"].pop()
    with pytest.raises(ValueError, match="exactly eight stages"):
        parse_policy_dict(payload)


def test_trace_stage_extra_stage_raises() -> None:
    payload = _valid_payload()
    payload["trace_stages"].append({"stage": "extra", "column": "x"})
    with pytest.raises(ValueError, match="exactly eight stages"):
        parse_policy_dict(payload)


def test_trace_stage_defaults_when_missing() -> None:
    payload = _valid_payload()
    payload.pop("trace_stages")
    policy = parse_policy_dict(payload)

    assert [stage.stage for stage in policy.trace_stages] == [
        "type",
        "group",
        "gender",
        "graduation_status",
        "center",
        "finance",
        "school",
        "capacity_gate",
    ]


def test_status_type_validation() -> None:
    payload = _valid_payload()
    payload["normal_statuses"] = [1, "bad"]  # type: ignore[list-item]
    with pytest.raises(TypeError, match="All normal_statuses items must be int"):
        parse_policy_dict(payload)


def test_version_mismatch_raise() -> None:
    payload = _valid_payload()
    payload["version"] = "1.0.2"
    with pytest.raises(ValueError, match="Policy version mismatch"):
        parse_policy_dict(payload, expected_version="1.0.3", on_version_mismatch="raise")


def test_version_mismatch_warn() -> None:
    payload = _valid_payload()
    payload["version"] = "1.0.2"
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        config = parse_policy_dict(
            payload,
            expected_version="1.0.3",
            on_version_mismatch="warn",
        )
        assert isinstance(config, PolicyConfig)
        assert any("Policy version mismatch" in str(item.message) for item in captured)


def test_version_major_mismatch_always_raises() -> None:
    payload = _valid_payload()
    payload["version"] = "2.0.0"
    with pytest.raises(ValueError, match="major incompatible"):
        parse_policy_dict(payload, expected_version="1.0.3", on_version_mismatch="warn")


def test_ranking_legacy_strings_supported() -> None:
    legacy_payload = _valid_payload()
    legacy_payload.pop("trace_stages")
    legacy_payload["ranking"] = [
        "min_occupancy_ratio",
        "min_allocations_new",
        "min_mentor_id",
    ]
    legacy_payload.pop("ranking_rules")

    policy = parse_policy_dict(legacy_payload)
    assert [rule.name for rule in policy.ranking_rules] == [
        "min_occupancy_ratio",
        "min_allocations_new",
        "min_mentor_id",
    ]


def test_load_policy_reads_default_config(tmp_path: Path) -> None:
    config_path = tmp_path / "policy.json"
    config_path.write_text(
        json.dumps(_valid_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    policy = load_policy(config_path)

    assert isinstance(policy, PolicyConfig)
    assert len(policy.join_keys) == 6
    assert policy.ranking == [
        "min_occupancy_ratio",
        "min_allocations_new",
        "min_mentor_id",
    ]
    assert policy.capacity_column == "remaining_capacity"
    assert load_policy(config_path) is policy


def test_load_policy_cache_invalidates_on_mtime(tmp_path: Path) -> None:
    config_path = tmp_path / "policy.json"
    config_path.write_text(json.dumps(_valid_payload(), ensure_ascii=False), encoding="utf-8")

    first = load_policy(config_path)
    payload = _valid_payload()
    payload["normal_statuses"] = [1, 1]
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    second = load_policy(config_path)
    assert first is not second
    assert second.normal_statuses == [1, 1]
