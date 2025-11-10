"""تست واحد برای اطمینان از اعتبارسنجی و بارگذاری سیاست."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import warnings

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
        "ranking": [
            "min_occupancy_ratio",
            "min_allocations_new",
            "min_mentor_id",
        ],
    }


def test_missing_keys() -> None:
    payload = _valid_payload()
    payload.pop("ranking")
    with pytest.raises(ValueError, match="Policy keys missing"):
        parse_policy_dict(payload)


def test_join_keys_constraints() -> None:
    payload = _valid_payload()
    payload["join_keys"] = ["a", "a", "b", "c", "d", "e"]
    with pytest.raises(ValueError, match="join_keys must be unique"):
        parse_policy_dict(payload)


def test_ranking_constraints() -> None:
    payload = _valid_payload()
    payload["ranking"] = ["r1", "r1", "r2"]
    with pytest.raises(ValueError, match="ranking must be unique"):
        parse_policy_dict(payload)


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


def test_load_policy_reads_default_config(tmp_path: Path) -> None:
    config_path = tmp_path / "policy.json"
    config_path.write_text(
        json.dumps(_valid_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    policy = load_policy(config_path, expected_version="1.0.3")

    assert isinstance(policy, PolicyConfig)
    assert len(policy.join_keys) == 6
    assert load_policy(config_path, expected_version="1.0.3") is policy
