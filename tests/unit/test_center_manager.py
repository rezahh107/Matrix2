from __future__ import annotations

from dataclasses import replace

import pytest

from app.core.center_manager import (
    resolve_center_manager_config,
    validate_center_config,
)
from app.core.policy_loader import CenterConfig, load_policy


class TestCenterManager:
    """تست‌های ماژول مدیریت مراکز."""

    @pytest.fixture()
    def sample_policy(self):
        policy = load_policy()
        centers = (
            CenterConfig(1, "گلستان", "شهدخت کشاورز"),
            CenterConfig(2, "صدرا", "آیناز هوشمند"),
            CenterConfig(0, "مرکزی", None),
        )
        center_management = replace(
            policy.center_management,
            enabled=True,
            centers=centers,
            priority_order=(1, 2, 0),
            strict_manager_validation=False,
        )
        return replace(policy, center_management=center_management)

    def test_resolve_center_manager_config_basic(self, sample_policy):
        managers, priority = resolve_center_manager_config(policy=sample_policy)

        assert managers == {1: ["شهدخت کشاورز"], 2: ["آیناز هوشمند"]}
        assert priority == [1, 2, 0]

    def test_resolve_center_manager_config_ui_override(self, sample_policy):
        managers, _ = resolve_center_manager_config(
            policy=sample_policy,
            ui_managers={1: ["مدیر جدید از UI"]},
        )

        assert managers[1] == ["مدیر جدید از UI"]
        assert managers[2] == ["آیناز هوشمند"]

    def test_resolve_center_manager_config_cli_override(self, sample_policy):
        managers, _ = resolve_center_manager_config(
            policy=sample_policy,
            cli_managers={2: ["مدیر جدید از CLI"]},
        )

        assert managers[1] == ["شهدخت کشاورز"]
        assert managers[2] == ["مدیر جدید از CLI"]

    def test_resolve_center_manager_config_priority_override(self, sample_policy):
        _, priority = resolve_center_manager_config(
            policy=sample_policy,
            cli_priority=[2, 1, 0],
        )

        assert priority == [2, 1, 0]

    def test_resolve_center_manager_config_strict_validation(self, sample_policy):
        centers = (
            CenterConfig(1, "گلستان", None),
            CenterConfig(2, "صدرا", "آیناز هوشمند"),
        )
        strict_policy = replace(
            sample_policy,
            center_management=replace(
                sample_policy.center_management,
                centers=centers,
                strict_manager_validation=True,
            ),
        )

        with pytest.raises(ValueError):
            resolve_center_manager_config(policy=strict_policy)

    def test_validate_center_config_valid(self, sample_policy):
        warnings = validate_center_config(
            sample_policy,
            {1: ["مدیر ۱"], 2: ["مدیر ۲"]},
            [1, 2, 0],
        )

        assert warnings == []

    def test_validate_center_config_undefined_center(self, sample_policy):
        warnings = validate_center_config(
            sample_policy,
            {1: ["مدیر ۱"], 99: ["مدیر ناموجود"]},
            [1, 2, 0],
        )

        assert any("مرکز 99" in warning for warning in warnings)
