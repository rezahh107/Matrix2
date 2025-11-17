from __future__ import annotations

from typing import Sequence

import pytest

from app.core.common.policy import load_selection_reason_policy
from app.core.policy import compute_schema_hash, validate_policy_columns


def test_validate_policy_columns_reject_reserved_and_duplicates() -> None:
    with pytest.raises(ValueError):
        validate_policy_columns(["نام", "نام"])
    with pytest.raises(ValueError):
        validate_policy_columns(["__hidden"])
    with pytest.raises(ValueError):
        validate_policy_columns(["Unnamed: 0"])
    with pytest.raises(ValueError):
        validate_policy_columns(["index"])


def test_compute_schema_hash_stable() -> None:
    columns: Sequence[str] = ("ستون۱", "ستون۲", "ستون۳")
    first = compute_schema_hash(columns)
    second = compute_schema_hash(list(columns))
    assert first == second
    assert len(first) == 40


def test_policy_loader_handles_version_mismatch_modes() -> None:
    legacy_policy = {
        "version": "1.0.2",
        "emission": {"selection_reasons": {"sheet_name": "x"}},
    }

    with pytest.raises(ValueError):
        load_selection_reason_policy(legacy_policy, expected_version="1.0.3", on_mismatch="raise")

    with pytest.warns(RuntimeWarning):
        config_warn = load_selection_reason_policy(
            legacy_policy, expected_version="1.0.3", on_mismatch="warn"
        )
    assert config_warn.version == "1.0.2"

    with pytest.warns(RuntimeWarning):
        config_migrate = load_selection_reason_policy(
            legacy_policy, expected_version="1.0.3", on_mismatch="migrate"
        )
    assert config_migrate.version == "1.0.3"
    assert config_migrate.columns == config_warn.columns
    assert config_migrate.schema_hash == compute_schema_hash(config_migrate.columns)
