from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.policy_loader import load_policy, parse_policy_dict


def _legacy_payload(base_path: Path) -> dict[str, object]:
    payload = json.loads((base_path / "config" / "policy.json").read_text(encoding="utf-8"))
    payload["version"] = "1.0.2"
    payload.pop("virtual_alias_ranges", None)
    payload.pop("virtual_name_patterns", None)
    excel = payload.get("excel", {})
    excel.pop("header_mode_internal", None)
    excel.pop("header_mode_write", None)
    payload["excel"] = excel
    return payload


def test_parse_policy_migrate_injects_defaults(tmp_path: Path) -> None:
    payload = _legacy_payload(Path(__file__).resolve().parents[2])
    with pytest.warns(RuntimeWarning, match="migrated in-memory"):
        config = parse_policy_dict(
            payload,
            expected_version="1.0.3",
            on_version_mismatch="migrate",
        )

    assert config.version == "1.0.3"
    assert config.virtual_alias_ranges == ((7000, 7999),)
    assert "fa_en" in {config.excel.header_mode_internal, config.excel.header_mode_write}


def test_load_policy_respects_migration_mode(tmp_path: Path) -> None:
    payload = _legacy_payload(Path(__file__).resolve().parents[2])
    legacy_path = tmp_path / "legacy-policy.json"
    legacy_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        load_policy(legacy_path, expected_version="1.0.3", on_version_mismatch="raise")

    config = load_policy(legacy_path, expected_version="1.0.3", on_version_mismatch="migrate")
    assert config.version == "1.0.3"
