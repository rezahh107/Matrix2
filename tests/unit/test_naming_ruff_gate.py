from __future__ import annotations

import tomllib
from pathlib import Path

import pytest


def test_pyproject_has_incremental_ruff_rules() -> None:
    config = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    ruff_cfg = config.get("tool", {}).get("ruff", {})
    lint_cfg = ruff_cfg.get("lint", {})
    select = lint_cfg.get("select", [])
    assert select == ["E", "F", "I", "N", "UP", "SIM"], select
    ignore = lint_cfg.get("ignore", [])
    assert ignore == ["E501", "D"], ignore
    assert "extend-exclude" in ruff_cfg, "extend-exclude must limit legacy paths"


def _parse_hook_file_map() -> dict[str, str]:
    hooks: dict[str, str] = {}
    current_id: str | None = None
    for raw_line in Path(".pre-commit-config.yaml").read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("- id:"):
            current_id = stripped.split(":", 1)[1].strip()
        elif current_id and stripped.startswith("files:"):
            hooks[current_id] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("repo:"):
            current_id = None
    return hooks


@pytest.mark.parametrize("hook_id", ["ruff", "ruff-format", "black"])
def test_precommit_scopes_changed_files(hook_id: str) -> None:
    hook_files = _parse_hook_file_map()
    assert hook_id in hook_files, f"Hook {hook_id} missing from pre-commit configuration"
    assert hook_files[hook_id] == "^(app/|tests/)", hook_files[hook_id]
