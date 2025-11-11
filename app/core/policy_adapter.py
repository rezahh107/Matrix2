"""آداپتور سبک‌وزن برای دسترسی Policy به‌صورت JSON-first."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Optional

from .policy_loader import PolicyConfig, load_policy


class PolicyAdapter:
    """آداپتور Policy که JSON را به ساختار اجرایی تبدیل می‌کند."""

    def __init__(self, path: str | Path = "config/policy.json"):
        self._path = Path(path)
        self._config: PolicyConfig | None = None
        self._raw: Mapping[str, Any] | None = None

    @property
    def config(self) -> PolicyConfig:
        if self._config is None:
            self._config = load_policy(self._path)
        return self._config

    def stage_column(self, stage: str) -> Optional[str]:
        for stage_def in self.config.trace_stages:
            if stage_def.stage == stage:
                return stage_def.column
        return None

    def aliases(self, namespace: str) -> dict[str, str]:
        return dict(self.config.column_aliases.get(namespace, {}))

    def required_student_fields(self) -> list[str]:
        return list(self.config.required_student_fields)

    def _load_raw(self) -> Mapping[str, Any]:
        if self._raw is None:
            text = self._path.read_text(encoding="utf-8")
            self._raw = json.loads(text)
        return self._raw


POLICY_PATH = Path("config/policy.json")
policy = PolicyAdapter(POLICY_PATH)

__all__ = ["PolicyAdapter", "policy", "POLICY_PATH"]
