from __future__ import annotations

from typing import Mapping

import pandas as pd

from app.core.common import columns


class _DummyPolicy:
    def __init__(self, alias_map: Mapping[str, Mapping[str, str]]):
        self.column_aliases = alias_map


def test_resolve_aliases_uses_policy_overrides(monkeypatch) -> None:
    df = pd.DataFrame({"alias": [1]})
    policy = _DummyPolicy({"inspactor": {"alias": "کد کارمندی پشتیبان"}})
    monkeypatch.setattr(columns, "get_policy", lambda: policy)

    resolved = columns.resolve_aliases(df, "inspactor")

    assert list(resolved.columns) == ["کد کارمندی پشتیبان"]
