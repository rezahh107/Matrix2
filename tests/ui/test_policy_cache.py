from __future__ import annotations

import types

from app.ui.policy_cache import get_cached_policy, invalidate_policy_cache


def test_policy_cache_memoizes(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_loader():
        calls["count"] += 1
        return "policy"

    monkeypatch.setattr("app.ui.policy_cache.load_policy", fake_loader)
    invalidate_policy_cache()

    first = get_cached_policy()
    second = get_cached_policy()

    assert first == "policy"
    assert second == "policy"
    assert calls["count"] == 1


def test_policy_cache_invalidate(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_loader():
        calls["count"] += 1
        return types.SimpleNamespace(version="test")

    monkeypatch.setattr("app.ui.policy_cache.load_policy", fake_loader)
    invalidate_policy_cache()

    get_cached_policy()
    invalidate_policy_cache()
    get_cached_policy()

    assert calls["count"] == 2
