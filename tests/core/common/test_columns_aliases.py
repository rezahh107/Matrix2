from __future__ import annotations

import pandas as pd

from app.core.common.columns import canonicalize_headers, resolve_aliases


def test_resolve_aliases_maps_persian_manager_header_to_manager_name() -> None:
    df = pd.DataFrame({"مدیر": ["علی", "زهرا"]})

    resolved = resolve_aliases(df, source="matrix")
    canonical = canonicalize_headers(resolved, header_mode="en")

    assert "manager_name" in canonical.columns
    assert canonical["manager_name"].tolist() == ["علی", "زهرا"]
