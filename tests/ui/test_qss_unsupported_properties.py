"""تست عدم وجود ویژگی‌های غیرپشتیبانی‌شده در QSS."""

from __future__ import annotations

from pathlib import Path

FORBIDDEN = ["transform", "transition", "box-shadow"]


def test_qss_has_no_unsupported_properties() -> None:
    qss_path = Path("app/ui/styles.qss")
    content = qss_path.read_text(encoding="utf-8").lower()
    for prop in FORBIDDEN:
        assert prop not in content, f"unsupported property found: {prop}"
