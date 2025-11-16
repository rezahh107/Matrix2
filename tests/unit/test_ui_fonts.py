"""تست واحد برای منطق کمکی فونت رابط کاربری."""

from __future__ import annotations

import base64

import pytest

from app.ui import fonts


@pytest.fixture(autouse=True)
def _reset_policy_cache():
    """پاک‌سازی کش فونت Policy قبل از هر تست."""

    fonts._policy_font_name.cache_clear()
    fonts._policy_font_size.cache_clear()
    yield
    fonts._policy_font_name.cache_clear()
    fonts._policy_font_size.cache_clear()


def test_dedupe_preserve_order_basic() -> None:
    """لیست ورودی باید بدون تکرار و با حفظ ترتیب پاک‌سازی شود."""

    items = ["Vazir", "vazirmatn", "", "Vazir", "Arial"]
    assert fonts._dedupe_preserve_order(items) == ["Vazir", "vazirmatn", "Arial"]


def test_iter_bundled_font_payloads_returns_valid_base64() -> None:
    """دادهٔ base64 فونت باید معتبر و قابل دیکود باشد."""

    payloads = fonts._iter_bundled_font_payloads("Vazir")
    assert payloads, "حداقل یک دادهٔ فونت باید برگردد"
    for payload in payloads:
        sanitized = "".join(payload.split())
        decoded = base64.b64decode(sanitized.encode("ascii"), validate=True)
        assert decoded.startswith(b"\x00\x01\x00\x00"), "ساختار TTF باید معتبر باشد"


def test_policy_font_name_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """در صورت خطا در Policy باید مقدار پیش‌فرض بازگردانده شود."""

    def _boom():
        raise RuntimeError("policy missing")

    monkeypatch.setattr(fonts, "get_policy", _boom)
    assert fonts._policy_font_name() == "Vazirmatn"
