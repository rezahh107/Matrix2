from __future__ import annotations

"""کش ساده برای سیاست بارگذاری‌شده.

نمونهٔ استفاده::

    policy = get_cached_policy()
    invalidate_policy_cache()
"""

from functools import lru_cache

from app.core.policy_loader import PolicyConfig, load_policy


@lru_cache(maxsize=1)
def get_cached_policy() -> PolicyConfig:
    """دریافت policy از دیسک با کش درون‌فرایندی."""

    return load_policy()


def invalidate_policy_cache() -> None:
    """پاک‌سازی کش تا در فراخوانی بعدی policy مجدداً خوانده شود."""

    get_cached_policy.cache_clear()
