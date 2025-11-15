"""توابع کمکی برای تفسیر ترجیحات مرکز از Policy و ورودی کاربر."""

from __future__ import annotations

from typing import Dict, Mapping, Sequence

from .policy_loader import PolicyConfig

__all__ = [
    "parse_center_manager_config",
    "normalize_center_priority",
]


def _normalize_names(payload: object) -> tuple[str, ...]:
    """تبدیل ورودی متنی یا لیستی به تاپل تمیز و یکتا."""

    if payload is None:
        return tuple()
    if isinstance(payload, (list, tuple)):
        items = payload
    else:
        items = [payload]
    seen: set[str] = set()
    cleaned: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)
    return tuple(cleaned)


def _normalize_override_map(source: Mapping[object, object] | None) -> Dict[int, tuple[str, ...]]:
    """تبدیل نگاشت ورودی (کلید عددی یا متنی) به ساختار پایدار."""

    if source is None:
        return {}
    normalized: Dict[int, tuple[str, ...]] = {}
    for key, value in source.items():
        try:
            center_id = int(key)
        except (TypeError, ValueError):
            continue
        names = _normalize_names(value)
        normalized[center_id] = names
    return normalized


def parse_center_manager_config(
    policy: PolicyConfig,
    ui_overrides: Mapping[object, object] | None = None,
    cli_overrides: Mapping[object, object] | None = None,
) -> Dict[int, tuple[str, ...]]:
    """ساخت نگاشت «مرکز → مدیران» با ادغام Policy و ورودی کاربر."""

    result: Dict[int, tuple[str, ...]] = {}
    for center in policy.center_management.centers:
        defaults = _normalize_names(center.default_managers)
        if defaults:
            result[center.id] = defaults
    for source in (_normalize_override_map(ui_overrides), _normalize_override_map(cli_overrides)):
        for center_id, names in source.items():
            if names:
                result[center_id] = names
            elif center_id in result:
                del result[center_id]
    return result


def normalize_center_priority(
    policy: PolicyConfig, priority: Sequence[int] | None
) -> tuple[int, ...]:
    """نرمال‌سازی لیست اولویت مراکز با تضمین پوشش تمام مراکز policy."""

    if priority is None:
        priority = []
    normalized: list[int] = []
    seen: set[int] = set()
    for value in priority:
        try:
            center_id = int(value)
        except (TypeError, ValueError):
            continue
        if center_id in seen:
            continue
        normalized.append(center_id)
        seen.add(center_id)
    default_order = policy.center_management.priority_order or policy.center_management.center_ids()
    for center_id in default_order:
        if center_id in seen:
            continue
        normalized.append(center_id)
        seen.add(center_id)
    fallback = policy.default_center_for_invalid
    if fallback is not None and fallback not in seen:
        normalized.append(fallback)
    return tuple(normalized)
