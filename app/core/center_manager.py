"""توابع خالص برای مدیریت پیکربندی مراکز (Policy-First)."""

from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from .policy_loader import PolicyConfig

__all__ = [
    "resolve_center_manager_config",
    "validate_center_config",
]


def _normalize_manager_mapping(
    mapping: Mapping[object, object] | None,
) -> Dict[int, List[str]]:
    """نرمال‌سازی نگاشت ورودی به ساختار پایدار «مرکز → لیست مدیران».

    مثال::

        >>> _normalize_manager_mapping({"1": "مدیر"})
        {1: ["مدیر"]}
    """

    if mapping is None:
        return {}
    normalized: Dict[int, List[str]] = {}
    for raw_center, raw_names in mapping.items():
        try:
            center_id = int(raw_center)
        except (TypeError, ValueError):
            continue
        items: Iterable[object]
        if isinstance(raw_names, (list, tuple, set)):
            items = raw_names
        else:
            items = (raw_names,)
        names: List[str] = []
        seen: set[str] = set()
        for item in items:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            names.append(text)
            seen.add(text)
        if names:
            normalized[center_id] = names
    return normalized


def _normalize_priority_sequence(
    policy: PolicyConfig, priority: Sequence[int] | None
) -> List[int]:
    """ترتیب مراکز را با تضمین پوشش همهٔ مراکز و fallback تکمیل می‌کند."""

    normalized: List[int] = []
    seen: set[int] = set()
    if priority:
        for item in priority:
            try:
                center_id = int(item)
            except (TypeError, ValueError):
                continue
            if center_id in seen:
                continue
            normalized.append(center_id)
            seen.add(center_id)
    default_order: Sequence[int] = (
        policy.center_management.priority_order
        or policy.center_management.center_ids()
    )
    for center_id in default_order:
        if center_id in seen:
            continue
        normalized.append(center_id)
        seen.add(center_id)
    fallback = policy.default_center_for_invalid
    if fallback is not None and fallback not in seen:
        normalized.append(fallback)
    return normalized


def resolve_center_manager_config(
    *,
    policy: PolicyConfig,
    ui_managers: Optional[Mapping[object, object]] = None,
    cli_managers: Optional[Mapping[object, object]] = None,
    cli_priority: Optional[Sequence[int]] = None,
    cli_strict_validation: bool = False,
) -> tuple[Dict[int, List[str]], List[int]]:
    """ادغام تنظیمات مراکز با اولویت: Policy < UI < CLI.

    مثال::

        >>> resolve_center_manager_config(
        ...     policy=policy,
        ...     ui_managers={1: ["UI"]},
        ...     cli_managers={1: ["CLI"]},
        ... )
        ({1: ["CLI"]}, [...])
    """

    final_map: Dict[int, List[str]] = {}
    for center in policy.center_management.centers:
        defaults = center.default_manager
        if defaults:
            final_map[center.id] = [defaults]
    for source in (
        _normalize_manager_mapping(ui_managers),
        _normalize_manager_mapping(cli_managers),
    ):
        for center_id, names in source.items():
            final_map[center_id] = list(names)
    final_priority = _normalize_priority_sequence(policy, cli_priority)
    strict_required = bool(
        policy.center_management.strict_manager_validation or cli_strict_validation
    )
    if strict_required:
        required_centers = [
            center.id for center in policy.center_management.centers if center.id != 0
        ]
        missing = [center_id for center_id in required_centers if center_id not in final_map]
        if missing:
            raise ValueError(
                "مدیر پیش‌فرض برای مراکز زیر مشخص نشده است: "
                + ", ".join(str(center) for center in missing)
            )
    return final_map, final_priority


def validate_center_config(
    policy: PolicyConfig,
    center_manager_map: Mapping[int, Sequence[str]],
    center_priority: Sequence[int],
) -> List[str]:
    """اعتبارسنجی پیکربندی مدیریت مراکز و تولید هشدارهای خوانا."""

    warnings_list: List[str] = []
    defined_centers = {center.id for center in policy.center_management.centers}
    for center_id in center_manager_map:
        if center_id not in defined_centers:
            warnings_list.append(
                f"مرکز {center_id} در تنظیم مدیران وجود دارد اما در Policy تعریف نشده است"
            )
    for center_id in center_priority:
        if center_id not in defined_centers:
            warnings_list.append(
                f"مرکز {center_id} در ترتیب اولویت وجود دارد اما در Policy تعریف نشده است"
            )
    if 0 in defined_centers and 0 not in set(center_priority):
        warnings_list.append("مرکز 0 در ترتیب اولویت مراکز وجود ندارد")
    return warnings_list
