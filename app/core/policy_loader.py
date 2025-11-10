"""Policy Loader (Core) — سبک، کش‌شونده و متکی بر SSoT.

نکتهٔ معماری: اگر قرار باشد I/O از Core خارج شود، کافی است دادهٔ JSON خوانده‌شده
در Infra به تابع :func:`parse_policy_dict` پاس داده شود. این ماژول هر دو مسیر
را فراهم می‌کند.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import List, Mapping, Optional, Sequence
from typing import Literal

VersionMismatchMode = Literal["raise", "warn", "ignore"]


@dataclass(frozen=True)
class PolicyConfig:
    """ساختار دادهٔ فقط‌خواندنی برای نگهداری سیاست بارگذاری‌شده."""

    version: str
    normal_statuses: List[int]
    school_statuses: List[int]
    join_keys: List[str]
    ranking: List[str]


def _validate_policy_dict(data: Mapping[str, object]) -> None:
    """اعتبارسنجی حداقلی بدون هاردکد قواعد دامنه."""

    required = ("version", "normal_statuses", "school_statuses", "join_keys", "ranking")
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f"Policy keys missing: {missing}")

    join_keys = data["join_keys"]
    if not isinstance(join_keys, Sequence) or isinstance(join_keys, (str, bytes)):
        raise TypeError("join_keys must be a sequence of strings")
    join_keys_list = [str(item) for item in join_keys]
    if len(join_keys_list) != 6:
        raise ValueError("join_keys must be 6")
    if len(set(join_keys_list)) != 6:
        raise ValueError("join_keys must be unique")

    ranking = data["ranking"]
    if not isinstance(ranking, Sequence) or isinstance(ranking, (str, bytes)):
        raise TypeError("ranking must be a sequence of strings")
    ranking_list = [str(item) for item in ranking]
    if len(ranking_list) != 3:
        raise ValueError("ranking must have exactly 3 items")
    if len(set(ranking_list)) != 3:
        raise ValueError("ranking must be unique")

    for key in ("normal_statuses", "school_statuses"):
        value = data[key]
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise TypeError(f"{key} must be a sequence of ints")
        if not all(isinstance(item, int) for item in value):
            raise TypeError(f"All {key} items must be int")


def _version_gate(
    loaded_version: str,
    expected_version: Optional[str],
    on_version_mismatch: VersionMismatchMode,
) -> None:
    if expected_version is None:
        return
    if loaded_version == expected_version:
        return

    message = (
        f"Policy version mismatch: loaded='{loaded_version}' "
        f"expected='{expected_version}'"
    )
    if on_version_mismatch == "raise":
        raise ValueError(message)
    if on_version_mismatch == "warn":
        warnings.warn(message, RuntimeWarning, stacklevel=2)


def _to_config(data: Mapping[str, object]) -> PolicyConfig:
    return PolicyConfig(
        version=str(data["version"]),
        normal_statuses=[int(item) for item in data["normal_statuses"]],  # type: ignore[index]
        school_statuses=[int(item) for item in data["school_statuses"]],  # type: ignore[index]
        join_keys=[str(item) for item in data["join_keys"]],  # type: ignore[index]
        ranking=[str(item) for item in data["ranking"]],  # type: ignore[index]
    )


def parse_policy_dict(
    data: Mapping[str, object],
    expected_version: Optional[str] = None,
    on_version_mismatch: VersionMismatchMode = "raise",
) -> PolicyConfig:
    """مسیر خالص برای تبدیل dict به :class:`PolicyConfig`."""

    _validate_policy_dict(data)
    _version_gate(str(data["version"]), expected_version, on_version_mismatch)
    return _to_config(data)


@lru_cache(maxsize=8)
def load_policy(
    path: str | Path = "config/policy.json",
    *,
    expected_version: Optional[str] = None,
    on_version_mismatch: VersionMismatchMode = "raise",
) -> PolicyConfig:
    """بارگذاری سیاست از فایل JSON و بازگشت ساختار کش‌شونده."""

    policy_path = Path(path)
    try:
        raw = policy_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:  # pragma: no cover - پیام واضح برای مصرف‌کننده
        raise FileNotFoundError(f"Policy file not found: {policy_path}") from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in policy file: {policy_path}") from exc

    return parse_policy_dict(
        data,
        expected_version=expected_version,
        on_version_mismatch=on_version_mismatch,
    )
