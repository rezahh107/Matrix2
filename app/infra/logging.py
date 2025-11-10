"""زیرساخت راه‌اندازی لاگ برای لایهٔ زیرساخت."""
from __future__ import annotations

import logging.config
from pathlib import Path
from typing import Any

import yaml

DEFAULT_LOGGING_CONFIG = Path("config/logging.yaml")


def setup_logging(config_path: str | Path = DEFAULT_LOGGING_CONFIG) -> None:
    """بارگذاری پیکربندی logging از فایل YAML و اعمال آن.

    مثال::

        >>> setup_logging()  # doctest: +SKIP

    Args:
        config_path: مسیر فایل پیکربندی YAML.

    Raises:
        FileNotFoundError: اگر فایل پیکربندی وجود نداشته باشد.
        ValueError: اگر ساختار YAML معتبر نباشد.
    """

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"logging config not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data: Any = yaml.safe_load(handle)

    if not isinstance(data, dict):
        raise ValueError("logging config must be a mapping")

    handlers = data.get("handlers", {})
    if isinstance(handlers, dict):
        for handler_cfg in handlers.values():
            if not isinstance(handler_cfg, dict):
                continue
            cls_name = str(handler_cfg.get("class", ""))
            if "FileHandler" in cls_name:
                filename = handler_cfg.get("filename")
                if filename:
                    Path(filename).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(data)


__all__ = ["setup_logging"]
