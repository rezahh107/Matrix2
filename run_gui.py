"""Entry-point ساده برای اجرای GUI با دوبار کلیک."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_sys_path() -> None:
    root = Path(__file__).resolve().parent
    app_dir = root / "app"
    for candidate in (root, app_dir):
        text = str(candidate)
        if text not in sys.path:
            sys.path.insert(0, text)


def main() -> None:
    """اجرای رابط کاربری گرافیکی."""

    _ensure_sys_path()
    from app import main as app_main

    app_main.run()


if __name__ == "__main__":
    main()
