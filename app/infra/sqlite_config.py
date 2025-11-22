"""پیکربندی ایمن اتصال SQLite برای لایهٔ Infra."""
from __future__ import annotations

import sqlite3


def _set_pragma(conn: sqlite3.Connection, name: str, value: str) -> None:
    """اجرای امن PRAGMA روی اتصال.

    Parameters
    ----------
    conn: sqlite3.Connection
        اتصال فعال SQLite.
    name: str
        نام PRAGMA.
    value: str
        مقدار موردنظر.

    مثال
    ----
    >>> import sqlite3
    >>> conn = sqlite3.connect(":memory:")
    >>> _set_pragma(conn, "foreign_keys", "ON")
    """
    if name not in {"foreign_keys", "journal_mode", "synchronous"}:
        raise ValueError(f"Unsupported PRAGMA: {name}")
    conn.execute(f"PRAGMA {name} = {value};")


def configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    """تنظیم PRAGMA های پایه برای اتصالات SQLite.

    این تابع اتصال داده‌شده را به‌صورت یکسان پیکربندی می‌کند تا
    محدودیت‌های کلید خارجی فعال باشد و ژورنال‌گذاری و همگام‌سازی
    برای استفادهٔ دسکتاپ تک‌کاربره تنظیم شود.

    Parameters
    ----------
    conn: sqlite3.Connection
        اتصال ساخته‌شده توسط ``sqlite3.connect``.

    Returns
    -------
    sqlite3.Connection
        همان اتصال پس از اعمال تنظیمات.

    مثال
    ----
    >>> import sqlite3
    >>> from app.infra.sqlite_config import configure_connection
    >>> connection = configure_connection(sqlite3.connect(":memory:"))
    >>> connection.execute("PRAGMA foreign_keys;").fetchone()[0]
    1
    """

    conn.row_factory = sqlite3.Row
    _set_pragma(conn, "foreign_keys", "ON")
    _set_pragma(conn, "journal_mode", "WAL")
    _set_pragma(conn, "synchronous", "NORMAL")
    return conn


__all__ = ["configure_connection"]
