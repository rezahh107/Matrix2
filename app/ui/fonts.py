"""ابزارک‌های فونت برای بارگذاری و اعمال وزیر به صورت متمرکز.

این ماژول تلاش می‌کند فونت «وزیر» را از مسیر محلی ``app/ui/fonts/``
بارگذاری کند و در صورت نبود، روی ویندوز از مسیرهای رایج توسعه‌دهنده
کپی می‌کند. خروجی نهایی یک ``QFont`` سراسری است که در صورت نبود وزیر
روی تاهوما بازمی‌گردد.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List

if TYPE_CHECKING:  # pragma: no cover
    from PySide6.QtGui import QFont
    from PySide6.QtWidgets import QApplication

__all__ = [
    "FONTS_DIR",
    "ensure_vazir_local_fonts",
    "load_vazir_font",
    "create_app_font",
    "prepare_default_font",
    "apply_default_font",
]

LOGGER = logging.getLogger(__name__)

FONTS_DIR: Path = Path(__file__).resolve().parent / "fonts"
FALLBACK_FAMILY = "Tahoma"
DEFAULT_POINT_SIZE = 9


def ensure_vazir_local_fonts() -> None:
    """اطمینان از وجود فایل‌های وزیر در مسیر محلی برنامه.

    - اگر پوشهٔ ``app/ui/fonts`` خالی باشد و سیستم‌عامل ویندوز باشد،
      تلاش می‌شود فایل‌های ``Vazir*.ttf`` از مسیرهای توسعه‌دهنده
      کپی شوند.
    - در سایر سیستم‌ها یا در صورت نبود مسیرها، بدون خطا رد می‌شود.
    """

    FONTS_DIR.mkdir(parents=True, exist_ok=True)

    if _has_vazir_files(FONTS_DIR.glob("*.ttf")):
        return

    if os.name != "nt":
        return

    for source in _iter_windows_sources():
        for path in source:
            _safe_copy_font(path, FONTS_DIR / path.name)


def _iter_windows_sources() -> Iterable[List[Path]]:
    for candidate in _windows_candidates():
        if not candidate.exists():
            continue
        if candidate.is_file():
            yield [candidate]
        else:
            fonts = sorted(candidate.glob("Vazir*.ttf"))
            if fonts:
                yield fonts


def _windows_candidates() -> list[Path]:
    candidates: list[Path] = []

    env_paths = os.environ.get("VAZIR_FONT_PATHS")
    if env_paths:
        for raw in env_paths.split(os.pathsep):
            if raw:
                candidates.append(Path(raw).expanduser())

    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        candidates.append(Path(local_appdata) / "Microsoft" / "Windows" / "Fonts")

    candidates.append(Path.home() / "Downloads")

    return candidates


def _safe_copy_font(src: Path, dst: Path) -> None:
    try:
        if not dst.exists():
            shutil.copy2(src, dst)
    except OSError as exc:  # pragma: no cover - خطای سیستم فایل
        LOGGER.debug("کپی فونت وزیر ناموفق بود: %s", exc)


def _has_vazir_files(paths: Iterable[Path]) -> bool:
    for path in paths:
        name = path.name.lower()
        if name.startswith("vazir") and path.suffix.lower() == ".ttf":
            return True
    return False


def _install_fonts_from_directory(directory: Path) -> list[str]:
    from PySide6.QtGui import QFontDatabase

    families: list[str] = []
    for ttf in sorted(directory.glob("*.ttf")):
        font_id = QFontDatabase.addApplicationFont(str(ttf))
        if font_id == -1:
            continue
        families.extend(QFontDatabase.applicationFontFamilies(font_id))
    return families


def _load_vazir_font_family_names() -> list[str]:
    """بارگذاری فونت وزیر و برگرداندن نام خانواده‌های ثبت‌شده."""

    ensure_vazir_local_fonts()
    families = _install_fonts_from_directory(FONTS_DIR)
    return [
        fam
        for fam in families
        if "vazir" in fam.casefold() or "وزیر" in fam
    ]


def load_vazir_font(point_size: int | None = None) -> "QFont" | None:
    """در صورت دسترسی به وزیر، نمونهٔ فونت آن را می‌سازد."""

    from PySide6.QtGui import QFont

    families = _load_vazir_font_family_names()
    if not families:
        return None
    size = point_size or DEFAULT_POINT_SIZE
    return QFont(families[0], size)


def create_app_font(point_size: int | None = None) -> "QFont":
    """ساخت فونت سراسری برنامه با اولویت وزیر سپس تاهوما.

    مثال::
        >>> font = create_app_font(point_size=10)  # doctest: +SKIP
        >>> bool(font.family())  # doctest: +SKIP
        True
    """

    vazir_font = load_vazir_font(point_size)
    if vazir_font is not None:
        return vazir_font

    size = point_size or DEFAULT_POINT_SIZE
    from PySide6.QtGui import QFont

    fallback = QFont(FALLBACK_FAMILY, size)
    fallback.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)
    return fallback


def prepare_default_font(*, point_size: int | None = None) -> "QFont":
    """سازگار برای کدهای قدیمی؛ معادل ``create_app_font``."""

    return create_app_font(point_size)


def apply_default_font(
    app: "QApplication", *, point_size: int | None = None
) -> "QFont":
    """اعمال فونت سراسری (وزیر یا تاهوما) روی QApplication."""

    font = create_app_font(point_size)
    app.setFont(font)
    return font
