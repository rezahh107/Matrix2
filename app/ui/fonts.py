"""ابزارک‌های فونت برای بارگذاری و اعمال وزیر به صورت متمرکز.

این ماژول تلاش می‌کند فونت «وزیر» را از مسیر محلی ``app/ui/fonts/``
بارگذاری کند و در صورت نبود، روی ویندوز از مسیرهای رایج توسعه‌دهنده
کپی می‌کند. خروجی نهایی یک ``QFont`` سراسری است که در صورت نبود وزیر
روی تاهوما بازمی‌گردد.
"""

from __future__ import annotations

import base64
import binascii
import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Sequence

from app.ui.assets.font_data_vazirmatn import VAZIRMATN_REGULAR_BASE64, VAZIRMATN_REGULAR_TTF_BASE64

if TYPE_CHECKING:  # pragma: no cover
    from PySide6.QtGui import QFont
    from PySide6.QtWidgets import QApplication

__all__ = [
    "FONTS_DIR",
    "ensure_vazir_local_fonts",
    "load_vazir_font",
    "create_app_font",
    "get_app_font",
    "get_heading_font",
    "prepare_default_font",
    "apply_default_font",
    "collect_font_diagnostics",
    "resolve_vazir_family_name",
]

LOGGER = logging.getLogger(__name__)

FONTS_DIR: Path = Path(__file__).resolve().parent / "fonts"
FALLBACK_FAMILY = "Tahoma"
DEFAULT_POINT_SIZE = 9

# وزن پیش‌فرض برای فونت سراسری برنامه: بولد برای خوانایی بیشتر.
DEFAULT_WEIGHT = "bold"
DEBUG_LOG_ENV = "MATRIX_FONT_LOG"

_FONT_DEBUG_HANDLER: logging.Handler | None = None


def _init_font_debug_log() -> None:
    """فعال‌سازی لاگ فایل در صورت ست شدن متغیر محیطی.

    مسیر از متغیر ``MATRIX_FONT_LOG`` خوانده می‌شود و در صورت موفقیت،
    سطح لاگ روی DEBUG برای این ماژول تنظیم می‌شود.
    """

    global _FONT_DEBUG_HANDLER
    if _FONT_DEBUG_HANDLER is not None:
        return

    log_path = os.environ.get(DEBUG_LOG_ENV)
    if not log_path:
        return

    path = Path(log_path).expanduser()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
    except OSError as exc:  # pragma: no cover - وابسته به سیستم فایل
        LOGGER.error("راه‌اندازی لاگ فونت ناموفق بود: %s", exc)
        return

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.DEBUG)
    _FONT_DEBUG_HANDLER = handler
    LOGGER.debug("لاگ فونت در مسیر %s فعال شد", path)


_init_font_debug_log()


def _teardown_font_debug_log() -> None:
    """حذف هندلر لاگ فایل (برای تست‌ها)."""

    global _FONT_DEBUG_HANDLER
    if _FONT_DEBUG_HANDLER is None:
        return
    LOGGER.removeHandler(_FONT_DEBUG_HANDLER)
    try:
        _FONT_DEBUG_HANDLER.close()
    finally:
        _FONT_DEBUG_HANDLER = None


def ensure_vazir_local_fonts() -> Path:
    """اطمینان از وجود فایل‌های وزیر/وزیرمتن در مسیر محلی برنامه.

    مسیر ``app/ui/fonts`` همیشه ساخته می‌شود. ابتدا اگر فونت‌های محلی
    موجود باشند بدون اقدام اضافی بازگردانده می‌شود. در غیر این صورت
    تلاش می‌شود فونت تعبیه‌شدهٔ «وزیرمتن Regular» روی دیسک نوشته شود؛
    اگر ناکام بود و سیستم ویندوز بود از مسیرهای رایج توسعه‌دهنده کپی
    می‌شود. نتیجهٔ نهایی مسیر پوشهٔ فونت است.
    """

    FONTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGGER.debug("بررسی فونت در مسیر %s", FONTS_DIR)

    if _has_vazir_files(FONTS_DIR.glob("*.ttf")):
        LOGGER.debug("فایل وزیر موجود است؛ بدون اقدام اضافی")
        return FONTS_DIR

    materialized = _materialize_embedded_font(FONTS_DIR)
    if materialized is not None:
        LOGGER.debug("فونت تعبیه‌شده استخراج شد: %s", materialized.name)
        return FONTS_DIR

    if os.name != "nt":
        LOGGER.debug("سیستم ویندوز نیست؛ عبور بدون کپی")
        return FONTS_DIR

    for source in _iter_windows_sources():
        for path in source:
            LOGGER.debug("تلاش برای کپی فونت از %s", path)
            _safe_copy_font(path, FONTS_DIR / path.name)

    return FONTS_DIR


def _iter_windows_sources() -> Iterable[List[Path]]:
    for candidate in _windows_candidates():
        if not candidate.exists():
            LOGGER.debug("مسیر فونت یافت نشد: %s", candidate)
            continue
        if candidate.is_file():
            yield [candidate]
        else:
            fonts = sorted(candidate.rglob("Vazir*.ttf"))
            if fonts:
                LOGGER.debug("%d فایل فونت در %s یافت شد", len(fonts), candidate)
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
            LOGGER.debug("کپی فونت به %s انجام شد", dst)
    except OSError as exc:  # pragma: no cover - خطای سیستم فایل
        LOGGER.debug("کپی فونت وزیر ناموفق بود: %s", exc)


def _has_vazir_files(paths: Iterable[Path]) -> bool:
    for path in paths:
        name = path.name.lower()
        if name.startswith("vazir") and path.suffix.lower() == ".ttf":
            LOGGER.debug("فایل وزیر یافت شد: %s", path)
            return True
    return False


def _materialize_embedded_font(target_dir: Path) -> Path | None:
    """استخراج فونت وزیرمتن از دادهٔ base64 در صورت نبود فایل محلی."""

    target = target_dir / "Vazirmatn-Regular.ttf"
    if target.exists():
        return target

    try:
        base64_data = VAZIRMATN_REGULAR_TTF_BASE64 or VAZIRMATN_REGULAR_BASE64
        data = base64.b64decode(base64_data)
    except (binascii.Error, ValueError) as exc:  # pragma: no cover - دادهٔ تعبیه‌شده ثابت است
        LOGGER.warning("دادهٔ فونت وزیرمتن نامعتبر بود: %s", exc)
        return None

    try:
        target.write_bytes(data)
    except OSError as exc:  # pragma: no cover - خطای سیستم فایل
        LOGGER.debug("نوشتن فونت تعبیه‌شده ناموفق بود: %s", exc)
        return None

    return target


def _install_fonts_from_directory(directory: Path) -> list[str]:
    from PySide6.QtGui import QFontDatabase

    families: list[str] = []
    for ttf in sorted(directory.glob("*.ttf")):
        font_id = QFontDatabase.addApplicationFont(str(ttf))
        if font_id == -1:
            LOGGER.debug("ثبت فونت %s ناموفق بود", ttf)
            continue
        families.extend(QFontDatabase.applicationFontFamilies(font_id))
        LOGGER.debug("فونت %s ثبت شد با خانواده‌ها: %s", ttf, families)
    if families:
        db = QFontDatabase()
        resolved = resolve_vazir_family_name(db, candidates=families)
        if resolved:
            LOGGER.debug("خانوادهٔ اصلی وزیر تشخیص داده شد: %s", resolved)
    return families


def _load_vazir_font_family_names() -> list[str]:
    """بارگذاری فونت وزیر و برگرداندن نام خانواده‌های ثبت‌شده."""

    ensure_vazir_local_fonts()
    families = _install_fonts_from_directory(FONTS_DIR)
    from PySide6.QtGui import QFontDatabase

    db = QFontDatabase()
    all_families = list(db.families()) + families
    vazir_like = [
        fam
        for fam in all_families
        if "vazir" in fam.casefold() or "وزیر" in fam
    ]
    unique_sorted = sorted(dict.fromkeys(vazir_like), key=str.casefold)
    LOGGER.debug("خانواده‌های ثبت‌شده: %s", unique_sorted)
    return unique_sorted


def resolve_vazir_family_name(
    db: "QFontDatabase", *, candidates: Sequence[str] | None = None
) -> str | None:
    """انتخاب نام خانوادهٔ اصلی وزیر/وزیرمتن از میان خانواده‌های موجود."""

    pool: list[str] = []
    if candidates:
        pool.extend(candidates)
    pool.extend(db.families())

    ordered_unique = sorted(dict.fromkeys(pool), key=str.casefold)
    needles = ("vazirmatn", "vazir", "وزیر")
    for needle in needles:
        for family in ordered_unique:
            if needle in family.casefold() or (needle == "وزیر" and "وزیر" in family):
                return family
    return None


def load_vazir_font(point_size: int | None = None) -> "QFont" | None:
    """در صورت دسترسی به وزیر، نمونهٔ فونت آن را می‌سازد."""

    from PySide6.QtGui import QFont
    from PySide6.QtGui import QFontDatabase

    families = _load_vazir_font_family_names()
    db = QFontDatabase()
    family = resolve_vazir_family_name(db, candidates=families)
    if not family:
        LOGGER.debug("هیچ خانوادهٔ وزیر ثبت نشد")
        return None
    size = point_size or DEFAULT_POINT_SIZE
    LOGGER.debug("فونت وزیر با خانوادهٔ %s و اندازهٔ %s ساخته شد", family, size)
    return QFont(family, size)


def create_app_font(
    point_size: int | None = None,
    *,
    fallback_family: str | None = None,
) -> "QFont":
    """ساخت فونت سراسری برنامه با اولویت وزیر سپس تاهوما.

    Args:
        point_size: اندازهٔ فونت؛ در صورت None از مقدار پیش‌فرض استفاده می‌شود.
        fallback_family: در صورت نیاز، خانوادهٔ فونت fallback سفارشی.

    مثال::
        >>> font = create_app_font(point_size=10, fallback_family="Arial")  # doctest: +SKIP
        >>> bool(font.family())  # doctest: +SKIP
        True

    نکته:
        وزن پیش‌فرض روی Bold تنظیم می‌شود تا هماهنگی سراسری با درخواست
        کاربر حفظ شود.
    """

    from PySide6.QtGui import QFont

    size = point_size or DEFAULT_POINT_SIZE
    vazir_font = load_vazir_font(size)
    if vazir_font is not None:
        vazir_font.setPointSize(size)
        vazir_font.setWeight(_resolve_weight())
        return _with_antialias(vazir_font)

    family = (fallback_family and fallback_family.strip()) or FALLBACK_FAMILY
    LOGGER.debug("استفاده از فونت جایگزین %s", family)
    fallback = QFont(family, size)
    fallback.setWeight(_resolve_weight())
    return _with_antialias(fallback)


def get_app_font(point_size: int | None = None) -> "QFont":
    """دریافت نسخهٔ کپی‌شده از فونت سراسری برنامه با اندازهٔ دلخواه."""

    return create_app_font(point_size=point_size)


def get_heading_font() -> "QFont":
    """فونت عناوین: مبتنی بر وزیر با اندازهٔ بزرگ‌تر و وزن بولد."""

    from PySide6.QtGui import QFont

    heading = create_app_font()
    heading.setPointSize(11)
    heading.setWeight(_resolve_weight())
    return heading


def collect_font_diagnostics() -> dict[str, object]:
    """بازگرداندن وضعیت فعلی فونت و ثبت آن در لاگ (در صورت فعال بودن)."""

    info: dict[str, object] = {
        "fonts_dir": str(FONTS_DIR),
        "fonts_present": sorted(path.name for path in FONTS_DIR.glob("*.ttf")),
        "platform": os.name,
        "env_paths": os.environ.get("VAZIR_FONT_PATHS", ""),
        "windows_candidates": [str(path) for path in _windows_candidates()],
        "debug_log_env": os.environ.get(DEBUG_LOG_ENV) or "",
    }

    try:
        import importlib

        info["pyside_available"] = importlib.util.find_spec("PySide6") is not None
    except Exception:  # pragma: no cover - فقط برای گزارش
        info["pyside_available"] = False

    LOGGER.debug("گزارش عیب‌یابی فونت: %s", info)
    return info


def prepare_default_font(*, point_size: int | None = None) -> "QFont":
    """سازگار برای کدهای قدیمی؛ معادل ``create_app_font``."""

    return create_app_font(point_size=point_size)


def apply_default_font(
    app: "QApplication", *, point_size: int | None = None, family_override: str | None = None
) -> "QFont":
    """اعمال فونت سراسری (وزیر یا تاهوما) روی QApplication با امکان override.

    Args:
        app: نمونهٔ Qt برای اعمال فونت.
        point_size: اندازهٔ فونت؛ در صورت None از مقدار پیش‌فرض استفاده می‌شود.
        family_override: در صورت نیاز، خانوادهٔ فونت fallback سفارشی.
    """

    font = create_app_font(point_size=point_size, fallback_family=family_override)
    app.setFont(font)
    return font


def _with_antialias(font: "QFont") -> "QFont":
    from PySide6.QtGui import QFont

    antialias_strategy = QFont.StyleStrategy.PreferAntialias | QFont.StyleStrategy.PreferQuality
    strategy = QFont.StyleStrategy(font.styleStrategy()) | antialias_strategy
    font.setStyleHint(QFont.StyleHint.SansSerif, antialias_strategy)
    font.setStyleStrategy(strategy)
    font.setHintingPreference(QFont.HintingPreference.PreferFullHinting)
    font.setKerning(True)
    return font


def _resolve_weight() -> "QFont.Weight":
    """تبدیل وزن پیش‌فرض متنی به مقدار مناسب QFont."""

    from PySide6.QtGui import QFont

    mapping = {
        "bold": QFont.Weight.Bold,
        "demibold": QFont.Weight.DemiBold,
    }
    return mapping.get(DEFAULT_WEIGHT.lower(), QFont.Weight.Normal)
