"""تست استخراج فونت تعبیه‌شدهٔ وزیرمتن بدون وابستگی به PySide."""

from __future__ import annotations

from app.ui import fonts


def test_embedded_vazirmatn_materialized(tmp_path, monkeypatch):
    fonts_dir = tmp_path / "fonts"
    monkeypatch.setattr(fonts, "FONTS_DIR", fonts_dir)
    monkeypatch.setattr(fonts, "_windows_candidates", lambda: [])

    fonts.ensure_vazir_local_fonts()

    files = list(fonts_dir.glob("*.ttf"))
    assert files, "فونت تعبیه‌شده باید تولید شود"
    assert any(path.name.startswith("Vazir") for path in files)
    assert all(path.stat().st_size > 0 for path in files)

    # فراخوانی مجدد باید idempotent باشد و فایل جدیدی اضافه نکند
    fonts.ensure_vazir_local_fonts()
    files_again = list(fonts_dir.glob("*.ttf"))
    assert files_again == files


def test_font_debug_log_and_diagnostics(tmp_path, monkeypatch):
    log_path = tmp_path / "font.log"
    monkeypatch.setenv("MATRIX_FONT_LOG", str(log_path))

    import importlib

    # reload برای اعمال متغیر محیطی روی ماژول
    reloaded = importlib.reload(fonts)
    monkeypatch.setattr(reloaded, "FONTS_DIR", tmp_path / "fonts")
    monkeypatch.setattr(reloaded, "_windows_candidates", lambda: [])

    reloaded.ensure_vazir_local_fonts()
    info = reloaded.collect_font_diagnostics()

    assert log_path.exists(), "لاگ فونت باید ساخته شود"
    log_content = log_path.read_text(encoding="utf-8")
    assert "فونت" in log_content
    assert info["fonts_present"], "حداقل یک فونت باید وجود داشته باشد"
    assert info["fonts_dir"] == str(reloaded.FONTS_DIR)

    reloaded._teardown_font_debug_log()
