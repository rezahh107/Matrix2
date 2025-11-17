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
