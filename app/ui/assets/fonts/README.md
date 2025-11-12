# Bundled UI Font

این پوشه شامل مستندات فونت پیش‌فرض رابط کاربری است. خود فایل فونت به‌صورت base64 در ماژول
`app/ui/assets/font_data_vazirmatn.py` ذخیره شده تا نیاز به فایل باینری جداگانه در مخزن نباشد و
همچنان مطابق Policy نسخهٔ 1.0.3 فونت وزیر/وزیرمتن بدون وابستگی به سیستم‌عامل در دسترس باشد.

> نکته: برای محیط‌های تولیدی می‌توانید این فایل را با نسخهٔ رسمی منتشر شده توسط پروژهٔ
> [Vazirmatn](https://github.com/rastikerdar/vazirmatn) جایگزین کنید. ساختار ماژول `app.ui.fonts`
> مسیر را به‌صورت خودکار تشخیص می‌دهد.

## به‌روزرسانی فونت

1. آخرین نسخهٔ فایل TTF فونت را از پروژهٔ رسمی Vazirmatn دریافت کنید.
2. با استفاده از اسکریپت زیر رشتهٔ base64 را به‌روزرسانی نمایید و نتیجه را در ماژول ذکرشده جایگزین کنید:

   ```bash
   python - <<'PY'
   import base64, textwrap
   from pathlib import Path

   data = Path('Vazirmatn-Regular.ttf').read_bytes()
   encoded = base64.b64encode(data).decode('ascii')
   wrapped = '\n'.join(textwrap.wrap(encoded, 76))
   target = Path('app/ui/assets/font_data_vazirmatn.py')
   target.write_text(f'"""دادهٔ base64 فونت وزیرمتن (مجوز SIL OFL 1.1)."""\n\n'
                     'from __future__ import annotations\n\n'
                     f'VAZIRMATN_REGULAR_TTF_BASE64 = """\n{wrapped}\n"""\n')
   PY
   ```

3. در صورت انتشار به‌عنوان محصول، متن مجوز فونت را هم‌راستا با نسخهٔ جدید بروزرسانی نمایید.
4. برای اعمال تغییرات، اجرای دوبارهٔ برنامه کافی است و نیازی به تغییر ماژول‌های دیگر نیست.
