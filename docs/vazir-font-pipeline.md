# مسیر بارگذاری و استفاده از فونت «وزیر/وزیرمتن» در Matrix2

این سند خلاصه می‌کند که فونت وزیر در UI/خروجی‌ها چگونه تأمین و اعمال می‌شود تا بتوان مشکل «عدم نمایش فونت وزیر» را ریشه‌یابی کرد.

## 1) تأمین فونت در لایهٔ UI (PySide6)
- **باندل تعبیه‌شده:** فایل TTF اصلی به‌صورت base64 در `app/ui/assets/font_data_vazirmatn.py` نگه‌داری می‌شود و نیاز به فایل باینری مجزا در مخزن نیست.
- **مادی‌سازی روی دیسک:** `ensure_vazir_local_fonts()` پوشهٔ `app/ui/fonts/` را می‌سازد، اگر TTFی با پیشوند `Vazir` نباشد ابتدا فونت تعبیه‌شده را روی دیسک می‌نویسد و در ویندوز در صورت نیاز از مسیرهای توسعه‌دهنده (`LOCALAPPDATA/Microsoft/Windows/Fonts`، `~/Downloads` یا متغیر محیطی `VAZIR_FONT_PATHS`) کپی می‌کند.
- **ثبت در Qt:** `_install_fonts_from_directory()` همهٔ فایل‌های `*.ttf` موجود را با `QFontDatabase.addApplicationFont` ثبت می‌کند و `_load_vazir_font_family_names()` فقط خانواده‌هایی که «vazir/وزیر» در نام دارند را برمی‌گرداند.
- **ایجاد فونت برنامه:** `create_app_font()` ابتدا وزیر را با اندازهٔ پیش‌فرض ۹pt می‌سازد؛ اگر در سیستم یافت نشود، به فونت `Tahoma` (یا مقدار `fallback_family`) برمی‌گردد و در `apply_default_font()` روی `QApplication` اعمال می‌شود.

## 2) تم و استک تایپوگرافی
- فایل `app/ui/theme.py` استک فارسی `Vazirmatn, IRANSansX, Tahoma, sans-serif` را تعریف می‌کند و در `apply_global_font()` از `create_app_font()` برای اعمال فونت در سطح اپ استفاده می‌شود. این یعنی در نبود وزیر، تاهوما به عنوان fallback استفاده می‌شود.

## 3) خروجی‌های Excel
- ماژول `app/infra/excel/styles.py` فونت خروجی را با `build_font_config()` می‌سازد؛ اگر نام فونت شامل «vazir/vazirmatn» باشد اندازهٔ پیش‌فرض را ۸pt می‌گذارد.
- `ensure_xlsxwriter_format()` و `ensure_openpyxl_named_style()` این پیکربندی را به استایل مشترک اعمال می‌کنند تا جدول‌ها در هر دو موتور xlsxwriter/openpyxl با همان فونت رندر شوند.
- `apply_workbook_formatting()` در `app/infra/excel/exporter.py` این استایل را به هر شیت اعمال می‌کند و هشدار می‌دهد که فونت در فایل Excel جاسازی نمی‌شود؛ برای اشتراک امن باید خروجی را به PDF تبدیل کرد.

## 4) تست‌ها و ممیزی
- تست `tests/ui/test_font_materialization.py` انتظار دارد در پوشهٔ فونت‌ها فایل‌هایی با پیشوند `Vazir` ساخته شود و صحت مادی‌سازی را پوشش می‌دهد.
- تست `tests/ui/test_theme_and_fonts.py` اطمینان می‌دهد که `QFont("Vazir", 11)` بدون خطا ساخته می‌شود و `create_app_font()` اندازهٔ سفارشی را اعمال می‌کند.
- تست‌های Excel (`tests/infra/excel/test_persian_excel_roundtrip.py` و `tests/infra/test_excel_export_formatting.py`) بررسی می‌کنند که فرمت‌های xlsxwriter/openpyxl، نام فونت `Vazirmatn` را در سلول‌ها ثبت کنند.

## 5) چک‌لیست عیب‌یابی عدم نمایش وزیر
1. اطمینان از اینکه در زمان اجرا مسیر `app/ui/fonts` حاوی `Vazirmatn-Regular.ttf` است (در صورت نبود، `_materialize_embedded_font` را در لاگ بررسی کنید).
2. در ویندوز، متغیر محیطی `VAZIR_FONT_PATHS` و مسیرهای پیش‌فرض (`LOCALAPPDATA/Microsoft/Windows/Fonts`، `~/Downloads`) باید دسترسی‌پذیر باشند تا کپی فونت ممکن شود.
3. اگر Qt وزیر را لود نمی‌کند، خروجی `_load_vazir_font_family_names()` را لاگ بگیرید تا ببینید آیا `QFontDatabase` خانواده‌ای با کلید «vazir» ثبت کرده است یا خیر.
4. برای خروجی Excel، چون فونت جاسازی نمی‌شود، باید اطمینان داشت که فونت روی سیستم مقصد نصب است یا فایل به PDF تبدیل شود تا نمایش صحیح تضمین گردد.
