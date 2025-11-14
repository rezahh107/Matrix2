# ساخت بستهٔ GUI Matrix2 با PyInstaller

این راهنما نحوهٔ تولید نسخهٔ قابل‌اجرا (بدون کنسول) برای ویندوز را توضیح می‌دهد.

## پیش‌نیازها

1. Windows 10/11 x64
2. Python 3.11 (همان نسخه‌ای که برای توسعه استفاده شده است)
3. نصب وابستگی‌ها:
   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -U pip
   pip install -r requirements.txt
   pip install pyinstaller
   ```

## اجرای ساخت

1. اطمینان حاصل کنید که مخزن در حالت تمیز است و دستور زیر را از ریشهٔ مخزن اجرا کنید:
   ```powershell
   pyinstaller tools/packaging/matrix2_gui.spec
   ```
2. PyInstaller ساخت نسخهٔ **one-dir** را انجام می‌دهد. خروجی در پوشهٔ `dist/Matrix2-GUI/` قرار می‌گیرد و شامل فایل `Matrix2-GUI.exe` است.
3. برای تست محلی، می‌توانید قبل از بسته‌بندی از اسکریپت توسعه استفاده کنید:
   ```powershell
   python run_gui.py
   ```

## محتوای خروجی

- `Matrix2-GUI.exe`: برنامهٔ اصلی بدون کنسول.
- پوشهٔ `config/`: شامل Policy و تنظیمات SmartAlloc که در حالت فریز شده نیز در دسترس خواهند بود.

برای اجرا، کافی است `Matrix2-GUI.exe` را دوبار کلیک کنید. تمام مسیرهای نسبی (مانند `config/policy.json`) به صورت خودکار در کنار exe قرار دارند.

## محل ذخیرهٔ تنظیمات و لاگ‌ها

- QSettings (AppPreferences) همچنان از مسیر استاندارد کاربر استفاده می‌کند (برای ویندوز: رجیستری/`HKCU\Software\Matrix2`).
- لاگ‌های اضطراری UI در `%USERPROFILE%\Matrix2\logs\gui_crash.log` ذخیره می‌شوند. در صورت بروز خطای غیرمنتظره، پیام فارسی نمایش داده شده و جزئیات در همین فایل و گزارش‌های پوشهٔ `logs/errors` قرار می‌گیرد.

## نکات تکمیلی

- اگر نیاز به نسخهٔ one-file دارید می‌توانید گزینهٔ `--onefile` را به فرمان PyInstaller بیفزایید، ولی باید اطمینان حاصل کنید که پوشهٔ `config` پس از Extract شدن در دسترس بماند.
- برای افزودن آیکون دلخواه، پارامتر `icon=` را در فایل `matrix2_gui.spec` تنظیم کنید.
