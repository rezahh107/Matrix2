# نکات QA برای تیم توسعه SmartAlloc

## مدیریت Golden Tests
- هر تغییری که روی خروجی اکسل‌ها (Build، Allocate، Sabt) اثر می‌گذارد باید با اجرای `pytest -q` کنترل شود.
- تست‌های طلایی در پوشه‌ی `tests/` نگه‌داری می‌شوند؛ در صورت تغییر موجه در schema یا محتوا، ابتدا خروجی جدید را با ابزار diff بررسی و سپس snapshotها را به‌روز کنید.

## تغییر policy.json
- هر ویرایش در `config/policy.json` باید همراه با مستندسازی نسخه‌ی Policy و به‌روزرسانی سند «Policy-Eligibility-Matrix» باشد.
- پیش از merge، سناریوهای دستی Build → Validate → Allocate → Sabt را روی داده‌ی واقعی یا نمونه اجرا کنید.
- اگر ستون یا فیلتر جدیدی اضافه می‌شود، از طریق Policy Adapter و تست‌های واحد پوشش دهید؛ UI نباید مستقیم به ستون‌ها اشاره کند.

## یادآوری معماری
- Core فقط منطق Policy-First را اجرا می‌کند؛ I/O، Excel و Qt در لایه‌ی Infra/UI باقی می‌مانند.
- تغییرات مربوط به AppPreferences یا مسیرها باید از `app/utils/path_utils.py` عبور کنند تا در حالت frozen و توسعه یکسان باشد.

## QA پیش از انتشار بسته PyInstaller
1. اجرای `pytest -q` روی ماشین build.
2. اجرای GUI از طریق `run_gui.py` برای اطمینان از نبودن وابستگی گم‌شده.
3. ساخت بسته طبق `tools/packaging/README_packaging.md` و تست آن روی کاربر تمیز (بدون Python).
4. بررسی وجود فایل‌های لاگ و Prefs در مسیر کاربر و اجرای سناریوهای چک‌لیست QA.
