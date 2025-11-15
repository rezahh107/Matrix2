# سامانه تخصیص دانشجو-منتور (نسخه معماری مدولار)

## اجرا
```bash
pip install -r requirements.txt
python -m app.main
```

## ساخت فایل اجرایی (PyInstaller)
```bash
pyinstaller --onefile --windowed --name تخصیص_دانشجو_منتور \
  --collect-all PySide6 --hidden-import openpyxl --hidden-import pandas.io.formats.excel \
  app/main.py
```

## Policy-First در Core

- تمامی نام ستون‌ها، مراحل Trace و فیلترها از فایل `config/policy.json` خوانده می‌شوند.
- کلاس `PolicyAdapter` در `app/core/policy_adapter.py` تنها نقطهٔ دسترسی به تنظیمات Policy است.
- برای تغییر رفتار فیلترها، تنها کافی است JSON را ویرایش کنید؛ نیازی به تغییر کد نیست.
- ستون ظرفیت از مرحلهٔ `capacity_gate` گرفته می‌شود و برای Override می‌توان پارامتر صریح به تابع تخصیص داد.

## نرمال‌سازی ورودی

- حروف عربی (ي/ك) و ارقام فارسی/عربی به معادل فارسی/لاتین تبدیل می‌شوند.
- فاصله‌ها، نیم‌فاصله و کاراکترهای ترکیبی حذف می‌شوند تا مقایسهٔ ستون‌ها پایدار بماند.
- آلیاس‌ها از Policy برای سازگاری با گزارش Inspactor و Crosswalk استفاده می‌شوند.

## تست‌های سیاست‌محور

- `pytest -q` سناریوهای ترجیح «کدرشته» بر نام گروه و مصرف ستون ظرفیت از Policy را پوشش می‌دهد.
- تغییر JSON (مثلاً تغییر نام ستون ظرفیت) باید بدون تغییر کد باعث تغییر رفتار تخصیص شود.

## تخصیص بر اساس مرکز

برای کنترل تخصیص دانش‌آموزان به پشتیبان‌ها براساس مرکز ثبت‌نام:

### از طریق UI
1. در تب «تخصیص»، بخش «تنظیمات مدیران مرکز» را باز کنید.
2. برای هر مرکز، مدیر دلخواه را از ComboBox انتخاب یا وارد کنید؛ دکمهٔ «بازگشت به پیش‌فرض‌ها» مقدار Policy را برمی‌گرداند.
3. پس از انتخاب فایل استخر، از گزینهٔ «به‌روزرسانی مدیران از استخر» استفاده کنید تا لیست مدیران به‌روز شود.

### از طریق CLI

```bash
python -m app.cli allocate \
  --students students.xlsx \
  --pool mentors.xlsx \
  --output allocations.xlsx \
  --center-manager 1="مدیر گلستان" \
  --center-manager 2="مدیر صدرا" \
  --center-priority 1,2,0
```

آرگومان `--center-managers` نیز یک نگاشت JSON می‌پذیرد (مثلاً `'{"3": ["مدیر جدید"]}'`) و در صورت عدم تعیین، مقادیر پیش‌فرض Policy استفاده می‌شود.

## هشدار خروجی Excel

- فایل‌های Excel تولیدشده فونت‌های Vazir/Vazirmatn را **جاسازی نمی‌کنند**؛ برای اشتراک‌گذاری با سیستم‌های فاقد فونت، خروجی را به PDF تبدیل کنید.

## مستندات تکمیلی

- راهنمای کامل اپراتور GUI: [docs/SmartAlloc_GUI_Operator_Guide.fa.md](docs/SmartAlloc_GUI_Operator_Guide.fa.md)
- چک‌لیست QA سرتاسری: [docs/SmartAlloc_E2E_QA_Checklist.fa.md](docs/SmartAlloc_E2E_QA_Checklist.fa.md)
- نکات QA برای تیم توسعه: [docs/SmartAlloc_Dev_QA_Notes.md](docs/SmartAlloc_Dev_QA_Notes.md)
