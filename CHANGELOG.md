# Changelog

## [Unreleased]
### Added
- center management config in `policy.json` with dynamic priority/order defaults.
- UI ComboBoxهای پویا و دکمهٔ «بازگشت به پیش‌فرض‌ها» برای انتخاب مدیر هر مرکز.
- CLI overrides عمومی `--center-manager`/`--center-managers` و نرمال‌سازی خودکار اولویت مراکز.
- هشدار `INVALID_CENTER` و fallback برای مقادیر نامعتبر ستون مرکز.

### Fixed
- رعایت ترتیب پردازش «دانش‌آموزان مدرسه‌ای قبل از دانش‌آموزان مرکزی» مطابق نیاز اولیه.
- اعتبارسنجی سخت‌گیرانه (اختیاری) برای نبود مدیر در استخر و پیام‌های کاربرپسند در UI هنگام خطا.
