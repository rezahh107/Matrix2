# SQLite Cache/Persistence Opportunities — Smart Student Allocation

## Scope and Assumptions
- سیاست و SSoT بدون تغییر باقی می‌مانند (Policy v1.0.3 / SSoT v1.0.2). Core همچنان فاقد آگاهی از SQLite است و فقط DataFrame/نوع‌های خالص می‌گیرد.
- الگوی لایه‌بندی (UI → Infra → Core) حفظ می‌شود؛ تعریف schema و اتصال SQLite فقط در Infra انجام می‌شود.
- این یادداشت روی بهبود کارایی و UX از طریق کش/پایداری متمرکز است و semantics ۶ کلید اتصال، رتبه‌بندی و Trace ۸ مرحله‌ای را دست‌نخورده می‌گذارد.

## خلاصهٔ وضعیت فعلی SQLite
- **History/Run metadata**: `LocalDatabase` جداول runs/run_metrics/qa_summary را می‌سازد و برای هر اجرا پر می‌کند؛ UI/CLI می‌توانند DB را غیرفعال کنند. 【F:app/infra/local_database.py†L72-L187】
- **مرجع مدارس**: ورود یک‌بارهٔ SchoolReport/Crosswalk از Excel به جداول schools و school_crosswalk_* با تبدیل `کد مدرسه` به Int64؛ لود مجدد از SQLite برای اجرای عادی. 【F:app/infra/reference_schools_repository.py†L1-L93】【F:app/infra/local_database.py†L205-L274】
- **معماری لایه‌ای**: سند معماری استفادهٔ Infra از SQLite برای متادیتای اجرا را تأیید و Core را کاملاً I/O-agnostic تعریف می‌کند. 【F:docs/System_Architecture_Blueprint_Smart_Student_Allocation_v1.0.md†L15-L130】
- **جریان فعلی CLI**: build/allocate هر بار فایل‌های دانش‌آموز/استخر را از Excel می‌خواند؛ کشی برای ورودی‌های پرتکرار وجود ندارد. 【F:app/infra/cli.py†L1777-L1858】

## کاندیداهای قوی (پیشنهاد اجرای فوری)
1) **کش نرمال‌شدهٔ StudentReport/MentorReport**
   - **دامنه/Artefact**: StudentReport (اعتبارسنجی اختیاری) و Inspactor/MentorPool اکسل‌های پرتکرار.
   - **نقاط ورود Infra**: بارگذارهای اکسل در `app/infra/io_utils.py` و مصرف آن‌ها در زیرفرمان‌های CLI `allocate`/`rule-engine`. 【F:app/infra/cli.py†L1777-L1858】
   - **فایده**: کاهش خواندن Excel حجیم در هر اجرا، ثبات نوعی (۶ کلید)، آماده برای UI quick-run بدون نیاز به آپلود مجدد.
   - **طرح**: افزودن repository `student_repository.py` و `mentor_repository.py` در Infra که DataFrame نرمال‌شده را در جداول جدید (students_cache, mentor_pool_cache) نگه می‌دارند و با کلیدهای `student_id`/`mentor_id` + شاخص ۶ کلید ایندکس می‌شوند. Core همچنان DataFrame می‌گیرد.
   - **اثر/پیچیدگی/ریسک**: اثر ↑، پیچیدگی میانه، ریسک پایین (بدون تغییر policy). توصیه: اجرا اکنون.

2) **کش Snapshot تاریخچهٔ QA/Trace برای drill-down**
   - **دامنه**: Trace و QA خروجی هر run.
   - **نقاط ورود Infra/UI**: `history_store.log_allocation_run` و UI dialogهای تاریخچه (log panel/history_metrics) می‌توانند از snapshot پایدار استفاده کنند به‌جای بازسازی از Excel هر بار.
   - **فایده**: تشخیص سریع رگرسیون، امکان بازبینی trace بدون داشتن فایل Excel، بهبود observability.
   - **طرح**: افزودن جداول `trace_snapshots` و `qa_reports` در LocalDatabase با JSON فشرده (یا Parquet blob) و کلید run_id؛ repository `qa_trace_repository.py` در Infra برای درج/بازیابی؛ UI فقط متادیتا و DataFrame آماده را می‌گیرد.
   - **اثر/پیچیدگی/ریسک**: اثر میانه، پیچیدگی میانه، ریسک پایین. توصیه: اجرا اکنون.

3) **کش ورودی WordPress/Gravity Forms (intake)**
   - **دامنه**: خروجی نرمال‌شدهٔ فرم‌های ورودی قبل از تبدیل به DataFrame Core.
   - **نقاط ورود Infra**: آداپتور WordPress (انتظار در `app/infra` مطابق معماری) می‌تواند نتایج fetch را در جدول `forms_entries` نگه دارد و با مهر زمان/نسخه در اختیار Core/CLI قرار دهد.
   - **فایده**: جلوگیری از دانلود/پردازش تکراری، امکان replay و audit، کارایی در محیط‌های کم‌اتصال.
   - **طرح**: مخزن `forms_repository.py` با جداول forms_entries، forms_meta و اندیس روی `entry_id`/`created_at`; UI فرمان «sync forms» و CLI زیرفرمان `sync-forms` برای به‌روزرسانی کش.
   - **اثر/پیچیدگی/ریسک**: اثر میانه، پیچیدگی میانه، ریسک میانه (نیاز به سیاست نگهداشت حریم خصوصی). توصیه: اجرا پس از تأیید حریم خصوصی.

## کاندیداهای میان‌مدت / نیازمند تصمیم سیاستی
- **کش ManagerReport/CenterMapping**: در صورت وجود فایل‌های مدیریتی/مرکز پرتکرار، می‌توان DataFrame نرمال‌شده را در جدول manager_center_map ذخیره کرد. نیازمند شفاف‌سازی دربارهٔ تناوب تغییر داده و مالکیت منبع.
- **کش خروجی Exporter (ImportToSabt)**: ذخیرهٔ snapshot خروجی تخصیص برای مقایسهٔ سریع؛ باید با الزامات حریم خصوصی و اندازهٔ فایل سنجیده شود.

## ضدالگوها (عدم استفاده از SQLite)
- انتقال منطق رتبه‌بندی یا Trace به SQL (خلاف Core خالص و Policy-First).
- ذخیرهٔ حالت‌های نیمه‌نرمال‌شده که ممکن است با Policy/SSoT انحراف پیدا کند.
- اجرای mergeهای policy/SSoT در SQL به‌جای premap برداری در Infra/Core.

## الگوهای پیشنهادی برای هر کاندیدای قوی
- **مالکیت Schema**: تمام جداول جدید توسط `LocalDatabase` یا ماژول‌های repository تازه در `app/infra` تعریف می‌شوند؛ Core فقط DataFrame می‌گیرد.
- **API Core-facing**: متدهای `load_*` در مخازن جدید DataFrame با انواع صحیح (Int64 برای کلیدها) برمی‌گردانند و هیچ `sqlite3` وارد نمی‌شود.
- **UI/CLI Surfacing**: زیرفرمان‌های جدید (`import-students`, `import-mentors`, `sync-forms`) و پیام‌های وضعیت (آخرین زمان refresh، نبود کش) بدون نمایش SQL.
- **ایندکس‌ها**: برای جداول cache از کلیدهای طبیعی (`student_id`, `mentor_id`, `entry_id`) و ستون‌های join (`کد مدرسه`، `کدرشته`، ...) ایندکس ایجاد شود تا lookup سریع و پایدار بماند.

## توصیه‌های فوری
- پیاده‌سازی مخازن cache برای Student/Mentor و QA/Trace با جدول‌های مجزا؛ CLI را برای import/refresh و استفادهٔ پیش‌فرض از SQLite گسترش دهید.
- تعریف سیاست retention و privacy برای WordPress cache قبل از اجرا، سپس اضافه‌کردن sync/refresh.

