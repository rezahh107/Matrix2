# Smart Student Allocation System — Vision & Scope (v1.0)

- **Status:** Draft
- **Related Documents:** SSoT v1.0.2; Policy v1.0.3; Eligibility Matrix AGENTS.md (v1.0.3); repository-level AGENTS.md (agentsmd.net-aligned); Phase 0 System Requirements
- **Audience:** Product owner, tech lead/architect, QA/release, desktop operator/mentor admin, WordPress/Gravity Forms maintainer, coding agents (Codex/LLM) following AGENTS.md

## Product Vision
- Provide a deterministic, auditable, Policy-First allocation system that assigns students to mentors fairly, reuses capacity efficiently, and produces explainable outputs.
- Replace fragile manual allocations with reproducible, traceable decisions while retaining strong Excel interoperability for local workflows.
- Serve administrators/operators (desktop UI), mentors/patrons, and downstream systems (e.g., Sabt/Hekmat) through consistent imports/exports and clear audit trails.
- Success means: same inputs → same outputs; every allocation has a reason trail; capacity usage and fairness follow Policy; humans and agents can navigate consistent specs (Policy/SSoT, Vision/Scope, AGENTS.md).

## System Context
Major components and flow:
- **WordPress + Gravity Forms intake:** collects student/mentor data in controlled schemas.
- **Eligibility Matrix builder:** normalizes inputs and builds the matrix per Policy.
- **Allocation engine:** ranks/assigns students to mentors deterministically.
- **Exporter:** emits ImportToSabt and other downstream-friendly formats.
- **PySide6 desktop app/UI shell:** orchestrates runs, surfaces status, and shows logs.
- **Logging & audit layer:** records trace (8-step) and allocation explanations.

Data flow (conceptual):
```
Intake (Gravity Forms) → Normalization/Matrix Builder → Allocation Engine → Trace/Audit → Exporter (ImportToSabt & others) → Downstream systems
                                     ↘ Desktop UI (PySide6) orchestration ↗
```

### حافظهٔ تخصیص و کانال‌های جریان دانش‌آموز
- **حافظهٔ تخصیص (HistoryStore):** زیرساخت، تاریخچهٔ تخصیص‌های موفق را با کلید طبیعی «کد ملی نرمال‌شده» نگه می‌دارد تا سیستم در هر اجرا بداند چه دانش‌آموزی قبلاً منتور گرفته است. این حافظه بخشی از تعهد شفافیت است و بدون تغییر در ۶ کلید Join یا سیاست رتبه‌بندی، ورودی‌های جدید را با گذشته می‌سنجد.
- **تشخیص تکرار با `dedupe_by_national_id`:** Core پیش از ورود به الگوریتم رتبه‌بندی، دانش‌آموزان را بر اساس کد ملی به دو دستهٔ «قبلاً تخصیص‌گرفته» و «کاندید جدید» تقسیم می‌کند؛ در نتیجه دانش‌آموزی که در تاریخچه وجود دارد دوباره وارد صف allocation نمی‌شود مگر اینکه سیاست اجازه دهد و علت در trace ثبت می‌شود.
- **کانال‌های تخصیص (AllocationChannel):** سیاست، هر دانش‌آموز را به یکی از چهار کانال SCHOOL / GOLESTAN / SADRA / GENERIC هدایت می‌کند تا جریان‌های مدرسه‌ای، مراکز گلستان/صدرا و مسیر عمومی متمایز و قابل توضیح باشند. کانال‌ها از PolicyConfig/AllocationChannelConfig خوانده می‌شوند و صرفاً مسیر اجرای همان رتبه‌بندی ثابت را مشخص می‌کنند.
- **Explainability:** گزارش‌های trace و خلاصهٔ مدیریتی باید نشان دهند دانش‌آموز در کدام کانال پردازش شده و آیا به دلیل تاریخچه از صف اصلی خارج شده است؛ این رفتار افزوده به شفافیت کمک می‌کند بدون اینکه اساس رتبه‌بندی یا ظرفیت را تغییر دهد.

### حاکمیت استخر پشتیبان‌ها (Mentor Pool Governance)
- **MentorProfile/mentor_status:** هر پشتیبان/مدیر یک پروفایل سیاستی دارد که در SSoT/Policy نگهداری می‌شود و حداقل شامل `mentor_id`, ظرفیت‌های اعلام‌شده و وضعیت `mentor_status` است. این وضعیت می‌تواند `ACTIVE` (در دسترس برای استخر عمومی)، `FROZEN` (خارج از استخر حتی اگر در InspactorReport دیده شود) یا در آینده حالت‌های محدود مانند `RESTRICTED_*` باشد.
- **Policy-First نه Excel-first:** حذف یا اضافه کردن افراد به استخر به‌صورت رسمی در Policy/SSoT ثبت می‌شود و Core فقط از روی همین داده‌ها تصمیم می‌گیرد؛ دستکاری دستی ردیف‌ها در InspactorReport فقط یک راهکار اضطراری است و مسیر اصلی محسوب نمی‌شود.
- **پوشش سناریوهای خاص:** اگر پشتیبانی فقط برای یک مدرسه/کانال خاص مجاز باشد، Policy می‌تواند پروفایل او را با الگوی `RESTRICTED_*` تعریف کند تا Core او را تنها در همان قیدها ببیند و از ورود او به استخر عمومی جلوگیری شود.
- **همگام با HistoryStore و AllocationChannel:** پیشنهادهای فریزشدن یا محدودسازی بر اساس الگوهای تاریخی در HistoryStore می‌تواند توسط انسان بررسی و سپس در MentorProfile ثبت شود؛ AllocationChannel صرفاً جریان دانش‌آموز را مشخص می‌کند و جایگزین mentor_status نیست. این فیلتر قبل از ساخت ماتریس و بدون دست‌کاری ۶ کلید Join یا سیاست رتبه‌بندی اعمال می‌شود تا دترمینیسم حفظ شود.
- **هدف تجاری:** این مکانیزم تضمین می‌کند پشتیبان‌هایی که دورهٔ مأموریت‌شان تمام شده یا فقط در شرایط خاص کمک می‌کنند، به شکل شفاف و قابل‌ردیابی از استخر عمومی کنار گذاشته شوند و اپراتورها مجبور نباشند قبل از هر اجرا فایل‌های Excel را ویرایش کنند.
- **UI/CLI رسمی برای مدیریت:** پنل «مدیریت استخر پشتیبان‌ها» در PySide6 و CLI باید همین MentorProfile/Policy را نمایش و ویرایش کند، یعنی هر تغییری که اپراتور اعمال می‌کند فوراً در SSoT ذخیره شود و اجرای بعدی Core دقیقاً همان وضعیت را مصرف کند. مسیر غیررسمی (Excel edit) فقط در صورت بروز بحران قابل استفاده است و باید در گزارش عملیات ثبت شود تا جلوی بازگشت ناخواستهٔ افراد فریز‌شده گرفته شود.
- **دترمینیسم و قابلیت ردیابی:** ورود یا حذف هر Mentor از استخر باید در trace/گزارش مدیریتی مشخص باشد (مثلاً ستون وضعیت یا پیغام «FROZEN by policy») تا در ممیزی بعدی بتوان نشان داد که تصمیم بر اساس Policy بوده است. این الزام مانع از بازگشت ناخواستهٔ افراد فریز شده در importهای بعدی می‌شود.

## Scope & Phases
### Overall Scope
Unified Smart Student Allocation ecosystem covering intake, matrix, allocation, audit, export, and operator UI, governed by Policy/SSoT and executed by agents following AGENTS.md.

### Phase 0 (foundation / existing)
- In-scope: Eligibility Matrix builder; initial allocation logic; basic Excel I/O; manual/WordPress intake alignment; core ranking policy; initial trace logging.
- Out-of-scope for Phase 0: advanced UI flows; automated Sabt integration; multi-center orchestration; web self-service.

### Phase 1 (MVP for unified system)
- In-scope: deterministic allocation with full 8-step trace; PySide6 desktop orchestration; Gravity Forms intake alignment; importer/exporter for Excel + ImportToSabt; clear AGENTS.md references; policy/SSoT version binding; QA checklists.
- Not in Phase 1: autoscaling multi-tenant deployments; complex analytics dashboards; fully automated WordPress-to-allocator pipelines without operator review.

### Phase 2+ (future, not required now)
- Candidate scope: richer dashboards/monitoring; automated policy change governance; high-scale web/CLI entrypoints; deeper downstream integrations (live APIs); advanced anomaly detection; multi-repo AGENTS.md federation.
- Explicitly out-of-scope until approved: changing ranking policy, changing 6 Join Keys, relaxing determinism or Core/Infra/UI separations.

## Invariants & Constraints
- **6 Join Keys (int, immutable):** `کدرشته`, `جنسیت`, `دانش آموز فارغ`, `مرکز گلستان صدرا`, `مالی حکمت بنیاد`, `کد مدرسه`; used consistently across intake, matrix, allocation, exporter.
- **Ranking policy (Policy §10.1):** sort by `occupancy_ratio` → `allocations_new` → `mentor_id` (natural + stable); deterministic ties.
- **حافظهٔ تخصیص:** تاریخچهٔ تخصیص‌ها با کلید ملی نگهداری می‌شود و قبل از ورود به رتبه‌بندی اعمال می‌شود؛ این مکانیسم صرفاً ورودی صف را مدیریت می‌کند و در هستهٔ سیاست رتبه‌بندی دخالتی ندارد.
- **کانال‌های تخصیص:** مقدار `allocation_channel` برای هر دانش‌آموز به‌صورت سیاست‌محور و بدون هاردکد مرکز/مدرسه تعیین می‌شود و به‌عنوان متادیتای trace/خلاصه نگه داشته می‌شود.
- **Determinism:** same inputs/policy versions yield the same outputs; stable sorts; explicit version tagging (policy_version, ssot_version, schema versions).
- **Policy-First boundaries:** Core has no I/O or Qt; Infra handles Excel/WordPress/filesystem; UI is PySide6-only for desktop, Gravity Forms-only for intake; CLI/web entrypoints wrap Infra/Core without redefining policy.
- **Explainability:** 8-step trace (`type`, `group`, `gender`, `graduation_status`, `center`, `finance`, `school`, `capacity_gate`) with candidate counts and allocation reasons per student; audit logs retained.
- **Data contracts:** Excel schemas and Gravity Forms fields must align with SSOT/Policy; Join Keys remain integer; crosswalks/premap and natural sort respected; no hardcoded dates/paths in Core.
- **Safe variability:** UI layout wording, minor Excel formatting, or operational scripts may evolve if they do not violate Policy/SSoT, join keys, ranking, determinism, or Core/Infra/UI separation.

## Quality Attributes
- **Determinism & reproducibility:** stable/natural sorts; seeded runs if needed; explicit policy_version/ssot_version in outputs.
- **Auditability & explainability:** per-student trace, status/reason codes, versioned logs; 8-step trace completeness.
- **تاریخچه و کانال‌محوری:** خروجی‌ها باید نشان دهند چه دانش‌آموزی از تاریخچه حذف شده یا در چه کانالی پردازش شده است؛ این اطلاعات برای یادگیری‌های بعدی و تحلیل انحراف‌ها ضروری است و در کنار trace اصلی ذخیره می‌شود.
- **Performance:** typical batches (10k students) under budget; avoid repeated merges; premap mentor code mapping; no pandas inplace.
- **Robustness to data issues:** Persian/number normalization, mobile/ID validation per policy, resilience to crosswalk drift with clear errors.
- **Maintainability:** clear separation of concerns; AGENTS.md for agent operations; SSoT/Policy for domain rules; this Vision/Scope for product boundaries; modular roles for agents.

## Relationship to SSoT, Policy, and AGENTS.md
- **SSoT v1.0.2 + Policy v1.0.3:** define domain semantics, join keys, ranking, trace steps, and rule constraints—the authoritative rule set.
- **Vision & Scope (this document):** defines why and what the product delivers, phase boundaries, invariants, and quality expectations; it does not redefine rules.
- **AGENTS.md (agentsmd.net compliant):** defines how coding agents operate (repo navigation, coding conventions, test commands, Core vs Infra vs UI boundaries); Eligibility Matrix AGENTS.md adds component-specific guidance. AGENTS.md must stay consistent with this Vision/Scope and Policy; if conflicts arise, Policy/SSoT win, then Vision/Scope, then AGENTS.md.
- **Evolution:** Vision/Scope changes only with product direction shifts; AGENTS.md can evolve operationally but must never contradict Policy/SSoT/this document; policy updates require re-alignment and version bumps in all artifacts.

## Risks, Assumptions, and Open Questions
- **Risks:** Excel template drift; Gravity Forms schema changes; crosswalk misalignment; policy/SSoT divergence; insufficient audit retention; deterministic sorting accidentally broken; performance regressions on large batches.
- **Assumptions:** Admins control form/schema changes; Join Keys remain integer and consistent; limited, known center list; desktop operators review pipelines; downstream ImportToSabt formats stay stable.
- **Open Questions:** Governance model for multiple AGENTS.md files (global vs component-specific); final ownership of policy updates and rollout cadence; exact boundary between desktop orchestration and WordPress automation in later phases; handling of future API-based downstream consumers.

## Glossary
- **Student:** person to be allocated; source from intake/Excel.
- **Mentor/پشتیبان:** capacity provider; may have allocations_new and occupancy_ratio.
- **Eligibility Matrix:** matrix built from normalized inputs per Policy to gate eligibility.
- **Allocation:** assignment of a student to a mentor using ranking policy and constraints.
- **ImportToSabt:** exporter format for downstream systems (e.g., Sabt/Hekmat).
- **SSoT:** single source of truth document (v1.0.2) describing canonical data/policy.
- **Policy:** governing rules (v1.0.3) including join keys, ranking, trace steps.
- **AGENTS.md:** agent-facing operational guide aligned with agentsmd.net; includes Eligibility Matrix-specific and repo-level instructions.
- **Join Keys:** six integer keys linking data across sources: `کدرشته`, `جنسیت`, `دانش آموز فارغ`, `مرکز گلستان صدرا`, `مالی حکمت بنیاد`, `کد مدرسه`.
- **occupancy_ratio:** capacity usage metric used in ranking (lower is better).
- **allocations_new:** count of new allocations per mentor used in ranking (lower is better).
