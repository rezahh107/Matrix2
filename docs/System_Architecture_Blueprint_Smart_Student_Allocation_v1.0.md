# System Architecture Blueprint — Smart Student Allocation (v1.0)

- **Status:** Authoritative architecture (aligned with Vision & Scope v1.0, Policy v1.0.3, SSoT v1.0.2)
- **Audience:** Architects, tech leads, coding agents (AGENTS.md), QA, UI/Infra developers, product/ops owners
- **Policy Alignment:** Policy-First (no rule drift), 6 Join Keys as `int`, deterministic ranking, 8-step trace
- **AGENTS Alignment:** Repository AGENTS.md (agentsmd.net compliant) + Eligibility Matrix AGENTS.md scope; conflicts resolved by Policy/SSoT > Vision/Scope > AGENTS.md

## 1. Architectural Overview
- Goal: Deterministic, auditable allocation from WordPress/Excel intake to exporter outputs with PySide6 desktop shell.
- Style: Layered (Core / Infra / UI / Agents) with strict dependency arrows downward only.
- Determinism: Stable + natural sorts, seeded operations where applicable, no hidden state.
- Traceability: 8-step trace (`type`, `group`, `gender`, `graduation_status`, `center`, `finance`, `school`, `capacity_gate`) plus allocation reason, version stamps (`policy_version`, `ssot_version`).
- Interoperability: Excel-first pipeline, WordPress Gravity Forms intake, ImportToSabt exporter, filesystem-based artifacts.

## 2. Layered Architecture
- **Core Layer (Pure Logic)**
  - Responsibilities: Policy interpretation, eligibility matrix logic, allocation engine, ranking, trace generation, validation rules, deterministic transforms, pure DataFrames، و اجرای منطق تاریخچه/کانال شامل تابع خالص `dedupe_by_national_id` و محاسبهٔ `allocation_channel`.
  - Prohibitions: No file/network/Qt I/O, no WordPress/Excel handling, no hardcoded paths/dates, no Qt signals; pandas allowed for table logic.
  - Inputs: In-memory DataFrames, policy JSON, progress callback (function), pre-mapped reference data.
  - Outputs: In-memory DataFrames/records (eligibility matrix, allocation results, traces, error taxonomies).
- **Infra Layer (I/O & Adapters)**
  - Responsibilities: Excel read/write (atomic fallback), filesystem layout, WordPress intake fetch/normalize (Gravity Forms), ImportToSabt exporter, logging, configuration loading (`config/policy.json`), premap construction, schema validation، و نگهداری `HistoryStore` (بارگذاری/به‌روزرسانی تاریخچه تخصیص بر اساس کد ملی).
  - Prohibitions: Allocation logic, ranking changes, trace mutation beyond recording, UI widgets.
  - Inputs/Outputs: Filesystem paths, HTTP (WordPress), Excel sheets, JSON configs.
- **UI Layer (PySide6 Shell + Optional CLI)**
  - Responsibilities: Thin orchestration of Infra/Core, progress display, operator commands, run status, log viewing, file pickers.
  - Prohibitions: Business rules, Policy interpretation, data mutation beyond user intent, Core imports must be via Infra façade; no WordPress/Excel logic directly.
  - Interfaces: Calls Infra services, injects `progress(pct:int, msg:str)` into Core, subscribes to logs/traces.
- **Agents Layer (LLM/Codex & Automation)**
  - Responsibilities: Follow AGENTS.md (repo) + agentsmd.net standard, execute tasks within allowed scopes, maintain version alignment, avoid policy drift, run QA checklists.
  - Boundaries: Agents may touch layers only via defined APIs; no cross-layer shortcuts; must respect Core purity, Infra ownership of I/O, UI thinness.
- **External Systems**
  - WordPress + Gravity Forms (intake), Sabt/Hekmat via ImportToSabt exporter, OS filesystem, optional CLI environment.

## 3. Dependency Rules
- Allowed imports (directional): UI → Infra → Core. Agents orchestrate but do not redefine dependencies. External systems → Infra only. Core never imports Infra/UI/external I/O.
- Policy/SSoT loading: Infra loads `config/policy.json` (must match Policy v1.0.3, SSoT v1.0.2) and passes immutable structures into Core.
- Excel/WordPress: Only Infra handles Excel engines, sheet sanitization, HTTP/auth; Core receives normalized frames.
- Join Keys: Always 6-key int vector; Core validates; Infra enforces schema; UI never mutates keys.
- Ranking & Sorting: Implemented in Core; Infra/UI must not override ordering; stable + natural sort required.
- Trace: Generated in Core, persisted by Infra, displayed by UI; no mutation post-generation.

## 4. Data Flow (Intake → Matrix → Allocation → Export → UI → Logs)
```
[WordPress/Gravity Forms] --(Infra intake adapter)--\
[Excel Inputs] --------------------------------------> [Normalized DataFrames]
[HistoryStore] --(Infra reads snapshot)-------------/      |
                                                           |
                                              (Infra passes policy + data + history)
                                                           |
                                                       [Core]
  Normalize → Validate → Build Eligibility Matrix → Load history → `dedupe_by_national_id` → derive `allocation_channel` → Allocate → Trace → QA Checks
                                                           |
                                            results/traces DataFrames (pure)
                                                           |
                                                     [Infra]
   write_xlsx_atomic → ImportToSabt exporter → filesystem artifacts/logs
                                                           |
                                                         [UI]
     display progress/logs → operator actions → re-run/inspect
                                                           |
                                                   [Agents Layer]
     governance, QA checklists, version alignment, CI hooks
```

## 5. Canonical Module Map
- **Core** (pure logic modules)
  - `app/core/policy_loader.py`: validates `config/policy.json` against Policy/SSoT schema (no I/O beyond data received).
  - `app/core/common`: natural sort helper, join-key enforcement, error taxonomy.
  - `app/core/matrix`: eligibility matrix builder using Policy rules, deterministic filters، نرمال‌سازی کد ملی و آماده‌سازی داده برای HistoryStore/کانال.
  - `app/core/allocation`: ranking & assignment engine, 8-step trace creation, occupancy/allocations counters، اجرای `dedupe_by_national_id`, محاسبهٔ `allocation_channel`, و الحاق خلاصهٔ تاریخچه/کانال به trace.
  - `app/core/qa`: validation of matrix/allocation outputs vs Policy invariants.
- **Infra** (adapters & I/O)
  - `app/infra/io_utils.py`: Excel read/write (atomic fallback), filesystem paths, schema validation, premap construction، و قراردادهای خواندن/نوشتن برای `HistoryStore`.
  - `app/infra/intake_wordpress.py`: Gravity Forms fetch, schema mapping, numeric normalization of Join Keys.
  - `app/infra/exporter_importtosabt.py`: transforms allocation outputs to ImportToSabt schema.
  - `app/infra/logging.py`: structured logs, trace persistence, version stamping.
  - `app/infra/config_loader.py`: reads `config/policy.json`, enforces version and required fields.
- **UI**
  - `run_gui.py` + `app/ui/*`: PySide6 shells, dialogs, progress callbacks, log viewers; thin orchestrator calling Infra services.
  - `app/cli/*` (optional): CLI entrypoints invoking Infra pipelines.
- **Agents Layer**
  - AGENTS.md (repo root): global rules for agents (agentsmd.net compliant).
  - Eligibility Matrix AGENTS.md: subsystem rules (already existing) guiding matrix/validator/allocator agents.
  - CI/CD scripts: enforce policy version, run QA checklist, prevent forbidden dependencies.

### 5.1 مدیریت تاریخچه و کانال سیاست‌محور
- **HistoryStore (Infra):** دادهٔ پایداری است که برای هر `national_id_normalized` رکوردهایی شامل `mentor_id`, `allocation_date`, ۶ کلید Join، و متادیتای مدرسه/مرکز را نگه می‌دارد. این مخزن تنها توسط Infra خوانده/نوشته می‌شود و Core به نسخهٔ درون‌حافظه‌ای آن دسترسی دارد.
- **`dedupe_by_national_id` (Core):** تابعی خالص/برداری که ورودی دانش‌آموزان را با تاریخچه مقایسه می‌کند، آن‌ها را به `allocated_before` و `new_candidates` تقسیم می‌نماید و `dedupe_reason` را به trace اضافه می‌کند. نرمال‌سازی اعداد فارسی/لاتین و حذف نویز (dash، فاصله) بخشی از همین تابع است.
- **AllocationChannel (Core + PolicyConfig):** کانال SCHOOL / GOLESTAN / SADRA / GENERIC به کمک `AllocationChannelConfig` تعیین می‌شود و به خروجی‌های trace/export افزوده می‌گردد تا UI/گزارش‌ها بتوانند جریان‌های مدرسه‌ای، مراکز گلستان/صدرا و مسیر GENERIC را جداگانه پایش کنند. هیچ شناسه‌ای در Core هاردکد نمی‌شود؛ همهٔ قواعد از PolicyConfig تغذیه می‌شوند.

### 5.2 MentorProfile و حاکمیت استخر پشتیبان‌ها (الزامات برنامه‌ریزی‌شده)
- **Domain Model (Planned):** علاوه‌بر ظرفیت و نگاشت گروه‌های join-key، هر پشتیبان/مدیر دارای شئ «MentorProfile» در SSoT خواهد بود که شامل `mentor_id`, مدیر مربوطه، ظرفیت اعلام‌شده، داده‌های وضعیت تخصیص و معیارهای رتبه‌بندی (مثلاً `allocations_new`) و فیلد `mentor_status` می‌شود. وضعیت فعلاً دو مقدار اصلی `ACTIVE` و `FROZEN` دارد و می‌تواند با الگوهای محدودکنندهٔ `RESTRICTED_*` توسعه یابد.
- **Governance & Storage (Planned):** MentorProfile در همان منبع Policy (مثلاً policy.json یا پروفایل مجزا زیر کنترل Infra) نگهداری می‌شود و هر تغییر UI/CLI باید با نسخه‌گذاری و audit log ذخیره شود. هیچ تغییری در استخر نباید صرفاً با حذف ردیف از InspactorReport اعمال شود؛ Infra موظف است چنین تغییراتی را rejected/flagged کند تا Policy-First نقض نشود.
- **Infra Responsibilities (Planned):**
  - نگهداری و بارگذاری MentorProfile از policy.json یا فایل پروفایل مجزا (همچنان تحت SSoT). تغییرات UI را به همان منبع برمی‌گرداند.
  - ادغام پروفایل‌ها با دادهٔ InspactorReport پیش از تحویل به Core و تهیهٔ گزارش‌های تحلیلی (مثلاً پیشنهاد فریز بر اساس HistoryStore) بدون اعمال خودکار.
- **Core Responsibilities (Planned):**
  - مرحلهٔ صریح «BuildMentorPool» قبل از ساخت ماتریس را اجرا می‌کند؛ فقط پروفایل‌های `ACTIVE` وارد می‌شوند و پروفایل‌های `FROZEN` ظرفیت صفر تلقی شده و حذف می‌شوند.
  - محدودیت‌های `RESTRICTED_*` در صورت تعریف، به‌عنوان قید eligibility هنگام ساخت ماتریس اعمال می‌شوند نه به‌عنوان استثناء رتبه‌بندی؛ ۶ کلید join و سیاست رتبه‌بندی ثابت می‌مانند.
- **UI Responsibilities (Planned):**
  - یک پنل PySide6/CLI برای «مدیریت استخر پشتیبان‌ها» نمایش می‌دهد که در آن اپراتور می‌تواند وضعیت mentor_status را مشاهده و میان `ACTIVE`/`FROZEN` (و حالت‌های محدود) جابه‌جا کند.
  - این پنل همچنین پیشنهادهای HistoryStore (مثلاً «این پشتیبان در سال جاری سهم خود را پر کرده است») را نشان می‌دهد؛ تصمیم نهایی و اعمال‌شدن فقط زمانی معتبر است که تغییر در SSoT ذخیره شود و در خروجی trace/گزارش عملیات یک رویداد «status_changed» ثبت شود.
- **HistoryStore & AllocationChannel Interaction (Planned):** HistoryStore صرفاً ورودی تحلیلی/پیشنهادی است و خودکار کسی را فریز نمی‌کند؛ AllocationChannel جریان دانش‌آموز را تعیین می‌کند و هیچ‌گاه mentor_status را override نمی‌کند. Core تنها به MentorProfile/Policy برای تصمیم ورود یا خروج از استخر تکیه دارد و در نتیجه رفتار سیستم قابل بازتولید باقی می‌ماند. این رفتارها هنوز در کد مستقر نشده‌اند و به‌عنوان الزام آینده مستند شده‌اند تا از سردرگمی QA/توسعه جلوگیری شود.

## 6. Dependency Graph (ASCII)
```
External (WP, Excel, FS) --> Infra --> Core
                                ^       ^
                                |       |
                             UI shell   |
                                ^       |
                             Agents ----+
```
- Arrows show allowed direction. Reverse imports are forbidden.

## 7. Determinism & Policy-First Enforcement
- Determinism: same inputs + policy → same outputs; stable/natural sort; seeded randomness disallowed; premap constructed once outside loops; no inplace pandas operations.
- Policy-First: Core reads only provided policy structures; no embedded constants for Join Keys/ranking; Infra ensures `config/policy.json` contains version `1.0.3` and 6 Join Keys (int).
- Traceability: 8-step trace emitted per student with candidate counts; retained by Infra; UI displays without mutation.
- Performance Budget: 10k rows ≤ 60s, 100k ≤ 5m, RAM ≤ 2GB; avoid repeated merges; use premap.

## 8. Workflow Lifecycle (Allocation Job)
1. **Intake**: Infra pulls Gravity Forms/WordPress submissions and Excel files; normalizes fields; coerces Join Keys to `int`; runs schema QA.
2. **Policy Load**: Infra reads `config/policy.json`; validates against Policy/SSoT schema; passes immutable config to Core.
3. **Matrix Build**: Core constructs eligibility matrix (filters per Policy §§3–9) using stable ordering; records intermediate counts.
4. **History Snapshot & Dedupe**: Infra loads HistoryStore snapshot; Core runs `dedupe_by_national_id` to split `allocated_before` و `new_candidates`, ثبت `dedupe_reason` در trace، و آماده‌سازی ورودی کانال‌ها.
5. **Channel Derivation & Allocation**: Core با `AllocationChannelConfig` مقدار `allocation_channel` را به ازای هر دانش‌آموز تعیین می‌کند و سپس همان رتبه‌بندی ثابت (`occupancy_ratio` → `allocations_new` → `mentor_id`) را اجرا می‌کند؛ trace/summary کانال‌محور تولید می‌شود.
6. **QA/Validation**: Core runs invariant checks; Infra logs results; Agents/CI apply QA checklist.
7. **Export & History Update**: Infra writes Excel artifacts via `write_xlsx_atomic`; converts to ImportToSabt; sanitizes sheet names; version stamps outputs؛ رکوردهای موفق جدید به HistoryStore به‌شکل اتمیک/مقاوم اضافه می‌شود (هم‌راستا با FR-EXPORT-01). اگر درج HistoryStore پس از موفقیت ImportToSabt/Excel شکست بخورد، Infra اجرای تخصیص را failed علامت می‌زند، خروجی را به UI/Core برنمی‌گرداند، حداقل سه تلاش مجدد با backoff انجام می‌دهد و تا بازیابی کامل، تحویل artifact به اپراتور را مسدود می‌کند تا HistoryStore و خروجی منطبق باقی بمانند.
8. **UI Orchestration**: PySide6 shell triggers pipeline, shows progress via injected callback, surfaces traces/logs; optional CLI mirrors flow.
9. **Audit & Governance**: Agents validate outputs, ensure AGENTS.md compliance, update meta reports; SupervisorAgent checks version drift؛ trace/کانال/تاریخچه برای تحلیل بعدی بایگانی می‌شود.

## 9. Extension & Plugin Model
- Safe extensions (must respect dependency rules):
  - Infra adapters for new intake/export formats (e.g., additional Excel sheets, CSV, REST) that feed normalized frames into Core.
  - UI widgets for new operator views (logs, QA summaries) that consume Infra outputs.
  - Agents automations (CI checks, lint rules) enforcing policy versions and dependency constraints.
- Unsafe/forbidden: Changing Join Keys, altering ranking policy, adding I/O inside Core, bypassing Infra for Excel/WordPress, emitting Qt signals from Core, mutating traces post-Core.

## 10. Relationship to Vision/Scope, Policy, SSoT, AGENTS.md
- **Policy v1.0.3 & SSoT v1.0.2:** authoritative for rules (Join Keys, ranking, trace steps, error taxonomy). Architecture never overrides them.
- **Vision & Scope v1.0:** defines product boundaries, phases, quality attributes; this blueprint instantiates the system design to satisfy those boundaries.
- **AGENTS.md (repo root + Eligibility Matrix):** operational rules for agents; architecture embeds them as governance for coding agents and CI. If conflicts occur: Policy/SSoT > Vision/Scope > AGENTS.md.
- **Agentsmd.net Standard:** ensures AGENTS.md structures remain parse-friendly for LLM agents; architecture mandates adherence for any new AGENTS.md.

## 11. Governance for Agents (Agents Layer)
- Agents must:
  - Read applicable AGENTS.md scopes before touching files; respect Core/Infra/UI boundaries.
  - Use deterministic commands (no `ls -R`), follow test commands in AGENTS.md, and include policy_version/ssot_version in outputs when required.
  - Avoid policy drift: no code changes that contradict Policy/SSoT; raise warnings if policy mismatch detected.
  - Maintain traceability: cite files/commands in PRs; ensure QA checklist passes; keep dependency graph intact.
- Supervisory controls: CI checks for forbidden imports, policy version mismatches, missing trace steps, and absence of stable sorts.

## 12. UI/Infra/Core Boundary Rules (Allowed Imports)
- UI may import: Infra service APIs, UI widgets/utilities. Must not import Core directly unless via Infra façade types.
- Infra may import: Core APIs, standard libs, pandas, Excel engines, HTTP clients. Must not import UI or Qt types inside Infra Core-facing modules.
- Core may import: standard libs, pandas for table logic, shared `common` helpers (natural sort). Must not import Infra/UI/Qt/I/O libs.
- Shared constants: Join Keys and ranking read from policy config, not duplicated across layers.

## 13. Data Contracts & Schemas
- Join Keys: `کدرشته`, `جنسیت`, `دانش آموز فارغ`, `مرکز گلستان صدرا`, `مالی حکمت بنیاد`, `کد مدرسه` — all `int`.
- Policy config (`config/policy.json`): must include `version=1.0.3`, `join_keys` array above, `ranking` sequence, status lists; Infra validates.
- Gravity Forms/WordPress: field mappings stored in Infra; numeric normalization and crosswalks apply before Core.
- Excel artifacts: Infra owns sheet naming sanitization, atomic writes, engine selection; Core only consumes/produces DataFrames.

## 14. Logging, Traceability, and Audit
- Trace: 8-step counts recorded in Core; Infra persists to markdown/Excel; UI renders read-only.
- Logging: Structured with version stamps, input hashes (optional), and timing; no personally sensitive data beyond policy scope.
- Progress: `progress(pct:int, msg:str)` injected from UI/CLI; Core calls it but never emits Qt signals.

## 15. Risk Controls and Testing Hooks
- Risks: schema drift, policy mismatch, deterministic ordering regression, capacity miscounts, agent non-compliance.
- Controls: schema validators in Infra; dependency lint rules; unit tests for natural sort, join-key coercion, trace completeness; integration tests for intake→allocation→export.
- Performance controls: avoid repeated merges by using premap; ensure stable sorts; avoid pandas inplace.

## 16. Future Evolutions (Non-breaking unless re-approved)
- Additional exporters/importers via Infra plugins; UI enhancements for dashboards; automated WordPress-to-allocator pipelines with human approval; multi-repo AGENTS.md federation.
- Breaking changes require Policy/SSoT updates and version bumps; Join Keys/ranking/trace steps are immutable unless Policy changes.

## 17. Glossary
- **Core/Infra/UI/Agents:** architectural layers defined above.
- **Eligibility Matrix:** Policy-driven table gating eligibility per student.
- **Allocation Trace:** 8-step candidate counts + decision reason per student.
- **ImportToSabt:** Export format for downstream Sabt/Hekmat.
- **Premap:** One-time mapping (e.g., mentor support code → employee code) built in Infra, reused in Core.
- **Natural Sort:** Ordering using numeric-aware keys for `mentor_id` tie-breakers, stable.
- **Policy-First:** No hardcoded rules in code; all come from policy config.
