# Rule Engine Evaluation Plan (Policy v1.0.3 / SSoT v1.0.2)

## 1. Current Architecture Snapshot

### Eligibility Filters
- `apply_join_filters` در `app/core/common/filters.py` هفت فیلتر type→school را به‌ترتیب Policy اجرا می‌کند و ستون‌ها را مستقیماً از `PolicyConfig.stage_column` می‌خواند؛ مدرسه با سناریوهای wildcard/zero از `resolve_student_school_code` پشتیبانی می‌شود و sanitation جداکننده‌ها در `_sanitize_school_series` تضمین شده است.【F:app/core/common/filters.py†L1-L197】
- در `allocate_student` ابتدا `policy.join_keys` به `int` تبدیل می‌شوند (`_collect_join_key_map`) و سپس join map به `apply_join_filters` پاس داده می‌شود تا هر فیلتر بدون mutate اجرا شود.【F:app/core/allocate_students.py†L126-L200】【F:app/core/allocate_students.py†L230-L255】

### Ranking & Capacity
- `apply_ranking_policy` از سیاست بارگذاری‌شده، ستون‌های `occupancy_ratio`، `allocations_new` و `mentor_sort_key` را محاسبه و طبق ترتیب Policy (`min occ → min alloc → natural mentor_id`) با sort پایدار مرتب می‌کند؛ state اولیه با `build_mentor_state` و natural key داخلی فراهم می‌شود.【F:app/core/common/ranking.py†L1-L193】
- `allocate_student` پس از فیلتر ظرفیت (`capacity_mask`) حالت mentor را از `build_mentor_state` یا state تزریقی خوانده و با `consume_capacity` capacity_before/after و occupancy_ratio را در log ثبت می‌کند.【F:app/core/allocate_students.py†L256-L412】
- `allocate_batch` هر بار پس از تخصیص، state و DataFrame اصلی را sync می‌کند تا دترمینیسم و trace حفظ شود؛ همچنین capacity داخلی در پایان sanity-check می‌شود.【F:app/core/allocate_students.py†L414-L631】

### Policy Loader & Schema
- `PolicyConfig` در `app/core/policy_loader.py` تمام join_keys، ranking_rules، trace_stages و گزینه‌های excel/emission را از JSON می‌خواند، نسخه را validate می‌کند و از `_EXPECTED_JOIN_KEYS_COUNT == 6` اطمینان می‌گیرد؛ تمام توابع normalization (مانند `_normalize_finance_variants`) برای تطابق با SSoT پیاده‌سازی شده‌اند.【F:app/core/policy_loader.py†L1-L400】【F:app/core/policy_loader.py†L400-L800】

## 2. Fixed Rule Checklist (Actionable)
1. **شش کلید join**: `PolicyConfig.join_keys` باید دقیقاً ۶ مقدار منحصربه‌فرد داشته باشد و در `_collect_join_key_map` همگی به `int` تبدیل شوند؛ خطای `JoinKeyDataMissingError` هر کمبود را گزارش می‌کند.【F:app/core/policy_loader.py†L23-L120】【F:app/core/allocate_students.py†L126-L200】
2. **Trace هشت‌مرحله‌ای**: `trace_stages` از policy خوانده و در `build_trace_plan`/`build_allocation_trace` مصرف می‌شود؛ باید تضمین شود همه مراحل اجرا شده و candidate_count هر مرحله در explain trace ذخیره گردد.【F:config/policy.json†L53-L105】【F:app/core/allocate_students.py†L232-L247】
3. **Ranking Policy**: ترتیب `occupancy_ratio` → `allocations_new` → `mentor_sort_key` اجباری است و sort باید stable/natural باشد؛ failure باید در تست (tie scenarios) شکست بخورد.【F:config/policy.json†L34-L71】【F:app/core/common/ranking.py†L117-L193】
4. **Capacity و occupancy_ratio**: state باید همیشه non-negative باشد؛ `allocate_batch` پس از حلقه sanity-check انجام می‌دهد، اما تستی برای underflow state لازم است.【F:app/core/allocate_students.py†L526-L580】
5. **Policy-First / بدون I/O**: Eligibility و ranking فقط از `PolicyConfig` می‌خوانند؛ تست‌های `test_core_no_io.py` و `test_core_no_logging.py` این اصل را پوشش می‌دهند اما در golden test جدید نیز باید مراقبت شود.【F:tests/unit/test_core_no_io.py†L1-L120】
6. **Join key types و school wildcard**: `enforce_join_key_types` و `resolve_student_school_code` باید تضمین کنند صفر به‌عنوان wildcard فقط وقتی `school_code_empty_as_zero` فعال است، اعمال شود.【F:app/core/allocate_students.py†L200-L239】【F:app/core/common/filters.py†L141-L197】

## 3. Static Review Gaps & Instrumentation Needs
- **Trace Stage Counters**: Trace builder فعلی در `allocate_student` فراخوانی می‌شود ولی instrumentation stage-by-stage candidate_count ذخیره نمی‌کند؛ نیاز به hook (مثلاً callback یا DataFrame with counts) برای مقایسه با policy trace order دارد.【F:app/core/allocate_students.py†L232-L247】
- **Join Filter Coverage**: فیلترهای type و group هر دو از `policy.stage_column` استفاده می‌کنند؛ اگر Policy اشتباهاً هر دو را به `کدرشته` نگاشت کند، تست باید deviation را flag کند. پیشنهاد: instrumentation که mapping stage→column را log کند و در QA گزارش بدهد.
- **Capacity Drift**: `consume_capacity` فقط روی state عمل می‌کند؛ instrumentation برای capture قبل/بعد (مثلاً ثبت `capacity_gate` stage در trace) ضروری است تا mismatch با DataFrame اصلی سریع کشف شود.【F:app/core/common/ranking.py†L195-L212】【F:app/core/allocate_students.py†L490-L560】
- **Deterministic Ranking**: تست موجود `test_ranking_determinism.py` فقط سه ردیف را پوشش می‌دهد؛ سناریوهای برابری occupancy/allocations با mentor_idهای پیچیده (EMP-2 vs EMP-10 vs EMP-010) نیازمند instrumentation snapshot هستند.【F:tests/unit/test_ranking_determinism.py†L1-L21】

## 4. Test Strategy (Three Layers)
1. **Static Review / Lint Tests**
   - تست جدید `tests/unit/test_policy_ranking_contract.py`: بارگذاری policy و assert کند `len(join_keys)==6`, `ranking_rules == policy.json`, trace stages مطابق لیست Policy باشد. هر گونه divergence باید شکست بخورد.
   - تست `tests/unit/test_filters_trace_counts.py`: دادهٔ کوچک (۲ mentor) و student فرضی برای assert اینکه `apply_join_filters` به‌ترتیب اجرا می‌شود و خروجی هر stage با trace stage plan match است.
2. **Unit Tests**
   - `tests/unit/test_allocate_student_capacity_underflow.py`: state ساختگی با capacity=1، دو دانش‌آموز متوالی؛ نفر دوم باید خطای `CAPACITY_UNDERFLOW` در log ببیند و trace مرحله capacity را ثبت کند.【F:app/core/allocate_students.py†L355-L412】
   - `tests/unit/test_ranking_tie_breakers.py`: DataFrame با occupancy/allocations مساوی اما mentor_idهای مختلف (EMP-1, EMP-02, EMP-10) تا natural sort و stability را enforce کند.【F:app/core/common/ranking.py†L117-L193】
   - `tests/unit/test_join_key_int_enforcement.py`: ساخت student با رشته‌های متنی؛ انتظار می‌رود `_collect_join_key_map` همه را به int تبدیل کرده و در log ثبت کند (با fixture `allocate_student`).
3. **Golden / Integration Tests**
   - فایل `tests/integration/test_rule_engine_golden.py`: dataset ≈15 دانش‌آموز + 6 mentor با capacity مختلف. سناریوها: پرشدن ظرفیت mentor، gender mismatch، school wildcard، تفاوت رشته. خروجی نهایی باید در snapshot (`tests/snapshots/golden_allocations.json`) قفل شود با `pandas.testing.assert_frame_equal`.
   - Trace Golden: stage candidate counts ذخیره و در snapshot (`tests/snapshots/golden_trace.parquet`) مقایسه شود تا هر تغییری در policy فوراً دیده شود.
   - Determinism Golden: همان تست integration دو بار allocation را اجرا می‌کند و با `assert_frame_equal` خروجی‌ها را مقایسه و همچنین hash ساده (sha256) از allocations/logs محاسبه می‌کند.

## 5. Instrumentation & Tooling Proposal
- افزودن hook ساده به `apply_join_filters` (مثلاً پارامتر اختیاری `tracker: Callable[[str, int], None]`) تا پس از هر فیلتر تعداد کاندیدهای باقی‌مانده را گزارش کند؛ در production با trace ادغام می‌شود و در تست‌ها برای assert استفاده می‌شود.【F:app/core/common/filters.py†L185-L197】
- در `build_mentor_state` و `consume_capacity` شمارنده‌های capacity_before/after به event bus تزریق شوند یا حداقل به log برگردند تا golden test بتواند انحراف ظرفیت را flag کند.【F:app/core/common/ranking.py†L67-L212】
- سیاست trace columns باید توسط ابزار lint بررسی شود: اسکریپت QA (pytest) که policy.json را می‌خواند و مطمئن می‌شود `trace_stages` با `TRACE_STAGE_ORDER` هم‌پوشانی کامل دارد؛ در صورت تغییر Policy، تست نیازمند به‌روزرسانی است و به‌عنوان signal استفاده می‌شود.【F:app/core/policy_loader.py†L73-L120】

## 6. Golden Test Dataset Sketch
- **Mentors**: 6 ردیف با ترکیب gender/center/finance متفاوت، ظرفیت 1 تا 3. برخی mentorها occupancy اولیه ≠0 دارند تا `allocations_new` اثر بگذارد.
- **Students**: 15 ردیف که شامل:
  - 2 دختر و 2 پسر با یکسان بودن تمام join keys برای تست tie-break.
  - 3 دانش‌آموز با کد مدرسه ناقص/خط دار برای sanitation school.
  - 2 دانش‌آموز مرکز صفر (wildcard) برای تست skip center filter.
  - 4 دانش‌آموز که ظرفیت mentor منتخب آن‌ها بعد از تخصیص صفر می‌شود تا مسیر `CAPACITY_FULL` در نفر بعدی فعال گردد.
- خروجی‌های مورد انتظار:
  - جدول allocations با ستون‌های `student_id`, `mentor_id`, `occupancy_ratio`.
  - جدول logs با error_type و candidate_count؛ از snapshot برای تشخیص regression استفاده می‌شود.
  - جدول trace stage counts (8 مرحله) برای حداقل دو دانش‌آموز نماینده.

## 7. Execution Checklist
- اجرای `pytest -q tests/unit/test_policy_ranking_contract.py tests/unit/test_allocate_student_capacity_underflow.py tests/unit/test_ranking_tie_breakers.py` برای تست‌های واحد.
- اجرای `pytest -q tests/integration/test_rule_engine_golden.py` برای golden.
- هر تغییر Policy یا core باید snapshotها را به‌روزرسانی کند؛ شکست تست golden نشانهٔ deviation است.
- گزارش QA باید شامل: عبور همه تست‌ها، hash خروجی golden، و فهرست هر قاعدهٔ ثابت که پوشش داده شده است.

## 8. Rule Evaluation Table (Executed 82-student Scenario)

| rule_id | description | status | evidence |
| --- | --- | --- | --- |
| R1_JOIN_KEYS | شش کلید Join همیشه `int` هستند و کمبود داده گزارش می‌شود. | PASS | `tests/unit/test_rule_engine_policy_contract.py::test_policy_join_keys_unique_and_int_enforced` و instrumentation `stage_candidate_counts` در `app/core/allocate_students.py`. |
| R2_TRACE | Trace هشت‌مرحله‌ای و شمارش کاندید بعد از هر فیلتر ثبت می‌شود. | PASS | `tests/unit/test_allocate_stage_counts.py::test_stage_candidate_counts_align_with_trace` + مقایسه Trace/Log در `tests/integration/test_rule_engine_golden_realistic.py`. |
| R3_RANKING | ترتیب `min occupancy → min alloc → natural mentor_id` پایدار است. | PASS | `tests/unit/test_rule_engine_policy_contract.py::test_ranking_policy_respects_order_and_natural_sort`. |
| R4_CAPACITY | ظرفیت منفی و underflow شناسایی و به `CAPACITY_FULL` ختم می‌شود. | PASS | `tests/integration/test_rule_engine_golden_realistic.py::test_realistic_high_no_match_scenario_golden` (۶ خطای ظرفیت پس از پر شدن Mentor-A/B/C). |
| R5_SCHOOL_WILDCARD | مقدار صفر برای مدرسه به‌عنوان wildcard پذیرفته و sanitation درست است. | PASS | `tests/unit/test_rule_engine_policy_contract.py::test_school_code_zero_behaves_as_wildcard`. |
| R6_DETERMINISM | اجرای مجدد Golden Test خروجی‌های کاملاً یکسان می‌دهد. | PASS | دو اجرای پشت‌سرهم در `tests/integration/test_rule_engine_golden_realistic.py` با `assert_frame_equal` روی allocations/pool/log/trace. |

## 9. Golden Dataset Notes (82-Student High ELIGIBILITY_NO_MATCH)

- فایل `tests/integration/test_rule_engine_golden_realistic.py` دیتاست ۸۲ نفره را می‌سازد: Mentorها فقط کدرشته‌های مستند در پیوست الف (۱، ۳، ۵، ۷) را پوشش می‌دهند و مراکزشان صرفاً در بازهٔ مجاز `0/1/2` تعریف شده است؛ ۶۶ دانش‌آموز باقی‌مانده از کدرشته‌های معتبر دیگری مثل ۸، ۹، ۲۴، ۲۲ و ۲۵ استفاده می‌کنند تا کمبود عرضه در همان کدها به‌عنوان ریشهٔ ELIGIBILITY_NO_MATCH ثبت شود.
- یک ثابت `ALLOWED_GROUP_CODES` در همان تست مجموعهٔ کامل Appendix A را به‌صورت `frozenset({1,3,5,7,8,9,20,22,23,24,25,26,27,31,33,35,36,37})` نگه می‌دارد و پس از تولید دیتاست assert می‌کند که همهٔ `کدرشته`ها داخل این مجموعه هستند؛ به این ترتیب بازگشت سناریوهای جعلی 8xx یا مراکز خارج از `0/1/2` غیرممکن می‌شود.
- خروجی انتظار (۱۰ تخصیص موفق، ۶ ظرفیت پر، ۶۶ عدم انطباق) به‌صورت `expected_allocations` و asserts مشخص شده و با اجرای دوباره کنترل می‌شود.
- ستون `stage_candidate_counts` در لاگ‌ها به trace پیوند خورده تا در گزارش QA بتوان مرحلهٔ شکست هر دانش‌آموز را pinpoint کرد.
