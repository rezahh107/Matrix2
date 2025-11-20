# History / Allocation Channel / Binding / Governance Implementation Status

This file captures the current implementation status (code vs. docs) for history-aware allocation, allocation channels, mentor school binding, mentor-pool governance, and QA across Core/Infra/UI. It reflects **Policy v1.0.3** and **SSoT v1.0.2** assumptions without redefining them.

## Summary Matrix

| feature_id | scope | status | core_modules | infra_modules | ui_modules | tests | docs/notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| HIST_01 | History dedupe & tagging | implemented | `app/core/allocation/dedupe.py` (`dedupe_by_national_id`, `HistoryStatus`, `HISTORY_SNAPSHOT_COLUMNS`) | n/a | n/a | `tests/core/allocation/test_dedupe_by_national_id.py` | Tags `history_status`/`dedupe_reason` plus history snapshot columns. |
| HIST_02 | Allocation channel + history flags in pipeline | implemented | `app/core/allocation/trace.py` (`attach_allocation_channel`, `attach_history_flags`, `attach_history_snapshot`, `attach_same_history_mentor`); `app/core/allocation/engine.py` (`enrich_summary_with_history`) | CLI: `app/infra/cli.py` (enrich + logging); Excel: `app/infra/excel/export_allocations.py` (`collect_trace_debug_sheets`) | Integrated button/dialog in `app/ui/main_window.py` + `app/ui/history_metrics.py` for viewing metrics | `tests/core/allocation/test_history_same_mentor.py`; `tests/core/allocation/test_allocation_channel.py` | Channels derived from policy; history flags carried into summary/trace. |
| HIST_03 | History metrics (Core/CLI/Excel/UI) | implemented | `app/core/allocation/history_metrics.py` (`compute_history_metrics`) | CLI logger `_log_history_metrics` and Excel sheet via `collect_trace_debug_sheets` | History metrics model/panel/dialog wired in main window | `tests/core/allocation/test_history_metrics.py`; `tests/infra/test_history_metrics_logging.py`; `tests/infra/excel/test_history_metrics_export.py`; `tests/ui/test_history_metrics.py` | Metrics include counts and same-history-mentor ratio per allocation_channel. |
| POOL_01 | Mentor pool governance (status-driven filtering) | missing | — | — | — | — | Docs describe MentorProfile/mentor_status, but no build/pool filtering exists in code. |
| POOL_02 | Mentor pool governance UI (enable/disable mentors/managers) | implemented | — | CLI: `_ui_overrides` → `mentor_pool_overrides` in `app/infra/cli.py` | `app/ui/main_window.py` (toolbar + allocate form), `app/ui/mentor_pool_dialog.py` | `tests/ui/test_mentor_pool_dialog.py`, `tests/ui/test_main_window_mentor_pool_integration.py` | Button/action opens governance dialog; overrides flow into CLI without changing Policy/SSoT. |
| BIND_01 | MentorSchoolBindingPolicy (global vs restricted) | implemented | `app/core/policy_loader.py` (`MentorSchoolBindingPolicy`); `app/core/build_matrix.py` (`collect_school_codes_from_row`, binding defaults); `app/core/common/filters.py` (`filter_by_school`) | — | — | `tests/unit/test_school_binding.py`; QA coverage in `tests/core/qa/test_invariants_join_and_school.py` | Empty/zero tokens → global; restricted rows require valid school code. |
| QA_01 | QA invariants engine | implemented | `app/core/qa/invariants.py` (`QaViolation`, `QaRuleResult`, `QaReport`, `run_all_invariants`, rules STU_01/STU_02/JOIN_01/SCHOOL_01/ALLOC_01`) | CLI invokes QA in `app/infra/cli.py` | — | `tests/core/qa/test_invariants_students.py`; `tests/core/qa/test_invariants_join_and_school.py` | QA enforced post-build/allocation with deterministic rule set. |
| QA_02 | QA Excel export (matrix_vs_students_validation.xlsx) | missing | — | — | — | — | Docs mention QA export, but no Infra exporter or tests exist. |

## Feature Details

### HIST_01 — dedupe_by_national_id & history tagging
- **Status:** implemented.
- **Core:** `app/core/allocation/dedupe.py` normalizes national codes, splits `already_allocated` vs `new_candidates`, and attaches `history_status`, `dedupe_reason`, and history snapshot columns (`history_mentor_id`, `history_center_code`).
- **Tests:** `tests/core/allocation/test_dedupe_by_national_id.py` covers deterministic splitting, idempotency, and tagging.
- **Gaps:** none observed in pipeline coverage.

### HIST_02 — allocation_channel & history flags through pipeline
- **Status:** implemented.
- **Core:** `attach_allocation_channel`, `attach_history_flags`, `attach_history_snapshot`, and `attach_same_history_mentor` (all in `app/core/allocation/trace.py`) are orchestrated by `enrich_summary_with_history` in `app/core/allocation/engine.py` to add channel and history fields to summary/trace data.
- **Infra:** CLI (`app/infra/cli.py`) calls `enrich_summary_with_history` during allocation and passes history info into trace attributes; Excel exporter (`app/infra/excel/export_allocations.py`) consumes the same to build debug sheets.
- **Tests:** `tests/core/allocation/test_history_same_mentor.py` exercises enrichment and same-mentor flag; `tests/core/allocation/test_allocation_channel.py` validates channel derivation and attachment.
- **Gaps:** none detected for pipeline wiring.

### HIST_03 — history metrics (Core/CLI/Excel/UI)
- **Status:** implemented.
- **Core:** `compute_history_metrics` in `app/core/allocation/history_metrics.py` aggregates counts per `allocation_channel`, including `same_history_mentor` ratios.
- **Infra:** `_log_history_metrics` in `app/infra/cli.py` logs metrics; `collect_trace_debug_sheets` in `app/infra/excel/export_allocations.py` emits a `HistoryMetrics` sheet using the same computation.
- **UI:** `HistoryMetricsModel`, `HistoryMetricsPanel`, and `HistoryMetricsDialog` in `app/ui/history_metrics.py` are hooked into `app/ui/main_window.py` for display during allocation runs.
- **Tests:** Core (`tests/core/allocation/test_history_metrics.py`), Infra logging (`tests/infra/test_history_metrics_logging.py`), Infra Excel (`tests/infra/excel/test_history_metrics_export.py`), and UI (`tests/ui/test_history_metrics.py`) cover data, export, and presentation behavior.
- **Gaps:** none.

### POOL_01 — policy-driven mentor pool filtering (mentor_status/governance)
- **Status:** missing.
- **Findings:** No `mentor_status`/governance flags appear in Core/Infra/UI. Build matrix and allocation operate without filtering mentors by active/frozen status, and no QA checks exist for governance.
- **Docs gap:** Vision/Scope and AGENTS describe MentorProfile and `mentor_status`, but code/tests lack implementation and persistence paths.

### POOL_02 — Mentor Pool Governance UI
- **Status:** implemented.
- **Findings:** `app/ui/main_window.py` exposes a toolbar action and inline button that open `MentorPoolDialog`; selections populate `mentor_pool_overrides` passed to CLI through `_ui_overrides` for `apply_mentor_pool_governance` without altering Policy/SSoT.
- **Docs gap:** Keep UX copy aligned with Blueprint; no changes to Policy required.

### BIND_01 — MentorSchoolBindingPolicy (global vs restricted)
- **Status:** implemented.
- **Core:** `MentorSchoolBindingPolicy` in `app/core/policy_loader.py` defines global/restricted modes and empty tokens. `collect_school_codes_from_row` in `app/core/build_matrix.py` treats empty/zero tokens as global (sets `has_school_constraint=False`, `mentor_school_binding_mode=global`) and marks non-empty tokens as restricted. `filter_by_school` in `app/core/common/filters.py` respects `has_school_constraint`/`mentor_school_binding_mode`, filtering only restricted mentors by school while leaving global mentors available.
- **Tests:** `tests/unit/test_school_binding.py` covers empty-vs-present tokens and filtering behavior; `tests/core/qa/test_invariants_join_and_school.py` ensures QA flags restricted mentors lacking valid school data.
- **Gaps:** none apparent relative to documented behavior.

### QA_01 — QA invariants engine
- **Status:** implemented.
- **Core:** `app/core/qa/invariants.py` defines `QaViolation`, `QaRuleResult`, `QaReport`, and rules STU_01, STU_02, JOIN_01, SCHOOL_01, ALLOC_01 executed via `run_all_invariants`.
- **Infra:** CLI (`app/infra/cli.py`) invokes `run_all_invariants` after matrix build/allocation and fails fast on violations.
- **Tests:** `tests/core/qa/test_invariants_students.py` and `tests/core/qa/test_invariants_join_and_school.py` verify student count, join-key, and school-binding QA behaviors.
- **Gaps:** No additional QA rules beyond the implemented set; extension would require new policy/test coverage.

### QA_02 — QA Excel export
- **Status:** missing.
- **Findings:** No Infra code exports a QA validation workbook (e.g., `matrix_vs_students_validation.xlsx` with `summary/checks/breakdown/meta` sheets). Tests/golden files for such an export are absent.
- **Docs gap:** Vision/SSoT mention QA exports, but the repository lacks implementation; only runtime QA logging/report objects exist.
