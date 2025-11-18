# AGENTS.md — Smart Student Allocation (Global)

**Spec Level:** agentsmd.net (HEADER→PURPOSE→SCOPE→ROLES→BOUNDARIES→TASK ROUTING→ALLOWED/PROHIBITED→QA→VERSIONING→EXAMPLES)
**Policy Ref:** Policy v1.0.3 (immutable, Policy-First)
**SSoT Ref:** SSoT v1.0.2 (immutable)
**Vision & Scope:** docs/System_Vision_Scope_Smart_Student_Allocation_v1.0.md (read-only)
**Architecture Blueprint:** docs/System_Architecture_Blueprint_Smart_Student_Allocation_v1.0.md (read-only)
**Supersedes:** any prior subsystem-only AGENTS (e.g., Eligibility Matrix-only rules)
**Audience:** All agents (CoderAgent, InfraAgent, UIAgent, DocumentationAgent, ReviewerAgent, SupervisorAgent)

---

## PURPOSE
Single, authoritative contract for all LLM and human agents to operate the Smart Student Allocation product with Policy-First and SSoT-First discipline. Defines roles, boundaries, determinism, routing, and QA expectations across Core/Infra/UI + Agents Layer.

## SCOPE
Applies to the entire repository. Local AGENTS.md files do not exist; this document governs every path. All agents must comply with Vision/Scope v1.0, Architecture Blueprint v1.0, Policy v1.0.3, and SSoT v1.0.2. Policy/SSoT content is referenced, never redefined.

## AGENT ROLES
- **CoderAgent**: Implements deterministic code within assigned layer, honoring dependency rules and reproducibility. No scope creep beyond assigned layer.
- **InfraAgent**: Builds adapters, I/O, Excel pipelines, WordPress intake bridges within `app/infra`, `scripts`, `tools` respecting Core contracts. Chooses engines explicitly; handles atomic Excel writes per Infra specs.
- **UIAgent**: Works in PySide6 shell within `app/ui` and `run_gui.py`; no business logic; consumes Infra/Core services only.
- **DocumentationAgent**: Updates docs in `docs/`, `README*`, guides. References Policy/SSoT without redefining. Maintains clarity for navigation and operations.
- **ReviewerAgent**: Enforces Policy/SSoT alignment, dependency boundaries, determinism, QA checklist, and join/ranking invariants. Blocks drift.
- **SupervisorAgent**: Routes tasks to roles, checks version coherence (Policy 1.0.3, SSoT 1.0.2), ensures CI/test coverage and adherence to Architecture Blueprint.

## REPOSITORY STRUCTURE & NAVIGATION
- **Core (`app/core`)**: Deterministic, pure logic (pandas allowed). No I/O, no Qt, no network. Honors natural+stable sorting. Uses injectable `progress(pct:int, msg:str)` only.
- **Infra (`app/infra`, `scripts`, `tools`)**: I/O, Excel pipelines, adapters, logging, WordPress intake, Excel fallback atomic writer. Calls Core; never the reverse.
- **UI (`app/ui`, `run_gui.py`)**: PySide6 shell/view. Consumes Infra/Core APIs; forbids business logic and file/network I/O beyond UI needs.
- **Agents Layer (`app/agents` if present)**: Orchestration only; must not break Core/Infra/UI boundaries.
- **Docs (`docs`, top-level READMEs, guides)**: Policy/SSoT references only. Do not embed business logic.
- **Config (`config/`)**: Policy JSON/YAML loaders; immutable schema from Policy v1.0.3 and SSoT v1.0.2. No hardcoded policy in code.
- **Tests (`tests/`)**: Layer-aligned; Core tests remain pure/deterministic; Infra/UI tests may use fixtures and golden outputs.
- **Navigation**: Use `rg` for search. Avoid `ls -R`/`grep -R`. Follow path by layer; do not cross-write.

## BINDING TO VISION/SCOPE v1.0 & ARCHITECTURE BLUEPRINT v1.0
- Always read **System_Vision_Scope_Smart_Student_Allocation_v1.0.md** for product goals, user journeys, and non-functional constraints.
- Always read **System_Architecture_Blueprint_Smart_Student_Allocation_v1.0.md** for layered rules, module boundaries, and dependency direction (UI → Infra → Core only).
- Any change conflicting with these documents is forbidden. If ambiguity arises, SupervisorAgent decides by referencing these documents and Policy/SSoT.

## POLICY & SSoT ENFORCEMENT
- **Immutable invariants (do NOT alter):**
  - **Join Keys (6, int):** `"کدرشته"` (group_code), `"جنسیت"` (gender), `"دانش آموز فارغ"` (graduation_status), `"مرکز گلستان صدرا"` (center), `"مالی حکمت بنیاد"` (finance), `"کد مدرسه"` (school_code).
    - **Trace mapping note:** `"کدرشته"` feeds both the `type` and `group` trace steps (one-to-many mapping from join key → trace steps).
  - **Ranking Policy (stable):** minimize `occupancy_ratio` → minimize `allocations_new` → natural sort `mentor_id` (stable sort required).
  - **Trace (8-step explainability):** `type, group, gender, graduation_status, center, finance, school, capacity_gate` with candidate counts after each filter.
  - **Determinism:** identical inputs yield identical outputs; stable sorts everywhere; no randomness/time-based logic.
  - **Policy Version:** 1.0.3; **SSoT Version:** 1.0.2. Never downgrade/upgrade silently.
- **Policy-First:** Load policy from config/policy.json (or policy.yaml equivalent); never hardcode policy constants in Core.
- **SSoT-First:** Use SSoT datasets/schemas as canonical truth; avoid schema drift.

## ALLOWED ACTIONS
- Edit only files within assigned agent scope and layer boundaries.
- Use pandas in Core for tabular logic; avoid inplace mutations; copy before transforms when needed.
- Apply natural+stable sort for any identifier ordering (e.g., mentor_id).
- Implement atomic Excel writes with sanitized sheet names in Infra.
- Reference Policy/SSoT/Vision/Architecture docs; cite versions in PRs/commits.
- Add tests (unit/snapshot) per layer; prefer deterministic fixtures and golden outputs.

## FORBIDDEN ACTIONS
- Breaking dependency direction (Core depending on Infra/UI/Agents).
- Introducing I/O, Qt signals, or network calls in Core.
- Hardcoding policy/SSoT constants inside Core logic; bypassing config loaders.
- Altering Join Keys, Ranking Policy, Trace stages, Policy/SSoT versions.
- Using non-deterministic operations (random, time-based ordering, unstable sorts).
- Merging data in loops (avoid repeated merges; use premap).
- Using `inplace=True` pandas mutations or lambda validators returning None.
- Hardcoding file paths/dates; embedding secrets; modifying policy documents directly.

## DEPENDENCY BOUNDARIES
- Allowed imports: UI → Infra → Core only. Agents/orchestration may call UI/Infra/Core but must not invert dependencies.
- Core exposes pure functions/classes; Infra wraps Core with I/O; UI consumes Infra/Core via adapters; Agents orchestrate without embedding business logic.
- Infra may depend on `config/` loaders; Core may depend on `config/` contracts but not on Infra/UI implementations.

## TASK ROUTING RULES
- SupervisorAgent assigns tasks per layer: Core logic → CoderAgent; I/O/Excel/WordPress adapters → InfraAgent; PySide6 view/controller → UIAgent; docs/guides → DocumentationAgent; reviews/QC → ReviewerAgent.
- Cross-layer tasks must be decomposed into layer-scoped sub-tasks; no single agent edits multiple layers unless explicitly authorized by SupervisorAgent.
- Any policy/SSoT ambiguity → escalate to SupervisorAgent with references to Policy v1.0.3 & SSoT v1.0.2.

## BEST-OF-N DECISION RULES FOR AGENTS
- **When to use best-of-N:**
  - Routine, well-specified, low-risk edits (typos, single-column additions, doc paragraph updates) → default to a single completion.
  - Non-trivial refactors within a single layer (**Core**, **Infra**, or **UI**) where multiple plausible designs exist → consider best-of-2 or best-of-3.
  - Ambiguous UX/copy or divergent structural designs (layout, error-reporting style) → best-of-2 or best-of-3 may be preferable so ReviewerAgent/SupervisorAgent can select the strongest fit.
  - Large or cross-cutting changes within one layer only use best-of-N if QA checklist and tests are well-defined; otherwise reduce scope and avoid variant sprawl.
- **How to compare best-of-N candidates:**
  - Never choose randomly; evaluate against Policy v1.0.3 and SSoT v1.0.2 invariants (join keys, ranking policy, determinism, trace steps) and layer boundaries (no Core↔Infra/UI inversion).
  - Prefer smaller, localized change sets that fully satisfy the task and align with stable natural sorting and deterministic behavior.
  - Prefer variants that add/update relevant tests and maintain clarity (minimal "magic", consistent with existing style).
  - If candidates are equivalent, choose the one touching fewer files, avoiding speculative abstractions, and reducing future maintenance cost.
- **Defaults and safeguards:**
  - Routine, well-specified tasks → N = 1.
  - Medium complexity or design choices within a single layer → default to N = 2, with N = 3 as an option.
  - High-risk refactors with strong tests/QA guardrails → at most N = 3.
  - Do not use best-of-N to brute-force unclear requirements; clarify or narrow scope instead. Chosen variant must keep final merged behavior deterministic.
- **Role-specific behavior:**
  - **CoderAgent / InfraAgent / UIAgent:** May request best-of-N for internal generation; must note in PLAN/DEBUG_REPORT why it was used.
  - **ReviewerAgent:** Evaluates variants against Policy/SSoT invariants, QA checklist, and scope; must state why the chosen variant wins (e.g., "Variant 2 preserves join-key invariants and adds tests; Variant 1 violates layering.").
  - **SupervisorAgent:** May enforce maximum N (N ≤ 3) and may disable best-of-N for high-risk areas (e.g., Core join/ranking logic) unless explicitly justified.
- **Interaction with user-specified N:**
  - Respect user-specified N while applying the comparison rules above to select the final variant.
  - If requested N is excessive for scope (e.g., N = 10 for trivial edits), note in comments/PLAN why a smaller effective N is preferable, even if tooling produces more candidates.

## TESTING COMMANDS
- **Core:** `pytest tests/core -q`
- **Infra:** `pytest tests/infra -q`
- **UI (headless where possible):** `pytest tests/ui -q`
- **All layers:** `pytest -q`
- **Lint (if configured):** `ruff check .` or `flake8` per CI settings.

## QA CHECKLISTS FOR AGENTS
- **Policy/SSoT:** Policy version == 1.0.3, SSoT version == 1.0.2; Join Keys (6) and Ranking Policy unchanged; Trace 8-step preserved.
- **Determinism:** Stable sorts, no randomness/time; natural key for identifiers; reproducible fixtures.
- **Boundaries:** UI→Infra→Core dependency flow; no I/O/Qt in Core; no policy constants hardcoded.
- **Data Contracts:** Join keys typed as int; schemas match SSoT; premap used to avoid repeated merges; no inplace pandas ops.
- **Excel/Adapters:** Atomic Excel writer with sanitized sheet names; engine selection explicit (openpyxl/xlsxwriter fallback).
- **Testing:** Relevant pytest suites updated; golden outputs refreshed deterministically.
- **Docs:** Changes reference Vision/Scope and Architecture Blueprint; no policy redefinition; navigation instructions intact.

## VERSIONING POLICY
- AGENTS.md version tracks Policy v1.0.3 & SSoT v1.0.2 alignment. Any Policy/SSoT change requires SupervisorAgent review and AGENTS.md update. Changelog must be appended; historical sections remain immutable.

## EXAMPLES
- **Valid:** CoderAgent updates `app/core` function to apply stable natural sort for mentor_id using existing helpers; adds unit test in `tests/core`; cites Policy v1.0.3 join/ranking invariants.
- **Invalid:** InfraAgent edits Core to read Excel directly; UIAgent hardcodes ranking policy; DocumentationAgent rewrites Join Keys; any agent introduces non-deterministic shuffle.

## CHANGELOG
- **v1.0 (Global):** Replaces Eligibility Matrix-only AGENTS with global, layered contract; embeds Policy 1.0.3 & SSoT 1.0.2 invariants, dependency boundaries, routing, testing, and QA rules across Smart Student Allocation.
- **v1.1:** Added BEST-OF-N decision rules for agents (supervised variant selection).
