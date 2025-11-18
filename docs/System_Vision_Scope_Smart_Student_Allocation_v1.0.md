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
- **Determinism:** same inputs/policy versions yield the same outputs; stable sorts; explicit version tagging (policy_version, ssot_version, schema versions).
- **Policy-First boundaries:** Core has no I/O or Qt; Infra handles Excel/WordPress/filesystem; UI is PySide6-only for desktop, Gravity Forms-only for intake; CLI/web entrypoints wrap Infra/Core without redefining policy.
- **Explainability:** 8-step trace (`type`, `group`, `gender`, `graduation_status`, `center`, `finance`, `school`, `capacity_gate`) with candidate counts and allocation reasons per student; audit logs retained.
- **Data contracts:** Excel schemas and Gravity Forms fields must align with SSOT/Policy; Join Keys remain integer; crosswalks/premap and natural sort respected; no hardcoded dates/paths in Core.
- **Safe variability:** UI layout wording, minor Excel formatting, or operational scripts may evolve if they do not violate Policy/SSoT, join keys, ranking, determinism, or Core/Infra/UI separation.

## Quality Attributes
- **Determinism & reproducibility:** stable/natural sorts; seeded runs if needed; explicit policy_version/ssot_version in outputs.
- **Auditability & explainability:** per-student trace, status/reason codes, versioned logs; 8-step trace completeness.
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
