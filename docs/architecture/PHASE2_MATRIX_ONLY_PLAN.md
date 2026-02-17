# Phase 2 Matrix-Only Plan

## Decision Context
- Basis: Architecture Council winner `option_a` ("Contract-First Creative Engine with Deterministic Validation Spine")
- Adaptation: Scope narrowed to **matrix planning only** (no angle/concept/script generation in this phase)

## Goal
Build a high-reliability Awareness x Emotion planning matrix where:
- X-axis = Awareness (fixed 5 levels)
- Y-axis = Emotion (dynamic, mined from Phase 1)
- Each cell stores a user-chosen integer count for how many briefs to generate later

## In Scope (Phase 2 Now)
1. Validate and load Phase 1 Foundation Research (`schema_version=2.0`).
2. Extract/freeze matrix axes for the run.
3. Render matrix UI and capture per-cell brief quantities.
4. Validate matrix payload with hard quality gates.
5. Require human approval and persist an immutable matrix plan artifact.

## Out of Scope (Next Architecture)
1. Marketing angle generation.
2. Web style/ad research.
3. Concept synthesis.
4. Brief/script generation and rewrite loops.

## Matrix-Only Workflow
1. Stage 0 - Contract Freeze
- Freeze these contracts before production:
  - Awareness enum (exactly 5 canonical levels)
  - Emotion row schema (normalized key + label + evidence refs)
  - Matrix plan schema (`MatrixPlan_v1`)

2. Stage 1 - Input Guard
- Validate Foundation Research exists, is `2.0`, and matches brand/product context.
- Block run with actionable error on mismatch/missing fields.

3. Stage 2 - Axis Builder
- Awareness axis: fixed 5 levels from canonical enum.
- Emotion axis: derive rows from `pillar_6_emotional_driver_inventory`.
- Attach traceability refs for each emotion row (`sample_quote_ids` + ledger refs when available).

4. Stage 3 - Matrix Planner UI
- Render grid cells for all Awareness x Emotion intersections.
- User sets integer `brief_count` per cell (0 allowed).
- Show totals and row/column rollups in real time.

5. Stage 4 - Matrix Validation Gates
- Gate A: Axis integrity (5 awareness levels, non-empty emotion set).
- Gate B: Cell integrity (all cells integer >= 0; respect max cap policy).
- Gate C: Traceability integrity (every emotion row has evidence linkage).
- Gate D: Structural integrity (100% schema-valid `MatrixPlan_v1`).

6. Stage 5 - Human Approval Gate
- Block downstream work until explicit approval.
- Persist approved matrix snapshot + timestamp + approver identity/hash.

## Required Artifact
`MatrixPlan_v1` (output of Phase 2)
- Run metadata (brand/product/run IDs, schema versions)
- `awareness_levels[5]`
- `emotion_rows[]` (normalized emotion keys + evidence refs)
- `cells[]` with `{awareness_level, emotion_key, brief_count}`
- Totals (`total_briefs`, row/column aggregates)
- Approval metadata (`approved_by`, `approved_at`, `snapshot_hash`)

## Quality Rules (Hard)
- Traceability: 100% of emotion rows must be evidence-linked.
- Structural Integrity: 100% schema validation pass.
- Input Validity: no Phase 2 run without valid Phase 1 `2.0`.
- Approval: no downstream generation without approved matrix snapshot.

## Handoff Contract to Next Architecture
Downstream brief/script architecture must consume **only** approved `MatrixPlan_v1` and fan out jobs from non-zero cells.
