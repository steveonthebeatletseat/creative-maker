# Phase 4 Video Generation (Test Mode) — Architecture Decision Source

## Context
Creative Maker currently produces branch-scoped script/hook/scene planning artifacts (Phase 3 v2).
We are now starting video generation. This is quality-critical and must run with strict operator control.

## Operator Intent (Current Scope)
For now, only support a **single video run at a time** (test mode) so we can optimize quality scene-by-scene.
Later, architecture should be extensible to multi-video parallel execution.

## Core Workflow (Steps 1-4 in scope)
1. System reads scene plan and produces a **Start Frame Brief** before generation.
2. Operator prepares/uploads required images to Google Drive and inputs folder URL.
3. System validates assets and then generates scene clips (Fal AI with reference start frame), with optional pre-edit of start frame via Gemini/Nano Banana.
4. Operator reviews each generated scene and requests revisions/regenerations until approved.

## Out of Scope (for later)
- Final handoff automation after approval: auto-create delivery Drive folder, upload final outputs, notify Notion/editor.

## Required Product Behavior

### A) Start Frame Brief generation
- Derive required start-frame specs from scene units (A-roll/B-roll needs, style, framing, motion intent).
- Produce explicit filename requirements and naming rules for every required image.
- Include required/optional tags and quality notes for each frame request.
- Output must be reviewable and operator-approved before upload step.

### B) Drive ingest + validation gate
- Operator pastes Drive folder URL.
- System scans listed files and maps them to required filenames.
- Hard validation gate before generation:
  - missing required files
  - duplicate ambiguous mappings
  - unsupported format/resolution
  - naming mismatch
- Must produce clear pass/fail report with exact fixes.

### C) Scene generation pipeline
Per scene unit:
1) select best start frame(s) from validated assets,
2) optional frame transformation pass (Gemini/Nano Banana),
3) Fal AI generation using chosen/transformed frame,
4) persist generated clip + full provenance.

Provenance must include:
- scene identifiers,
- selected source frame id/path,
- whether edited frame used (and reference),
- prompts/settings/model/version,
- provider response ids,
- timestamps and status.

### D) Human review + revision loop
- Review queue at scene level.
- Operator can approve, request regenerate, swap frame, or adjust instructions.
- Re-run only affected scene(s); preserve history and lineage.
- Run completes only when all scene units are approved.

## Critical Constraints
- Deterministic traceability and auditability are mandatory.
- Quality over speed in test mode.
- Idempotent operations to avoid duplicate runs/cost burn.
- Strong error isolation: one failed scene must not corrupt entire run state.
- Keep compatibility with existing brand/branch/run structure from Phase 3 v2.

## Integration Requirements
- Must consume existing Phase 3 v2 scene artifacts as source contract.
- Must store outputs/artifacts in branch-scoped structure under outputs/<brand>/branches/<branch>/...
- Must expose backend APIs for:
  - start-frame brief generation,
  - drive validation,
  - generation run start/status,
  - scene-level revision actions,
  - approval state transitions.

## Test Mode Requirements
- Enforce a single active video run per branch (or global single-run lock in initial milestone).
- Concurrency settings should exist but default to 1 in test mode.
- Provide explicit "test mode" marker in manifests and API responses.

## Future-Scale Readiness (not active now)
- Architecture should allow moving from one-video-at-a-time to multi-video parallel jobs.
- State model should already support per-video/per-scene isolation and queueing.
- Storage contracts should avoid assumptions that only one video exists forever.

## Decision Objective for Council
Design the highest-reliability backend architecture for Steps 1-4 above, optimized for:
1) strict quality control,
2) deterministic state management,
3) auditability/provenance,
4) low operational ambiguity for a non-technical operator,
5) clean path to future parallel scale.
