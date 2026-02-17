# Phase 3 Scripting Architecture Decision Source

## Current State
- Phase 1 (Foundation Research) produces evidence-grounded brand intelligence and emotional-driver inventory.
- Phase 2 has been rebuilt as matrix-only: Awareness x Emotion planning with per-cell brief counts.
- Phase 3 (script generation) is intentionally disabled and must be redesigned from scratch.

## Decision Goal Context
Design the highest-quality possible architecture for generating high-converting, high-ROAS ad scripts from matrix-planned briefs.

## Upstream Input Contract (from matrix planner)
Each planned brief corresponds to a matrix cell and has at minimum:
- awareness_level (hard 5 stages: unaware/problem_aware/solution_aware/product_aware/most_aware)
- emotion_key and emotion_label (dynamic from Phase 1 research)
- brief_count (how many ad briefs/scripts to create at that cell)

Phase 3 must consume these planned brief units and output production-ready script packages.

## Core Dilemma To Resolve
Evaluate and choose the best architecture pattern:
1) Single all-in-one generator per brief:
   - one agent produces full script + scene plan + hook variations in one pass.
2) Staged specialist pipeline:
   - step A: script core
   - step B: hook variations
   - step C: scene breakdown/shot plan
   - with explicit gates between stages.
3) Hybrid variants:
   - e.g., generate core script first, then parallel hook and scene specialists with a final consistency reconciler.

## Non-Negotiables
- Must preserve strict traceability to Phase 1 evidence and matrix cell intent.
- Must support high throughput (many matrix cells in one run) without quality collapse.
- Must include quality gates that block low-quality outputs before downstream use.
- Must provide deterministic artifact structure and IDs for each script package.
- Must support human approval gate(s) at the right stage(s).
- Must be practical to operate in production (retry/recovery, monitoring, failure isolation).

## Optimization Objective
Primary optimization target:
- maximize conversion/ROAS potential of generated ads.

Secondary targets:
- consistency of quality across many briefs
- speed/cost efficiency
- operational reliability and debuggability

## Architecture Questions The Council Must Explicitly Answer
- Should script/hook/scene be one pass, two steps, or three steps?
- Which components should run in parallel vs serial?
- Where should hard quality gates sit and what should they enforce?
- How should the system validate awareness/emotion alignment for each output?
- What minimal artifact schema should each stage emit to keep downstream deterministic?
- What retry/escalation strategy should be used when outputs fail gates?
- What human approval design best balances quality and throughput?

## Deliverable Shape Expected from Chosen Architecture
Per generated script package, output should include:
- brief_id, matrix_cell coordinates, and traceability references
- core script (with clear structure)
- hook set (multiple variants)
- scene/shot breakdown aligned to the script
- quality report with pass/fail by gate
- recommendation status: approved / needs revision / rejected

## Constraints
- This decision is architecture-only; no code implementation in this step.
- Assume existing stack can call multiple LLM providers and run parallel workers.
- Assume matrix planning is complete before script generation starts.
