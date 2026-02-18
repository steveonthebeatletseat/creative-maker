# Phase 3 Scene Writer Architecture Decision Source

## Current State
- Phase 1 Foundation Research is complete and provides evidence-grounded truth (VoC, proof, mechanism).
- Phase 2 Matrix Planner is complete and outputs planned Brief Units (Awareness x Emotion with counts).
- Phase 3 Script Writer (v2) is live and outputs editable core scripts per Brief Unit.
- Phase 3 Hook Generator is live and outputs selectable hook variants per Brief Unit.
- Scene generation is not yet implemented as its own dedicated stage.

## Decision Goal Context
Design the highest-quality possible Scene Writer architecture that converts script + hook outputs into high-performing, line-level scene direction for UGC-style ads.

## Critical Creative Reality (Must Be Modeled)
- Ads will be delivered by UGC creators in talking-head format.
- Therefore, every script line must decide one of two execution modes:
  1. `a_roll`: creator speaks the line on camera (talking head), with specific direction.
  2. `b_roll`: creator voiceover over supporting visuals, with specific shot direction.
- `a_roll` is not a single style. Scene Writer should choose the best variant for each line, such as:
  - direct-to-camera static
  - handheld selfie walk-and-talk
  - creator speaking while demonstrating product
  - creator speaking while holding/adjusting/using product
  - creator speaking from problem context environment (desk, couch, gym, car, etc.)
- `b_roll` must be intentional and concrete (what exactly is seen, why now, and how it supports persuasion).

## Upstream Contract (Inputs)
Each Scene Writer task starts from one Brief Unit package with:
- brief_unit_id
- awareness_level (hard 5)
- emotion_key and emotion_label
- latest edited script lines (line IDs + text + evidence IDs)
- selected hook(s)
- evidence pack (VoC/proof/mechanism refs)
- brand guardrails + compliance context

## Output Contract Target (What Scene Writer Must Produce)
Per Brief Unit, output must include:
- scene_run_id, brief_unit_id
- line-by-line scene plan aligned to exact script line IDs
- for each line:
  - chosen mode: `a_roll` or `b_roll`
  - if `a_roll`: camera framing, creator action, performance direction, product interaction notes
  - if `b_roll`: shot description, subject/action, motion, prop/product visibility, transition intent
  - on-screen text guidance (if any)
  - pacing estimate (seconds)
  - evidence/claim safety mapping
- continuity layer:
  - transitions, rhythm, visual variety, repeated motif control
- production feasibility metadata:
  - difficulty score, required assets/props, creator complexity flags
- quality report + recommendation status (`approved`, `needs_revision`, `rejected`)

## Core Dilemmas To Resolve
1. Granularity and structure:
- Should Scene Writer plan per line only, or line + beat/sub-beat?
- Should it output one primary scene plan or multiple alternates?

2. A-roll vs B-roll decision logic:
- What decision policy determines when a line should stay talking-head vs cut to b-roll?
- How to prevent monotony from too much talking-head while keeping authenticity?

3. Model architecture:
- Single model end-to-end vs staged specialists (planner -> validator -> polisher)?
- Which parts should use Claude Agent SDK vs structured non-SDK models?

4. Hook integration:
- If multiple hooks are selected, should Scene Writer generate one scene plan per hook or a merged adaptable plan?

5. Quality assurance:
- What hard gates should fail scene outputs (misalignment, impossible shots, weak visual intent, compliance risk)?
- How should claim/evidence safety be enforced at line level?

6. Human workflow:
- Where should manual edit/approval sit before downstream production execution?
- What should be editable without breaking traceability?

## Non-Negotiables
- Scene Writer is a dedicated standalone stage (not bundled into script writer).
- Must preserve strict line-level traceability to script text and evidence context.
- Must explicitly choose `a_roll` or `b_roll` for every line.
- Must support UGC-native directions for talking-head performance.
- Must include quality gates for persuasion alignment + production realism.
- Must be production-operable: deterministic IDs, retries, failure isolation, artifacts.

## Optimization Objective
Primary objective:
- maximize practical conversion potential by pairing each script line with the strongest visual execution mode (A-roll or B-roll) for attention + persuasion.

Secondary objectives:
- maintain awareness/emotion alignment
- preserve evidence/claim integrity
- reduce creator confusion on set
- maintain operational speed/cost at 20-100+ Brief Units

## Architecture Questions The Council Must Answer Explicitly
- What exact line-level schema should Scene Writer emit?
- What policy decides A-roll vs B-roll per line?
- Should scene generation be one-pass or multi-stage with critique/repair?
- Which model(s) should ideate scenes, which should gate, and which should finalize?
- Should Claude Agent SDK be mandatory for scene generation, and where?
- How should the system handle multiple selected hooks?
- What minimum quality gates are required before marking a scene plan production-ready?
- What retry/repair loop should run for failing line-level scenes?

## Constraints
- This is architecture-only; no implementation in this decision step.
- Assume existing stack already supports branch-scoped runs, parallel workers, and model routing.
- Assume script and hook stages are complete before scene generation starts.
