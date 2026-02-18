# Phase 3 Scene Writer Final Head-to-Head Decision Source (A vs C)

## Purpose
Run a final architecture decision between the two previously winning finalists only, and select exactly one recommended architecture for implementation.

Allowed finalists only:
- Finalist A: Deterministic Line-Level Compiler (Plan -> Validate -> Repair -> Polish)
- Finalist C: Adversarial Multi-Proposal Generation (N candidates per line -> tournament selection -> formal safety proofing -> final composer)

No third architecture is allowed as the winner.

## Decision Goal
Choose the single architecture (A or C) that will most reliably produce the highest-quality Scene Writer outputs for UGC ads.

"Highest-quality outputs" means:
1. Strongest line-by-line scene ideas (A-roll/B-roll choice quality)
2. Strongest first-3-second attention potential
3. Strongest persuasion continuity across full script
4. Highest practical filmability for UGC creators
5. Lowest risk of unsafe/unsupported visual implication
6. Stable quality across scale (20-100+ brief units)

## Upstream Reality (Locked)
- Scripts and hooks are already generated before Scene Writer starts.
- Scene Writer receives latest edited script lines + evidence map + selected hooks.
- Every script line must output one explicit mode: a_roll or b_roll.
- UGC creator is always present in the execution context.

## Finalist A Summary (Deterministic Compiler)
Strength profile:
- One clear pass per line with strict validation and targeted repair
- Very strong traceability and operability
- Easier to reason about and debug

Risk profile:
- Can lock into an average first draft and become less creatively surprising
- May reduce diversity of scene concepts unless specifically boosted

## Finalist C Summary (Multi-Proposal Tournament)
Strength profile:
- Generates multiple line-level scene options, improving odds of finding standout ideas
- Better creative ceiling for pattern interrupts and varied A-roll/B-roll scenes
- Strong comparative selection artifacts (winner vs runner-up)

Risk profile:
- More moving parts and scoring calibration complexity
- More operational overhead if not tightly constrained

## Critical Creative Constraint
The system must make practical per-line decisions such as:
- A-roll direct talking head
- A-roll demo while holding product
- A-roll movement context (walk-and-talk, desk setup, mirror setup)
- B-roll tactile product close-up
- B-roll pain-state reenactment
- B-roll proof overlay / social proof visual support

The winner must handle this variety while preserving script intent and evidence safety.

## Forced Decision Instructions
The council must:
1. Compare only Finalist A vs Finalist C.
2. Return exactly one winner from these two finalists.
3. Explicitly explain why this winner will produce better scene outputs in practice.
4. Explicitly explain why the runner-up is worse for this exact use case.
5. Provide concrete quality gates and failure recovery rules for the chosen winner.

If any stage produces a third strategy variant, mark it non-admissible and do not allow it to win.

## Expected Deliverable from Winner
Per brief unit, the recommended architecture must support:
- line-level scene plan with explicit a_roll/b_roll decisions
- UGC filming direction per line
- evidence-safe visual claims
- quality report and repair handling
- operator-friendly review/edit workflow before production handoff

## Constraints
- Architecture decision only; no code implementation in this step.
- Must optimize for output quality first, then reliability and throughput.
