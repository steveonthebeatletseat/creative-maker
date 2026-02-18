# Phase 3 Scene Writer Binary Forced Decision (Finalist A vs Finalist C)

## Mandatory Option Mapping
For this run, strategy options are locked as:
- option_a = Finalist A (Deterministic Line-Level Compiler)
- option_b = Finalist C (Adversarial Multi-Proposal Tournament)
- option_c = Non-admissible placeholder (must be auto-disqualified and cannot win)

The judge must select winner_option_id as either `option_a` or `option_b` only.

## Finalists
### option_a — Finalist A
Deterministic line-by-line compile path:
- Draft one scene direction per line
- Validate hard gates
- Repair failing lines
- Polish and revalidate

Pros:
- high traceability and operational clarity
- strong deterministic quality controls

Risks:
- lower creative ceiling
- can converge to safe but average scenes

### option_b — Finalist C
Multi-proposal tournament path:
- Generate multiple scene candidates per line
- Gate for safety/feasibility
- Score/rank and select winner per line
- Compose sequence with cohesion controls

Pros:
- higher creative upside and variety
- better chance of standout scroll-stoppers

Risks:
- more moving parts/calibration complexity
- needs strong guardrails to avoid instability

### option_c — Non-admissible placeholder
This option exists only to satisfy portfolio schema cardinality.
It must be scored as non-admissible and cannot be selected.

## Decision Goal
Pick the architecture (A or C) that will produce the best practical Scene Writer outputs for UGC creator ads.

## Output Quality Criteria (Priority Order)
1. Strong line-level scene ideas (A-roll/B-roll choice quality)
2. High scroll-stop strength in the first 3 seconds
3. Persuasion continuity across the whole script
4. UGC filmability and creator clarity
5. Evidence-safe visuals and claim integrity
6. Stable quality across many brief units

## Hard Constraints
- Every script line must explicitly choose a_roll or b_roll.
- UGC talking head context is always present.
- Evidence safety is zero-tolerance for unsupported implied claims.
- Decision must include concrete why-winner-beats-runner-up rationale.

## Required Judge Behavior
- Disqualify option_c by rule.
- Return winner_option_id = option_a or option_b only.
- Explain why winner gives higher quality outputs in practice.
- Explain why runner-up is weaker for this exact use case.

## Constraints
- Architecture decision only.
- No code implementation in this step.
