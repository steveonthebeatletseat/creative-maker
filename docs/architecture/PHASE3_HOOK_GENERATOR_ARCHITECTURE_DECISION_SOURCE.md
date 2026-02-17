# Phase 3 Hook Generator Architecture Decision Source

## Current State
- Phase 1 Foundation Research is complete and provides evidence-grounded brand truth.
- Phase 2 Matrix Planner is complete and outputs planned Brief Units (Awareness x Emotion cells with counts).
- Phase 3 v2 currently generates core scripts per Brief Unit using Claude Agent SDK.
- Hook variation generation is not yet implemented as a dedicated stage.

## Decision Goal Context
Design the highest-quality possible Hook Generator architecture (as a standalone agent) that maximizes scroll-stopping performance while preserving persuasion integrity and brand/evidence alignment.

## Why This Decision Matters
Hooks are the top-leverage performance driver in paid social. If the hook layer is weak, even great scripts underperform. If the hook layer is overly constrained, creativity dies. If it's unconstrained, relevance and conversion can collapse.

## Upstream Contract (Inputs)
Each hook-generation task starts from one Brief Unit package with:
- brief_unit_id
- awareness_level (hard 5)
- emotion_key and emotion_label
- core script draft (approved or latest editable state)
- evidence pack (VoC, proof, mechanism refs)
- brand voice constraints and compliance context

## Primary Dilemmas To Resolve
1. Context depth:
- Should hook generation use no context, minimal context, selective context, or full context?
- What exact context gives best hook quality without over-constraining creativity?

2. Generation flow:
- One-pass hook generation vs multi-stage (divergent ideation -> scoring/ranking -> polish)?
- Should variants be generated in parallel with a downstream evaluator?

3. Model strategy:
- Single-model vs multi-model architecture?
- Which model should ideate, which should critique/rank, and which should finalize?
- Should Claude Agent SDK be required for hook generation or optional?

4. Data/training strategy:
- Is custom training/fine-tuning necessary now, or should we use retrieval + few-shot libraries first?
- If training is recommended, what is the minimum viable training set and feedback loop?

5. Quality controls:
- What hard gates should block weak hooks?
- How should the system detect cliche hooks, weak specificity, or awareness/emotion mismatch?

## Non-Negotiables
- Hook Generator must be a dedicated agent, separate from Core Script Drafter.
- Must support multiple hook variants per Brief Unit.
- Must preserve awareness-stage and emotional alignment.
- Must include explicit quality gating and ranking artifacts.
- Must be production-operable: retry strategy, failure isolation, traceability, deterministic IDs.
- Must support human review/editability before downstream Scene Generator.

## Optimization Objective
Primary objective:
- maximize probability of high scroll-stop performance (thumb-stop / hold in first seconds).

Secondary objectives:
- conversion intent alignment with the rest of the script
- creative diversity across variants
- throughput and stability at scale (20-100+ Brief Units)
- cost/latency efficiency

## Architecture Questions The Council Must Answer Explicitly
- What exact context payload should the Hook Agent consume?
- What is the best stage design for hook creation (1-step, 2-step, or 3-step)?
- Should generation and evaluation be done by same model or separate specialist models?
- Should Claude Agent SDK be used here, and for which sub-step(s)?
- Is model fine-tuning needed now, later, or never?
- What should the hook artifact schema be?
- What gate rules should approve/reject hook variants?
- What retry/repair loop should run when hooks fail gates?

## Expected Output Contract (from chosen architecture)
Per Brief Unit, Hook Agent should emit:
- hook_run_id, brief_unit_id
- hook_variants[] with stable hook_id
- each variant split into: verbal_line, visual_pattern_interrupt, optional on-screen text
- variant metadata: category tags, intended awareness/emotion mapping, confidence
- gate report per variant + overall ranking report
- selected top hooks + fallback hooks
- recommendation status: approved / needs revision / rejected

## Constraints
- This is architecture-only; no implementation in this step.
- Assume existing system supports API-based multi-model calls and parallel workers.
- Assume human review exists before Scene Generator stage.
