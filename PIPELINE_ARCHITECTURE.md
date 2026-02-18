# Ad Pipeline — Full Architecture

## Project Overview

An automated paid social ad creation pipeline. Specialized AI agents pass structured outputs to each other in sequence — from customer research through creative production to campaign launch, performance analysis, and scaling.

Agents marked with **◆** have completed deep research (comprehensive practitioner-level research docs available as separate `.md` files). These research docs should be used as the primary knowledge source when building those agents' system prompts.

**Current implementation status (live in code):**
- Active runtime path: Phase 1 + Phase 2 Matrix Planner + Phase 3 v2 Script Writer (+ Hook/Scene stages when enabled)
- `Start Pipeline` runs Phase 1 only; Phase 2+ is branch-scoped per creative branch
- Phase 1 is now two internal steps within Agent 1A:
  - Step 1/2: parallel collectors (Gemini Deep Research + Claude Agent SDK) with checkpointed snapshot
  - Step 2/2: evidence normalization, contradiction audit, pillar synthesis, adjudication, and quality gates
- Phase 1 Step 1 -> Step 2 auto-continues by default (no manual gate between them)
- Foundation Step 2 default synthesis model is Claude Opus 4.6 (agent default), with override support
- Retry policy is single focused collector only, max one retry round, no synthetic evidence padding
- First Creative Engine run auto-creates the default branch
- Phase 2 Matrix Planner writes awareness × emotion brief planning per branch
- All Phase 2/3 outputs are isolated per branch (`outputs/<brand>/branches/<branch_id>/...`) with no cross-pollination
- Phase 3 v2 script generation runs Claude SDK core drafting with bounded parallel execution (`PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL`, default `4`)
- Phase 3 v2 hook generation is now a standalone stage (Milestone 2) behind `PHASE3_V2_HOOKS_ENABLED`
- Hook stage executes Diverge -> Gate/Score -> Repair -> Rank with bounded per-unit parallelism (`PHASE3_V2_HOOK_MAX_PARALLEL`, default `4`)
- Hook artifacts include candidates, gate reports, scores, bundles, selections, and `scene_handoff_packet.json`
- Phase 3C v2 Scene Writer is implemented behind `PHASE3_V2_SCENES_ENABLED`
- Scene stage runs deterministic constraint validation with Claude creative draft/repair/polish and bounded per-unit parallelism (`PHASE3_V2_SCENE_MAX_PARALLEL`, default `4`)
- Scene artifacts include scene plans, scene gate reports, scene chat threads, and `production_handoff_packet.json`
- Script edits automatically invalidate stale hook selections for that brief unit/arm pair
- Script edits and hook edits/re-selections invalidate stale scene plans for affected brief-unit + hook paths
- Console logs emit per-brief-unit lifecycle telemetry for both script and hook stages
- Phase 4-6 agents are documented architecture targets, not active in the current runtime path

---

## Pipeline Flow

```
PHASE 1 — RESEARCH
  ┌─────────────────────────────────────────────────────┐
  │ Agent 1A ◆ — Foundation Research                    │
  │ Step 1: Parallel collectors (Gemini + Claude)       │
  │ Step 2: Synthesis + QA (auto-continues)             │
  │ Deep customer/market intelligence (truth layer)      │
  └────────────────────────┬────────────────────────────┘
                           ▼
BRANCHING — Creative Direction Exploration
  ┌─────────────────────────────────────────────────────┐
  │ BRANCH SYSTEM                                        │
  │ 1) First Phase 2 run creates/runs the default         │
  │    branch automatically                              │
  │ 2) Additional branches rerun Phase 2 matrix planning  │
  │    with branch-specific intent                        │
  │ 3) Each branch then runs Phase 3 v2 in complete       │
  │    isolation from other branches                     │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 2 — MATRIX PLANNING (per branch)
  ┌─────────────────────────────────────────────────────┐
  │ Agent 02 — Matrix Planner                           │
  │ Builds Awareness × Emotion grid from Phase 1        │
  │ Sets per-cell brief_count plan                      │
  │ Output: matrix plan + branch-scoped brief units     │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 3A — SCRIPT WRITER (v2)
  ┌─────────────────────────────────────────────────────┐
  │ Core Script Drafter (Claude SDK, bounded parallel)  │
  │ Inputs: Brief Unit + evidence pack + script spec    │
  │ Output: editable script drafts per Brief Unit        │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Human Editorial Loop                                 │
  │ - line-by-line edits                                 │
  │ - Claude chat suggest/apply per output               │
  │ - skip/include weak outputs                          │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 3B — STANDALONE HOOK GENERATOR (v2 M2)
  ┌─────────────────────────────────────────────────────┐
  │ Diverge -> Gate/Score -> Repair -> Rank             │
  │ Generation/Repair: Claude SDK                       │
  │ Gate/Score: GPT structured eval                     │
  │ Output: ranked hook bundles + hook selections       │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 3C — SCENE WRITER (v2 M3)
  ┌─────────────────────────────────────────────────────┐
  │ Deterministic Constraint Compiler + Scene Gates      │
  │ Claude SDK creative draft/repair/polish per unit     │
  │ Output: line-by-line A-roll/B-roll scene plans       │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Production Handoff Packet (ready/not-ready gate)     │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 4 — PRODUCTION
  ┌─────────────────────────────────────────────────────┐
  │ Agent 08 — Screen Writer / Video Director            │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────┬─────────────────────────┐
  │ Agent 09                │ Agent 10                │
  │ Clip Maker              │ AI UGC Maker            │
  │ (runs in parallel)      │ (runs in parallel)      │
  └────────────┬────────────┴────────────┬────────────┘
               └────────────┬────────────┘
                            ▼
PHASE 5 — QA & LAUNCH
  ┌─────────────────────────────────────────────────────┐
  │ Agent 11 — Clip Verify (QA Gate)                    │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 12 ◆ — Compliance / Brand Safety              │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 13 — Pre-Launch QA & Editor Handoff           │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 14 — Launch to Facebook (Meta API)            │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 6 — ANALYZE & SCALE
  ┌─────────────────────────────────────────────────────┐
  │ Agent 15A ◆ — Performance Analyzer                  │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────┬─────────────────────────┐
  │ Agent 15B ◆             │ Agent 16 ◆              │
  │ Learning & Brief        │ Winner Scaling          │
  │ Updater (parallel)      │ Agent (parallel)        │
  └────────────┬────────────┴────────────┬────────────┘
               └────────────┬────────────┘
                            ▼
               ┌────────────────────────┐
               │  FEEDBACK LOOP         │
               │  Back to Agents        │
               │  Back to Agents        │
               │  2, 5                  │
               └────────────────────────┘
```

---

## Agent Details

### PHASE 1 — RESEARCH

#### Agent 1A: Foundation Research v2 ◆ DEEP RESEARCH COMPLETE

- **Role:** Hybrid DAG research engine. Runs parallel collectors (Gemini Deep Research + Claude SDK), synthesizes 7 pillars, adjudicates consistency, and enforces quality gates with auditable artifacts.
- **Cadence:** Runs quarterly (not per-batch)
- **Inputs:** Brand/product info, customer reviews & feedback, competitor landscape, previous performance data
- **Persistence:** Saved to shared output `outputs/agent_01a_output.json` and reused by all Phase 2 branch runs
- **Runtime shape (current):**
  - **Step 1/2 — Parallel collectors**
    - Gemini Deep Research collector + Claude Agent SDK collector run in parallel
    - Step 1 output saved separately as `foundation_research_collectors_snapshot.json`
    - Collector checkpoint saved as `phase1_collector_checkpoint.json` and reused by reruns when context hash matches
  - **Step 2/2 — Synthesis + QA**
    - Normalizes and dedupes evidence
    - Runs contradiction detection and flags evidence conflicts
    - Synthesizes all 7 pillars + adjudication pass + hardening
    - Evaluates quality gates
    - If gates fail, runs one focused recollection round (single selected collector only), then re-synthesizes once
    - Returns soft warning output by default when unresolved gates remain (`PHASE1_STRICT_HARD_BLOCK=false`)
- **Model behavior (current):**
  - Collectors: Gemini Deep Research + Claude Agent SDK
  - Step 2 synthesis/adjudication default: Anthropic Claude Opus 4.6 (agent default)
- **Retry behavior (current):**
  - Strategy: `single_focused_collector`
  - Max retry rounds: `1`
  - No second escalation round, no synthetic evidence padding
- **Primary Phase 1 artifacts (current):**
  - `foundation_research_collectors_snapshot.json`
  - `foundation_research_output.json`
  - `foundation_research_quality_report.json`
  - `foundation_research_contradictions.json`
  - `foundation_research_evidence_ledger.json`
  - `foundation_research_evidence_summary.json`
  - `foundation_research_retry_audit.json`
  - `foundation_research_trace.json`
- **Safety guard (Phase 2):**
  - Creative Engine is blocked if Foundation Research output is missing
  - Creative Engine is blocked if Foundation Research output is not schema_version `2.0`
  - Creative Engine is blocked if Foundation Research brand/product does not match the current brief brand/product
  - System no longer silently overwrites brief brand/product when loading saved Foundation Research
- **Outputs → downstream (Foundation v2 contract):**
  - `pillar_1_prospect_profile`
  - `pillar_2_voc_language_bank`
  - `pillar_3_competitive_intelligence`
  - `pillar_4_product_mechanism_analysis`
  - `pillar_5_awareness_classification`
  - `pillar_6_emotional_driver_inventory`
  - `pillar_7_proof_credibility_inventory`
  - `evidence_ledger`
  - `quality_gate_report`
  - `cross_pillar_consistency_report`
- **Output format:** Structured JSON with `schema_version: \"2.0\"` and hard-gate pass/fail report
- **Deep research file:** `agent_1a_foundation_research.md` — covers Schwartz awareness/sophistication frameworks, 4-Lens Research Stack, VoC methodology, desire mapping, JTBD, competitive positioning, output schema

#### Agent 1B: Creative Format Scout (FUTURE — not in pipeline)

> **Status:** Planned as a standalone daily agent that discovers the best video concepts and creative formulas across all niches (not tied to any specific brand). Will feed a creative format library that the pipeline can reference. Not yet implemented.

---

### PHASE 2 — CREATIVE ENGINE

#### Agent 02: Matrix Planner (Awareness × Emotion)

- **Status:** Live.
- **Role:** Expands Phase 1 emotional and awareness intelligence into a branch-scoped matrix plan with per-cell brief counts.
- **Cadence:** Runs per branch whenever matrix planning is rerun.
- **Inputs:** Foundation Research v2 output + branch settings.
- **Outputs:**
  - Awareness axis (fixed 5 levels).
  - Emotion rows (research-driven).
  - Cell-level `brief_count`.
- **Primary artifact:** `creative_engine_output.json` under branch output directory.
- **Downstream contract:** Phase 3 v2 consumes matrix cells as deterministic Brief Units.

---

### PHASE 3 — SCRIPTING

> **Current runtime note:** The active scripting path is **Phase 3 v2** only.

#### Stage A: Script Writer (Brief Unit Core Drafting)

- **Status:** Live.
- **Role:** Generates one full core script per Brief Unit using Claude Opus 4.6 via Claude Agent SDK.
- **Inputs:** Brief Unit metadata + evidence pack + deterministic script spec.
- **Parallelism:** bounded (`PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL`, default `4`).
- **Outputs:** one script draft per Brief Unit (`arm_claude_sdk_core_scripts.json`), plus run manifest + summary.
- **Editorial layer (live):**
  - Manual skip/include per Brief Unit.
  - Expanded editor for line-level editing and reorder.
  - Per-output Claude chat (suggest/apply) for script refinement.

#### Stage B: Standalone Hook Generator (Milestone 2)

- **Status:** Implemented behind `PHASE3_V2_HOOKS_ENABLED`.
- **Role:** Generates standalone hook bundles from latest edited scripts using:
  - **Generation + repair:** Claude Opus 4.6 via Claude Agent SDK.
  - **Gate + scoring:** GPT-5.2 structured evaluation.
- **Execution pattern:** Diverge -> Gate/Score -> Repair (bounded) -> Rank.
- **Parallelism:** bounded (`PHASE3_V2_HOOK_MAX_PARALLEL`, default `4`).
- **Quality checks:**
  - awareness/emotion alignment,
  - evidence validity,
  - scroll-stop and specificity thresholds,
  - diversity filtering.
- **Operator workflow:** Prepare Hooks -> Run Hooks -> select one hook per eligible unit (or skip unit).
- **Artifacts:**
  - `hook_stage_manifest.json`
  - `arm_claude_sdk_hook_candidates.json`
  - `arm_claude_sdk_hook_gate_reports.json`
  - `arm_claude_sdk_hook_bundles.json`
  - `hook_selections.json`
  - `scene_handoff_packet.json`
- **Scene handoff contract:** generated now; full Scene Generator implementation is next milestone.

#### Stage C: Scene Writer (Milestone 3)

- **Status:** Implemented behind `PHASE3_V2_SCENES_ENABLED`.
- **Role:** Builds one scene plan per selected hook (per Brief Unit) with explicit per-line mode (`a_roll` or `b_roll`) and UGC-feasible direction.
- **Architecture:**
  - Deterministic compiler and validators are final pass/fail authority.
  - Claude Opus 4.6 via Claude Agent SDK handles creative draft, targeted repair, and optional polish.
  - GPT-5.2 structured evaluator contributes advisory quality metadata only.
- **Execution pattern:** Prepare -> Draft -> Deterministic gates -> targeted repair (bounded) -> re-gate -> optional polish -> re-gate.
- **Parallelism:** bounded (`PHASE3_V2_SCENE_MAX_PARALLEL`, default `4`) with per-scene-unit failure isolation.
- **Hard gates:** line coverage, explicit mode, minimum A-roll, evidence subset, claim safety, feasibility ceiling, pacing ceiling, post-polish revalidation.
- **Operator workflow (live):**
  - Run Scenes (auto-prepare),
  - review per scene unit card,
  - expand modal for line-by-line scene editing,
  - per-scene-unit Claude chat suggest/apply,
  - production handoff readiness computed from gate results + staleness.
- **Artifacts:**
  - `scene_stage_manifest.json`
  - `arm_claude_sdk_scene_plans.json`
  - `arm_claude_sdk_scene_gate_reports.json`
  - `scene_chat_threads.json`
  - `production_handoff_packet.json`
- **Staleness rules:**
  - script edits invalidate scene plans for that brief unit/arm,
  - hook edits or hook reselection invalidate affected brief unit/arm/hook paths.

---

### PHASE 4 — PRODUCTION

#### Agent 08: Screen Writer / Video Director

- **Role:** Scene-by-scene visual direction for every version
- **Inputs:** Scripts with refined hooks from Agent 05
- **Outputs → Agents 9+10:**
  - Shot-by-shot storyboard: shot type, composition, motion, transitions, pacing
  - Asset requirements per scene (B-roll, A-roll, overlays, screenshots)
  - Visual style references

#### Agent 09: Clip Maker (runs parallel with Agent 10)

- **Role:** Sources and assigns video footage
- **Inputs:** Storyboard from Agent 8
- **Outputs → Agent 11:**
  - B-roll clips from clip database + AI video (Veo/Kling)
  - Footage assigned to each scene
  - B-roll sequences assembled

#### Agent 10: AI UGC Maker (runs parallel with Agent 09)

- **Role:** Generates talking-head A-roll content
- **Inputs:** Scripts + storyboard from Agent 8
- **Outputs → Agent 11:**
  - AI-generated UGC creator footage
  - Talking-head A-roll from script
  - Synced with B-roll timing

---

### PHASE 5 — QA & LAUNCH

#### Agent 11: Clip Verify (QA Gate)

- **Role:** Technical quality check on all produced clips
- **Inputs:** Assembled clips from Agents 9+10
- **Outputs → Agent 12:**
  - QA-approved video clips + copy
  - Checks: visual consistency, audio sync, timing, resolution, safe zones
  - Flags technical issues before compliance review

#### Agent 12: Compliance / Brand Safety ◆ DEEP RESEARCH COMPLETE

- **Role:** Final policy gate before launch — one mistake = disabled account
- **Inputs:** QA-approved clips + copy from Agent 11, brand vertical/category, target markets, landing page URLs
- **Outputs → Agent 13:**
  - Compliance pass/fail per ad with risk score (0-100, Green/Yellow/Orange/Red)
  - Specific policy violations flagged with evidence snippets
  - 2-5 compliant rewrite alternatives per violation
  - Required disclosures
  - Landing page issues
  - Special category flags, age gate requirements, certification needs
  - Appeal package (for borderline holds)
- **Policy coverage:** Meta ad policies (prohibited + restricted + personal attributes + vertical-specific), TikTok ad policies, FTC guidelines (endorsements, testimonials, click-to-cancel), claim taxonomy by risk level (HIGH/MEDIUM/LOW with safe rewrites)
- **Automated detection:** Regex patterns for personal attributes, guaranteed/absolute claims, time-bound promises, before/after imagery, authority impersonation. Visual detectors for body comparisons, medical imagery, false UI. Landing page detectors for negative options, fake scarcity, missing disclosures.
- **Deep research file:** `agent_12_compliance.md` — covers complete Meta/TikTok policies, FTC guidelines, vertical-specific rules (health, beauty, weight loss, CBD, finance, alcohol, dating, housing/employment), claim taxonomy, personal attributes deep dive, 2024-2026 policy updates, compliance workflow

#### Agent 13: Pre-Launch QA & Editor Handoff

- **Role:** Measurement setup verification + human editor handoff
- **Inputs:** Compliance-approved ads from Agent 12
- **Outputs → Agent 14:**
  - Verified pixel/CAPI setup, event prioritization, UTMs, naming conventions
  - Ensures every test is measurable (Agent 15A depends on clean data)
  - Full brief packaged in Notion: scripts, clips, scenes
  - Human editor notified for final assembly approval

#### Agent 14: Launch to Facebook (Meta API)

- **Role:** Pushes approved ads live via Meta Marketing API
- **Inputs:** Editor-approved creative package from Agent 13
- **Outputs → Agent 15A:**
  - Live campaign with proper structure
  - Campaign/ad set/ad naming conventions (for attribution)
  - Targeting, budget allocation, placement configuration
  - Bid strategy setup

---

### PHASE 6 — ANALYZE & SCALE

#### Agent 15A: Performance Analyzer ◆ DEEP RESEARCH COMPLETE

- **Role:** Turns raw campaign data into winner/loser decisions with "why" diagnosis
- **Inputs:** Live campaign performance data, historical database, campaign structure from Agent 14
- **Outputs → Agent 15B + Agent 16:**
  - Winner / Loser / Watchlist classification with confidence tiers
  - Bottleneck stage diagnosis (hook problem vs. body problem vs. offer problem vs. landing page problem)
  - Element-level attribution hypotheses with supporting metrics
  - Fatigue mode classification (audience saturation vs. creative wear-out vs. auction pressure vs. offer fatigue)
  - Fatigue curve parameters (slope, half-life estimate)
  - Comment sentiment summary
- **Metrics framework by funnel stage:**
  - Hook: 3s view rate, thumb-stop rate, FFIQ index
  - Content: completion rate, avg watch time, quartile hold rates, ThruPlay rate
  - Engagement: CTR, engagement rate, save rate, share rate
  - Conversion: CPA, ROAS, CVR
  - Efficiency: CPM, CPC, cost per ThruPlay
  - Fatigue: frequency, CTR trend (3d/7d rolling), CPM trajectory, first-time impression ratio
- **Fatigue leading indicators:** Hook rate decline slope, CTR slope, frequency trajectory, FTIR collapse, comment sentiment shift
- **Deep research file:** `agent_15_performance_learning_scaling.md` — covers complete metrics framework with benchmarks (Meta + TikTok by vertical), fatigue detection/prediction, element attribution, statistical significance, creative analytics platforms

#### Agent 15B: Learning & Brief Updater ◆ DEEP RESEARCH COMPLETE

- **Role:** Captures institutional knowledge and updates creative playbook
- **Runs in parallel with Agent 16**
- **Inputs:** Performance analysis from Agent 15A + historical playbook
- **Outputs → Agents 02, 05 (feedback loop):**
  - New validated patterns (with conditions)
  - Anti-patterns / "do not do" list
  - Updated hook library with performance data
  - Updated proof modules
  - Explore queue: under-tested hypotheses with rationale
  - Format performance trends
  - Audience insight updates
  - Seasonal/timing pattern updates
- **Explore/exploit allocation:** 60-80% exploit (iterate winners), 20-40% explore (new concepts)
- **Deep research file:** `agent_15_performance_learning_scaling.md` (shared with 15A + 16)

#### Agent 16: Winner Scaling ◆ DEEP RESEARCH COMPLETE

- **Role:** Extends winner lifespan and grows spend without killing efficiency
- **Runs in parallel with Agent 15B**
- **Inputs:** Winner list from Agent 15A + fatigue diagnosis
- **Outputs → Back to pipeline + Agent 15A (telemetry):**
  - Scaling actions: budget changes, audience expansion, placement expansion
  - Creative iterations: new hooks on proven bodies, format extensions, avatar swaps
  - Winner lifecycle management: active → dormant → retired
- **Iteration priority order:**
  1. Swap hooks (highest ROI — resets novelty without breaking persuasion)
  2. Swap first-frame visual
  3. Swap CTA phrasing
  4. Swap offer framing
  5. Swap avatar
- **Budget scaling rules:** 20-30% increases, observe 24-72h before next increase
- **Deep research file:** `agent_15_performance_learning_scaling.md` (shared with 15A + 15B)

---

## Deep Research Files Index

These 5 files contain exhaustive, practitioner-level research for the 7 highest-impact agents. Use them as the primary knowledge base when building system prompts.

| File | Agents Covered | Key Frameworks |
|------|---------------|----------------|
| `agent_1a_foundation_research.md` | Agent 1A | Schwartz awareness/sophistication, 4-Lens Research Stack, VoC, JTBD, desire mapping, output schema |
| `agent_04_copywriter.md` | Agent 04 | Schwartz, Halbert, Ogilvy, Bencivenga, Georgi RMBC, PAS/AIDA/BAB, UGC pacing, beat sheet format |
| `agent_05_hook_specialist.md` | Agent 05 | Hook taxonomy (verbal+visual+combined), TikTok/CreatorIQ data, hook engineering, testing methodology |
| `agent_12_compliance.md` | Agent 12 | Meta/TikTok policies, FTC guidelines, claim taxonomy, personal attributes, regex detectors, risk scoring |
| `agent_15_performance_learning_scaling.md` | Agents 15A, 15B, 16 | Metrics framework, fatigue detection, element attribution, playbook design, scaling strategies, testing flywheel |

---

## Data Flow Summary

```
Agent 1A (quarterly) ──→ Agent 02 + all downstream (foundation truth layer)
Agent 02 ──→ Human Gate (angles with video concept options)
Human Gate ──→ Agent 04 (selected concepts)
Agent 04 ──→ Agent 05 (scripts with 1 hook each)
Agent 05 ──→ Agent 08 (scripts with refined hooks)
Agent 08 ──→ Agents 09+10 (storyboards)
Agents 09+10 ──→ Agent 11 (assembled clips)
Agent 11 ──→ Agent 12 (QA-approved clips)
Agent 12 ──→ Agent 13 (compliance-approved ads)
Agent 13 ──→ Agent 14 (measurement-verified, editor-approved)
Agent 14 ──→ Agent 15A (live campaigns)
Agent 15A ──→ Agent 15B + Agent 16 (analysis)
Agent 15B ──→ Agents 02, 05 (FEEDBACK LOOP)
Agent 16 ──→ Agent 15A (scaling telemetry)
```

---

## Claude Agent SDK Strategy

Some agents benefit from autonomous tool use (web search, API calls, file operations) rather than pure reasoning. These agents use the [Claude Agent SDK](https://platform.claude.com/docs/en/agent-sdk/overview) to give Claude built-in tools like `WebSearch`, `WebFetch`, and `Bash`.

### Which agents use the SDK and why

| Agent | SDK? | Reason |
|-------|------|--------|
| **1A** — Foundation Research | No (Claude SDK) | Uses Gemini Deep Research (Interactions API) for web research, then structured synthesis. Not implemented via Claude Agent SDK tools. |
| **1B** — Creative Format Scout | **Future** | Will autonomously discover trending video formats across niches. Not yet in pipeline. |
| **02** — Creative Engine | **Yes (Step 2)** | Step 2 runs via Claude Agent SDK on Opus with structured JSON output and citation-backed research per `angle_id`. Fallback chain: legacy Anthropic web_search API → Gemini Deep Research → built-in knowledge. Agent-level hard cap: `$20` total for full Agent 02 run. |
| **04–05** — Scripting | No | Pure creative reasoning. Multi-provider flexibility needed. No external interaction. |
| **08** — Screen Writer | No | Pure creative direction from scripts → storyboards. |
| **09** — Clip Maker | **Future** | Will use SDK to call stock footage APIs and AI video generation (Veo, Kling). |
| **10** — AI UGC Maker | **Future** | Will use SDK to call AI avatar APIs (HeyGen, Synthesia, D-ID). |
| **11** — Clip Verify | No | QA evaluation on metadata — pure reasoning. |
| **12** — Compliance | No | Must be deterministic. Applies policy rules, no autonomous web interpretation. |
| **13** — Pre-Launch QA | Borderline | Could use `WebFetch` to verify pixel setup on landing pages. |
| **14** — Launch to Meta | **Future** | Will use SDK with `Bash` to call Meta Marketing API, upload creatives, create campaigns. |
| **15A** — Performance Analyzer | **Future** | Will use SDK to pull live campaign data from Meta Ads API. |
| **15B** — Learning Updater | No | Pure synthesis from 15A's analysis into playbook updates. |
| **16** — Winner Scaling | **Future** | Will use SDK to execute scaling actions via Meta API. |

### Pattern: Two-Phase SDK Agents

SDK agents follow a two-phase pattern:
1. **Research phase (Claude Agent SDK):** Autonomous tool use — web search, API calls, file operations. Always uses Claude as the backbone model (SDK requirement).
2. **Synthesis phase (Structured LLM):** Takes raw research/data and produces the structured Pydantic output. Uses the configured provider (OpenAI, Google, or Anthropic — multi-provider flexibility preserved).

This pattern keeps the structured output quality high while adding real-world data gathering capabilities.

---

## Execution Model — Agent-by-Agent with Inline Model Picker

The pipeline runs as a **hybrid**:
- Most agents run one at a time with gates
- Copywriter runs per-script jobs in parallel (max 4), then returns to gated flow

After each gated step, a **phase gate** appears with:

1. **Review period** — inspect the agent's output before proceeding
2. **Model picker dropdown** — choose the LLM for the *next* agent (Default, Claude Opus 4.6, GPT 5.2, Gemini 3.0 Pro)
3. **Start button** — triggers only the next single agent

This gives full human control over every step: review outputs, swap models mid-pipeline, and stop at any point.

### Gate sequence

| After agent completes | Gate shows | Next agent |
|---|---|---|
| Agent 1A Step 1/2 | No manual gate by default; auto-continues to Step 2/2 | Agent 1A Step 2/2 |
| Agent 1A (Foundation Research final) | Review research + pick model | Agent 02 (Creative Engine) |
| Agent 02 (Creative Engine) | Select concepts + pick model | Agent 04 (Copywriter) |
| Agent 04 (Copywriter) | Review scripts + pick model | Agent 05 (Hook Specialist) |
| Agent 05 (Hook Specialist) | Review hooks | Pipeline complete (Phase 3 end) |

### Model options

| Value | Label | Notes |
|---|---|---|
| *(empty)* | Default | Uses the per-agent default from `config.py` |
| `anthropic/claude-opus-4-6` | Claude Opus 4.6 | |
| `openai/gpt-5.2` | GPT 5.2 | |
| `google/gemini-3.0-pro` | Gemini 3.0 Pro | |

Agent 1A also supports "Deep Research" as its default (Gemini-powered multi-step research).

### Implementation

- **Backend** (`server.py`):
  - `_wait_for_agent_gate()` emits a `phase_gate` WebSocket event and waits for user approval
  - Standard gates continue via `/api/continue` with optional `model_override`
  - Creative Engine -> Copywriter gate continues via `/api/select-concepts` (selected concepts + optional model override)
  - Copywriter failures can be retried via `/api/rewrite-failed-copywriter`
- **Frontend** (`app.js`): `showPhaseGate()` renders the gate UI with `buildModelPicker()`, `continuePhase()` calls `/api/continue` or `/api/select-concepts` based on gate type
- **Branch pipelines** use the same gate system

---

## Building Order (Recommended)

Build agents in dependency order — upstream agents must exist before downstream ones can reference their output schemas.

1. Agent 1A (foundation — everything depends on this)
2. Agent 02 (creative engine — 3-step with Claude Web Search, needs 1A output schema)
3. Agent 04 (copywriter — needs selected concepts from 02 + 1A)
4. Agent 05 (hook specialist — needs 04 schema)
5. Agent 08 (screen writer — needs 05 schema)
6. Agents 09+10 (clip maker + AI UGC — need 08 schema)
7. Agent 11 (clip verify — needs 09+10 output)
8. Agent 12 (compliance — needs 11 output)
9. Agent 13 (pre-launch QA — needs 12 output)
10. Agent 14 (launch — needs 13 output)
11. Agent 15A (performance — needs 14 output + metrics)
12. Agents 15B + 16 (learning + scaling — need 15A output)

---

## Tentative List

1. Hook Generator expanded modal parity with Script Writer (full edit controls in expanded view).
2. Enforce no framework/meta terms in customer-facing copy (for example: "pattern interrupt/interupt") across Script Writer + Hook Generator verbal outputs.
