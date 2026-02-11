# 16-Agent Ad Pipeline — Full Architecture

## Project Overview

An automated paid social ad creation pipeline. 16 specialized AI agents pass structured outputs to each other in sequence — from customer research through creative production to campaign launch, performance analysis, and scaling.

Agents marked with **◆** have completed deep research (comprehensive practitioner-level research docs available as separate `.md` files). These research docs should be used as the primary knowledge source when building those agents' system prompts.

---

## Pipeline Flow

```
PHASE 1 — RESEARCH
  ┌─────────────────────────┬─────────────────────────┐
  │ Agent 1A ◆              │ Agent 1B                │
  │ Foundation Research      │ Trend & Competitive     │
  │ (runs in parallel)       │ Intel (runs in parallel)│
  └────────────┬────────────┴────────────┬────────────┘
               └────────────┬────────────┘
                            ▼
PHASE 2 — IDEATION
  ┌─────────────────────────────────────────────────────┐
  │ Agent 02 — Idea Generator                           │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 03 — Stress Tester Pass 1 (Strategic)         │
  └────────────────────────┬────────────────────────────┘
                           ▼
PHASE 3 — SCRIPTING
  ┌─────────────────────────────────────────────────────┐
  │ Agent 04 ◆ — Copywriter                             │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 05 ◆ — Hook Specialist                        │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 06 — Stress Tester Pass 2 (Script-Level)      │
  └────────────────────────┬────────────────────────────┘
                           ▼
  ┌─────────────────────────────────────────────────────┐
  │ Agent 07 — Versioning Engine                        │
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
               │  1B, 2, 5, 7           │
               └────────────────────────┘
```

---

## Agent Details

### PHASE 1 — RESEARCH

#### Agent 1A: Foundation Research ◆ DEEP RESEARCH COMPLETE

- **Role:** Deep customer/market intelligence. The "truth layer" everything downstream depends on.
- **Cadence:** Runs quarterly (not per-batch)
- **Inputs:** Brand/product info, customer reviews & feedback, competitor landscape, previous performance data
- **Outputs → All downstream agents:**
  - Customer awareness level map (Schwartz 5 levels)
  - Market sophistication stage diagnosis (Schwartz stages 1-5)
  - Verbatim customer language bank (pains, desires, objections, metaphors)
  - Core desires, fears, objection taxonomy
  - Competitive messaging map + white space analysis
  - Angle inventory (20-60 angles with hooks, claims, proof types, compliance flags)
  - Testing plan + compliance pre-brief
- **Output format:** Structured JSON with stable keys: `segments[]`, `awareness_playbook{}`, `sophistication_diagnosis{}`, `voc_library[]`, `competitor_map[]`, `angle_inventory[]`, `testing_plan{}`, `compliance_prebrief{}`
- **Deep research file:** `agent_1a_foundation_research.md` — covers Schwartz awareness/sophistication frameworks, 4-Lens Research Stack, VoC methodology, desire mapping, JTBD, competitive positioning, output schema

#### Agent 1B: Trend & Competitive Intel

- **Role:** Real-time competitive and cultural intelligence
- **Cadence:** Runs fresh every batch
- **Inputs:** Agent 1A foundation brief, current ad libraries, cultural landscape
- **Outputs → Agent 2:**
  - Trending formats & sounds
  - Competitor ad analysis (hooks, visuals, offers)
  - Cultural moments to tap into
  - Currently-working hooks in niche
- **Note:** No deep research — web research + Agent 15B feedback loop covers this well

---

### PHASE 2 — IDEATION

#### Agent 02: Idea Generator

- **Role:** Produces creative concepts across the funnel
- **Inputs:** Agent 1A research brief + Agent 1B competitive intel
- **Outputs → Agent 3:**
  - 30 ad ideas (10 ToF, 10 MoF, 10 BoF)
  - Each idea = angle + emotional lever + format + hook direction
  - Mapped to specific avatar segments
  - Diversity rules enforced: varied angles, emotions, formats
  - 2-3 bold "swing" ideas per stage

#### Agent 03: Stress Tester — Pass 1 (Strategic)

- **Role:** Quality gate — evaluates ideas against research brief
- **Inputs:** 30 ideas from Agent 2 + Agent 1A research brief
- **Outputs → Agent 4:**
  - 15 surviving ideas (5 per funnel stage)
  - Filtered on: angle strength, differentiation, emotional resonance, compliance viability
  - Kill reasons documented for rejected ideas

---

### PHASE 3 — SCRIPTING

#### Agent 04: Copywriter ◆ DEEP RESEARCH COMPLETE

- **Role:** Core persuasion engine — writes actual ad scripts
- **Inputs:** 15 approved concept briefs from Agent 3 + customer language bank from Agent 1A
- **Outputs → Agent 5:**
  - Full production-ready scripts (15s, 30s, 60s)
  - Each script includes: spoken dialogue, on-screen text, visual direction, SFX/music cues, timing
  - Copy framework used per script (PAS, AIDA, BAB, Star-Story-Solution, etc.)
  - Metadata: awareness target, big idea, single core promise, mechanism line, proof assets needed, compliance flags
  - CTA variations
- **Output format:** Human-readable beat sheet (time-coded table) + strict JSON with `beats[]`, `proof[]`, `cta{}`, `mechanism{}`
- **Quality gates:** One idea per script, mechanism line exists, proof moment exists, top objection addressed, CTA singular, pacing within 150-160 WPM, on-screen text readable
- **Deep research file:** `agent_04_copywriter.md` — covers Schwartz, Halbert, Ogilvy, Bencivenga, Georgi (RMBC), Hopkins, Goff, Albuquerque. Full framework breakdown: PAS, AIDA, BAB, Star-Story-Solution, 4 U's. UGC pacing rules, awareness-level copy strategies, script output format

#### Agent 05: Hook Specialist ◆ DEEP RESEARCH COMPLETE

- **Role:** Engineers the first 3 seconds — highest-leverage element in the pipeline
- **Inputs:** Complete scripts from Agent 4 + target awareness level + platform targets + hook performance data from previous batches
- **Outputs → Agent 6:**
  - 3-5 hook variations per script
  - Each hook = verbal (first words spoken/shown) + visual (first frames) as a matched pair
  - Sound-on and sound-off versions
  - Hook category tags for testing taxonomy
  - Platform-specific versions (Meta Feed, Reels, TikTok)
  - Risk flags (compliance/claims)
- **Output format:** JSON objects per hook: `hook_id`, `hook_family`, `verbal_open`, `on_screen_text`, `visual_first_frame`, `edit_notes`, `sound_on_variant`, `sound_off_variant`, `risk_flags`, `intended_awareness_stage`, `expected_metric_target`
- **Key metric:** Hook Rate = 3-second views / impressions. Tiers: <20% failing, 20-30% serviceable, 30-40% good, 40-55% excellent, 55%+ elite
- **Deep research file:** `agent_05_hook_specialist.md` — covers TikTok/CreatorIQ data, comprehensive hook taxonomy (verbal + visual + combined), hook engineering psychology, testing methodology, platform-specific patterns, trends 2024-2026

#### Agent 06: Stress Tester — Pass 2 (Script-Level)

- **Role:** Quality gate for actual scripts and hooks
- **Inputs:** Scripts with hooks from Agents 4+5 + Agent 1A research brief
- **Outputs → Agent 7:**
  - 9 winning scripts (3 per funnel stage)
  - Filtered on: hook strength, flow, persuasion, emotional arc, compliance pre-screen
  - Light compliance flag for Agent 12

#### Agent 07: Versioning Engine

- **Role:** Creates strategic variations for testing
- **Inputs:** 9 winning scripts from Agent 6 + testing priorities from Agent 15B
- **Outputs → Agent 8:**
  - Length versions (15s, 30s, 60s)
  - CTA variations (urgency vs. curiosity vs. social proof)
  - Tone variations (casual vs. authoritative vs. emotional)
  - Platform variations (FB feed, IG Reels, TikTok)
  - Testing matrix with naming conventions for attribution

---

### PHASE 4 — PRODUCTION

#### Agent 08: Screen Writer / Video Director

- **Role:** Scene-by-scene visual direction for every version
- **Inputs:** Versioned scripts from Agent 7
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
- **Inputs:** Live campaign performance data, testing matrix from Agent 7, historical database, campaign structure from Agent 14
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
- **Outputs → Agents 1B, 2, 5, 7 (feedback loop):**
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
Agent 1A (quarterly) ──→ All agents (foundation truth layer)
Agent 1B (per batch) ──→ Agent 2
Agent 2 ──→ Agent 3 (30 ideas)
Agent 3 ──→ Agent 4 (15 survivors)
Agent 4 ──→ Agent 5 (scripts)
Agent 5 ──→ Agent 6 (scripts + hooks)
Agent 6 ──→ Agent 7 (9 winners)
Agent 7 ──→ Agent 8 (versioned scripts)
Agent 8 ──→ Agents 9+10 (storyboards)
Agents 9+10 ──→ Agent 11 (assembled clips)
Agent 11 ──→ Agent 12 (QA-approved clips)
Agent 12 ──→ Agent 13 (compliance-approved ads)
Agent 13 ──→ Agent 14 (measurement-verified, editor-approved)
Agent 14 ──→ Agent 15A (live campaigns)
Agent 15A ──→ Agent 15B + Agent 16 (analysis)
Agent 15B ──→ Agents 1B, 2, 5, 7 (FEEDBACK LOOP)
Agent 16 ──→ Agent 15A (scaling telemetry)
```

---

## Building Order (Recommended)

Build agents in dependency order — upstream agents must exist before downstream ones can reference their output schemas.

1. Agent 1A (foundation — everything depends on this)
2. Agent 1B (competitive intel — parallel with 1A)
3. Agent 2 (idea generator — needs 1A+1B output schemas)
4. Agent 3 (stress tester P1 — needs 1A+2 schemas)
5. Agent 4 (copywriter — needs 1A+3 schemas)
6. Agent 5 (hooks — needs 4 schema)
7. Agent 6 (stress tester P2 — needs 4+5 schemas)
8. Agent 7 (versioning — needs 6 schema)
9. Agent 8 (screen writer — needs 7 schema)
10. Agents 9+10 (clip maker + AI UGC — need 8 schema)
11. Agent 11 (clip verify — needs 9+10 output)
12. Agent 12 (compliance — needs 11 output)
13. Agent 13 (pre-launch QA — needs 12 output)
14. Agent 14 (launch — needs 13 output)
15. Agent 15A (performance — needs 14 output + metrics)
16. Agents 15B + 16 (learning + scaling — need 15A output)
