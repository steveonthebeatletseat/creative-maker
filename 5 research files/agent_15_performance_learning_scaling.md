# Agent 15 — Performance + Learning + Scaling

*Generated: 2026-02-11 16:46 | Model: gpt-5.2 | Time: 130.5s*

---

Below is a practitioner-grade research spec for the **complete feedback loop**: **Agent 15A (Performance Analyzer) → Agent 15B (Learning Updater) → Agent 16 (Winner Scaling)**. It’s written as if you’re building these agents as deterministic “operators” that pass structured artifacts downstream—so you can actually automate decisions, not just generate commentary.

I’ll cover:
- **Metric system + thresholds** by funnel stage (including hook/hold metrics)
- **Benchmarks (Meta + TikTok)** and how to normalize them
- **Fatigue detection + prediction** (leading indicators, not lagging CPA)
- **Element attribution** (modular variable isolation + creative analytics tooling)
- **Significance rules** for “directional” vs “stat-sig”
- **Institutional learning system** that compounds (playbooks + database design)
- **Scaling strategy** that extends winner lifespan (iteration trees, budget scaling, anti-fatigue)
- A **continuous testing flywheel** with velocity targets and budget allocation

Where platform definitions matter: Meta’s **3-second video views** is counted when the video plays **≥3 seconds** (or **97%** if shorter than 3s). ([facebook.com](https://www.facebook.com/help/1443647412620316/?utm_source=openai))  
Meta’s **ThruPlay** is completion for videos **<15s (≈97%)**, or **15 seconds watched** for videos longer than 15s. ([theadspend.com](https://theadspend.com/glossary/thruplay?utm_source=openai))

---

## SECTION A — PERFORMANCE ANALYSIS (Agent 15A)

### Agent 15A mission
Turn raw platform + attribution data into:
1) **Winner / Loser / Watchlist** decisions  
2) A **“Why it won/lost” diagnosis** (element-level hypotheses with confidence)  
3) A **next-action plan** for Agent 16 (scale/iterate) and Agent 15B (capture learning)

### A0) The core model: Creative = a funnel inside a funnel
Every paid social creative has its own internal funnel:

**Impression → Hook → Hold → Click/Engage → Landing conversion → Purchase/Lead quality**

Your analyzer should score each stage separately so you don’t “blame the offer” for a hook problem, or “blame the hook” for a landing page problem.

---

# 1) KEY METRICS FRAMEWORK (by funnel stage)

## 1.1 Hook metrics (first 1–3 seconds)
These are your earliest “signal” metrics and best predictors of scalable winners.

**Primary hook KPIs**
- **3-second view rate (“Hook Rate”)** = 3s views / impressions  
  - Use for Meta video placements where available; on TikTok use 2s/6s view rates if accessible, but keep a unified “hook rate” metric.
- **Thumb-stop rate (TSR)** (your definition):  
  - Typically proxied by **3s view rate** or **2s view rate**, plus **negative signals** (swipes away, hides, “not interested”) when available.
- **First-frame impression quality (FFIQ)** (proxy index, see below)

**FFIQ (First-Frame Impression Quality) — build an index**
Because platforms don’t expose “first-frame quality,” create an index from proxies:
- Hook rate vs account baseline
- CTR vs account baseline
- CPM discount/premium vs ad set average (high engagement often yields more efficient CPM over time, especially on TikTok)

**Hook diagnosis rules**
- **Low Hook Rate + normal CTR** rarely happens. If it does: you’re likely getting clicks from a small captivated niche while most bounce instantly → creative is polarizing or misleading.
- **High Hook Rate + low CTR** = “entertaining, not persuading” → the body/offer/CTA is the bottleneck.

## 1.2 Content / retention metrics (3s → end)
These tell you whether the ad *keeps* attention after stopping the scroll.

- **Average watch time**
- **Completion rate**
- **Quartile hold rates**: % reaching 25/50/75/95%
- **ThruPlay rate** (Meta) + **Cost per ThruPlay** ([theadspend.com](https://theadspend.com/glossary/thruplay?utm_source=openai))

**Interpretation**
- Hook gets you cheap reach; **hold** gets you cheap persuasion.
- The best scaling ads often have *both*:
  - above-median hook
  - above-median hold past ~25–50%

## 1.3 Engagement metrics (social proof + algorithmic lift)
- **CTR (link clicks / impressions)**
- **Engagement rate** = (reactions + comments + shares) / impressions
- **Save rate** (esp. TikTok/IG Reels; often correlates with “value content”)
- **Share rate** (one of the strongest “cheap distribution” signals on TikTok)

## 1.4 Conversion metrics (the business outcome)
- **CPA / CPL**
- **ROAS** (but guard against early attribution noise)
- **CVR** = conversions / link clicks
- **Cost per result by objective**

## 1.5 Efficiency metrics (media buying mechanics)
- **CPM**
- **CPC**
- **Cost per ThruPlay** (Meta video) ([theadspend.com](https://theadspend.com/glossary/thruplay?utm_source=openai))

## 1.6 Fatigue metrics (lifespan & decay)
- **Frequency**
- **CTR trend** (3-day + 7-day rolling)
- **CPM trajectory**
- **First-time impression ratio** (FTIR): new impressions / total impressions
- **Comment sentiment trend** (see fatigue section)

---

# 1.7 Benchmarks (Meta + TikTok) + how to use them correctly

### The right way to benchmark
Benchmarks are not “targets.” Your agent should use them as:
- **Plausibility bounds** (is this metric broken?)
- **Triage** (what to fix first)
- **Creative-type expectations** (UGC vs polished, direct-response vs brand)

Also: normalize by **objective**, **placement mix**, **geo**, and **spend tier**.

---

## Meta benchmarks (broad, by industry)
There is wide variance across sources; treat these as *starting medians*.

A 2026 benchmark compilation shows Meta averages by industry roughly like:  
- Apparel: CTR ~1.24%, CPC ~$0.45, CPM ~$5.58  
- Beauty: CTR ~1.16%, CPC ~$0.48, CPM ~$5.57  
- Finance: CTR ~0.56%, CPC ~$1.92, CPM ~$10.75  
- Technology: CTR ~1.04%, CPC ~$1.27, CPM ~$13.21 ([adbid.me](https://adbid.me/blog/advertising-performance-benchmarks-guide-2026?utm_source=openai))

Another 2025 ecom benchmark source reports higher CTRs in some vertical slices (e.g., Fashion CTR ~2.64%). ([madgicx.com](https://madgicx.com/blog/meta-ads-benchmarking?utm_source=openai))

**Practical rule for Agent 15A:**  
Store benchmarks as **ranges** and always compare to:
1) **account baseline (last 30 days)**  
2) **campaign type baseline (TOF vs BOF)**  
3) then industry medians

---

## TikTok benchmarks (broad, by industry + video consumption)
A 2026 compilation reports TikTok by industry approximately:  
- eCommerce: CPM ~$6.20, CTR ~1.40%, CPC ~$0.44  
- Beauty: CPM ~$6.40, CTR ~1.75%, CPC ~$0.37  
- Finance: CPM ~$12.50, CTR ~0.90%, CPC ~$1.39 ([adbid.me](https://adbid.me/blog/advertising-performance-benchmarks-guide-2026?utm_source=openai))

A separate 2025 report suggests TikTok overall CTR averaged ~0.84% in 2025. ([shno.co](https://www.shno.co/marketing-statistics/tiktok-ads-statistics?utm_source=openai))

TikTok video consumption benchmarks (useful for hold scoring):  
- Avg watch time: ~8–12s (top performers 15s+)  
- Completion (15s): ~40–50% (top 65%+)  
- Completion (30s): ~25–35% (top 50%+) ([adbid.me](https://adbid.me/blog/advertising-performance-benchmarks-guide-2026?utm_source=openai))

**Important:** TikTok “good CTR” for in-feed is often under 1% depending on vertical; don’t import Meta CTR expectations into TikTok. ([billo.app](https://billo.app/blog/what-is-a-good-ctr/?utm_source=openai))

---

## 1.8 How top buyers actually analyze creative (operationally)
Even when top media buyers disagree on exact thresholds, their workflow tends to converge:

### A) “Stage-gate” evaluation (TOF → MOF → BOF)
- Gate 1: **Hook** (if it doesn’t stop scroll, nothing else matters)
- Gate 2: **Hold** (if it doesn’t persuade, clicks get expensive)
- Gate 3: **Click intent** (CTR/CPC)
- Gate 4: **Post-click reality** (CVR, CPA, MER)

### B) “Creative as the targeting”
They treat creative variants as *audience matchers*:
- hook frames the problem → selects the audience
- proof mechanism reduces skepticism → increases conversion
- offer framing increases urgency → increases AOV/ROAS

### C) “Creative scorecards” > gut feel
The best teams use **consistent scorecards** that separate:
- creative quality (hook/hold)
- media efficiency (CPM/CPC)
- offer/landing performance (CVR)

Your Agent 15A should implement this as a weighted model.

---

# 2) CREATIVE FATIGUE — DETECTION AND PREDICTION

## 2.1 Fatigue is not “CPA went up”
CPA is a lagging indicator. Fatigue is primarily a **distribution + attention** decay.

### Fatigue modes (separate them!)
1) **Audience saturation fatigue**: same people have seen it too many times  
2) **Creative wear-out**: novelty is gone even in new audiences  
3) **Auction pressure**: CPM rising due to competition/seasonality  
4) **Offer fatigue**: the creative still works, but the offer is stale  
5) **Measurement drift**: attribution / pixel / mix changes look like fatigue

Agent 15A should classify fatigue mode, because Agent 16’s fix depends on it.

---

## 2.2 Leading indicators (catch fatigue early)

### A) Hook-rate decline patterns
Track **Hook Rate slope** daily:
- 3-day slope (fast signal)
- 7-day slope (stable signal)

**Early fatigue signature:** Hook Rate down while CPM stable → creative wear-out  
**Auction signature:** CPM up while Hook Rate stable → market pressure, not creative failure

### B) CTR slope (3-day and 7-day)
CTR is often the best early warning:
- If Hook Rate stable but CTR is falling → body/offer/CTA fatigue (people recognize it and ignore the pitch)

### C) Frequency trajectory + thresholds
Frequency is placement- and audience-dependent, but you can still automate heuristics:
- Watch **rate of change** (Δfreq/day) more than the absolute number
- Combine frequency with **FTIR (First-time impression ratio)**  
  - If FTIR is dropping, you’re re-serving the same users.

### D) First-time impression ratio (FTIR)
Define: **new impressions (first time a user saw ad) / total impressions**  
- If FTIR falls steadily, expect CTR to decay next.

### E) Comment sentiment as early warning
Build a simple classifier:
- “seen this already”
- “stop showing me this”
- sarcasm / annoyance
- repetitive objections (“scam”, “doesn’t work”, “too expensive”)

Often comment negativity spikes *before* KPI collapse, especially on TikTok.

---

## 2.3 Distinguish normal dips vs real fatigue (decision rules)

**Normal volatility**
- day-of-week patterns
- learning phase shifts (especially after edits)
- budget step-changes
- placement mix changes

**Fatigue confirmation**
Trigger “fatigue likely” when **2 of 3** happen:
1) CTR 7-day rolling average down ≥ X% vs prior 7-day  
2) Hook Rate down ≥ Y% vs prior 7-day  
3) FTIR down ≥ Z% or frequency accelerating

You’ll need to set X/Y/Z per account, but good starting defaults:
- X = 15–25%
- Y = 10–20%
- Z = 15–30%

---

## 2.4 Do fatigue curves follow predictable patterns?
Yes, often **piecewise**:
1) **Launch spike** (novelty + algorithm exploration)
2) **Stabilization plateau**
3) **Slow decay** (saturation)
4) **Cliff** (when negative feedback dominates or audience is exhausted)

Agent 15A should fit a simple curve model:
- exponential decay on CTR or Hook Rate
- detect “cliff risk” when curvature increases

---

## 2.5 How frequency capping extends lifespan
Frequency caps can delay saturation in small audiences, but:
- caps often reduce volume and can push spend to less efficient pockets
- caps don’t fix “creative wear-out” in new users

So the agent should recommend caps only when:
- FTIR is low (saturated)
- audience size is constrained
- the creative still performs well among *first-time* impressions

---

# 3) ELEMENT ATTRIBUTION — WHAT CAUSED THE WIN/LOSS?

## 3.1 The modular attribution approach (the only scalable approach)
To attribute “why,” you need:
- a **creative schema**
- a **testing discipline** where variants isolate variables
- a **taxonomy** that tags every asset

### Creative schema (store as structured fields)
At minimum, tag:
- Hook type (pattern interrupt / claim / question / shock / demo / social proof)
- Hook message (the promise)
- Avatar (who speaks; identity cues)
- Visual style (UGC selfie, studio, b-roll, screen recording)
- Proof mechanism (testimonial, demo, before/after, authority, data)
- Offer framing (discount, bundle, trial, guarantee)
- CTA type (direct, soft, quiz, DM, lead form)
- Audio (trending sound, voiceover, silence)
- Length bucket (6–10s / 11–20 / 21–35 / 36–60)
- Placement intent (Reels-first, Feed-first, TikTok-first)

Then attribution becomes a **join problem**: performance metrics grouped by tags.

---

## 3.2 “Not all elements fatigue at the same rate”
Operationally true in most accounts:
- **Hooks fatigue fastest** (novelty dies)
- **Bodies fatigue slower** (proof mechanisms remain persuasive)
- **Offers fatigue episodically** (when competitors match or audience has seen it repeatedly)
- **Avatars** can fatigue (face recognition) but also build trust across series

Agent 15A should therefore diagnose fatigue at element-level:
- Hook fatigue: Hook Rate down more than Hold
- Body fatigue: Hook stable, Hold/CTR down
- Offer fatigue: CTR stable, CVR down

---

## 3.3 Creative analytics platforms — what they’re good for
These tools don’t replace modular testing; they accelerate it.

### Motion / “creative analytics” category
Typically best for:
- organizing creatives by concepts
- visualizing fatigue and iteration history
- comparing hook/hold metrics across variants

### Triple Whale / Northbeam / Hyros (measurement layer)
Best for:
- more reliable conversion attribution vs platform-reported
- cohorting (new vs returning)
- creative performance by revenue, not just CPA

Triple Whale’s positioning emphasizes first-party / server-side style data capture to reduce signal loss vs platform pixels. ([triplewhale.com](https://www.triplewhale.com/triple-pixel?utm_source=openai))

### Rockerbox / MTA-MMM-testing platforms
Best for:
- triangulating “truth” with MTA/MMM/testing rather than one model ([rockerbox.com](https://www.rockerbox.com/?utm_source=openai))
- understanding channel overlap and incremental lift

**Agent design implication:**  
Agent 15A should output two “truths”:
1) **In-platform performance truth** (fast optimization)
2) **Business truth** (incremental / blended truth)

…and it should *explicitly reconcile* them when they diverge.

---

# 4) STATISTICAL SIGNIFICANCE IN AD TESTING

## 4.1 Don’t use one universal “winner” threshold
Creative performance is heteroskedastic (variance differs by audience, placement, spend). So use **two-tier decisions**:

### Tier 1: Directional decisions (fast)
Purpose: decide what to iterate next.
- Use when you need velocity and can tolerate false positives.

### Tier 2: Statistical confidence decisions (slow)
Purpose: decide what to scale hard.
- Use when you’ll allocate meaningful budget.

---

## 4.2 The “50 conversions” rule — is it enough?
It can be enough for **directional** if:
- conversion event is stable (purchase, qualified lead)
- attribution is consistent
- you’re comparing within the same campaign structure

But for true statistical confidence on small lifts, you often need more.

**Agent 15A recommended output:**  
For each comparison, output:
- conversions per variant
- spend per variant
- effect size (CPA delta, CVR delta)
- “decision tier”: directional vs confident
- risk rating: low/med/high

---

## 4.3 Practical data minimums (usable heuristics)
These are pragmatic rules top buyers use (not academic perfection):

### For TOF creative triage
- 3s views: you can decide fast (few thousand impressions)
- CTR: often stabilizes after a few thousand impressions, but depends on audience breadth

### For purchase CPA decisions
- try to get **20–50 conversions per creative concept** before calling “scale”
- if you can’t, scale by *leading indicators* + small incremental budget steps

### For small CPA improvements (e.g., 5–10%)
You may need far more conversions to be truly confident. In practice:
- treat sub-10% differences as “within noise” unless repeated across multiple tests.

---

# Outputs Agent 15A should produce (structured)
1) **Creative Scorecard (per ad / per concept)**
- Hook score (Hook Rate vs baseline)
- Hold score (watch time, quartiles)
- Click score (CTR/CPC)
- Conversion score (CPA/ROAS/CVR)
- Fatigue risk score (slope + FTIR + freq)
- Confidence tier (directional/confident)

2) **Root-cause diagnosis**
- Primary bottleneck stage
- Hypothesized failing element(s)
- Evidence (metrics that support hypothesis)

3) **Next actions**
- Scale / iterate / pause
- Suggested iteration plan (for Agent 16)
- Learning payload (for Agent 15B)

---

---

## SECTION B — LEARNING SYSTEMS (Agent 15B)

### Agent 15B mission
Turn analysis into **institutional memory** that:
- is searchable
- is reusable by upstream agents
- doesn’t collapse into “we think hooks matter”

This is where most teams fail: they “learn” in Slack and forget in two weeks.

---

# 5) BUILDING INSTITUTIONAL CREATIVE KNOWLEDGE

## 5.1 What to capture after every batch (minimum viable learning)
For each batch, store:

### A) Winning patterns (positive)
- Top hooks by avatar + offer + mechanism
- Top proof mechanisms by product type
- Top CTA phrasing patterns
- High-performing lengths by placement

### B) Losing patterns (negative)
- Hooks that spike Hook Rate but fail CTR (entertainment trap)
- Claims that trigger negative comments / compliance risk
- Formats that drive cheap clicks but poor CVR (curiosity clickbait)

### C) Audience insights
- Top objections from comments
- Unexpected use cases
- Vocabulary that resonates (phrases customers repeat)

### D) Seasonality / context
- promo calendar
- competitor events (if captured by Agent 1B)
- shipping cutoffs / holidays

---

## 5.2 How agencies structure playbooks so they actually compound
The key is **playbook = decision rules + examples**, not “inspiration.”

A usable playbook entry has:
- “When to use” conditions
- hook script templates
- proof modules (demo/testimonial/data)
- failure modes
- example ads (creative IDs)
- metric signatures (“high hook, low hold” etc.)

---

## 5.3 The learning database that gets used (design)
If it’s not queryable by your upstream agents, it won’t get used.

### Recommended schema (tables / collections)
**CreativeAsset**
- creative_id, platform, placement, date, spend, metrics snapshot

**CreativeComponents**
- creative_id → hook_id, body_id, offer_id, CTA_id, avatar_id, audio_id, format_id

**Concept**
- concept_id, angle, mechanism, audience pain point

**Test**
- test_id, hypothesis, variants, start/end dates, constraints

**Outcome**
- creative_id, classification (winner/loser/watchlist), confidence tier

**Learning**
- learning_id, type (hook/body/offer/audience), statement, evidence links, conditions, expiry date

### Critical: “expiry date” / decay
Creative truths expire. Add:
- last_validated_date
- confidence score
- re-test trigger

---

# 6) FEEDBACK LOOP DESIGN (what flows back to whom)

## 6.1 Learning routing map (what each upstream agent needs)

### To Agent 1B (Competitive Intel)
Send:
- competitor formats that are “winning archetypes”
- new claims/angles appearing in market
- fatigue signals tied to competitive pressure (CPM rising + stable hooks)

### To Agent 2 (Idea Generator)
Send:
- top performing angles by avatar
- under-tested angles (explore quota)
- “concept adjacency map” (what to try next near winners)

### To Agent 5 (Hook Specialist)
Send:
- best hook *structures* (not just lines)
- hook fatigue patterns (which hook families decay fastest)
- hook-to-CTR correlations (hooks that stop scroll but don’t sell)

### To Agent 7 (Versioning Engine)
Send:
- winning component library (hooks, proof, CTAs)
- constraints: what not to change when iterating (protect the winning mechanism)
- iteration tree priorities (see Agent 16)

---

## 6.2 Avoid the “creative loop trap”
If you only make more of what worked, you overfit.

### Solution: enforce explore/exploit allocation
Hard-code creative allocation:
- **Exploit**: iterate winners (60–80% of new variants)
- **Explore**: new concepts (20–40%), including:
  - new avatars
  - new mechanisms
  - new offers
  - new formats

Then have Agent 15B track:
- explore hit rate
- exploit hit rate
- diminishing returns on exploit families

---

---

## SECTION C — WINNER SCALING (Agent 16)

### Agent 16 mission
Extend winner lifespan and grow spend **without collapsing efficiency**.

Agent 16 should do 3 things:
1) **Scale budgets responsibly**
2) **Scale reach (audiences/placements)**
3) **Scale creative surface area** (new hooks, formats, avatars) while preserving what caused the win

---

# 7) SCALING WITHOUT KILLING WINNERS

## 7.1 Horizontal vs vertical scaling (automatable rules)

### Vertical scaling = increase budget on same structure
Use when:
- fatigue risk low
- FTIR still healthy
- CPA stable within guardrails

**Rule of thumb steps**
- Increase budget in **small increments** (e.g., 20–30%) then observe 24–72h  
- If CPA worsens but Hook/Hold stable, you may be pushing into colder pockets—consider horizontal expansion instead.

### Horizontal scaling = expand distribution
Methods:
- duplicate into new ad set (new geo, broad vs interest, new seed)
- expand placements
- switch objective or optimization event carefully
- launch same concept on TikTok if Meta winner (after format adaptation)

Agent 16 should choose based on:
- CPM elasticity
- audience size
- FTIR trend

---

## 7.2 Modular creative scaling: “new hooks on proven bodies”
This is the highest-ROI iteration pattern in most DTC accounts.

### Methodology (iteration tree)
Assume you have a winner creative W with components:
- Hook H*
- Body B*
- Offer O*
- CTA C*
- Avatar A*

**Iteration priority order (common best practice):**
1) **Swap hooks (H1..H10) while keeping B*/O*/A***  
   - Goal: reset novelty without breaking persuasion
2) Swap **first-frame visual** (within same hook script)
3) Swap **CTA phrasing** (C variants)
4) Swap **offer framing** (O variants) — higher risk, higher upside
5) Swap **avatar** (A variants) — can reset fatigue strongly, but can also break trust

### How Agent 16 decides what to change
Use Agent 15A’s diagnosis:
- Hook fatigue → replace hook
- CTR fatigue with stable hook → tweak body/CTA
- CVR drop with stable CTR → fix offer/landing alignment, not creative

---

# 8) CREATIVE LIFESPAN EXTENSION (tactics)

## 8.1 Pause and reintroduce
Works best when fatigue was **audience saturation**, not “creative wear-out.”
Agent rule:
- pause when FTIR collapses + frequency accelerates
- reintroduce to a **fresh audience** (new geo, broad, new seed) after cooldown

## 8.2 Format extension strategies
Turn one winner into multiple “skins”:
- UGC selfie → captioned meme edit
- demo → unboxing → comparison → FAQ
- 30s narrative → 15s cutdown → 6–10s punchy version

## 8.3 Refresh frequency by platform (practical guidance)
- TikTok often requires faster refresh due to swipe culture and trend cycles.
- Meta can sustain longer if you keep producing hook refreshes and broaden distribution.

(Exact day counts vary massively by spend and audience size; your system should learn per account using fatigue curves rather than fixed timelines.)

## 8.4 Audience rotation
Rotate:
- broad vs interest vs LAL
- new vs returning
- high-LTV cohorts (if you have signal)
- placement-first segmentation (Reels-first vs Feed-first)

---

# 9) THE CONTINUOUS TESTING FLYWHEEL

## 9.1 How top DTC brands sustain 50–100+ variants/month
They industrialize:
- 5–10 **concepts** per month
- 5–15 **variants per concept** (mostly hook swaps)
- tight measurement + learning capture
- strict creative taxonomies

Your 15A/15B/16 trio is what makes that scalable.

## 9.2 The “30% budget for new creative tests” rule
Common operating principle:
- keep a non-trivial share of spend for exploration
- prevents overfitting and keeps pipeline fed with future winners

Agent 16 should enforce a dynamic rule:
- if exploit hit rate is high → you can temporarily reduce explore
- if fatigue is accelerating across winners → increase explore

## 9.3 Testing velocity benchmarks by spend level (how to operationalize)
Instead of hard external benchmarks, use a **spend-indexed velocity target**:

Define:
- **Variants per $10k spend/month** (or per $100k)
- **Concepts per month**
- **Iteration depth** (avg variants per concept)

Then set targets that scale with spend and team capacity:
- low spend: fewer concepts, faster pruning
- high spend: more concepts, larger variant trees, more audience splits

## 9.4 When to retire vs archive winners
Agent 16 should classify winners into:
- **Active winner**: scale + iterate
- **Dormant winner**: fatigue high, but historically strong → park for future reintro
- **Retired**: compliance risk, brand risk, or consistently decays even in fresh audiences

Use rules combining:
- fatigue risk score
- performance vs baseline
- reintro test results

---

# Putting it all together: the compounding loop (what each agent hands off)

## Agent 15A → Agent 15B (Learning payload)
- Winner/loser list with confidence tiers
- Bottleneck stage classification
- Element-level hypotheses + supporting metrics
- Fatigue curve params (slope, half-life estimate)
- Comment sentiment summary (top objections, annoyance signals)

## Agent 15B → Agents 1B/2/5/7 (Playbook updates)
- New validated patterns (with conditions)
- Anti-patterns / “do not do”
- Updated hook library + proof modules
- Explore queue: under-tested hypotheses with rationale

## Agent 16 → Agent 15A (Scaling telemetry)
- Scaling actions taken (budgets, audiences, placements, iterations)
- Post-scale performance deltas (did we break it?)
- Lifespan extension outcomes (which iteration type reset fatigue best)

---

## If you want, I can turn this into build-ready artifacts
1) **JSON schemas** for:
- CreativeScorecard
- FatigueRiskReport
- ElementAttributionHypothesis
- LearningEntry
- ScalingPlan + IterationTree

2) A **scoring model** (weights + guardrails) for automatic “scale/pause/iterate” decisions.

Tell me:
- your primary business type (DTC ecom vs lead gen vs app)
- average monthly spend tier (e.g., <$50k, $50–250k, $250k+)
- whether you have Triple Whale/Northbeam/Hyros/Rockerbox or only platform data