# Agent 05 — Hook Specialist

*Generated: 2026-02-11 16:46 | Model: gpt-5.2 | Time: 155.1s*

---

Below is a **hook-specialist research doc** designed to become the operating system for **Agent 5 (Hook Specialist)** in your 15-agent paid-social pipeline. It’s written as a practitioner blueprint: **definitions → measurable metrics → platform rules → hook taxonomy → engineering principles → testing & fatigue ops → trend layer (2024–2026)**. Where hard platform studies exist, I cite them; where the ecosystem relies on practitioner benchmarks, I label them as such.

---

## 0) Agent 5 role definition (so you can spec outputs + score it)

### Agent 5 = “0–3 second conversion of attention into intent”
**Inputs:** a complete script from Agent 4 (Copywriter) that already includes: target, angle, offer, mechanism, proof, CTA.  
**Output:** for each script, generate **3–5 hook variations** with:
- **Verbal hook**: first words spoken + first on-screen text.
- **Visual hook**: first frames (composition, action, motion, subject, overlay).
- **Hook “pairing”**: verbal + visual are engineered as a matched unit (not separately “randomly good ideas”).
- **Sound-on and sound-off** versions (same idea; different encoding).

### Hook Specialist “definition of done”
A hook is done when it:
1) Wins **attention** fast (stops scroll).  
2) Pre-qualifies the right viewer (reduces “curiosity clicks” that don’t buy).  
3) Creates a **tension loop** that makes the viewer accept the next beat (3–8s).

---

# 1) Data: why hooks matter (stats, studies, and what to actually measure)

## 1.1 The most defensible platform-backed “first seconds” findings

### TikTok (Marketing Science + partners)
TikTok / CreatorIQ findings (Dec 2023):
- **90% of ad recall impact** and **80% of awareness impact** happen in the **first 6 seconds**. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- Showing a **person/creator in the first 2 seconds** increases **“hooking power” by 50%** and improves **ad recognition by 32%**. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- “You” early matters: ads where creators **say “you” within the first 5 seconds** show **+128% uplift in purchase intent** (per the same report). ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- Text overlays: creator ads with **text overlays** are **1.4× more likely to hook** than those without. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))

TikTok + VidMob analysis (2023 campaigns; published via VidMob/press coverage):
- VidMob analyzed **1,678 ads / 7.3B impressions** and reported that early creative strategies can **boost engagement by 2×** and **increase purchase intent by 43%** in the first seconds of TikTok ads. ([vidblog.vidmob.com](https://vidblog.vidmob.com/vidmob-resources/the-science-of-the-hook-how-brands-can-cultivate-curiosity-on-tiktok?utm_source=openai))  
- “Everyday people” vs celebrity: the VidMob write-up notes “everyday people” are **1.7× more likely to hook**, while celebrity talent showed a **13% decrease in 6s view-through rate** in that analysis. ([vidblog.vidmob.com](https://vidblog.vidmob.com/vidmob-resources/the-science-of-the-hook-how-brands-can-cultivate-curiosity-on-tiktok?utm_source=openai))

TikTok blog (Marketing Science) reinforces timing concentration:
- “50% of the impact” of a TikTok ad is realized in the **first 2 seconds**, and **first 6 seconds capture 90%** cumulative impact on ad recall (and ~80% for awareness). ([ads.tiktok.com](https://ads.tiktok.com/business/en/blog/resonance-key-factor-ad-effectiveness?utm_source=openai))

### YouTube / Google (skippable reality = first 5 seconds)
Think with Google’s “First 5 Seconds” research (TrueView analysis):
- Google examined **thousands of TrueView ads**, coded against **170 creative attributes**, linking early creative choices to **watch time and brand lift**; they stress the first 5 seconds as decisive, including findings like “smiling characters” / recognizable faces early correlate with performance. ([thinkwithgoogle.com](https://www.thinkwithgoogle.com/intl/en-apac/marketing-strategies/data-and-measurement/creating-youtube-ads-that-break-through-in-a-skippable-world/?utm_source=openai))  
Google’s attention guidance (“ABCDs”):
- YouTube ads following ABCDs have been shown to yield **+30% lift in short-term sales likelihood** and **+17% lift in long-term brand perception** over time (this is broader than hooks, but it’s an “attention first” framework). ([thinkwithgoogle.com](https://www.thinkwithgoogle.com/intl/en-emea/marketing-strategies/video/viewer-attention-for-youtube-ads/?utm_source=openai))

## 1.2 “Hook rate” (thumbstop) benchmarks: average vs good vs elite (by platform)

### Meta / Facebook / IG Reels
**Canonical definition (industry-standard):**  
**Hook Rate (aka Thumbstop Rate)** = **3-second video plays / impressions**. ([win.varos.com](https://win.varos.com/en/articles/8055146-how-video-facebook-ads-benchmark-metrics-are-calculated?utm_source=openai))

**Benchmarks (practitioner + benchmark providers):**
- “Below ~25% needs work; 25–35% solid; >35% strong.” ([motionapp.com](https://motionapp.com/blog/key-creative-performance-metrics?utm_source=openai))  
- Another common benchmark range cited: **20–25% hook rate** as an aim for many accounts; “top performers” often **30%+**. ([insights.vaizle.com](https://insights.vaizle.com/hook-rate-hold-rate/?utm_source=openai))  
- Smart Marketer’s paid dataset example shows thumbstop rates like **~19% vs ~42%** under different creative choices (not a universal benchmark, but illustrates the magnitude of change from hook decisions). ([smartmarketer.com](https://smartmarketer.com/2025-facebook-ads-report-part-1/?utm_source=openai))  
- Superads claims a “strong thumbstop ratio” often falls **~30%–42%** (varies by industry). ([superads.ai](https://www.superads.ai/blog/facebook-ads-metrics?utm_source=openai))

**Practical tiering for Agent 5 (Meta):**
- **<20%:** hook is failing hard (likely first frame + first line mismatch, or “ad-looking intro”)
- **20–30%:** serviceable; can work if offer + proof is very strong
- **30–40%:** good; “algorithm has something to work with”
- **40–55%:** excellent; protect + replicate pattern
- **55%+:** rare/elite; often indicates a very native, high-curiosity, high-identification opener

### TikTok
TikTok does not standardize “hook rate” publicly the same way Meta does, but common operational metrics in teams:
- **2-second view rate**, **6-second view-through**, early retention curves, etc.
TikTok’s own research heavily emphasizes 2s and 6s windows. ([ads.tiktok.com](https://ads.tiktok.com/business/en/blog/resonance-key-factor-ad-effectiveness?utm_source=openai))

**Practical tiering for Agent 5 (TikTok):**
- Track: **2s view rate**, **6s VTR**, and “drop in first 2 seconds”
- Goal: minimize early drop; maximize 6s survival because that’s where recall/awareness impact is mostly captured. ([ads.tiktok.com](https://ads.tiktok.com/business/en/blog/resonance-key-factor-ad-effectiveness?utm_source=openai))

### YouTube In-Stream (skippable)
Key metric: **5-second retention** (percentage who remain past skip point).  
Google emphasizes making the first 5 seconds break through. ([thinkwithgoogle.com](https://www.thinkwithgoogle.com/intl/en-apac/marketing-strategies/data-and-measurement/creating-youtube-ads-that-break-through-in-a-skippable-world/?utm_source=openai))  
Many advertisers also track “skip rate”, “view rate” (paid view definition), and attention/brand lift.

---

## 1.3 Hook rate → downstream metrics (how it links to CTR/CPA/ROAS)

Platforms don’t publish a single universal formula like “+10% hook rate = +X ROAS,” because it’s conditional on offer, targeting, and landing page. But operationally:

### The causal chain most accounts experience
**Higher hook rate** typically drives:
1) **More engaged impressions** (you earn more “evaluation time”)
2) Often **lower CPM** over time if the platform reads it as higher-quality inventory engagement (not guaranteed, but frequently observed in practice)
3) Higher **hold rate** (if the body supports the promise)
4) Higher **CTR** (if the payoff + CTA land)
5) Lower **CPA** (if clicks are qualified + LP matches)
6) Higher **ROAS** (if unit economics hold)

On TikTok, TikTok itself says early seconds capture most recall/awareness impact. ([ads.tiktok.com](https://ads.tiktok.com/business/en/blog/resonance-key-factor-ad-effectiveness?utm_source=openai))  
On Meta, the ecosystem treats 3-second view/impression as the earliest “meaningful engagement threshold.” ([motionapp.com](https://motionapp.com/blog/key-creative-performance-metrics?utm_source=openai))

### Complement metric you must track with hook rate (Meta)
**Hold Rate** = **ThruPlays / 3-second video plays**. ([insights.vaizle.com](https://insights.vaizle.com/hook-rate-hold-rate/?utm_source=openai))  
Meaning:
- Hook Rate answers: “Did you stop them?”
- Hold Rate answers: “Did you keep the promise?”

If Hook Rate is high but Hold Rate collapses, your hook is “clickbait” (or misaligned). If Hook Rate is low but Hold Rate is high, your body is good but you need better packaging.

---

## 1.4 Economics: ROI difference between 20% vs 40% hook rate (how to model it)

You asked for ROI difference. There isn’t a universal published constant, so you need a **model** your pipeline can use in simulation + post-hoc attribution.

### Practical model (Meta example)
Let:
- **I** = impressions
- **HR** = hook rate (3s views / impressions)
- **CTR\_h** = click rate among hooked viewers (clicks / 3s views)  
- **CVR** = conversion rate post-click
- **AOV** = average order value
- **CPM** = cost per 1,000 impressions

Then:
- Hooked views = I × HR  
- Clicks = (I × HR) × CTR\_h  
- Purchases = Clicks × CVR  
- Revenue = Purchases × AOV  
- Spend = (I / 1000) × CPM  
- ROAS = Revenue / Spend

**Key:** improving HR from 20% → 40% doubles the “hooked population.” If CTR\_h and CVR are stable, clicks and purchases scale ~linearly with HR—**but** in reality:
- CTR\_h often rises when hook is clearer + better-qualified
- CVR can rise if hook pre-qualifies (identity + problem specificity)
- CPM may improve if engagement signals improve (not guaranteed)

So: **HR is a multiplicative lever** early in the funnel; it can produce superlinear gains when it also improves qualification.

### How to operationalize this in Agent 15A (Performance Analyzer)
Log:
- Hook Rate
- Hold Rate
- CTR
- CPC
- LPV rate
- CVR
- CPA
- ROAS  
Then fit a simple regression or causal forest per account to estimate **marginal ROAS per +1pp hook rate** under your constraints.

---

## 1.5 Hook fatigue: how quickly hooks burn out (what’s real + what to track)

There’s limited “official” public research quantifying hook decay half-life by platform. In practice, fatigue is visible in:
- Hook rate declining first (people stop pausing)
- Then CTR declines
- Then CPA rises
- Frequency climbs (or audience saturates)

**Most reliable rule:** fatigue is spend + audience-size dependent, not just time.  
So Agent 15A should define “fatigue onset” as:
- Hook rate down **X% relative to its first 20–30k impressions**
- Over a rolling window (ex: last 3 days vs baseline)

**Practical fatigue thresholds to implement:**
- **Warning:** HR down **15–25%** vs baseline at similar placements
- **Action:** HR down **25–40%** OR CPA up **20%+** with stable targeting
- **Kill/refresh:** HR down **40%+** and hold rate also deteriorates

**Creative-first fix order:**
1) Swap hook only (“new hooks on proven bodies”)
2) If hold rate drops too → rebuild body transitions
3) If CVR drops but hook+hold ok → LP mismatch or offer fatigue

---

# 2) Comprehensive Hook Taxonomy (verbal, visual, and paired systems)

A “complete taxonomy” has to be **mechanism-based**, not just “list of lines.” The strongest classification is by what the hook is doing to the brain:

## 2.1 Verbal hook types (most complete classification)

Below are **families**, then patterns inside each family. Agent 5 should pick **1 primary family** per hook variant, not mix 3 in one line.

### A) Identity Callout (self-relevance recognition)
Purpose: “This is about me.”
- “If you’re a [persona]…”
- “For anyone who [situation]…”
- “If you’ve tried [common attempt] and it didn’t work…”
- “POV: you’re [identity] and [pain]”
Best for: broad cold, when persona clarity is high.

### B) Problem/Agitation (threat / discomfort)
Purpose: trigger problem salience.
- “Stop doing X if you have Y…”
- “This is why your [goal] isn’t working…”
- “If [symptom], it’s probably [hidden cause]…”
Best for: high pain categories (beauty, health, finance anxiety, B2B inefficiency).

### C) Outcome/Transformation (future pacing)
Purpose: show “end state” immediately.
- “I went from [before] to [after] in [time]…”
- “This is how I [result] without [cost]…”
- “My [thing] changed after I did this one step…”
Best for: UGC demos, before/after-friendly products.

### D) Mechanism reveal (the “why it works” hook)
Purpose: curiosity via explanation.
- “The reason [common advice] fails is…”
- “It’s not [A], it’s [B]…”
- “Here’s the 30-second science behind…”
Best for: skeptical audiences, higher ticket, supplements/beauty (careful on claims).

### E) Myth-busting / contrarian hot take
Purpose: pattern interrupt + belief violation.
- “Unpopular opinion: [industry norm] is trash…”
- “Everyone’s doing [X] wrong…”
- “Don’t buy [category] until you know this…”
Best for: crowded markets, commoditized products.

### F) Social proof / credibility front-load
Purpose: borrow trust instantly.
- “500,000 people switched because…”
- “Dermatologists hate this, but… (careful)”
- “I tested 27 [things] so you don’t have to…”
- “We spent $X testing this…”
Best for: high skepticism categories, when you truly have proof.

### G) Curiosity gap / open loop
Purpose: create an unresolved question.
- “I wasn’t supposed to say this, but…”
- “This is the one thing nobody tells you about…”
- “Watch what happens when I…”
Best for: top-of-funnel, TikTok/Shorts.

### H) Fear / urgency / loss aversion
Purpose: threat detection + time compression.
- “If you’re over 35 and you’re not doing this…”
- “You’re leaking $X every month if…”
- “This expires tonight, but here’s the real reason you should care…”
Best for: promos, time-bound offers, finance.

### I) Confession / vulnerability / authenticity
Purpose: trust through imperfection.
- “I’m embarrassed to admit this…”
- “I wasted $X before I found this…”
- “I thought this was a scam until…”
Best for: TikTok-first UGC, trust building.

### J) Challenge / gamified hook
Purpose: invites participation.
- “Try this with me…”
- “Can you spot what’s wrong here?”
- “I bet you can’t guess…”
Best for: TikTok, Reels engagement mechanics.

### K) Instructional / “Do this now” (command hook)
Purpose: immediate utility.
- “Do this before you [common mistake]…”
- “Save this—here’s the 10-second fix…”
Best for: how-to categories; also good for sound-off.

### L) Reverse psychology / disqualification
Purpose: intrigue + pre-qualify.
- “Don’t buy this if you like [pain]…”
- “This is not for people who want [easy path]…”
Best for: premium positioning, reducing low-quality clicks.

### M) Numeric specificity (precision hook)
Purpose: credibility + curiosity.
- “$0.37/day…”
- “3 ingredients…”
- “7 days…”
- “I tested 12 prompts…”
Best for: performance offers, subscriptions, apps, B2B.

---

## 2.2 Visual hook taxonomy (first frames that stop thumbs)

Think in *visual primitives* that cause interruption:

### 1) Face + emotion (human salience)
- Extreme close-up reaction
- Direct eye contact + “lean-in”
- Confusion/anger/disgust/surprise micro-expression
TikTok explicitly reports benefits of showing a person/creator early. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))

### 2) Motion interrupt (kinetic shock)
- Sudden camera push-in
- Hard jump cut at 0.3–0.6s
- Object thrown into frame
- Snap zoom / whip pan (used carefully to avoid blur illegibility)

### 3) “Problem evidence” visual
- Stain, acne, clutter, messy inbox, low bank balance screenshot (compliance permitting)
- Side-by-side comparison (before/after)
- “What you’re doing wrong” demonstration

### 4) Product-in-use immediately (no context)
- Hands performing the satisfying part (peel, spread, pour, click, swipe)
- UI screen recording showing the “aha moment”
- Unboxing (TikTok/CreatorIQ: unboxing +31% attention uplift) ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))

### 5) Pattern-mismatch setting
- Bathroom mirror confession
- Car rant
- Bed selfie
- Warehouse / behind-the-scenes
(“not an ad” vibe)

### 6) Green screen / proof board
- Screenshot proof behind speaker
- “receipt” overlays (careful on claims)
- Comment screenshots (“replying to @…”) for native feel

### 7) Reveal devices
- Blur → unblur
- Post-it cover reveal
- “Wait for it” masked object

### 8) ASMR / sensory hooks
- Crunch, peel, foam, texture, scratch test
- Close mic + texture visuals

### 9) “Caught on camera” / surveillance vibe
- Doorbell cam angle
- Overhead “security” angle
- Screen recording as “evidence”

### 10) High-contrast text-first frame (sound-off dominance)
- Big claim line as the thumbnail frame
- Minimal background motion
- Strong contrast, few words

---

## 2.3 Verbal + visual pairing matrix (the part most teams miss)

Agent 5 should not output hooks as “lines” and “visuals” separately. It should output **paired recipes**.

Here are pairings that consistently work:

### Pairing 1: Identity Callout + Face-to-camera
- Visual: creator face, direct eye contact, close framing
- Verbal: “If you’re a [persona] and you’re dealing with [pain]…”
Why it works: self-relevance + human salience.

### Pairing 2: Myth-bust + split-screen “wrong vs right”
- Visual: split screen (left “common method,” right “new method”)
- Verbal: “Everyone thinks X, but it’s actually Y…”
Why: belief violation is visually encoded instantly.

### Pairing 3: Outcome reveal + before/after evidence
- Visual: before/after first frame
- Verbal: “I fixed [problem] in [time]—here’s what I changed.”
Why: outcome credibility comes from immediate evidence.

### Pairing 4: Curiosity gap + blur/reveal
- Visual: blurred object / covered label
- Verbal: “I’m not supposed to show this, but…”
Why: visual and verbal both open loops.

### Pairing 5: Numeric specificity + “proof artifact”
- Visual: receipt, analytics dashboard, calendar, timer, scale, measurement tool
- Verbal: “$0.37/day…” / “7 days…”
Why: numbers need *evidence* to feel real.

### Pairing 6: Confession + imperfect setting
- Visual: messy bathroom, bed selfie, car rant
- Verbal: “I wasted $X before…”
Why: authenticity cues reduce ad skepticism.

---

## 2.4 Platform-specific hook constraints (what “wins” where)

### TikTok (sound-on default; native > polished)
- Person early increases hooking power and ad recognition. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- Text overlay helps hooking (1.4× more likely to hook). ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- Avoid overly promotional early seconds (TikTok + CreatorIQ guidance). ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
**Operational implication:** open like a creator post, not a brand message.

### Meta Feed / IG Feed (sound-off heavy)
- First frame and overlay do disproportionate work.
- “Thumbstop / hook rate” is measured cleanly (3s views / impressions). ([win.varos.com](https://win.varos.com/en/articles/8055146-how-video-facebook-ads-benchmark-metrics-are-calculated?utm_source=openai))  
**Operational implication:** your first frame should be readable as a static image.

### Reels (hybrid)
- Needs TikTok pacing + Meta legibility (bigger type, clearer first frame).

### YouTube In-Stream (skippable)
- First 5 seconds are strategic; Google’s work shows early creative attributes correlate with watch + brand lift; humor and faces can matter. ([thinkwithgoogle.com](https://www.thinkwithgoogle.com/intl/en-apac/marketing-strategies/data-and-measurement/creating-youtube-ads-that-break-through-in-a-skippable-world/?utm_source=openai))  
**Operational implication:** you’re competing against the skip button—pre-qualify fast.

---

# 3) Hook Engineering Principles (attention science + “ugly ads” + first frame mechanics)

## 3.1 The “ugly ads / native ads” principle (why organic-looking wins)
Even when the message is identical, native-looking ads often outperform polished because they:
- reduce *ad recognition resistance* (“here comes a pitch”)
- increase trust (authenticity heuristic)
- match platform content grammar (so users don’t swipe reflexively)

TikTok/CreatorIQ data aligns with this: creator authenticity drives trust and purchase behavior, and early creator presence boosts hooking power. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  

**Make “organic” a spec, not a vibe:**
- hand-held micro-shake
- imperfect lighting
- real environment
- no brand bumper
- immediate human presence
- captions like organic posts (“okay so…” “I didn’t expect this…”)

## 3.2 The “first frame” principle (pre-verbal attention)
Before audio is processed, the first frame must deliver:
- **Subject**: who/what (face or object)
- **Conflict**: why I should care (problem evidence / surprise)
- **Legibility**: what is happening (avoid blur, tiny product, wide shots)

Think of first frame as “thumbnail in motion.” If it fails as a still image, it will often fail in-feed.

## 3.3 Curiosity gap engineering (open loops that don’t feel clickbait)
High-performing curiosity hooks have:
1) **Specificity** (not “this will change your life”)
2) **Stakes** (why it matters to viewer)
3) **Credibility anchor** (evidence artifact, person, context)
4) **Implied payoff timing** (“in 10 seconds I’ll show…”)

Bad curiosity = vague teasing with no stakes.

## 3.4 Sound-on vs sound-off dual encoding
Agent 5 should output two encodings for each idea:

### Sound-off encoding (Meta-first)
- On-screen text conveys: persona + promise + curiosity
- Visual shows: proof or tension
- Spoken line can be optional

### Sound-on encoding (TikTok-first)
- Spoken line carries personality + tension
- On-screen text supports comprehension and retention (TikTok/CreatorIQ calls overlays important). ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  

## 3.5 Text overlay engineering (mobile legibility rules)
Make it engineering, not aesthetics:

**Constraints:**
- 1 idea per screen
- 4–8 words max in first frame (rarely more)
- Big, high-contrast, safe-zone aware (avoid UI overlays)
- Animate quickly (0.2–0.5s) or not at all; slow fades waste time
- Use “headline case” and numeric anchors

**Hierarchy:**
- Line 1 (largest): identity/promise
- Line 2 (smaller): specificity/time/number

---

# 4) Hook Testing & Optimization (how elite teams do it)

## 4.1 “New hooks on proven bodies” = highest ROI creative iteration
Reason: you isolate the highest-variance variable (the opener) while keeping:
- proof
- offer
- CTA
- structure

This increases learning velocity and prevents “creative confounding.”

**Implementation:**
- Keep body video/audio identical from second 3 onward
- Only replace: first frame → first 1–2 cuts → first caption line → first spoken sentence

## 4.2 How many hook variations per script?
For a pipeline like yours:
- Minimum: **3**
- Ideal baseline: **5**
- If you have low production constraints (AI UGC, template editing): **8–12** for top angles

**Rule:** more hooks > more bodies early, until you find 1–2 “attention winners,” then iterate bodies for conversion.

## 4.3 Hook-specific A/B methodology
You need to prevent platform delivery bias from selecting winners too early.

**Best-practice test cell:**
- One ad set (or ABO cell) with **same targeting + placements**
- Same post-click destination
- Same optimization event
- 3–8 hook variants, same body
- Use minimum impression thresholds before judgment (ex: 10–30k impressions each on Meta; on TikTok often 5–20k depending on CPM)

**Primary decision metric:** Hook Rate (Meta) / 2s view rate + 6s VTR (TikTok)  
**Secondary:** Hold rate / CTR  
**Tertiary:** CPA (needs more data)

## 4.4 Diagnosing why a hook worked/failed (not just “it worked”)

Agent 15A should tag each hook with attributes so you can learn patterns:
- Hook family (identity / myth-bust / mechanism / etc.)
- Visual primitive (face / demo / split screen / proof artifact / etc.)
- Overlay style (question / claim / number / disqualifier)
- Presence of person in first 2s (yes/no) — TikTok shows benefit. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  
- Product shown in first 1s (yes/no)
- Emotional tone (humor / fear / empathy / surprise)

Then correlate attributes with:
- Hook Rate
- Hold Rate
- CTR
- CPA

Over time, Agent 15B can build a **hook library** that becomes a predictive model.

## 4.5 Hook library design (so it gets smarter)
Store each hook as an object:

- `platform`: Meta / TikTok / Shorts
- `industry`: beauty, health, finance, etc.
- `persona`
- `awareness_stage`: unaware / problem-aware / solution-aware / product-aware / most-aware
- `hook_family`
- `visual_primitive`
- `first_words`
- `first_frame_description`
- `overlay_text`
- `metrics`: HR, hold, CTR, CPA, ROAS, spend, date range
- `fatigue_curve`: HR by impression quartile
- `notes`: compliance risks, claim flags, what it pairs with

---

# 5) Hook trends & patterns (2024–2026): what’s working, what’s saturated, how it evolved

## 5.1 Patterns currently strong (2024–2026)
Based on TikTok/CreatorIQ findings plus what high-performing teams operationalize:

### A) “Creator-led direct address”
Open with face + “you” language.
TikTok/CreatorIQ explicitly reports large uplifts tied to direct address and “you” early. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))

### B) Proof-first micro-demonstrations
Show the mechanism in 0–1 seconds:
- stain removal in first swipe
- app automation in first tap
- “before/after” immediately

### C) Comment-reply format
“Replying to @…” + immediate claim / correction  
Native and algorithm-friendly.

### D) Contrarian corrections
“Stop doing X…” “You don’t need Y…”  
Works in commoditized categories (fitness, skincare, business tools).

## 5.2 Patterns that are burned out / risky
- Generic “Hey guys” intros
- Slow cinematic establishing shots
- Overly broad clickbait (“this changed my life” without specificity)
- Aggressive claim language that triggers compliance review (Agent 12 will care)

## 5.3 Evolution over last ~3 years (high-level)
- 2022–2023: heavy UGC “testimonial storytelling”
- 2023–2024: faster pattern interrupts; more proof artifacts; more comment-style
- 2024–2026: more “creator authenticity” + direct address + immediate demos; heavier reliance on captions/overlays (supported by TikTok/CreatorIQ overlay findings). ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  

## 5.4 Category-specific hook tendencies (operational heuristics)

### Beauty
- Visual proof dominates (skin texture, hair results)
- Strong: before/after + mechanism
- Risk: policy-sensitive claims

### Health / wellness
- Strong: myth-bust + “here’s what actually worked”
- Must be careful with medical claims; make it “experience-led,” not diagnostic.

### Finance
- Strong: numeric specificity + “leak” framing (“you’re losing $X”)
- Use dashboards, receipts, “bill shock” visuals.

### Tech / apps / SaaS
- Strong: “I automated X” + screen recording as proof artifact
- Open with the *output*, not the feature list.

### Food / CPG
- Strong: ASMR + “first bite” + fast recipe payoff
- Unboxing / product-in-use is supported as attention-lifting by TikTok/CreatorIQ. ([creatoriq.com](https://www.creatoriq.com/press/releases/tiktok-creatoriq-release-special-report-with-data-backed-keys-to-success-for-advertisers?hs_amp=true&utm_source=openai))  

---

# 6) What Agent 5 should output (structured spec you can enforce)

For each script, Agent 5 returns a JSON-like object list:

- `hook_id`
- `hook_family` (from taxonomy)
- `verbal_open` (0–1.5s spoken)
- `on_screen_text` (first frame + optional second beat)
- `visual_first_frame` (shot type, subject, action, props)
- `edit_notes` (cut timing: 0.0–0.7s, 0.7–1.5s, 1.5–3.0s)
- `sound_on_variant` (line delivery, SFX, music/no music)
- `sound_off_variant` (caption-first rewrite)
- `risk_flags` (compliance/claims/policy risk tags)
- `intended_awareness_stage`
- `expected_metric_target` (Meta HR tier / TikTok 2s+6s goals)

---

## 7) Gaps / what I did NOT find publicly (so you don’t build on fake certainty)
- A single official Meta study publicly stating “first 3 seconds determine 71% of whether someone watches” (that number circulates widely, but I didn’t locate a primary Meta source in this pass).
- A universal published “fatigue half-life” for hooks by platform.

If you want, I can do a second web pass specifically hunting **primary-source Meta documentation** (Meta Business help center, Meta research blogs, Meta Creative Shop PDFs) for any quant stats on 3-second view predictive power and hook retention.

---

## Next step (so this becomes executable in your pipeline)
If you paste:
1) one “complete script” from Agent 4, and  
2) the brand/category + offer + constraints (claims you can/can’t make),  

…I’ll produce:
- **5 hook variations** using the taxonomy above,
- each with explicit **verbal + visual + text overlay + cut timing**, and
- a suggested **testing matrix** for Agent 7 (Versioning Engine) that isolates hook impact.