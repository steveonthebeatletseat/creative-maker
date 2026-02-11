# Agent 12 — Compliance

*Generated: 2026-02-11 16:46 | Model: gpt-5.2 | Time: 158.5s*

---

Below is a **practitioner-grade research + implementation spec** for **Agent 12 (Compliance)** in your 15-agent paid-social pipeline. It’s designed to work like a **policy firewall**: (1) detect violations across **creative + copy + landing page + product/offer**, (2) propose **conversion-preserving compliant rewrites**, (3) assign a **risk score** that predicts *account-level* consequences, and (4) output **structured flags** for downstream agents (Launch, Analytics, Learning).

I’m going to treat “compliance” as **three simultaneous rule systems**:

1) **Platform policy (Meta / TikTok)** = what gets disapproved / restricted / accounts disabled.  
2) **Consumer protection law (FTC + state AGs)** = what triggers enforcement / refunds / injunctions.  
3) **Vertical regs (FDA/DSHEA for supplements, alcohol rules, financial disclosure laws, etc.)** = what makes #1 and #2 worse.

---

# 0) Agent 12 mission + input/output contract (so it’s automatable)

## Inputs Agent 12 must receive (minimum viable)
**From Agent 11 (Clip Verify) + Agent 7 (Versioning) + Agent 14 preflight:**
- `ad_primary_text`, `headline`, `description`, `CTA`
- `video_script` (full)
- `on_screen_text` (OCR output)
- `audio_transcript` (ASR output)
- `thumbnail_frame` (keyframes)
- `creative_tags` (vertical guess, product type, claims detected upstream)
- `landing_page_url` + **HTML snapshot** (important: Meta/TikTok review the destination experience)
- `offer_details`: pricing, subscription/negative option, guarantee, trial, shipping, refund terms
- `targeting_intent`: geo, age, interests (esp. Special Ad Categories, 18+ requirements)
- `disclosure_assets`: disclaimer text blocks, references to substantiation, citations, clinical study links
- `brand_auth`: approvals/certs (LegitScript, alcohol license, etc. if needed)

## Outputs Agent 12 should emit (structured)
- `overall_risk_score` (0–100)
- `risk_tier` (Green / Yellow / Orange / Red)
- `platform_decision`: {Meta: pass/hold, TikTok: pass/hold}
- `violations[]`: array of objects:
  - `platform` (Meta/TikTok/FTC/Other)
  - `policy_area` (Personal Attributes / Misleading / Restricted Goods / Before-After / etc.)
  - `evidence_snippet` (exact phrase / frame timestamp)
  - `why_it’s_a_problem` (short)
  - `rewrite_options[]` (2–5 compliant alternatives)
  - `required_disclosures[]`
  - `must_change_on_landing_page[]`
  - `severity` (low/med/high/account-threatening)
- `special_category_required` (housing/employment/credit/financial products & services)
- `age_gate_required` (true/false and minimum age)
- `certification_required` (e.g., LegitScript / Meta written permission)
- `appeal_package` (only if holding):
  - `recommended_appeal_text`
  - `supporting_docs_needed`
  - `what_to_remove_before_appeal`

---

# 1) META (Facebook/Instagram) Advertising Standards — “what the reviewer actually enforces”

Meta’s policy surface area is huge; the Compliance Agent should treat it as **(A) Ad Content**, **(B) Ad Targeting / Special Categories**, and **(C) Destination / Landing Page experience**.

## 1.1 Prohibited vs Restricted (Meta) — operational view

### A) “Hard Prohibited” (never allowed / account-threatening)
Common “instant rejection + strike pattern” buckets:
- **Illegal products/services, illicit drugs, unsafe supplements**, and anything facilitating wrongdoing.
- **Weapons, explosives**, etc.
- **Fraud/deception**: impersonation, false documents, fake endorsements, fake celebrity/authority.
- **Tobacco/vapes** (generally prohibited).
- **Adult sexual content** (and frequently many adult products/services).
- **Discriminatory practices** (especially around housing/employment/credit).
- **Deceptive claims**: “guaranteed results,” fake scarcity, or misleading landing page behavior.

Where you *must* be exact: Meta publishes restricted-content subpolicies (e.g., Drugs & Pharmaceuticals) with **explicit requirements** for CBD/hemp, prescription/OTC, etc. ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))

### B) “Restricted Content” (allowed with constraints)
- **Alcohol** (age + country targeting constraints; creative limitations).
- **Gambling** (permission/authorization).
- **Financial products/services** (increasingly tied to **Special Ad Category** and targeting limitations).
- **Health & wellness**: can run, but copy/creative are heavily scrutinized and “personal attributes” violations are common.

### C) “Sensitive verticals” (where Meta also tightens measurement)
Meta has been moving toward **restricted data-sharing for sensitive categories** (health/wellness in particular), which affects CAPI/pixel event usability and can change how you design the funnel. Many practitioners reference a 2025 shift here; treat it as “measurement risk,” not just “ad approval risk.” (You referenced “restricted event tracking update”—Agent 12 should at least flag *measurement risk* when it detects sensitive verticals.)  

---

## 1.2 Personal Attributes Policy (Meta) — the #1 avoidable disapproval cause
### The rule in practice
Meta disallows ads that **assert or imply personal attributes** about the viewer (or their family). This includes:
- Health/medical conditions (physical or mental)
- Financial status
- Race/ethnicity/religion
- Sexual orientation/practices
- Gender identity
- Disability
- Voting status, union membership, criminal record, etc.

Even if you’re *targeting* a segment, you can’t **say** you know they are that segment.

### Violations (patterns)
**Direct**:
- “Do you have diabetes?”  
- “Your anxiety is getting worse.”  
- “Tired of being overweight?”  
- “Bad credit? We can help.”

**Indirect / implied** (still triggers):
- “Finally, a solution for *people like you* who can’t lose weight.”  
- “If you’re a single mom struggling to pay bills…”  
- “Stop your depression before it ruins your life.”

### Compliant rewrite patterns (keep persuasion)
**Rewrite strategy = “shift from *YOU-ARE* to *THIS-IS-FOR* + optional self-identification.”**

Bad → Better:
- “Lose your belly fat” → “Support a leaner-feeling midsection”  
- “Fix your insomnia” → “Wind down easier and improve your sleep routine”  
- “Bad credit?” → “Explore options designed for a range of credit profiles”  
- “Diabetics: lower your A1C” → “Talk to a clinician about options that may support metabolic health”

**Safe grammar templates Agent 12 can enforce:**
- “If you’re looking for…”  
- “For anyone who wants to…”  
- “Designed to support…”  
- “Some people use X to…”  
- “May help with…” (but only if substantiation exists)

**Key nuance:** “you/your” is not banned by itself; it becomes risky when paired with a sensitive attribute.

---

## 1.3 Health, Wellness, Supplements — what actually gets ads rejected
### Meta’s “Drugs & Pharmaceuticals” restrictions (CBD/hemp, prescription/OTC)
Meta’s restricted-content policy is explicit that:
- **THC** promotion is not allowed.
- **CBD** ads can be allowed in the **US only**, **18+ only**, and typically require **certification and written authorization** (Meta references LegitScript + authorization flows).
- CBD ads **must not make health/medical claims** (treat/cure/prevent/diagnose/mitigate disease). ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))

**Hemp (non-CBD; ≤0.3% THC)**
- Generally treated more leniently if it’s truly hemp (fiber/seed) and compliant with local laws; still avoid medical claims. ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))

### “Trigger words” (Meta) — how to model this for automated review
Meta doesn’t publish a single official “keyword blacklist,” but real-world rejections cluster around:

**Disease/medical verbs (High risk)**
- cure, treat, heal, reverse, prevent, diagnose, mitigate  
- “clinically proven” (unless you can substantiate and the rest of the ad is conservative)

**Guaranteed outcomes**
- guaranteed, 100%, permanent, instant, “works every time”

**Time-bound promises**
- “in 7 days,” “in 24 hours,” “lose 10 lbs this week”

**Condition naming**
- diabetes, ADHD, depression, anxiety, psoriasis, eczema, PCOS, arthritis, IBS, etc.  
Even if you don’t claim cure, naming conditions often escalates scrutiny.

**Body-shaming / negative self-perception hooks**
- “Stop being ugly/fat,” “fix your disgusting belly,” “don’t be embarrassed anymore”  
(TikTok is extremely explicit about body image harm; Meta enforcement is also aggressive.)

---

## 1.4 Before-and-after: what’s banned vs what sometimes passes (Meta practical rules)
Meta tends to reject:
- **Side-by-side transformations** implying dramatic results (especially weight loss, skin, hair regrowth).
- **Zoomed body-part “problem area” close-ups** framed as shame (“Look at this cellulite!”).
- **Unrealistic/rapid transformations** or “AI” transformations.

What sometimes passes (still risk):
- **Routine/process comparisons** (e.g., “morning routine vs evening routine”) not framed as “results.”
- **Product texture demos** (cosmetic application) without “transformation” framing.

Because enforcement changes and is heavily automated, your agent should treat “before/after visuals” as **Orange risk** by default in:
- weight loss
- skin “wrinkle removal”
- hair regrowth
- teeth whitening “shade change”
- posture correction “instant fix”

Also note: state AG scrutiny has increased around **AI-generated before/after weight-loss ads** on Meta (not policy text, but enforcement pressure). ([attorneygeneral.gov](https://www.attorneygeneral.gov/taking-action/ag-sunday-leads-bipartisan-coalition-urging-meta-to-crack-down-on-misleading-a-i-fueled-weight-loss-ads/?utm_source=openai))

---

## 1.5 Financial claims + “Financial Products & Services” special ad category (Meta)
Meta has expanded/added **special category handling** for financial products/services in the US with major targeting restrictions starting around **January 2025** (as reported by multiple industry sources). ([getelevar.com](https://getelevar.com/news/meta-financial-products-services-ads-category/?utm_source=openai))

**Agent 12 should flag:**
- If the offer is any of: banking, insurance, investments, payment platforms, loans, credit, fintech advice → likely needs the special category selection (and thus limits targeting). ([getelevar.com](https://getelevar.com/news/meta-financial-products-services-ads-category/?utm_source=openai))
- Avoid copy that implies personal financial status:
  - “In debt?” “Behind on bills?” “Bad credit?” (Personal attributes + predatory lending vibes)
- Avoid income guarantees:
  - “Make $500/day” “Quit your job” “Guaranteed approval”

---

## 1.6 Landing page policy (Meta) — what gets ads rejected even if ad copy is clean
Meta reviews the **destination experience**. Agent 12 must scan landing pages for:

### “Misleading / deceptive UX”
- Fake system warnings
- Forced redirects
- Hidden subscription terms
- Auto-playing aggressive popups that prevent exit
- “Countdown timers” with no truth basis
- False scarcity (“only 3 left” with no inventory logic)
- Unverifiable “as seen on” logos
- Fabricated reviews/testimonials

### “Claim escalation” on landing page
Ad may be compliant, but landing page says:
- “Cures diabetes”
- “Lose 20 lbs in 14 days guaranteed”
- “Doctor-approved” with no substantiation
This is *still* a rejection source.

### “Missing required disclosures”
- Subscription/negative option not clearly disclosed
- Refund/return policy hidden
- Financial APR/fees missing (for lending/credit)

FTC “click-to-cancel”/negative option modernization increases legal risk here: cancellation must be as easy as signup; material terms must be clear before billing info, informed consent, etc. ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/10/federal-trade-commission-announces-final-click-cancel-rule-making-it-easier-consumers-end-recurring?utm_source=openai))

---

## 1.7 Appeals + account health escalation (Meta) — how Compliance should “think”
Meta enforcement is pattern-based:
- One rejection is normal.
- Repeated violations in the same domain (health claims, personal attributes, deceptive practices) = increased review friction, delivery suppression, then restrictions.

**Agent 12 should generate an appeal package only when:**
- The ad is compliant and rejected due to false positives (common in health-ish wording).
- You can provide **clear evidence** (e.g., license, certification, accurate substantiation, corrected landing page).

**Appeal structure (what tends to work):**
- 1–2 sentences: “We believe this is compliant with policy X.”
- Bullet list mapping **each flagged element** → the **fix**.
- If applicable: certification docs (LegitScript), disclosures, updated landing page screenshots.

---

# 2) TikTok Advertising Policies — key differences vs Meta (and what to automate)

TikTok is generally **stricter** on:
- body image harm
- misleading “product effect” edits
- before/after comparisons
- AI-generated content disclosure (explicit requirement)

## 2.1 Misleading & false content + before/after (TikTok)
TikTok policy explicitly states:
- Ads and landing pages must not **promise or exaggerate results** and restrict absolute terms. ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))
- **Before-and-after comparisons** are **not allowed** for product effects (with limited exceptions where evidence/disclaimer may be accepted, but assume “no” operationally). ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))

**Practical consequence:** if Meta might pass a “soft” comparison, TikTok often rejects it.

## 2.2 Weight management & body image (TikTok)
TikTok has a dedicated section:
- Weight loss claims must be **18+** and framed around **healthy lifestyle**, not “product alone” as sole solution. ([ads.tiktok.com](https://ads.tiktok.com/help/article/tiktok-ads-policy-weight-management?utm_source=openai))
- Prohibits:
  - unrealistic gain/loss
  - guarantees
  - unhealthy relationships with food
  - body shaming / ideal body claims ([ads.tiktok.com](https://ads.tiktok.com/help/article/tiktok-ads-policy-weight-management?utm_source=openai))

## 2.3 Financial services (TikTok)
TikTok is explicit about disallowed financial categories (varies by country) and commonly bans:
- bail bonds, binary options, get-rich-quick, ICOs, payday loans, pyramid schemes, many crypto activities without permission, etc. ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-financial-services?utm_source=openai))

**Compliance automation:** if offer matches these categories → **Red** for TikTok.

## 2.4 AI-generated content (TikTok) — disclosure requirement
TikTok states significantly edited media / AIGC is allowed **if disclosed** via:
- TikTok AIGC label **or** clear disclaimer/caption/watermark/sticker. ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))

**Agent 12 rule:** if creative contains AI avatar, AI voice cloning, deepfake-ish edits, or fully synthetic humans → require:
- “AI-generated” label overlay (on-screen)
- disclosure in caption/description where possible

(Also note: separate legal requirements can apply—e.g., New York passed an AI avatar disclosure law in late 2025; if you’re running geo-targeted ads in NY, consider state compliance too.) ([theverge.com](https://www.theverge.com/news/842848/new-york-law-ai-advertisements-sag-aftra-labor?utm_source=openai))

## 2.5 Spark Ads compliance
Spark Ads add an additional layer:
- you need rights/authorization to use the creator post
- ensure the original post content is compliant (not just your ad caption)
Agent 12 should require a `spark_authorization_token` or proof of whitelisting + check creator’s spoken claims.

---

# 3) FTC advertising compliance (US) — what your pipeline must enforce to avoid “business-ending” exposure

Your Compliance Agent should treat FTC as **claim substantiation + disclosure + testimonial integrity + negative option**.

## 3.1 Health claims substantiation (FTC)
FTC is explicit:
- Ads must be truthful and not misleading.
- Advertisers must have **adequate substantiation** for objective claims **before** running the ad.
- For health/safety claims, substantiation generally requires **competent and reliable scientific evidence**. ([ftc.gov](https://www.ftc.gov/business-guidance/resources/health-products-compliance-guidance?utm_source=openai))

FTC also warns advertisers about civil penalties for unsupported claims. ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2023/04/ftc-warns-almost-700-marketing-companies-they-could-face-civil-penalties-if-they-cant-back-their?utm_source=openai))

**Agent 12 enforcement:**
- Any objective health claim → require a `substantiation_reference` object:
  - study type, population, endpoints, limitations
  - internal evidence file link
- If none exists: rewrite to **structure/function** style language (support/promote/may help) or remove claim.

## 3.2 Reviews/testimonials + AI-generated endorsements
FTC’s Consumer Reviews and Testimonials Rule:
- Effective **Oct 21, 2024** per FTC Q&A. ([ftc.gov](https://www.ftc.gov/business-guidance/resources/consumer-reviews-testimonials-rule-questions-answers?utm_source=openai))
- Prohibits fake/false testimonials, including **AI-generated** fake reviews/testimonials representing nonexistent people or people without experience. ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/08/federal-trade-commission-announces-final-rule-banning-fake-reviews-testimonials?utm_source=openai))

FTC actions also show scrutiny of “AI-enabled” review ecosystems. ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/11/ftc-order-against-ai-enabled-review-platform-sitejabber-will-ensure-consumers-get-truthful-accurate?utm_source=openai))

**Agent 12 enforcement:**
- If ad includes testimonial: require `testimonial_proof`:
  - real person identity + consent
  - proof they used product
  - typicality context (see below)
- If UGC is AI-generated “person”: label it and do **not** present it as a real consumer experience.

## 3.3 “Typical results” disclaimers (FTC reality)
FTC doesn’t give you a magic sentence that makes deception OK. The rule-of-thumb:
- If you show extraordinary results, you must have evidence typical consumers achieve similar results **or** clearly disclose what typical results are and that results vary.
- Disclosures must be **clear and conspicuous** (not hidden in footer; not 2pt gray text).

**Agent 12 should implement:**
- If the ad uses:
  - “I lost 37 lbs in 30 days”
  - “We made $120k in 90 days”
  then require either:
  - “typical results” distribution statement (real data), OR
  - remove numbers and use generalized outcome language (“Some customers report…”)

## 3.4 Negative option / subscriptions — “click-to-cancel” final rule (2024)
FTC final “click-to-cancel” rule:
- requires cancellation as easy as signup
- prohibits material misrepresentations
- requires clear disclosure of material terms before billing info
- requires express informed consent
- requires simple cancellation mechanism ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/10/federal-trade-commission-announces-final-click-cancel-rule-making-it-easier-consumers-end-recurring?utm_source=openai))

**Agent 12 landing page scan must detect:**
- “free trial” that rolls into paid without clear disclosure
- cancellation hidden behind phone-only or long flows (if signup was online)
- pre-checked boxes

---

# 4) Claim Taxonomy by Risk Level (with safe rewrites that still sell)

This is the heart of automating compliance without killing performance: **rewrite patterns** that preserve *benefit + mechanism + proof* while removing disallowed certainty, medical framing, and personal attributes.

## 4.1 HIGH RISK (often instant rejection / account risk)
### Patterns
1) **Disease claims**
- “Treats diabetes / lowers A1C”
- “Cures anxiety / depression”
- “Reverses arthritis”
2) **Guaranteed or extreme outcomes**
- “Lose 20 lbs in 2 weeks”
- “Guaranteed approval”
- “Permanent results”
3) **Medical authority impersonation**
- “Doctor says this cures…”
4) **Before/after transformation claims**
- “Wrinkles disappear” + side-by-side
5) **Personal attribute + problem callout**
- “Your obesity is ruining your life”
- “Bad credit? You’re approved”

### Safe rewrite templates
- Replace **treat/cure** → “supports / helps maintain / promotes”
- Replace outcome guarantees → “may / can / designed to”
- Replace disease framing → “general wellness” or “talk to a professional”

Examples:
- “Cures insomnia” → “Helps support a better sleep routine”
- “Guaranteed weight loss” → “Designed to support healthy weight management alongside diet + activity”
- “Fix your anxiety” → “Supports calm and everyday stress management”

**Agent 12 action:** if high-risk claim detected and no substantiation package exists → force rewrite or block launch.

## 4.2 MEDIUM RISK (may pass; commonly flagged; needs guardrails)
### Patterns
- Superlatives: “#1”, “best”, “most effective”
- Time-bound but not absolute: “in 30 days”
- Comparative claims: “better than Ozempic”
- Strong implication language: “Watch this belly fat melt”
- “Clinical” language without providing evidence

### Safe rewrite patterns
- Add **basis** for comparisons (or remove)
- Use “in as little as” only with proof and context (still risky)
- Convert superlatives into verifiable statements:
  - “#1” → “One of our most popular”
  - “Clinically proven” → “Clinically studied ingredients” (still requires careful substantiation)

## 4.3 LOW RISK (usually safe, but not a free pass)
### Patterns
- “Supports”, “promotes”, “helps maintain”
- “Designed for”, “formulated with”
- “Some customers report…”

### Safe upgrades that maintain persuasion
- Pair low-risk claims with **concrete, non-medical benefits**:
  - “supports energy” + “helps you feel less sluggish mid-afternoon”
- Use mechanism language without medical endpoints:
  - “supports hydration” vs “treats kidney issues”

---

# 5) Vertical-specific compliance playbooks (Meta + TikTok + FTC)

## 5.1 Supplements / health products
**Do:**
- Use structure/function language: support/promote/may help
- Require substantiation files for any objective claim (FTC)
- Avoid disease names and drug-like promises
- Avoid before/after transformations

**Don’t:**
- “Treat/cure/prevent”
- “FDA approved” unless it truly is (and for supplements it typically isn’t in that sense)

Meta-specific: drug/cannabis policies are explicit; CBD requires authorization/certification + 18+ + US-only and no medical claims. ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))

## 5.2 Beauty / anti-aging
High rejection triggers:
- “Wrinkles disappear” + before/after
- “Look 10 years younger in 7 days”
- Body shaming / negative self-perception hooks

Safer:
- “Visibly smoother-looking skin” (careful: still implies effect)
- “Hydrates + improves the look of…” language
- Demonstration (application) > transformation claims

TikTok explicitly uses “wrinkles disappearing via before-and-after” as a not-allowed example under misleading content. ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))

## 5.3 Weight loss (highest risk)
TikTok: weight loss claims must be 18+, must not present product as sole solution, no unrealistic claims, no body shaming. ([ads.tiktok.com](https://ads.tiktok.com/help/article/tiktok-ads-policy-weight-management?utm_source=openai))  
Meta: also restricts weight loss ads to adults in many contexts; Meta notes restricted topics like weight loss not shown to teens. ([facebook.com](https://www.facebook.com/help/980264326141711/?utm_source=openai))

Operational rules:
- Never: “melt fat,” “burn fat fast,” “drop 10 lbs by Friday,” “summer body”
- Avoid: body-part shame closeups, waist pinch, scale dramatization
- Prefer: performance/wellbeing framing (“fuel workouts,” “balanced habits,” “high-protein meal support”)

## 5.4 CBD / hemp
Meta: CBD allowed only with certification + authorization + US-only + 18+; no health/medical claims; THC prohibited. ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))  
TikTok: financial policies are explicit; CBD specifics vary by region and are often tightly restricted—Agent 12 should default CBD to **Orange/Red** on TikTok unless you have explicit current permission and local policy validation.

## 5.5 Financial services / fintech
TikTok provides explicit lists of “not allowed” (payday loans, bail bonds, get-rich-quick, etc.) and disclosure requirements. ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-financial-services?utm_source=openai))  
Meta: requires special category selection for many financial offers in US starting around 2025 (targeting restrictions). ([getelevar.com](https://getelevar.com/news/meta-financial-products-services-ads-category/?utm_source=openai))

Compliance rules:
- No income guarantees
- No “instant money in 10 seconds” (TikTok example of misleading claim). ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))
- Landing page must clearly disclose APR/fees/repayment terms where applicable (TikTok financial policy highlights disclosures on LP). ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-financial-services?utm_source=openai))

## 5.6 Alcohol
Both platforms require strict age gating + jurisdiction compliance. Build a hard check:
- `min_age >= legal_drinking_age_in_target_geo`
- No targeting minors
- Creative must not glamorize irresponsible consumption

## 5.7 Dating
Treat as sensitive:
- Avoid sexual content
- Avoid “you’re lonely / single” personal attribute style
- Avoid explicit promises (“meet your soulmate tonight”)

## 5.8 Housing / employment / credit / real estate (Special Ad Categories)
Meta’s Special Ad Categories restrict targeting options and require correct classification. If you miss the category, you can see rejections and/or delivery issues. (Multiple industry sources document these constraints; Meta is strict operationally.)

---

# 6) Automated review triggers (what Agent 12 should detect programmatically)

## 6.1 “Phrase patterns” (regex-style detectors)
**Personal attribute triggers**
- `\byour (depression|anxiety|acne|eczema|diabetes|pcos|adhd|autism|bipolar|debt|bankruptcy|foreclosure|credit score|race|religion)\b`
- `\bdo you have (.*)\b` (if (.*) matches sensitive list)
- `\b(stop|fix|cure|treat) your\b`

**Guaranteed/absolute**
- `\b(guarantee(d)?|100%|permanent(ly)?|instant(ly)?|works every time)\b`

**Time-bound promises**
- `\b(in|within) \d+ (days?|hours?|weeks?)\b` + outcome verb nearby (`lose|burn|erase|reverse|double`)

**Before/after**
- detect “before” and “after” on-screen text via OCR
- detect split-screen transformation frames
- detect “Day 1 / Day 7” with same subject, implying result

**Authority/impersonation**
- “doctor says,” “pharmacist recommends,” “FDA approved” (require proof)

## 6.2 Visual detectors (frame-level)
- side-by-side body comparisons
- waist pinch / scale display
- “problem area” closeups (cellulite/rolls)
- medical imagery (syringes/injections) in weight loss context
- false UI elements (fake news, fake warnings)

## 6.3 Landing page detectors (HTML)
- negative option: “trial” + hidden renewal terms
- “countdown timer” scripts (scarcity risk)
- review widgets with no provenance
- missing business identity/contact
- inconsistent claims vs ad

---

# 7) Compliance workflow at top agencies (systemization that doesn’t crush performance)

## 7.1 The “3-lane” compliance model
**Lane 1: Automated preflight (Agent 12)**
- deterministic pattern detection (above)
- policy checklists by vertical
- risk scoring

**Lane 2: Human audit (only for Orange/Red)**
- 5–10 minute review by specialist
- focuses on nuance: implied claims, visuals, LP deception, testimonial legitimacy

**Lane 3: Evidence vault**
- store substantiation + permissions + certifications
- every claim maps to a doc

## 7.2 The “Risk budget” concept (so you’re not too conservative)
Give your creative system a **risk allowance**:
- Each ad can include at most:
  - 0 high-risk claims
  - ≤2 medium-risk claims (must be qualified)
  - unlimited low-risk claims
This keeps copy persuasive but controlled.

## 7.3 A/B compliance versioning (compliance-safe variants)
Agent 12 should output:
- `Safe Variant A` (conservative, high-approval)
- `Safe Variant B` (slightly punchier, still compliant)
so Agent 7/14 can launch both and learn.

---

# 8) What changed 2024–2026 (practical updates you must encode)
**TikTok**
- Explicit AIGC disclosure requirement (policy last updated Sep 2025). ([ads.us.tiktok.com](https://ads.us.tiktok.com/help/article/tiktok-ads-policy-misleading-and-false-content?utm_source=openai))
- Weight management/body image policy last updated Sep 2025; 18+ requirement and strong restrictions. ([ads.tiktok.com](https://ads.tiktok.com/help/article/tiktok-ads-policy-weight-management?utm_source=openai))

**FTC**
- Consumer Reviews & Testimonials Rule effective Oct 21, 2024 (civil penalties for knowing violations; includes AI-generated fake reviews). ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/08/federal-trade-commission-announces-final-rule-banning-fake-reviews-testimonials?utm_source=openai))
- “Click-to-cancel” final rule announced Oct 2024; broad negative option requirements. ([ftc.gov](https://www.ftc.gov/news-events/news/press-releases/2024/10/federal-trade-commission-announces-final-click-cancel-rule-making-it-easier-consumers-end-recurring?utm_source=openai))

**Meta**
- Explicit restricted-content documentation continues to evolve (Meta’s Drugs & Pharmaceuticals pages show 2024–2025 change logs). ([facebook.com](https://www.facebook.com/policies/ads/restricted_content/pharmacies?utm_source=openai))
- Financial Products & Services special category enforcement in/around Jan 2025 is widely reported and operationally important. ([getelevar.com](https://getelevar.com/news/meta-financial-products-services-ads-category/?utm_source=openai))

---

# 9) Build Agent 12’s scoring model (simple enough to ship, predictive enough to matter)

## 9.1 Risk scoring (0–100)
- **0–20 (Green):** low-risk language, no sensitive targeting, LP clean
- **21–45 (Yellow):** medium-risk phrases but qualified; minor LP disclosure improvements
- **46–70 (Orange):** likely disapproval; personal attributes risk; borderline before/after; needs rewrite
- **71–100 (Red):** prohibited category, disease claims, deceptive LP, fake testimonials, unauthorized CBD/prescription, etc.

## 9.2 Account threat multiplier
Multiply risk if:
- advertiser has prior violations (Agent 15B can feed historical account health)
- same violation type repeated across multiple ads
- landing page is the source (Meta often treats that as more severe than a wording slip)

---

# 10) Next: I can generate your “Agent 12 Compliance Checklist + Rewrite Library” as JSON
If you tell me:
1) your top 3–5 verticals (e.g., supplements, skincare, fintech, info products, alcohol)  
2) the markets (US-only? multi-geo?)  
3) whether you run **subscription/trials** often  
…I’ll output:
- a **machine-readable ruleset** (JSON) with detectors, severity, rewrites
- a **compliance prompt** for Agent 12
- a **landing-page scanner checklist**
- a **policy evidence schema** (how to store substantiation/certs)

That will make Agent 12 deterministic and auditable instead of “LLM vibes.”