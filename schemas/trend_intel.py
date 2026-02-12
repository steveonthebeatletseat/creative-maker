"""Agent 1B output schema — Trend & Competitive Intel (v2.0).

Real-time competitive and cultural intelligence with scoring,
confidence tagging, gap analysis, and strategic priority stack.

Runs fresh every batch (not quarterly like 1A).
Output feeds directly into Agent 2 (Idea Generator).
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import AwarenessLevel, FunnelStage


# ---------------------------------------------------------------------------
# Enums / Literals (used as plain strings for LLM compatibility)
# ---------------------------------------------------------------------------

# confidence: "observed" | "inferred" | "supplemented"
# dr_conversion_potential: "high" | "medium" | "low"
# lifecycle_stage: "emerging" | "peaking" | "mainstream" | "fading"
# fatigue_risk: "high" | "medium" | "low"
# brand_safety_risk: "low" | "medium" | "high"


# ---------------------------------------------------------------------------
# Meta — Research Quality
# ---------------------------------------------------------------------------

class ResearchMeta(BaseModel):
    brand: str
    niche: str
    research_date: str = Field(..., description="YYYY-MM-DD")
    data_quality_score: int = Field(
        ..., ge=1, le=10,
        description="Honest assessment of how rich the raw research was (1=thin, 10=exhaustive)"
    )
    data_quality_notes: str = Field(
        ..., description="What was well-covered vs thin in the research"
    )


# ---------------------------------------------------------------------------
# Trending Formats
# ---------------------------------------------------------------------------

class FormatWhyItWorks(BaseModel):
    psychology: str = Field(..., description="What psychological lever does this pull?")
    algorithm: str = Field(..., description="Why does the platform reward this format?")


class TrendingFormat(BaseModel):
    format_name: str = Field(
        ..., description="Precise name (e.g., 'split-screen before/after with day counter')"
    )
    description: str = Field(..., description="Exactly how this format works, step by step")
    platforms: list[str] = Field(
        ..., description="e.g. ['meta_feed', 'tiktok', 'ig_reels']"
    )
    why_it_works: FormatWhyItWorks
    dr_conversion_potential: str = Field(
        ..., description="high / medium / low — will this drive PURCHASES, not just views?"
    )
    dr_conversion_reasoning: str = Field(..., description="Why you rated it this way")
    lifecycle_stage: str = Field(
        ..., description="emerging / peaking / mainstream / fading"
    )
    estimated_shelf_life: str = Field(
        ..., description="Concrete estimate (e.g., '~4 weeks', 'seasonal through March', 'evergreen')"
    )
    fatigue_risk: str = Field(..., description="high / medium / low")
    brand_application: str = Field(
        ..., description="Specific creative direction for how THIS brand should use this format"
    )
    example_reference: str = Field(
        ..., description="Specific example from research or 'no direct example found'"
    )
    confidence: str = Field(..., description="observed / inferred / supplemented")
    priority_score: int = Field(
        ..., ge=1, le=10,
        description="How strongly should Agent 2 lean into this format?"
    )


# ---------------------------------------------------------------------------
# Competitor Ad Analysis
# ---------------------------------------------------------------------------

class AdHookBreakdown(BaseModel):
    verbal: str = Field(..., description="What is said in first 3 seconds (or 'none')")
    visual: str = Field(..., description="What is shown in first 3 seconds")
    text_overlay: str = Field(
        default="none", description="Any text on screen in first 3 seconds"
    )


class PerformanceSignals(BaseModel):
    estimated_run_time: str = Field(
        default="unknown", description="e.g. '3+ weeks', 'recently launched'"
    )
    variation_count: str = Field(
        default="unknown", description="e.g. '5+ variations', 'single version'"
    )
    estimated_spend_tier: str = Field(
        default="unknown", description="low / medium / high / unknown"
    )
    scaling_signals: str = Field(
        default="", description="Any evidence this ad is being scaled"
    )


class CompetitorAd(BaseModel):
    competitor: str
    platform: str = Field(..., description="meta / tiktok / youtube")
    ad_format: str = Field(..., description="Precise format name")
    hook: AdHookBreakdown
    persuasion_structure: str = Field(
        ..., description="Full flow from hook to CTA (e.g., 'hook → problem → mechanism → proof → CTA')"
    )
    offer: str = Field(..., description="What offer is presented")
    visual_style: str = Field(
        ..., description="Production approach (lo-fi UGC / polished UGC / studio / screen recording / etc.)"
    )
    performance_signals: PerformanceSignals
    funnel_position: FunnelStage
    awareness_level: AwarenessLevel
    what_works: str = Field(
        ..., description="Specific element that makes this effective"
    )
    what_is_weak: str = Field(
        ..., description="Specific flaw or missed opportunity"
    )
    steal_worthy_element: str = Field(
        ..., description="The ONE thing worth adapting from this ad"
    )
    confidence: str = Field(..., description="observed / inferred / supplemented")
    source: str = Field(default="", description="URL or source description")


# ---------------------------------------------------------------------------
# Cultural Moments
# ---------------------------------------------------------------------------

class MomentTiming(BaseModel):
    deploy_window: str = Field(
        ..., description="now / this_week / this_month / seasonal"
    )
    specific_date: Optional[str] = Field(
        None, description="Specific date if seasonal"
    )
    urgency: str = Field(..., description="Why timing matters")


class CulturalMoment(BaseModel):
    moment: str = Field(..., description="The trend, event, or cultural conversation")
    why_relevant: str = Field(
        ..., description="Natural connection to brand (if forced, don't include)"
    )
    timing: MomentTiming
    brand_safety_risk: str = Field(..., description="low / medium / high")
    brand_safety_notes: str = Field(
        default="", description="Specific risks if medium or high"
    )
    creative_direction: str = Field(
        ..., description="How to incorporate into an ad — specific enough to brief"
    )
    confidence: str = Field(..., description="observed / inferred / supplemented")
    priority_score: int = Field(..., ge=1, le=10)


# ---------------------------------------------------------------------------
# Working Hooks
# ---------------------------------------------------------------------------

class WorkingHook(BaseModel):
    hook_text: str = Field(..., description="The hook verbatim or closely paraphrased")
    hook_category: str = Field(
        ...,
        description="question / bold_claim / story_open / pattern_interrupt / social_proof / "
                    "contrarian / pov / shock_stat / before_after / us_vs_them / "
                    "curiosity_gap / negative_hook / direct_address"
    )
    platform: str = Field(..., description="meta / tiktok / both")
    hook_format: str = Field(
        default="combined",
        description="verbal / text_overlay / visual / combined"
    )
    performance_signal: str = Field(
        ..., description="Evidence this hook is working (long-running ad, multiple variations, etc.)"
    )
    psychology: str = Field(
        ..., description="Why this hook stops the scroll and compels attention"
    )
    brand_adaptation: str = Field(
        ..., description="The actual adapted hook written out for our brand — not a suggestion"
    )
    funnel_fit: list[str] = Field(
        default_factory=lambda: ["tof"],
        description="Which funnel stages this hook suits: tof, mof, bof"
    )
    confidence: str = Field(..., description="observed / inferred / supplemented")
    priority_score: int = Field(
        ..., ge=1, le=10,
        description="How strongly should Agent 5 (Hook Specialist) model this?"
    )


# ---------------------------------------------------------------------------
# Gap Analysis
# ---------------------------------------------------------------------------

class UnaddressedObjection(BaseModel):
    objection: str = Field(..., description="Customer objection no competitor is handling")
    opportunity: str = Field(..., description="How we can exploit this gap")
    priority_score: int = Field(..., ge=1, le=10)


class UntappedEmotionalAngle(BaseModel):
    emotion: str = Field(..., description="The emotional lever nobody is pulling")
    why_untapped: str = Field(
        default="", description="Possible reason competitors avoid it"
    )
    opportunity: str = Field(..., description="How we can own this angle")
    priority_score: int = Field(..., ge=1, le=10)


class IgnoredFunnelStage(BaseModel):
    stage: str = Field(..., description="tof / mof / bof")
    gap_description: str
    opportunity: str


class UnderservedAudience(BaseModel):
    segment: str = Field(..., description="Who is being ignored")
    opportunity: str


class UnusedFormat(BaseModel):
    format: str = Field(..., description="Format working in adjacent niches but absent here")
    adjacent_niche: str
    opportunity: str


class UnderutilizedProofType(BaseModel):
    proof_type: str = Field(
        ..., description="e.g., 'process demonstration', 'authority endorsement'"
    )
    current_usage: str = Field(
        default="", description="How competitors use proof now (if at all)"
    )
    opportunity: str


class OfferGap(BaseModel):
    gap: str = Field(..., description="Offer structure nobody is testing")
    opportunity: str


class GapAnalysis(BaseModel):
    unaddressed_objections: list[UnaddressedObjection] = Field(default_factory=list)
    untapped_emotional_angles: list[UntappedEmotionalAngle] = Field(default_factory=list)
    ignored_funnel_stages: list[IgnoredFunnelStage] = Field(default_factory=list)
    underserved_audiences: list[UnderservedAudience] = Field(default_factory=list)
    unused_formats: list[UnusedFormat] = Field(default_factory=list)
    underutilized_proof_types: list[UnderutilizedProofType] = Field(default_factory=list)
    offer_gaps: list[OfferGap] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Strategic Priority Stack
# ---------------------------------------------------------------------------

class PriorityDirective(BaseModel):
    directive: str = Field(..., description="Clear action item for Agent 2")
    reasoning: str = Field(..., description="Why this is prioritized")
    references: list[str] = Field(
        default_factory=list,
        description="IDs or names of related findings above"
    )


class AvoidDirective(BaseModel):
    directive: str = Field(..., description="What NOT to do")
    reasoning: str = Field(
        ..., description="Why (played out, brand unsafe, low DR potential, etc.)"
    )


class StrategicPriorityStack(BaseModel):
    must_act_on: list[PriorityDirective] = Field(
        ..., description="Top 3-5 — 'drop everything and build around these'"
    )
    strong_opportunities: list[PriorityDirective] = Field(default_factory=list)
    worth_testing: list[PriorityDirective] = Field(default_factory=list)
    avoid: list[AvoidDirective] = Field(
        ..., description="At least 1 — there's always something to avoid"
    )


# ---------------------------------------------------------------------------
# Platform-Specific Notes
# ---------------------------------------------------------------------------

class PlatformNotes(BaseModel):
    meta_feed: str = Field(
        default="", description="What's specifically working on Meta feed right now"
    )
    meta_reels: str = Field(
        default="", description="What's specifically working on Reels"
    )
    tiktok: str = Field(
        default="", description="What's specifically working on TikTok"
    )
    cross_platform: str = Field(
        default="", description="What works across all platforms"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Trend Intel Brief v2.0
# ---------------------------------------------------------------------------

class TrendIntelBrief(BaseModel):
    """Complete Agent 1B output — real-time competitive + cultural intel.

    v2.0: Includes scoring, confidence tagging, gap analysis,
    strategic priority stack, and DR conversion assessment.
    """

    meta: ResearchMeta

    trending_formats: list[TrendingFormat] = Field(
        ..., description="5-15 trending formats with scoring and DR assessment"
    )

    competitor_ads: list[CompetitorAd] = Field(
        ..., description="5-20 competitor ad breakdowns with hook anatomy and performance signals"
    )

    cultural_moments: list[CulturalMoment] = Field(
        ..., description="3-10 cultural moments with timing and brand safety"
    )

    working_hooks: list[WorkingHook] = Field(
        ..., description="10-30 working hooks with written-out brand adaptations"
    )

    gap_analysis: GapAnalysis = Field(
        ..., description="What competitors are NOT doing — whitespace opportunities"
    )

    strategic_priority_stack: StrategicPriorityStack = Field(
        ..., description="Prioritized action items for Agent 2"
    )

    platform_notes: PlatformNotes = Field(
        default_factory=PlatformNotes,
        description="Platform-specific intelligence"
    )

    raw_intelligence_notes: str = Field(
        default="",
        description="Anything else useful (policy changes, algorithm shifts, emerging competitors)"
    )
