"""Agent 1B output schema — Trend & Competitive Intel.

Real-time competitive and cultural intelligence.
Runs fresh every batch (not quarterly like 1A).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from schemas.foundation_research import AwarenessLevel, ComplianceRisk, FunnelStage


# ---------------------------------------------------------------------------
# Trending Formats
# ---------------------------------------------------------------------------

class TrendingFormat(BaseModel):
    format_name: str = Field(..., description="e.g. 'Split-screen reaction', 'POV story', 'Green screen rant'")
    platform: str = Field(..., description="Meta, TikTok, YouTube Shorts, etc.")
    why_trending: str
    relevance_to_brand: str = Field(..., description="How this could work for the brand")
    estimated_lifespan: str = Field(..., description="e.g. 'peaking now', '2-4 weeks left', 'evergreen'")
    example_description: str = Field(..., description="Description of a strong example")


class TrendingSound(BaseModel):
    sound_name: str
    platform: str
    usage_notes: str
    brand_fit: str = Field(..., description="How/if this sound fits the brand tone")


# ---------------------------------------------------------------------------
# Competitor Ad Analysis
# ---------------------------------------------------------------------------

class CompetitorAd(BaseModel):
    competitor_name: str
    ad_description: str
    hook_used: str = Field(..., description="The opening hook / first 3 seconds")
    visual_style: str
    offer_shown: str
    estimated_spend: str = Field(default="unknown", description="Rough spend tier if inferrable")
    what_works: str = Field(..., description="Why this ad likely performs")
    what_to_steal: str = Field(..., description="Specific element worth adapting")
    awareness_target: AwarenessLevel
    funnel_position: FunnelStage


# ---------------------------------------------------------------------------
# Cultural Moments
# ---------------------------------------------------------------------------

class CulturalMoment(BaseModel):
    moment: str = Field(..., description="The trend, event, or cultural conversation")
    relevance: str = Field(..., description="How it connects to the brand/category")
    timing: str = Field(..., description="When to deploy: now, this week, this month, seasonal")
    risk_level: ComplianceRisk = Field(
        ..., description="Risk of riding this trend (brand safety)"
    )
    creative_direction: str = Field(
        ..., description="How to incorporate into an ad"
    )


# ---------------------------------------------------------------------------
# Currently Working Hooks
# ---------------------------------------------------------------------------

class WorkingHook(BaseModel):
    hook_text: str = Field(..., description="The hook verbatim or paraphrased")
    hook_category: str = Field(
        ...,
        description="question, bold_claim, story_open, pattern_interrupt, social_proof, contrarian, pov"
    )
    platform: str
    niche: str
    why_it_works: str
    adaptation_for_brand: str = Field(
        ..., description="How to adapt this hook for our brand"
    )


# ---------------------------------------------------------------------------
# Top-Level Output
# ---------------------------------------------------------------------------

class TrendIntelBrief(BaseModel):
    """Complete Agent 1B output — real-time competitive + cultural intel."""
    brand_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier for this run")

    trending_formats: list[TrendingFormat] = Field(
        ..., min_length=5, max_length=15,
        description="5-15 trending formats with platform + brand relevance"
    )
    trending_sounds: list[TrendingSound] = Field(
        default_factory=list,
        description="Trending sounds/audio worth considering"
    )
    competitor_ads: list[CompetitorAd] = Field(
        ..., min_length=5, max_length=20,
        description="5-20 competitor ad breakdowns"
    )
    cultural_moments: list[CulturalMoment] = Field(
        ..., min_length=3, max_length=10,
        description="3-10 cultural moments to tap into"
    )
    working_hooks: list[WorkingHook] = Field(
        ..., min_length=10, max_length=30,
        description="10-30 currently working hooks in the niche"
    )
    key_takeaways: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="Top 3-7 strategic takeaways for this batch"
    )
