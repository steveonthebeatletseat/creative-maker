"""Agent 07 output schema — Versioning Engine.

Creates strategic variations of the 9 winning scripts for testing.
Produces length versions, CTA variations, tone variations,
platform variations, and a testing matrix with naming conventions.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import FunnelStage


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class VersionType(str, Enum):
    LENGTH = "length"
    CTA = "cta"
    TONE = "tone"
    PLATFORM = "platform"
    HOOK = "hook"


class ToneVariant(str, Enum):
    CASUAL_UGC = "casual_ugc"
    AUTHORITATIVE = "authoritative"
    EMOTIONAL = "emotional"
    HUMOROUS = "humorous"
    FOUNDER_DIRECT = "founder_direct"


class PlatformTarget(str, Enum):
    META_FEED = "meta_feed"
    IG_REELS = "ig_reels"
    TIKTOK = "tiktok"
    YOUTUBE_SHORTS = "youtube_shorts"


# ---------------------------------------------------------------------------
# Version Components
# ---------------------------------------------------------------------------

class LengthVersion(BaseModel):
    """A script adapted to a specific duration."""
    version_id: str = Field(..., description="e.g. script_tof_01_15s")
    duration_seconds: int = Field(..., description="15, 30, or 60")
    beats_summary: str = Field(
        ..., description="How the beat structure changes for this duration"
    )
    what_was_cut: list[str] = Field(
        default_factory=list,
        description="What was removed from the base script (for shorter versions)"
    )
    what_was_added: list[str] = Field(
        default_factory=list,
        description="What was added (for longer versions)"
    )
    word_count: int
    notes: str = Field(default="", description="Adaptation notes")


class CTAVariation(BaseModel):
    """A CTA variant for testing."""
    version_id: str
    cta_type: str = Field(
        ..., description="urgency, curiosity, social_proof, risk_reversal, direct"
    )
    spoken_cta: str
    on_screen_cta: str
    rationale: str = Field(..., description="Why this CTA variant might win")


class ToneVersion(BaseModel):
    """A tone variant of the script."""
    version_id: str
    tone: ToneVariant
    key_changes: list[str] = Field(
        ..., description="What changes in the script for this tone"
    )
    opening_line: str = Field(
        ..., description="How the opening line changes"
    )
    narrator_description: str = Field(
        ..., description="Who 'says' this version (UGC creator, founder, expert, etc.)"
    )
    rationale: str


class PlatformVersion(BaseModel):
    """A platform-specific adaptation."""
    version_id: str
    platform: PlatformTarget
    aspect_ratio: str = Field(..., description="9:16, 1:1, 4:5, 16:9")
    safe_zone_notes: str = Field(
        ..., description="Text/CTA placement for platform UI overlays"
    )
    pacing_adjustments: str = Field(
        ..., description="How pacing changes for this platform"
    )
    sound_strategy: str = Field(
        ..., description="Sound-on focus, sound-off focus, or hybrid"
    )
    key_differences: list[str] = Field(
        ..., description="What changes from the base version"
    )


# ---------------------------------------------------------------------------
# Versioned Script Package
# ---------------------------------------------------------------------------

class VersionedScript(BaseModel):
    """Complete version package for one winning script."""
    base_script_id: str = Field(
        ..., description="References script_id from Agent 06 winners"
    )
    idea_name: str
    funnel_stage: FunnelStage
    recommended_hook_id: str = Field(
        ..., description="Lead hook from Agent 06 evaluation"
    )

    # Length versions
    length_versions: list[LengthVersion] = Field(
        ..., min_length=2, max_length=3,
        description="2-3 length versions (e.g. 15s + 30s, or 15s + 30s + 60s)"
    )

    # CTA variations
    cta_variations: list[CTAVariation] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 CTA variations for testing"
    )

    # Tone variations
    tone_variations: list[ToneVersion] = Field(
        ..., min_length=1, max_length=3,
        description="1-3 tone variations"
    )

    # Platform variations
    platform_variations: list[PlatformVersion] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 platform versions (at least Meta Feed + TikTok)"
    )

    # Total version count
    total_versions: int = Field(
        ..., description="Total number of distinct ad versions from this script"
    )

    # Testing priority
    testing_priority: int = Field(
        ..., ge=1, le=9,
        description="Priority rank among all 9 scripts (1=highest)"
    )


# ---------------------------------------------------------------------------
# Testing Matrix
# ---------------------------------------------------------------------------

class TestCell(BaseModel):
    """A single cell in the testing matrix."""
    test_id: str = Field(
        ..., description="Naming convention: {brand}_{stage}_{script}_{version}_{variant}"
    )
    base_script_id: str
    version_type: VersionType
    variant_description: str
    hypothesis: str = Field(
        ..., description="What we're testing with this variant"
    )
    primary_metric: str = Field(
        ..., description="hook_rate, hold_rate, ctr, cpa, roas"
    )
    minimum_spend_before_decision: str = Field(
        ..., description="e.g. '$50' or '10k impressions'"
    )


class NamingConvention(BaseModel):
    """Naming convention for campaign attribution."""
    pattern: str = Field(
        ..., description="e.g. {brand}_{stage}_{angle}_{duration}_{hook}_{cta}_{platform}"
    )
    example: str = Field(
        ..., description="Concrete example of a named ad"
    )
    field_definitions: dict[str, str] = Field(
        ..., description="What each field in the pattern means"
    )


class TestingMatrix(BaseModel):
    """Complete testing matrix for all versions."""
    total_test_cells: int
    cells: list[TestCell] = Field(
        ..., description="All test cells"
    )
    naming_convention: NamingConvention
    testing_sequence: list[str] = Field(
        ..., description="Recommended order of tests (what to launch first)"
    )
    budget_allocation: str = Field(
        ..., description="Recommended budget split across test phases"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Versioning Engine Brief
# ---------------------------------------------------------------------------

class VersioningEngineBrief(BaseModel):
    """Complete Agent 07 output — versioned scripts with testing matrix."""
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Versioned scripts
    versioned_scripts: list[VersionedScript] = Field(
        ..., min_length=9, max_length=9,
        description="Version packages for all 9 winning scripts"
    )

    # Testing matrix
    testing_matrix: TestingMatrix

    # Summary stats
    total_base_scripts: int = Field(default=9)
    total_versions_created: int = Field(
        ..., description="Total distinct ad versions across all scripts"
    )
    versions_by_type: dict[str, int] = Field(
        ..., description="Count of versions by type (length, cta, tone, platform)"
    )

    # Production notes for Agent 08
    production_notes: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="Key notes for Agent 08 (Screen Writer / Video Director)"
    )

    # Testing priorities from Agent 15B (if available)
    learning_priorities_applied: list[str] = Field(
        default_factory=list,
        description="Testing priorities from Agent 15B that influenced versioning"
    )
