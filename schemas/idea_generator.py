"""Agent 02 output schema — Idea Generator (Creative Collision Engine).

Takes trend-informed angles from the Angle Architect (1A2) and collides
them with live trend intelligence from Trend Intel (1B) to produce
specific, platform-native, executable ad concepts.

Each idea = strategic angle x trend execution, with scene concepts,
platform targets, hook marriages, and copywriter handoff notes.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import (
    ComplianceRisk,
    CreativeFormat,
    FunnelStage,
    ProofType,
)


# ---------------------------------------------------------------------------
# Hook Direction (kept from original — Agent 5 engineers the final hook)
# ---------------------------------------------------------------------------

class HookDirection(BaseModel):
    """High-level hook direction (Agent 5 will engineer the actual hook)."""
    hook_type: str = Field(
        ...,
        description=(
            "Hook family: identity_callout, problem_agitation, outcome_reveal, "
            "mechanism_reveal, myth_bust, social_proof, curiosity_gap, "
            "fear_urgency, confession, challenge, instructional, reverse_psychology, "
            "numeric_specificity"
        ),
    )
    hook_concept: str = Field(
        ..., description="1-2 sentence description of the hook idea"
    )
    first_line_draft: str = Field(
        ..., description="Draft of the opening spoken/text line"
    )


# ---------------------------------------------------------------------------
# Individual Ad Idea — Creative Collision Output
# ---------------------------------------------------------------------------

class AdIdea(BaseModel):
    """A single executable ad concept — the collision of a strategic angle
    and a live trend, ready for downstream scripting and production."""

    idea_id: str = Field(..., description="Unique ID: e.g. tof_01, mof_05, bof_03")
    funnel_stage: FunnelStage
    idea_name: str = Field(..., description="Short, memorable label for the concept")

    # --- Traceability (references back to upstream agents) ---
    angle_reference: str = Field(
        ..., description="angle_name from the Angle Architect's inventory — "
        "the strategic backbone of this idea"
    )
    trend_source_reference: str = Field(
        ..., description="Specific trend element from Trend Intel used — "
        "format name, hook text, cultural moment, or competitor ad reference"
    )

    # --- The Creative Concept (the core output) ---
    one_line_concept: str = Field(
        ..., description="Vivid, filmable concept in one sentence — a creative "
        "director should say 'I want to see that' when reading this"
    )
    scene_concept: str = Field(
        ..., description="What the viewer sees: setting, characters, visual flow, "
        "key moments, emotional arc. Specific enough to brief a director."
    )
    format_execution: str = Field(
        ..., description="How the trending format is adapted for this angle — "
        "not just the format name, but the specific creative treatment"
    )

    # --- Hook Marriage ---
    hook_direction: HookDirection
    hook_x_angle_marriage: str = Field(
        ..., description="How the hook and angle reinforce each other — "
        "why this opening earns the right to deliver this message"
    )

    # --- Platform & Production ---
    platform_targets: list[str] = Field(
        ..., description="Target platforms: meta_reels, tiktok, meta_feed, ig_stories, etc."
    )
    duration_per_platform: dict[str, str] = Field(
        ..., description="Duration per platform, e.g. {'meta_reels': '30s', 'tiktok': '15s'}"
    )
    sound_music_direction: str = Field(
        ..., description="Sound/music approach: trending sound, voiceover style, "
        "original music, silence-then-hit, etc."
    )

    # --- Conversion Architecture ---
    mechanism_hint: str = Field(
        ..., description="The 'why it works' unique mechanism for this idea — "
        "the key differentiator that drives belief"
    )
    proof_approach: ProofType
    proof_description: str = Field(
        ..., description="What specific proof would be shown and how it's presented"
    )

    # --- Risk & Metadata ---
    compliance_risk: ComplianceRisk
    compliance_notes: str = Field(
        default="", description="Specific compliance concerns for this idea"
    )
    is_swing_idea: bool = Field(
        default=False,
        description="True if this is a bold/unconventional concept",
    )
    swing_rationale: Optional[str] = Field(
        None, description="Why this swing idea is worth the risk"
    )

    # --- Downstream Handoff ---
    execution_notes_for_copywriter: str = Field(
        ..., description="Specific direction for the Copywriter on tone, pacing, "
        "key emotional beats, proof placement, and CTA approach"
    )


# ---------------------------------------------------------------------------
# Funnel Stage Group
# ---------------------------------------------------------------------------

class FunnelStageGroup(BaseModel):
    """Group of ideas for a single funnel stage."""
    stage: FunnelStage
    ideas: list[AdIdea] = Field(
        ..., min_length=8, max_length=12,
        description="~10 ideas for this funnel stage (target exactly 10)"
    )
    swing_idea_count: int = Field(
        ..., ge=0, le=10,
        description="Number of bold 'swing' ideas in this group (target 2-3)"
    )


# ---------------------------------------------------------------------------
# Collision Audit (replaces Diversity Audit)
# ---------------------------------------------------------------------------

class CollisionAudit(BaseModel):
    """Self-check that ideas represent genuine angle x trend collisions
    with sufficient variety across the strategic and tactical dimensions."""
    unique_angles_referenced: int = Field(
        ..., description="Count of distinct angles from the Angle Architect used"
    )
    unique_trend_sources_referenced: int = Field(
        ..., description="Count of distinct trend elements from Trend Intel used"
    )
    unique_formats_used: int = Field(
        ..., description="Count of distinct creative format executions"
    )
    platform_coverage: dict[str, int] = Field(
        ..., description="Count of ideas targeting each platform"
    )
    collision_quality_notes: str = Field(
        ..., description="Self-assessment of collision quality — are the marriages "
        "natural or forced? Which are the strongest?"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Idea Generator Brief
# ---------------------------------------------------------------------------

class IdeaGeneratorBrief(BaseModel):
    """Complete Agent 02 output — 30 executable ad concepts across the funnel.

    Each idea is the collision of a strategic angle (from the Angle Architect)
    and a live trend element (from Trend Intel), producing a specific,
    filmable, platform-native creative concept.
    """
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Core output: 30 ideas in 3 groups of 10
    tof_ideas: FunnelStageGroup = Field(
        ..., description="10 Top-of-Funnel ideas (Unaware → Problem Aware)"
    )
    mof_ideas: FunnelStageGroup = Field(
        ..., description="10 Middle-of-Funnel ideas (Solution Aware → Product Aware)"
    )
    bof_ideas: FunnelStageGroup = Field(
        ..., description="10 Bottom-of-Funnel ideas (Product Aware → Most Aware)"
    )

    # Quality checks
    collision_audit: CollisionAudit

    # Strategic notes for Stress Tester
    key_themes: list[str] = Field(
        ..., min_length=1, max_length=10,
        description="3-7 dominant creative themes across the ideas"
    )
    boldest_bets: list[str] = Field(
        ..., min_length=1, max_length=10,
        description="The 3-6 riskiest/most original ideas worth protecting in stress testing"
    )
    recommended_priority_order: list[str] = Field(
        ..., description="Top 10 idea IDs in priority order for production"
    )
