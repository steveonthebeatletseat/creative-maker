"""Agent 02 output schema — Idea Generator.

Produces 30 ad ideas (10 ToF, 10 MoF, 10 BoF) from the foundation
research brief (Agent 1A) and trend intel (Agent 1B).

Each idea = angle + emotional lever + format + hook direction,
mapped to a specific avatar segment with diversity enforced.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import (
    AwarenessLevel,
    ComplianceRisk,
    CreativeFormat,
    FunnelStage,
    ProofType,
)


# ---------------------------------------------------------------------------
# Individual Ad Idea
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


class AdIdea(BaseModel):
    """A single ad concept — the creative seed that downstream agents develop."""
    idea_id: str = Field(..., description="Unique ID: e.g. tof_01, mof_05, bof_03")
    funnel_stage: FunnelStage
    idea_name: str = Field(..., description="Short human-readable name for the idea")
    one_line_concept: str = Field(
        ..., description="The idea in one sentence — what the viewer experiences"
    )

    # Strategic grounding
    angle: str = Field(..., description="The persuasion angle (from Agent 1A angle inventory)")
    target_segment: str = Field(..., description="Which avatar segment this targets")
    target_awareness: AwarenessLevel
    emotional_lever: str = Field(
        ...,
        description=(
            "Primary emotion: relief, pride, disgust, hope, fear, curiosity, "
            "envy, belonging, shame, excitement, urgency"
        ),
    )
    secondary_emotion: Optional[str] = Field(
        None, description="Optional secondary emotion for depth"
    )

    # Creative direction
    format: CreativeFormat
    suggested_duration: str = Field(
        ..., description="15s, 30s, or 60s"
    )
    hook_direction: HookDirection
    mechanism_hint: str = Field(
        ..., description="The 'why it works' angle for this idea"
    )
    proof_approach: ProofType
    proof_description: str = Field(
        ..., description="What specific proof would be shown"
    )

    # Differentiation & risk
    differentiation_from_competitors: str = Field(
        ..., description="How this idea stands apart from competitor messaging"
    )
    compliance_risk: ComplianceRisk
    compliance_notes: str = Field(
        default="", description="Specific compliance concerns for this idea"
    )

    # Metadata
    is_swing_idea: bool = Field(
        default=False,
        description="True if this is a bold/unconventional 'swing' idea",
    )
    swing_rationale: Optional[str] = Field(
        None, description="Why this swing idea is worth the risk"
    )
    inspiration_source: Optional[str] = Field(
        None, description="Trend, competitor ad, cultural moment, or research insight"
    )


# ---------------------------------------------------------------------------
# Funnel Stage Group
# ---------------------------------------------------------------------------

class FunnelStageGroup(BaseModel):
    """Group of ideas for a single funnel stage."""
    stage: FunnelStage
    ideas: list[AdIdea] = Field(
        ..., min_length=10, max_length=10,
        description="Exactly 10 ideas for this funnel stage"
    )
    swing_idea_count: int = Field(
        ..., ge=2, le=3,
        description="Number of bold 'swing' ideas in this group (2-3)"
    )


# ---------------------------------------------------------------------------
# Diversity Audit
# ---------------------------------------------------------------------------

class DiversityAudit(BaseModel):
    """Self-check that ideas cover sufficient variety."""
    unique_angles_used: int = Field(
        ..., description="Count of distinct angles across all 30 ideas"
    )
    unique_segments_covered: int = Field(
        ..., description="Count of distinct segments targeted"
    )
    unique_emotions_used: int = Field(
        ..., description="Count of distinct emotional levers used"
    )
    unique_formats_used: int = Field(
        ..., description="Count of distinct creative formats used"
    )
    awareness_level_coverage: dict[str, int] = Field(
        ..., description="Count of ideas per awareness level"
    )
    diversity_notes: str = Field(
        ..., description="Notes on any intentional clustering or gaps"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Idea Generator Brief
# ---------------------------------------------------------------------------

class IdeaGeneratorBrief(BaseModel):
    """Complete Agent 02 output — 30 ad ideas across the funnel."""
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
    diversity_audit: DiversityAudit

    # Strategic notes for Agent 3
    key_themes: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="3-7 dominant themes across the ideas"
    )
    boldest_bets: list[str] = Field(
        ..., min_length=3, max_length=6,
        description="The 3-6 riskiest/most original ideas worth protecting in stress testing"
    )
    recommended_priority_order: list[str] = Field(
        ..., description="Top 10 idea IDs in priority order for production"
    )
