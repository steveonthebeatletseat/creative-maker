"""Agent 03 output schema — Stress Tester Pass 1 (Strategic).

Evaluates 30 creative collision ideas from Agent 02 against:
- The Angle Architect's angle inventory (strategic grounding)
- The quality of the angle x trend collision (creative execution)
Filters down to 15 survivors (5 per funnel stage).
Documents kill reasons for all rejected ideas.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import ComplianceRisk, FunnelStage


# ---------------------------------------------------------------------------
# Evaluation Scores
# ---------------------------------------------------------------------------

class IdeaEvaluation(BaseModel):
    """Detailed evaluation of a single ad idea from the Creative Collision Engine."""
    idea_id: str = Field(..., description="References AdIdea.idea_id from Agent 02")
    idea_name: str

    # --- Scoring (1-10 each) ---

    # Strategic dimensions (inherited from angle quality)
    angle_strength: int = Field(
        ..., ge=1, le=10,
        description="How strong is the underlying angle? Sharp, specific, "
        "grounded in research, tapping a real desire/fear?"
    )
    differentiation_score: int = Field(
        ..., ge=1, le=10,
        description="Does this concept stand apart from competitor messaging? "
        "Would a viewer say 'that's different'?"
    )
    emotional_resonance: int = Field(
        ..., ge=1, le=10,
        description="Will this make the target segment FEEL something? "
        "Authentic emotion, not manufactured?"
    )

    # Collision-specific dimensions (NEW)
    collision_quality: int = Field(
        ..., ge=1, le=10,
        description="Does the trend element genuinely enhance the angle, or is it "
        "forced? Does the marriage feel natural and create something stronger "
        "than either element alone? 10 = perfect synergy, 1 = forced/artificial"
    )
    execution_specificity: int = Field(
        ..., ge=1, le=10,
        description="Is this a filmable, platform-native concept with clear scene "
        "direction? Or an abstract strategy summary dressed up as an idea? "
        "10 = ready to brief a director, 1 = vague concept"
    )
    creative_originality: int = Field(
        ..., ge=1, le=10,
        description="Would a creative director say 'I haven't seen that before'? "
        "Is this concept surprising within the category? "
        "10 = genuinely novel, 1 = derivative/predictable"
    )

    # Viability dimensions
    compliance_viability: int = Field(
        ..., ge=1, le=10,
        description="Can this run on Meta and TikTok without getting flagged? "
        "10 = clean, 1 = guaranteed rejection"
    )
    production_feasibility: int = Field(
        ..., ge=1, le=10,
        description="Can this be produced with AI UGC + stock footage? "
        "Is the format achievable? Are required assets available?"
    )

    # Composite
    composite_score: float = Field(
        ..., ge=1.0, le=10.0,
        description="Weighted average: angle_strength (15%) + differentiation (10%) + "
        "emotional_resonance (15%) + collision_quality (20%) + "
        "execution_specificity (15%) + creative_originality (10%) + "
        "compliance_viability (10%) + production_feasibility (5%)"
    )

    # Verdict
    verdict: str = Field(
        ..., description="SURVIVE or KILL"
    )
    verdict_rationale: str = Field(
        ..., description="2-3 sentence explanation of the verdict"
    )

    # If killed
    kill_reason: Optional[str] = Field(
        None,
        description=(
            "Primary kill reason: weak_angle, undifferentiated, emotional_mismatch, "
            "compliance_risk, forced_collision, abstract_not_filmable, lazy_execution, "
            "production_impractical, redundant_with_stronger_idea"
        ),
    )
    kill_detail: Optional[str] = Field(
        None, description="Specific explanation of why this idea was killed"
    )

    # If survived — improvement notes for Copywriter
    improvement_notes: Optional[str] = Field(
        None, description="Specific execution refinements for the Copywriter — "
        "what to emphasize, which proof to lead with, pacing guidance"
    )
    compliance_flags_for_agent12: list[str] = Field(
        default_factory=list,
        description="Compliance concerns to flag for the Compliance agent"
    )


# ---------------------------------------------------------------------------
# Funnel Stage Results
# ---------------------------------------------------------------------------

class FunnelStageResult(BaseModel):
    """Stress test results for one funnel stage."""
    stage: FunnelStage
    survivors: list[IdeaEvaluation] = Field(
        ..., min_length=5, max_length=5,
        description="Exactly 5 surviving ideas for this stage"
    )
    killed: list[IdeaEvaluation] = Field(
        ..., min_length=5, max_length=5,
        description="Exactly 5 killed ideas for this stage"
    )
    stage_notes: str = Field(
        ..., description="Overall observations about idea quality for this stage"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Stress Tester P1 Brief
# ---------------------------------------------------------------------------

class StressTesterP1Brief(BaseModel):
    """Complete Agent 03 output — 15 surviving ideas with full evaluations."""
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Results by funnel stage
    tof_results: FunnelStageResult = Field(
        ..., description="Top-of-Funnel evaluation results"
    )
    mof_results: FunnelStageResult = Field(
        ..., description="Middle-of-Funnel evaluation results"
    )
    bof_results: FunnelStageResult = Field(
        ..., description="Bottom-of-Funnel evaluation results"
    )

    # Summary
    total_ideas_evaluated: int = Field(
        default=30, description="Total ideas evaluated"
    )
    total_survivors: int = Field(
        default=15, description="Total surviving ideas"
    )

    # Cross-stage observations
    strongest_collisions: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="The best angle x trend marriages across stages — "
        "which collisions produced the most compelling concepts"
    )
    weakest_areas: list[str] = Field(
        ..., min_length=2, max_length=5,
        description="Common weaknesses across killed ideas"
    )
    compliance_summary: str = Field(
        ..., description="Overall compliance risk assessment across survivors"
    )
    recommendations_for_copywriter: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="Specific guidance for the Copywriter on the 15 survivors"
    )
