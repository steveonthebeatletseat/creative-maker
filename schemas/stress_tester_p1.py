"""Agent 03 output schema — Stress Tester Pass 1 (Strategic).

Evaluates 30 ideas from Agent 02 against the research brief from Agent 1A.
Filters down to 15 survivors (5 per funnel stage).
Documents kill reasons for all rejected ideas.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import ComplianceRisk, FunnelStage
from schemas.idea_generator import AdIdea


# ---------------------------------------------------------------------------
# Evaluation Scores
# ---------------------------------------------------------------------------

class IdeaEvaluation(BaseModel):
    """Detailed evaluation of a single ad idea."""
    idea_id: str = Field(..., description="References AdIdea.idea_id from Agent 02")
    idea_name: str

    # Scoring (1-10 each)
    angle_strength: int = Field(
        ..., ge=1, le=10,
        description="How strong is the persuasion angle? Grounded in research? Fresh?"
    )
    differentiation_score: int = Field(
        ..., ge=1, le=10,
        description="How well does this stand apart from competitor messaging?"
    )
    emotional_resonance: int = Field(
        ..., ge=1, le=10,
        description="Will this hit the target segment emotionally?"
    )
    compliance_viability: int = Field(
        ..., ge=1, le=10,
        description="Can this run without compliance issues? (10=no risk, 1=DOA)"
    )
    research_grounding: int = Field(
        ..., ge=1, le=10,
        description="How well does this trace back to Agent 1A data?"
    )
    audience_segment_fit: int = Field(
        ..., ge=1, le=10,
        description="How well does idea match the target segment's needs/language?"
    )
    production_feasibility: int = Field(
        ..., ge=1, le=10,
        description="How practical is this to produce? (assets, talent, complexity)"
    )

    # Composite
    composite_score: float = Field(
        ..., ge=1.0, le=10.0,
        description="Weighted average of all scores"
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
            "compliance_risk, poor_research_grounding, segment_mismatch, "
            "production_impractical, redundant_with_stronger_idea"
        ),
    )
    kill_detail: Optional[str] = Field(
        None, description="Specific explanation of why this idea was killed"
    )

    # If survived — improvement notes for Agent 4
    improvement_notes: Optional[str] = Field(
        None, description="Specific suggestions to strengthen this idea before scripting"
    )
    compliance_flags_for_agent12: list[str] = Field(
        default_factory=list,
        description="Compliance concerns to flag for Agent 12"
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
    strongest_angles: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="Angles that scored highest across stages"
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
        description="Specific guidance for Agent 04 (Copywriter) on the 15 survivors"
    )
