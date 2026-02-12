"""Agent 06 output schema — Stress Tester Pass 2 (Script-Level).

Evaluates actual scripts + hooks from Agents 04 and 05.
Filters 15 scripts down to 9 winners (3 per funnel stage).
Includes light compliance pre-screen for Agent 12.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import ComplianceRisk, FunnelStage


# ---------------------------------------------------------------------------
# Script Evaluation
# ---------------------------------------------------------------------------

class ScriptEvaluation(BaseModel):
    """Detailed evaluation of a complete script + its hooks."""
    script_id: str = Field(
        ..., description="References script_id from Agent 04"
    )
    idea_name: str
    funnel_stage: FunnelStage

    # Scoring (1-10 each)
    hook_strength: int = Field(
        ..., ge=1, le=10,
        description="How strong are the hook variations? Will they stop the scroll?"
    )
    narrative_flow: int = Field(
        ..., ge=1, le=10,
        description="Does the script flow naturally from hook → body → CTA?"
    )
    persuasion_power: int = Field(
        ..., ge=1, le=10,
        description="Is the argument compelling? Mechanism + proof + objection handling?"
    )
    emotional_arc: int = Field(
        ..., ge=1, le=10,
        description="Does the script create and resolve emotional tension?"
    )
    pacing_quality: int = Field(
        ..., ge=1, le=10,
        description="Beat changes every 2-4s? WPM within range? Natural cadence?"
    )
    production_readiness: int = Field(
        ..., ge=1, le=10,
        description="Clear visual direction? Feasible assets? Editor-ready?"
    )
    compliance_pre_screen: int = Field(
        ..., ge=1, le=10,
        description="Will this pass Meta/TikTok review? (10=clean, 1=guaranteed rejection)"
    )

    # Composite
    composite_score: float = Field(
        ..., ge=1.0, le=10.0,
        description="Weighted average of all scores"
    )

    # Verdict
    verdict: str = Field(..., description="WIN or CUT")
    verdict_rationale: str = Field(
        ..., description="2-3 sentence explanation"
    )

    # If cut
    cut_reason: Optional[str] = Field(
        None,
        description=(
            "weak_hooks, poor_flow, weak_persuasion, flat_emotion, "
            "bad_pacing, not_production_ready, compliance_risk, "
            "redundant_with_stronger_script"
        ),
    )
    cut_detail: Optional[str] = Field(
        None, description="Specific explanation"
    )

    # Best hook selected
    recommended_hook_id: Optional[str] = Field(
        None, description="The hook_id that should lead in testing"
    )
    hook_ranking: list[str] = Field(
        default_factory=list,
        description="All hook_ids ranked best → worst"
    )

    # For winners: refinement notes for downstream agents
    refinement_notes: Optional[str] = Field(
        None, description="What to refine before production"
    )

    # Compliance flags for Agent 12
    compliance_flags: list[str] = Field(
        default_factory=list,
        description="Specific compliance concerns for Agent 12"
    )
    compliance_risk_level: ComplianceRisk = Field(
        default=ComplianceRisk.LOW,
        description="Overall compliance risk for this script"
    )


# ---------------------------------------------------------------------------
# Funnel Stage Results
# ---------------------------------------------------------------------------

class FunnelStageP2Result(BaseModel):
    """Stress test results for one funnel stage — script level."""
    stage: FunnelStage
    winners: list[ScriptEvaluation] = Field(
        ..., min_length=3, max_length=3,
        description="Exactly 3 winning scripts for this stage"
    )
    cuts: list[ScriptEvaluation] = Field(
        ..., min_length=2, max_length=2,
        description="Exactly 2 cut scripts for this stage"
    )
    stage_analysis: str = Field(
        ..., description="Overall quality assessment for this funnel stage"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Stress Tester P2 Brief
# ---------------------------------------------------------------------------

class StressTesterP2Brief(BaseModel):
    """Complete Agent 06 output — 9 winning scripts with full evaluations."""
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Results by funnel stage
    tof_results: FunnelStageP2Result
    mof_results: FunnelStageP2Result
    bof_results: FunnelStageP2Result

    # Summary
    total_scripts_evaluated: int = Field(default=15)
    total_winners: int = Field(default=9)

    # Cross-stage analysis
    strongest_scripts: list[str] = Field(
        ..., min_length=3, max_length=5,
        description="Top 3-5 script_ids by composite score"
    )
    weakest_areas: list[str] = Field(
        ..., min_length=2, max_length=5,
        description="Common weaknesses across cut scripts"
    )
    hook_performance_summary: str = Field(
        ..., description="Overall assessment of hook quality across scripts"
    )

    # Compliance summary for Agent 12
    compliance_summary: str = Field(
        ..., description="Overall compliance risk assessment"
    )
    high_risk_scripts: list[str] = Field(
        default_factory=list,
        description="script_ids with high compliance risk"
    )

    # Guidance for Agent 07 (Versioning Engine)
    versioning_priorities: list[str] = Field(
        ..., min_length=3, max_length=7,
        description=(
            "Specific guidance for Agent 07: which scripts get which versions, "
            "what to test, what to vary"
        ),
    )
