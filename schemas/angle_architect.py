"""Agent 1A2 output schema — Angle Architect Brief.

Receives the Foundation Research Brief from Agent 1A AND the Trend Intel
Brief from Agent 1B. Produces a comprehensive, distribution-enforced angle
inventory with trend opportunities pre-attached and a testing plan.

Each angle is explicitly linked to specific segments, desires, VoC phrases,
and white-space hypotheses from 1A's output, and paired with 2-3 best-fit
trend elements from 1B's output.
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
# Trend Opportunity (from 1B Trend Intel)
# ---------------------------------------------------------------------------

class TrendOpportunity(BaseModel):
    """A specific trend element from 1B that pairs well with this angle."""
    source_type: str = Field(
        ...,
        description="trending_format | competitor_ad | cultural_moment | working_hook",
    )
    source_name: str = Field(
        ..., description="Name/identifier of the trend element from 1B"
    )
    marriage_rationale: str = Field(
        ..., description="Why this trend pairs well with this angle — "
        "what psychological or format synergy exists"
    )
    execution_hint: str = Field(
        ..., description="Brief creative direction for how to execute "
        "the angle using this trend element"
    )


# ---------------------------------------------------------------------------
# Angle (enhanced with research linkage + trend fusion)
# ---------------------------------------------------------------------------

class Angle(BaseModel):
    """A single persuasion hypothesis grounded in 1A research and fused with 1B trends."""
    angle_name: str = Field(..., description="Descriptive, internally referenceable name")
    target_segment: str = Field(
        ..., description="Must reference a specific segment name from 1A's output"
    )
    target_awareness: AwarenessLevel
    target_funnel_stage: FunnelStage
    core_desire_addressed: str = Field(
        ..., description="The specific desire from 1A's segment data this angle targets"
    )
    desired_emotion: str = Field(
        ..., description="Primary emotion: relief, pride, disgust, hope, fear, curiosity, anger, belonging, FOMO, etc."
    )
    white_space_link: str = Field(
        ..., description="Which white-space hypothesis from 1A's competitive map this angle exploits"
    )
    voc_anchors: list[str] = Field(
        ..., description="1-3 specific verbatim VoC phrases from 1A that this angle is built on"
    )
    hook_templates: list[str] = Field(
        ..., description="3-5 hook templates ready for the Hook Specialist"
    )
    claim_template: str = Field(
        ..., description="Core claim with [placeholders] for specifics"
    )
    proof_type_required: ProofType
    recommended_format: CreativeFormat
    objection_preempted: str = Field(
        ..., description="Which objection from 1A's taxonomy this angle handles"
    )
    compliance_risk: ComplianceRisk
    compliance_notes: str = Field(default="", description="Specific compliance concerns for this angle")

    # Trend fusion (from 1B)
    trend_opportunities: list[TrendOpportunity] = Field(
        default_factory=list,
        description="2-3 best-fit trend elements from 1B for this angle. "
        "Each links to a specific trending format, competitor ad, cultural moment, "
        "or working hook and explains why the marriage works."
    )


# ---------------------------------------------------------------------------
# Distribution Audit
# ---------------------------------------------------------------------------

class DistributionCount(BaseModel):
    """Count + percentage for a distribution category."""
    label: str
    count: int
    percentage: float = Field(..., description="Percentage of total angles (0-100)")


class DistributionAudit(BaseModel):
    """Proof that distribution minimums are met."""
    total_angles: int
    by_segment: list[DistributionCount]
    by_awareness_level: list[DistributionCount]
    by_funnel_stage: list[DistributionCount]
    by_emotion: list[DistributionCount]
    by_format: list[DistributionCount]
    violations_found: list[str] = Field(
        default_factory=list,
        description="Any distribution violations and how they were resolved"
    )


# ---------------------------------------------------------------------------
# Testing Plan
# ---------------------------------------------------------------------------

class TestHypothesis(BaseModel):
    hypothesis: str
    variable: str = Field(..., description="What's being tested: angle, hook, format, etc.")
    related_angles: list[str] = Field(
        ..., description="Angle names involved in this test"
    )
    impact_score: int = Field(..., ge=1, le=10, description="Estimated conversion lift")
    confidence_score: int = Field(..., ge=1, le=10, description="Evidence strength")
    ease_score: int = Field(..., ge=1, le=10, description="Production complexity (10=easiest)")
    priority_score: float = Field(
        ..., description="ICE average: (impact + confidence + ease) / 3"
    )
    expected_leading_indicator: str = Field(
        ..., description="thumbstop, hold_rate, ctr, engagement_rate"
    )
    expected_lagging_indicator: str = Field(
        ..., description="cpa, roas, mer, ltv"
    )


class TestCluster(BaseModel):
    """A group of angles that share a hypothesis for A/B testing."""
    cluster_name: str
    hypothesis: str = Field(..., description="What this cluster tests")
    angle_names: list[str] = Field(
        ..., description="3-5 angle names in this cluster"
    )


class TestingPlan(BaseModel):
    test_matrix_summary: str = Field(
        ..., description="High-level: angles x hooks x formats"
    )
    test_clusters: list[TestCluster] = Field(
        ..., description="Grouped angles for structured testing"
    )
    hypotheses: list[TestHypothesis]
    guardrails: list[str] = Field(
        ..., description="Frequency caps, fatigue indicators, etc."
    )
    creative_fatigue_indicators: list[str]
    kill_criteria: list[str] = Field(
        default_factory=list,
        description="When to kill an angle vs iterate"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Angle Architect Brief
# ---------------------------------------------------------------------------

class AngleArchitectBrief(BaseModel):
    """Complete Agent 1A2 output — the angle inventory and testing plan.

    Every angle is traceable back to 1A's research (segments, desires,
    VoC phrases, white-space hypotheses) AND paired with 2-3 best-fit
    trend elements from 1B's live intelligence.
    """
    brand_name: str
    product_name: str
    generated_date: str

    # The core deliverable
    angle_inventory: list[Angle] = Field(
        ..., description="20-60 angles, each grounded in 1A research"
    )

    # Testing strategy
    testing_plan: TestingPlan

    # Proof of distribution compliance
    distribution_audit: DistributionAudit
