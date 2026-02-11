"""Agent 1A output schema — Foundation Research Brief.

This is the "truth layer" the entire pipeline depends on.
Modeled directly from the research doc sections 4.1 and 5.3.

Stable keys:
  segments[], awareness_playbook{}, sophistication_diagnosis{},
  voc_library[], competitor_map[], angle_inventory[],
  testing_plan{}, compliance_prebrief{}
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class AwarenessLevel(str, Enum):
    UNAWARE = "unaware"
    PROBLEM_AWARE = "problem_aware"
    SOLUTION_AWARE = "solution_aware"
    PRODUCT_AWARE = "product_aware"
    MOST_AWARE = "most_aware"


class SophisticationStage(int, Enum):
    STAGE_1 = 1
    STAGE_2 = 2
    STAGE_3 = 3
    STAGE_4 = 4
    STAGE_5 = 5


class ComplianceRisk(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class FunnelStage(str, Enum):
    TOF = "tof"
    MOF = "mof"
    BOF = "bof"


class CreativeFormat(str, Enum):
    UGC = "ugc"
    DEMO = "demo"
    FOUNDER_STORY = "founder_story"
    COMPARISON = "comparison"
    EXPLAINER = "explainer"
    TESTIMONIAL = "testimonial"
    LISTICLE = "listicle"
    UNBOXING = "unboxing"
    BEFORE_AFTER = "before_after"
    VLOG_STYLE = "vlog_style"


class ProofType(str, Enum):
    DEMONSTRATION = "demonstration"
    TESTIMONIAL = "testimonial"
    THIRD_PARTY = "third_party"
    GUARANTEE = "guarantee"
    SCIENTIFIC = "scientific"
    SOCIAL_PROOF = "social_proof"
    CASE_STUDY = "case_study"
    AUTHORITY = "authority"


# ---------------------------------------------------------------------------
# Section 1 — Category Snapshot
# ---------------------------------------------------------------------------

class CategorySnapshot(BaseModel):
    category_definition: str = Field(
        ..., description="Category name + substitutes (doing nothing, DIY, professional service)"
    )
    seasonality_notes: str = Field(
        ..., description="Seasonality & buying windows"
    )
    avg_order_value: Optional[str] = Field(
        None, description="Typical AOV/LTV expectations"
    )
    dominant_formats: list[str] = Field(
        default_factory=list,
        description="Formats that dominate the category (UGC, demos, founder, etc.)"
    )
    channel_truths: list[str] = Field(
        default_factory=list,
        description="Platform-specific realities for this category"
    )


# ---------------------------------------------------------------------------
# Section 2 — Segments
# ---------------------------------------------------------------------------

class Objection(BaseModel):
    objection: str = Field(..., description="The objection verbatim")
    category: str = Field(
        ...,
        description="Category: efficacy, mechanism_skepticism, risk, time, effort, price_value, trust, fit, complexity"
    )
    best_proof_type: ProofType
    best_creative_format: CreativeFormat
    handle_lines: list[str] = Field(
        default_factory=list, description="Copy fragments that address this objection"
    )
    verbatim_examples: list[str] = Field(
        default_factory=list, description="Exact customer quotes expressing this objection"
    )


class SwitchingForces(BaseModel):
    """JTBD forces of progress for messaging."""
    push: str = Field(..., description="Pain of old way")
    pull: str = Field(..., description="Attraction to new way")
    anxiety: str = Field(..., description="Fear of switching")
    habit: str = Field(..., description="Inertia/comfort")


class Segment(BaseModel):
    name: str = Field(..., description="Human-readable segment name")
    situation: str = Field(..., description="'When I'm…' JTBD context")
    core_desire: str = Field(..., description="Ranked #1 desire")
    alternate_desires: list[str] = Field(
        default_factory=list, description="2 alternate desires"
    )
    core_fear: str
    top_objections: list[Objection] = Field(
        ..., min_length=3, max_length=9, description="Top 3-9 objections"
    )
    awareness_distribution: dict[AwarenessLevel, float] = Field(
        ..., description="Estimated % distribution across awareness levels"
    )
    top_triggers: list[str] = Field(
        ..., description="Events that create purchase urgency"
    )
    best_offer_framing: list[str] = Field(
        ..., description="trial, bundle, subscription, etc."
    )
    compliance_sensitivities: list[str] = Field(
        default_factory=list,
        description="Medical claims, personal attributes, etc."
    )
    job_statement: str = Field(
        ...,
        description="JTBD: When I'm in [situation], I want to [progress], so I can [outcome], despite [constraints]"
    )
    switching_forces: Optional[SwitchingForces] = None


# ---------------------------------------------------------------------------
# Section 3 — Awareness Playbook
# ---------------------------------------------------------------------------

class AwarenessPlaybookEntry(BaseModel):
    level: AwarenessLevel
    opening_line_types: list[str] = Field(
        ..., description="Hook patterns appropriate for this level"
    )
    headline_patterns: list[str]
    what_you_can_say_first: list[str]
    what_you_cannot_say_first: list[str]
    bridge_sentence: str = Field(
        ..., description="Sentence pattern that moves them one step forward"
    )


class AwarenessPlaybook(BaseModel):
    entries: list[AwarenessPlaybookEntry] = Field(
        ..., min_length=5, max_length=5,
        description="One entry per awareness level"
    )


# ---------------------------------------------------------------------------
# Section 4 — Market Sophistication Diagnosis
# ---------------------------------------------------------------------------

class MechanismSaturation(BaseModel):
    mechanism: str
    saturation_score: int = Field(
        ..., ge=1, le=10, description="1=fresh, 10=completely overused"
    )
    competitors_using: list[str] = Field(
        default_factory=list, description="Which competitors use this mechanism"
    )


class SophisticationDiagnosis(BaseModel):
    stage: SophisticationStage
    evidence: list[str] = Field(
        ..., description="Evidence supporting this stage diagnosis"
    )
    mechanism_saturation_map: list[MechanismSaturation]
    claim_inflation_notes: str
    what_will_be_believed: list[str] = Field(
        ..., description="Rules for what claims are still credible"
    )
    recommended_differentiation: str = Field(
        ..., description="new_mechanism, proof_stack, reframe, identity, or combination"
    )


# ---------------------------------------------------------------------------
# Section 5 — VoC Language Bank
# ---------------------------------------------------------------------------

class VocEntry(BaseModel):
    verbatim: str = Field(..., description="Exact customer quote")
    category: str = Field(
        ...,
        description="pain, desire, skepticism, proof_moment, metaphor, identity"
    )
    segment: str = Field(..., description="Which segment this belongs to")
    awareness_level: AwarenessLevel
    emotional_intensity: int = Field(
        ..., ge=1, le=5, description="1=mild, 5=extreme"
    )
    suggested_creative_use: str = Field(
        ..., description="hook, body, cta, caption"
    )
    specificity: str = Field(
        ..., description="generic, moderate, concrete"
    )


# ---------------------------------------------------------------------------
# Section 6 — Competitive Messaging Map
# ---------------------------------------------------------------------------

class CompetitorEntry(BaseModel):
    name: str
    primary_promise: str = Field(..., description="End benefit they lead with")
    mechanism: str = Field(..., description="The 'because' / how it works")
    proof_style: str = Field(..., description="reviews, science, authority, demo, etc.")
    offer_style: str = Field(..., description="trial, bundle, subscription, etc.")
    identity_tone: str = Field(..., description="snarky, clinical, luxury, eco, etc.")
    target_awareness: AwarenessLevel
    sophistication_approach: str = Field(
        ..., description="claim, mechanism, proof_stack, reframe"
    )
    creative_cluster: str = Field(
        ..., description="Which group of similar competitors they belong to"
    )


class WhiteSpaceHypothesis(BaseModel):
    hypothesis: str
    why_white_space: str = Field(..., description="Evidence from competitor patterns")
    gap_type: str = Field(
        ...,
        description="unclaimed_mechanism, underserved_segment, different_metric, different_enemy, different_proof, different_tone"
    )
    risks: list[str]
    best_awareness_stage: AwarenessLevel
    compliance_risk: ComplianceRisk


class CompetitorMap(BaseModel):
    competitors: list[CompetitorEntry]
    creative_clusters: list[str] = Field(
        ..., description="Named groups of competitors with similar messaging"
    )
    white_space_hypotheses: list[WhiteSpaceHypothesis] = Field(
        ..., min_length=5, max_length=15
    )


# ---------------------------------------------------------------------------
# Section 7 — Angle Inventory
# ---------------------------------------------------------------------------

class Angle(BaseModel):
    angle_name: str
    target_segment: str
    target_awareness: AwarenessLevel
    target_funnel_stage: FunnelStage
    desired_emotion: str = Field(
        ..., description="relief, pride, disgust, hope, fear, curiosity, etc."
    )
    hook_templates: list[str] = Field(
        ..., min_length=3, max_length=5,
        description="3-5 hook templates"
    )
    claim_template: str = Field(
        ..., description="Claim with [placeholders] for specifics"
    )
    proof_type_required: ProofType
    recommended_format: CreativeFormat
    compliance_risk: ComplianceRisk
    compliance_notes: str = Field(default="", description="Specific compliance concerns")


# ---------------------------------------------------------------------------
# Section 8 — Testing Plan
# ---------------------------------------------------------------------------

class TestHypothesis(BaseModel):
    hypothesis: str
    variable: str = Field(..., description="What's being tested: angle, hook, format, etc.")
    priority_score: int = Field(
        ..., ge=1, le=10, description="ICE-style priority, 10=highest"
    )
    expected_leading_indicator: str = Field(
        ..., description="thumbstop, hold_rate, ctr"
    )
    expected_lagging_indicator: str = Field(
        ..., description="cpa, roas, mer"
    )


class TestingPlan(BaseModel):
    test_matrix_summary: str = Field(
        ..., description="High-level: angles x hooks x formats"
    )
    hypotheses: list[TestHypothesis]
    guardrails: list[str] = Field(
        ..., description="Frequency caps, fatigue indicators, etc."
    )
    creative_fatigue_indicators: list[str]


# ---------------------------------------------------------------------------
# Section 9 — Compliance Pre-Brief
# ---------------------------------------------------------------------------

class CompliancePrebrief(BaseModel):
    prohibited_claim_areas: list[str] = Field(
        ..., description="Claim areas likely to arise that are prohibited"
    )
    safe_phrasing_patterns: list[str] = Field(
        ..., description="Approved ways to say common things"
    )
    personal_attribute_risks: list[str] = Field(
        ..., description="Disallowed personal-attribute callouts"
    )
    before_after_guidelines: str
    required_disclaimers: list[str]
    platform_specific_notes: list[str] = Field(
        default_factory=list, description="Meta, TikTok specific flags"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Foundation Research Brief
# ---------------------------------------------------------------------------

class FoundationResearchBrief(BaseModel):
    """Complete Agent 1A output — the foundation truth layer.

    All downstream agents consume slices of this structure.
    """
    brand_name: str
    product_name: str
    generated_date: str

    # Section 1
    category_snapshot: CategorySnapshot

    # Section 2
    segments: list[Segment] = Field(
        ..., min_length=3, max_length=7,
        description="3-7 customer segments"
    )

    # Section 3
    awareness_playbook: AwarenessPlaybook

    # Section 4
    sophistication_diagnosis: SophisticationDiagnosis

    # Section 5
    voc_library: list[VocEntry] = Field(
        ..., description="Voice of customer language bank"
    )

    # Section 6
    competitor_map: CompetitorMap

    # Section 7
    angle_inventory: list[Angle] = Field(
        ..., min_length=20, max_length=60,
        description="20-60 angles with hooks, claims, proof types"
    )

    # Section 8
    testing_plan: TestingPlan

    # Section 9
    compliance_prebrief: CompliancePrebrief
