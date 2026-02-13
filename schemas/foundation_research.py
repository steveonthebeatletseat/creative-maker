"""Agent 1A output schema — Foundation Research Brief.

This is the "truth layer" the entire pipeline depends on.
Modeled directly from the research doc sections 4.1 and 5.3.

Stable keys:
  segments[], awareness_playbook{}, sophistication_diagnosis{},
  voc_library[], competitor_map[]
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class _LenientStrEnum(str, Enum):
    """Base for string enums that tolerate LLM output quirks.

    Handles: wrong case, spaces instead of underscores, hyphens, etc.
    If no match, falls back to the first member instead of crashing.
    """

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            normalised = value.strip().lower().replace(" ", "_").replace("-", "_")
            for member in cls:
                if member.value == normalised or member.name.lower() == normalised:
                    return member
        # Last resort: return first member so pipeline doesn't crash
        return list(cls)[0]


class AwarenessLevel(_LenientStrEnum):
    UNAWARE = "unaware"
    PROBLEM_AWARE = "problem_aware"
    SOLUTION_AWARE = "solution_aware"
    PRODUCT_AWARE = "product_aware"
    MOST_AWARE = "most_aware"


class SophisticationStage(int, Enum):
    """Market sophistication 1-5. Accepts int or string."""
    STAGE_1 = 1
    STAGE_2 = 2
    STAGE_3 = 3
    STAGE_4 = 4
    STAGE_5 = 5

    @classmethod
    def _missing_(cls, value):
        """Allow string values like '3' or 'stage_3'."""
        if isinstance(value, str):
            # Try direct int conversion: "3" -> 3
            try:
                return cls(int(value))
            except (ValueError, KeyError):
                pass
            # Try name match: "stage_3" or "STAGE_3"
            upper = value.upper().replace(" ", "_")
            if not upper.startswith("STAGE_"):
                upper = f"STAGE_{upper}"
            for member in cls:
                if member.name == upper:
                    return member
        return None


class ComplianceRisk(_LenientStrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class FunnelStage(_LenientStrEnum):
    TOF = "tof"
    MOF = "mof"
    BOF = "bof"


class CreativeFormat(_LenientStrEnum):
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
    STATIC_IMAGE = "static_image"
    CAROUSEL = "carousel"
    STORY_AD = "story_ad"


class ProofType(_LenientStrEnum):
    DEMONSTRATION = "demonstration"
    TESTIMONIAL = "testimonial"
    THIRD_PARTY = "third_party"
    GUARANTEE = "guarantee"
    SCIENTIFIC = "scientific"
    SOCIAL_PROOF = "social_proof"
    CASE_STUDY = "case_study"
    AUTHORITY = "authority"
    COMPARISON = "comparison"
    STATISTICAL = "statistical"
    USER_GENERATED = "user_generated"
    EXPERT_ENDORSEMENT = "expert_endorsement"
    BEFORE_AFTER = "before_after"


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
        ..., description="Top 3-9 objections"
    )
    awareness_distribution: dict[str, float] = Field(
        ..., description="Estimated % distribution across awareness levels (keys: unaware, problem_aware, solution_aware, product_aware, most_aware)"
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
        ..., description="One entry per awareness level (5 entries)"
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
        ..., description="5-15 white space hypotheses"
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
        ..., description="3-7 customer segments"
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
