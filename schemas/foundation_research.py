"""Agent 1A output schema — Foundation Research v2.

Hard-cut schema for the new 7-pillar architecture.
This module also keeps shared enums used by downstream schemas.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared Enums (kept for cross-schema imports)
# ---------------------------------------------------------------------------


class _LenientStrEnum(str, Enum):
    """Base for string enums that tolerate LLM formatting quirks."""

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            normalised = value.strip().lower().replace(" ", "_").replace("-", "_")
            for member in cls:
                if member.value == normalised or member.name.lower() == normalised:
                    return member
        return list(cls)[0]


class AwarenessLevel(_LenientStrEnum):
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

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            try:
                return cls(int(value))
            except (ValueError, KeyError):
                pass
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
# Shared Phase 1 Models
# ---------------------------------------------------------------------------


class ResearchModelTraceEntry(BaseModel):
    stage: str = Field(..., description="collector | synthesis | adjudication | quality_gate")
    provider: str
    model: str
    status: str = Field(..., description="success | failed | skipped")
    started_at: str
    finished_at: str
    duration_seconds: float
    notes: str = ""


class EvidenceItem(BaseModel):
    evidence_id: str
    claim: str
    verbatim: str = ""
    source_url: str = ""
    source_type: str = Field(
        ..., description="review | reddit | forum | ad_library | landing_page | support | survey | social | other"
    )
    published_date: str = ""
    pillar_tags: list[str] = Field(default_factory=list)
    confidence: float = Field(..., ge=0.0, le=1.0)
    provider: str = Field(..., description="gemini | claude | synthesis | adjudication")
    conflict_flag: str = Field(default="", description=" | low | medium | high_unresolved")


class MechanismSaturationEntry(BaseModel):
    mechanism: str
    saturation_score: int = Field(..., ge=1, le=10)


# ---------------------------------------------------------------------------
# Pillar 1 — Prospect Profile
# ---------------------------------------------------------------------------


class ProspectSegment(BaseModel):
    segment_name: str
    goals: list[str] = Field(default_factory=list)
    pains: list[str] = Field(default_factory=list)
    triggers: list[str] = Field(default_factory=list)
    information_sources: list[str] = Field(default_factory=list)
    objections: list[str] = Field(default_factory=list)


class Pillar1ProspectProfile(BaseModel):
    segment_profiles: list[ProspectSegment] = Field(default_factory=list)
    synthesis_summary: str = ""


# ---------------------------------------------------------------------------
# Pillar 2 — VOC Language Bank
# ---------------------------------------------------------------------------


class VocQuote(BaseModel):
    quote_id: str
    quote: str
    category: str = Field(..., description="pain | desire | objection | trigger | proof")
    theme: str
    segment_name: str = ""
    awareness_level: AwarenessLevel = AwarenessLevel.PROBLEM_AWARE
    dominant_emotion: str = ""
    emotional_intensity: int = Field(default=3, ge=1, le=5)
    source_type: str = "other"
    source_url: str = ""


class Pillar2VocLanguageBank(BaseModel):
    quotes: list[VocQuote] = Field(default_factory=list)
    saturation_last_30_new_themes: int = Field(default=0, ge=0)


# ---------------------------------------------------------------------------
# Pillar 3 — Competitive Intelligence
# ---------------------------------------------------------------------------


class CompetitorProfile(BaseModel):
    competitor_name: str
    primary_promise: str
    mechanism: str
    offer_style: str
    proof_style: str
    creative_pattern: str
    source_url: str = ""


class Pillar3CompetitiveIntelligence(BaseModel):
    direct_competitors: list[CompetitorProfile] = Field(default_factory=list)
    substitute_categories: list[str] = Field(default_factory=list)
    mechanism_saturation_map: list[MechanismSaturationEntry] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pillar 4 — Product and Mechanism Analysis
# ---------------------------------------------------------------------------


class Pillar4ProductMechanismAnalysis(BaseModel):
    why_problem_exists: str
    why_solution_uniquely_works: str
    primary_mechanism_name: str
    mechanism_supporting_evidence_ids: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pillar 5 — Awareness Classification
# ---------------------------------------------------------------------------


class AwarenessSegmentClassification(BaseModel):
    segment_name: str
    primary_awareness: AwarenessLevel
    awareness_distribution: dict[str, float] = Field(default_factory=dict)
    support_evidence_ids: list[str] = Field(default_factory=list)


class Pillar5AwarenessClassification(BaseModel):
    segment_classifications: list[AwarenessSegmentClassification] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pillar 6 — Emotional Driver Inventory
# ---------------------------------------------------------------------------


class DominantEmotion(BaseModel):
    emotion: str
    tagged_quote_count: int = Field(..., ge=0)
    share_of_voc: float = Field(..., ge=0.0, le=1.0)
    sample_quote_ids: list[str] = Field(default_factory=list)


class Pillar6EmotionalDriverInventory(BaseModel):
    dominant_emotions: list[DominantEmotion] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pillar 7 — Proof and Credibility Inventory
# ---------------------------------------------------------------------------


class ProofAsset(BaseModel):
    asset_id: str
    proof_type: str = Field(..., description="statistical | testimonial | authority | story")
    title: str
    detail: str
    strength: str = Field(..., description="top_tier | strong | moderate")
    source_url: str = ""


class Pillar7ProofCredibilityInventory(BaseModel):
    assets: list[ProofAsset] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Governance Reports
# ---------------------------------------------------------------------------


class CrossPillarConsistencyReport(BaseModel):
    objections_represented_in_voc: bool
    mechanism_alignment_with_competition: bool
    dominant_emotions_traced_to_voc: bool
    issues: list[str] = Field(default_factory=list)


class ContradictionReport(BaseModel):
    claim_a_id: str
    claim_b_id: str
    provider_a: str
    provider_b: str
    conflict_description: str
    severity: str = Field(..., description="high | medium | low")
    resolution: str = ""
    resolved: bool = False


class QualityGateCheck(BaseModel):
    gate_id: str
    passed: bool
    required: str
    actual: str
    details: str = ""


class QualityGateReport(BaseModel):
    overall_pass: bool
    failed_gate_ids: list[str] = Field(default_factory=list)
    checks: list[QualityGateCheck] = Field(default_factory=list)
    retry_rounds_used: int = 0


class RetryAuditEntry(BaseModel):
    round_index: int = 0
    failed_gate_ids_before: list[str] = Field(default_factory=list)
    selected_collector: str = ""
    added_evidence_count: int = 0
    failed_gate_ids_after: list[str] = Field(default_factory=list)
    status: str = Field(..., description="improved | unchanged | resolved | collector_failed")
    warning: str = ""


# ---------------------------------------------------------------------------
# Final Output
# ---------------------------------------------------------------------------


class FoundationResearchBriefV2(BaseModel):
    brand_name: str
    product_name: str
    generated_date: str

    schema_version: str = "2.0"
    phase1_runtime_seconds: float
    research_model_trace: list[ResearchModelTraceEntry] = Field(default_factory=list)

    pillar_1_prospect_profile: Pillar1ProspectProfile
    pillar_2_voc_language_bank: Pillar2VocLanguageBank
    pillar_3_competitive_intelligence: Pillar3CompetitiveIntelligence
    pillar_4_product_mechanism_analysis: Pillar4ProductMechanismAnalysis
    pillar_5_awareness_classification: Pillar5AwarenessClassification
    pillar_6_emotional_driver_inventory: Pillar6EmotionalDriverInventory
    pillar_7_proof_credibility_inventory: Pillar7ProofCredibilityInventory

    evidence_ledger: list[EvidenceItem] = Field(default_factory=list)
    contradictions: list[ContradictionReport] = Field(default_factory=list)
    retry_audit: list[RetryAuditEntry] = Field(default_factory=list)
    quality_gate_report: QualityGateReport
    cross_pillar_consistency_report: CrossPillarConsistencyReport


# Backward alias for existing imports in this codebase.
FoundationResearchBrief = FoundationResearchBriefV2
