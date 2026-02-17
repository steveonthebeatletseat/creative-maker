"""Phase 3 v2 schemas — Brief Unit pipeline (Milestone 1)."""

from __future__ import annotations

from statistics import median
from typing import Any, Literal

from pydantic import BaseModel, Field


ArmName = Literal["control", "claude_sdk"]
ReviewDecision = Literal["approve", "revise", "reject"]
ChatRole = Literal["user", "assistant"]
DraftEditSource = Literal["manual", "chat_apply"]


class BriefUnitV1(BaseModel):
    brief_unit_id: str = Field(..., description="Stable ID: bu_{awareness}_{emotion}_{nnn}")
    matrix_cell_id: str = Field(..., description="Cell ID: cell_{awareness}_{emotion}")
    branch_id: str
    brand_slug: str
    awareness_level: str
    emotion_key: str
    emotion_label: str = ""
    ordinal_in_cell: int = Field(..., ge=1)
    source_matrix_plan_hash: str


class VocQuoteRefV1(BaseModel):
    quote_id: str
    quote_excerpt: str
    source_url: str = ""
    source_type: str = ""


class ProofRefV1(BaseModel):
    asset_id: str
    proof_type: str = ""
    title: str
    detail: str
    source_url: str = ""


class MechanismRefV1(BaseModel):
    mechanism_id: str
    title: str
    detail: str
    support_evidence_ids: list[str] = Field(default_factory=list)


class EvidenceCoverageReportV1(BaseModel):
    has_voc: bool
    has_proof: bool
    has_mechanism: bool
    voc_count: int = 0
    proof_count: int = 0
    mechanism_count: int = 0
    blocked_evidence_insufficient: bool = False


class EvidencePackV1(BaseModel):
    pack_id: str
    brief_unit_id: str
    voc_quote_refs: list[VocQuoteRefV1] = Field(default_factory=list)
    proof_refs: list[ProofRefV1] = Field(default_factory=list)
    mechanism_refs: list[MechanismRefV1] = Field(default_factory=list)
    coverage_report: EvidenceCoverageReportV1


class ScriptSpecV1(BaseModel):
    brief_unit_id: str
    required_sections: list[str] = Field(default_factory=list)
    tone_instruction: str
    word_count_min: int = Field(..., ge=1)
    word_count_max: int = Field(..., ge=1)
    cta_rule: str
    citation_rule: str


class CoreScriptSectionsV1(BaseModel):
    hook: str
    problem: str
    mechanism: str
    proof: str
    cta: str


class CoreScriptLineV1(BaseModel):
    line_id: str
    text: str
    evidence_ids: list[str] = Field(default_factory=list)


class CoreScriptGeneratedV1(BaseModel):
    sections: CoreScriptSectionsV1
    lines: list[CoreScriptLineV1] = Field(default_factory=list)


class CoreScriptDraftV1(BaseModel):
    script_id: str
    brief_unit_id: str
    arm: ArmName
    sections: CoreScriptSectionsV1 | None = None
    lines: list[CoreScriptLineV1] = Field(default_factory=list)
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    gate_report: dict[str, Any] = Field(default_factory=dict)
    status: Literal["ok", "blocked", "error"] = "ok"
    error: str = ""
    latency_seconds: float = 0.0
    cost_usd: float = 0.0


class HumanQualityReviewV1(BaseModel):
    run_id: str
    brief_unit_id: str
    arm: ArmName
    reviewer_role: str
    reviewer_id: str = ""
    quality_score_1_10: int = Field(..., ge=1, le=10)
    decision: ReviewDecision
    notes: str = ""


class BriefUnitDecisionV1(BaseModel):
    run_id: str
    brief_unit_id: str
    arm: ArmName
    reviewer_role: str
    reviewer_id: str = ""
    decision: ReviewDecision
    updated_at: str


class Phase3V2DecisionProgressV1(BaseModel):
    total_required: int = 0
    approved: int = 0
    revise: int = 0
    reject: int = 0
    pending: int = 0
    all_approved: bool = False


class Phase3V2FinalLockV1(BaseModel):
    run_id: str
    locked: bool = False
    locked_at: str = ""
    locked_by_role: str = ""


class Phase3V2ChatMessageV1(BaseModel):
    role: ChatRole
    content: str
    created_at: str
    provider: str = ""
    model: str = ""
    has_proposed_draft: bool = False


class Phase3V2ChatReplyV1(BaseModel):
    assistant_message: str
    proposed_draft: CoreScriptGeneratedV1 | None = None


class ArmSummaryV1(BaseModel):
    arm: ArmName
    total_units: int = 0
    generated_units: int = 0
    blocked_units: int = 0
    failed_units: int = 0
    gate_pass_rate: float = 0.0
    mean_quality_score: float | None = None
    median_quality_score: float | None = None
    rejection_rate: float | None = None
    median_latency_seconds: float | None = None
    median_cost_usd: float | None = None


class Phase3V2ABSummaryV1(BaseModel):
    run_id: str
    arms: list[ArmSummaryV1] = Field(default_factory=list)
    winner: Literal["control", "claude_sdk", "tie", "insufficient_reviews"] = "insufficient_reviews"
    winner_reason: str = ""


def compute_score_stats(values: list[float]) -> tuple[float | None, float | None]:
    """Return mean/median or (None, None) when no values exist."""
    if not values:
        return None, None
    mean_value = round(sum(values) / len(values), 4)
    median_value = round(float(median(values)), 4)
    return mean_value, median_value
