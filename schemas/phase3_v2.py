"""Phase 3 v2 schemas — Brief Unit pipeline (Milestone 1)."""

from __future__ import annotations

from statistics import median
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


ArmName = Literal["control", "claude_sdk"]
ReviewDecision = Literal["approve", "revise", "reject"]
ChatRole = Literal["user", "assistant"]
DraftEditSource = Literal["manual", "chat_apply"]
HookSelectionStatus = Literal["candidate", "selected", "skipped", "stale"]
SceneMode = Literal["a_roll", "b_roll"]


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


class HookContextV1(BaseModel):
    run_id: str = ""
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    awareness_level: str
    emotion_key: str
    emotion_label: str = ""
    script_id: str = ""
    script_sections: CoreScriptSectionsV1 | None = None
    script_lines: list[CoreScriptLineV1] = Field(default_factory=list)
    evidence_ids_allowed: list[str] = Field(default_factory=list)
    evidence_catalog: dict[str, str] = Field(default_factory=dict)


class HookCandidateV1(BaseModel):
    candidate_id: str = Field(..., description="Deterministic ID: hc_{brief_unit_id}_{lane}_{nnn}")
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    lane_id: str
    lane_label: str = ""
    verbal_open: str
    visual_pattern_interrupt: str
    on_screen_text: str = ""
    awareness_level: str
    emotion_key: str
    evidence_ids: list[str] = Field(default_factory=list)
    rationale: str = ""
    model_metadata: dict[str, Any] = Field(default_factory=dict)


class HookGateResultV1(BaseModel):
    candidate_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    alignment_pass: bool = False
    evidence_pass: bool = False
    claim_boundary_pass: bool = False
    scroll_stop_score: int = Field(default=0, ge=0, le=100)
    specificity_score: int = Field(default=0, ge=0, le=100)
    gate_pass: bool = False
    failure_reasons: list[str] = Field(default_factory=list)
    evaluator_metadata: dict[str, Any] = Field(default_factory=dict)


class HookScoreV1(BaseModel):
    candidate_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    scroll_stop_score: int = Field(default=0, ge=0, le=100)
    specificity_score: int = Field(default=0, ge=0, le=100)
    diversity_penalty: float = 0.0
    composite_score: float = 0.0


class HookVariantV1(BaseModel):
    hook_id: str = Field(..., description="Deterministic final ID: hk_{brief_unit_id}_{nnn}")
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    verbal_open: str
    visual_pattern_interrupt: str
    on_screen_text: str = ""
    awareness_level: str
    emotion_key: str
    evidence_ids: list[str] = Field(default_factory=list)
    scroll_stop_score: int = Field(default=0, ge=0, le=100)
    specificity_score: int = Field(default=0, ge=0, le=100)
    lane_id: str = ""
    selection_status: HookSelectionStatus = "candidate"
    gate_pass: bool = False
    rank: int = 0


class HookBundleV1(BaseModel):
    hook_run_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    variants: list[HookVariantV1] = Field(default_factory=list)
    candidate_count: int = 0
    passed_gate_count: int = 0
    repair_rounds_used: int = 0
    deficiency_flags: list[str] = Field(default_factory=list)
    status: Literal["ok", "shortfall", "error", "skipped"] = "ok"
    error: str = ""
    generated_at: str = ""


class HookSelectionV1(BaseModel):
    run_id: str
    hook_run_id: str = ""
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    selected_hook_ids: list[str] = Field(default_factory=list)
    selected_hook_id: str = ""
    skip: bool = False
    stale: bool = False
    stale_reason: str = ""
    updated_at: str

    @model_validator(mode="after")
    def _normalize_selected_hooks(self) -> "HookSelectionV1":
        seen: set[str] = set()
        ids: list[str] = []
        for value in (self.selected_hook_ids or []):
            hook_id = str(value or "").strip()
            if not hook_id or hook_id in seen:
                continue
            seen.add(hook_id)
            ids.append(hook_id)
        if not ids:
            legacy = str(self.selected_hook_id or "").strip()
            if legacy:
                ids = [legacy]
        if self.skip:
            ids = []
        self.selected_hook_ids = ids
        self.selected_hook_id = ids[0] if ids else ""
        return self


class HookStageManifestV1(BaseModel):
    run_id: str
    hook_run_id: str = ""
    status: Literal["idle", "running", "completed", "failed"] = "idle"
    created_at: str = ""
    started_at: str = ""
    completed_at: str = ""
    error: str = ""
    eligible_count: int = 0
    processed_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    candidate_target_per_unit: int = 0
    final_variants_per_unit: int = 0
    max_parallel: int = 0
    max_repair_rounds: int = 0
    model_registry: dict[str, str] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)


class SceneHandoffUnitV1(BaseModel):
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    script_id: str = ""
    selected_hook_ids: list[str] = Field(default_factory=list)
    selected_hooks: list[HookVariantV1] = Field(default_factory=list)
    selected_hook_id: str = ""
    selected_hook: HookVariantV1 | None = None
    stale: bool = False
    status: Literal["ready", "missing_selection", "stale", "skipped"] = "missing_selection"


class SceneHandoffPacketV1(BaseModel):
    run_id: str
    hook_run_id: str = ""
    ready: bool = False
    ready_count: int = 0
    total_required: int = 0
    generated_at: str = ""
    items: list[SceneHandoffUnitV1] = Field(default_factory=list)


class ARollDirectionV1(BaseModel):
    framing: str = ""
    creator_action: str = ""
    performance_direction: str = ""
    product_interaction: str = ""
    location: str = ""


class BRollDirectionV1(BaseModel):
    shot_description: str = ""
    subject_action: str = ""
    camera_motion: str = ""
    props_assets: str = ""
    transition_intent: str = ""


class SceneLinePlanV1(BaseModel):
    scene_line_id: str
    script_line_id: str
    mode: SceneMode = "a_roll"
    a_roll: ARollDirectionV1 | None = None
    b_roll: BRollDirectionV1 | None = None
    on_screen_text: str = ""
    duration_seconds: float = Field(default=2.0, ge=0.1, le=30.0)
    evidence_ids: list[str] = Field(default_factory=list)
    difficulty_1_10: int = Field(default=5, ge=1, le=10)


class ScenePlanV1(BaseModel):
    scene_plan_id: str
    run_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    hook_id: str
    lines: list[SceneLinePlanV1] = Field(default_factory=list)
    total_duration_seconds: float = 0.0
    a_roll_line_count: int = 0
    b_roll_line_count: int = 0
    max_consecutive_mode: int = 0
    status: Literal["ok", "needs_repair", "error", "stale"] = "ok"
    stale: bool = False
    stale_reason: str = ""
    error: str = ""
    generated_at: str = ""


class SceneGateReportV1(BaseModel):
    scene_plan_id: str
    scene_unit_id: str
    run_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    hook_id: str
    line_coverage_pass: bool = False
    mode_pass: bool = False
    ugc_pass: bool = False
    evidence_pass: bool = False
    claim_safety_pass: bool = False
    feasibility_pass: bool = False
    pacing_pass: bool = False
    post_polish_pass: bool = False
    overall_pass: bool = False
    failure_reasons: list[str] = Field(default_factory=list)
    failing_line_ids: list[str] = Field(default_factory=list)
    repair_rounds_used: int = 0
    evaluated_at: str = ""
    evaluator_metadata: dict[str, Any] = Field(default_factory=dict)


class SceneStageManifestV1(BaseModel):
    run_id: str
    scene_run_id: str = ""
    status: Literal["idle", "running", "completed", "failed"] = "idle"
    created_at: str = ""
    started_at: str = ""
    completed_at: str = ""
    error: str = ""
    eligible_count: int = 0
    processed_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    stale_count: int = 0
    max_parallel: int = 0
    max_repair_rounds: int = 0
    max_difficulty: int = 0
    max_consecutive_mode: int = 0
    min_a_roll_lines: int = 0
    model_registry: dict[str, str] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)


class SceneChatReplyV1(BaseModel):
    assistant_message: str
    proposed_scene_plan: ScenePlanV1 | None = None


class ProductionHandoffUnitV1(BaseModel):
    scene_unit_id: str
    scene_plan_id: str = ""
    run_id: str
    brief_unit_id: str
    arm: ArmName = "claude_sdk"
    hook_id: str
    selected_hook_ids: list[str] = Field(default_factory=list)
    selected_hook_id: str = ""
    status: Literal["ready", "stale", "failed", "missing"] = "missing"
    stale: bool = False
    stale_reason: str = ""
    lines: list[SceneLinePlanV1] = Field(default_factory=list)
    gate_report: SceneGateReportV1 | None = None


class ProductionHandoffPacketV1(BaseModel):
    run_id: str
    scene_run_id: str = ""
    ready: bool = False
    ready_count: int = 0
    total_required: int = 0
    generated_at: str = ""
    items: list[ProductionHandoffUnitV1] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)


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
