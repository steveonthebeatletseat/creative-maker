"""Phase 4 video generation schemas (test-mode workflow + provenance)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


Phase4RunWorkflowState = Literal[
    "draft",
    "brief_generated",
    "brief_approved",
    "validating_assets",
    "assets_validated",
    "generating",
    "review_pending",
    "completed",
    "validation_failed",
    "failed",
    "aborted",
]

Phase4RunStatus = Literal["active", "completed", "failed", "aborted"]

Phase4ClipStatus = Literal[
    "pending",
    "transforming",
    "generating_tts",
    "generating_a_roll",
    "generating_b_roll",
    "qc_pending",
    "pending_review",
    "approved",
    "needs_revision",
    "new_revision_created",
    "failed",
]

Phase4ClipMode = Literal["a_roll", "b_roll"]
Phase4ReviewDecision = Literal["approve", "needs_revision"]

Phase4AssetType = Literal[
    "start_frame",
    "transformed_frame",
    "narration_audio",
    "talking_head",
    "broll",
    "validation_file",
]


class VoicePresetV1(BaseModel):
    voice_preset_id: str
    name: str
    provider: str
    tts_model: str
    style: str = ""
    settings: dict[str, Any] = Field(default_factory=dict)


class Phase4RunManifestV1(BaseModel):
    video_run_id: str
    phase3_run_id: str
    branch_id: str
    brand_slug: str
    status: Phase4RunStatus = "active"
    workflow_state: Phase4RunWorkflowState = "draft"
    voice_preset_id: str
    reviewer_role: str = "operator"
    created_at: str
    updated_at: str
    completed_at: str = ""
    drive_folder_url: str = ""
    parallelism: int = 1
    error: str = ""
    metrics: dict[str, Any] = Field(default_factory=dict)


class StartFrameBriefItemV1(BaseModel):
    brief_request_id: str
    brief_unit_id: str
    hook_id: str
    script_line_id: str
    scene_line_id: str
    mode: Phase4ClipMode
    file_role: Literal["avatar_master", "line_start_frame", "a_roll_override"]
    required: bool = True
    filename: str
    rationale: str = ""


class StartFrameBriefV1(BaseModel):
    video_run_id: str
    phase3_run_id: str
    generated_at: str
    filename_pattern: str = "sf__<brief_unit_id>__<hook_id>__<script_line_id>__<mode>.<ext>"
    required_items: list[StartFrameBriefItemV1] = Field(default_factory=list)
    optional_items: list[StartFrameBriefItemV1] = Field(default_factory=list)
    naming_rules: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class StartFrameBriefApprovalV1(BaseModel):
    video_run_id: str
    approved: bool = True
    approved_by: str = ""
    approved_at: str
    notes: str = ""


class DriveAssetV1(BaseModel):
    name: str
    mime_type: str = ""
    size_bytes: int = 0
    checksum_sha256: str = ""
    readable: bool = True
    source_id: str = ""
    source_url: str = ""


class DriveValidationItemV1(BaseModel):
    filename: str
    file_role: str
    required: bool = True
    status: Literal["ok", "missing", "duplicate", "invalid"] = "ok"
    issue_code: str = ""
    message: str = ""
    remediation: str = ""
    matched_asset: DriveAssetV1 | None = None


class DriveValidationReportV1(BaseModel):
    report_id: str
    video_run_id: str
    folder_url: str
    validated_at: str
    status: Literal["passed", "failed"]
    required_total: int = 0
    required_ok: int = 0
    optional_ok: int = 0
    errors: list[str] = Field(default_factory=list)
    items: list[DriveValidationItemV1] = Field(default_factory=list)


class SceneLineMappingRowV1(BaseModel):
    clip_id: str
    scene_unit_id: str
    scene_line_id: str
    brief_unit_id: str
    hook_id: str
    arm: str
    script_line_id: str
    mode: Phase4ClipMode
    duration_seconds: float = 2.0
    narration_text: str = ""
    line_index: int = 0


class ClipInputSnapshotV1(BaseModel):
    mode: Phase4ClipMode = "b_roll"
    voice_preset_id: str = ""
    narration_text: str = ""
    narration_text_hash: str = ""
    planned_duration_seconds: float = 0.0
    start_frame_filename: str = ""
    start_frame_checksum: str = ""
    avatar_filename: str = ""
    avatar_checksum: str = ""
    transform_prompt: str = ""
    transform_hash: str = ""
    model_ids: dict[str, str] = Field(default_factory=dict)


class ClipProvenanceV1(BaseModel):
    idempotency_key: str = ""
    provider_call_ids: list[str] = Field(default_factory=list)
    voice_preset_id: str = ""
    tts_model: str = ""
    audio_asset_id: str = ""
    talking_head_asset_id: str = ""
    broll_asset_id: str = ""
    start_frame_asset_id: str = ""
    transformed_frame_asset_id: str = ""
    timestamps: dict[str, str] = Field(default_factory=dict)
    completeness_pct: int = 0


class ClipQcReportV1(BaseModel):
    narration_audio_exists: bool = False
    narration_audio_nonzero: bool = False
    narration_duration_seconds: float = 0.0
    planned_duration_seconds: float = 0.0
    narration_duration_within_tolerance: bool = False
    talking_head_has_audio_stream: bool = False
    talking_head_duration_seconds: float = 0.0
    talking_head_duration_match: bool = False
    broll_duration_seconds: float = 0.0
    pass_qc: bool = False
    notes: list[str] = Field(default_factory=list)


class ClipRevisionV1(BaseModel):
    revision_id: str
    video_run_id: str
    clip_id: str
    revision_index: int
    status: Phase4ClipStatus = "pending"
    created_at: str
    created_by: str = ""
    operator_note: str = ""
    input_snapshot: ClipInputSnapshotV1 = Field(default_factory=ClipInputSnapshotV1)
    provenance: ClipProvenanceV1 = Field(default_factory=ClipProvenanceV1)
    qc_report: ClipQcReportV1 = Field(default_factory=ClipQcReportV1)


class ClipV1(BaseModel):
    clip_id: str
    video_run_id: str
    scene_unit_id: str
    scene_line_id: str
    brief_unit_id: str
    hook_id: str
    arm: str
    script_line_id: str
    mode: Phase4ClipMode
    status: Phase4ClipStatus = "pending"
    current_revision_index: int = 1
    line_index: int = 0
    created_at: str
    updated_at: str


class ProviderCallV1(BaseModel):
    provider_call_id: str
    video_run_id: str
    clip_id: str
    revision_id: str
    provider_name: str
    operation: str
    idempotency_key: str
    status: Literal["submitted", "completed", "failed"] = "submitted"
    request_payload: dict[str, Any] = Field(default_factory=dict)
    response_payload: dict[str, Any] = Field(default_factory=dict)
    error: str = ""
    created_at: str
    updated_at: str


class ReviewDecisionRequestV1(BaseModel):
    decision: Phase4ReviewDecision
    note: str = ""
    reviewer_id: str = ""

    @model_validator(mode="after")
    def _validate_note(self) -> "ReviewDecisionRequestV1":
        if self.decision == "needs_revision" and not str(self.note or "").strip():
            raise ValueError("note is required when decision=needs_revision")
        return self


class ReviseClipRequestV1(BaseModel):
    note: str = ""
    reviewer_id: str = ""
    transform_prompt: str = ""
    a_roll_avatar_override_filename: str = ""


class CreateVideoRunRequestV1(BaseModel):
    brand: str = ""
    phase3_run_id: str
    voice_preset_id: str
    reviewer_role: str = "operator"


class GenerateBriefRequestV1(BaseModel):
    brand: str = ""


class ApproveBriefRequestV1(BaseModel):
    brand: str = ""
    approved_by: str = ""
    notes: str = ""


class DriveValidateRequestV1(BaseModel):
    brand: str = ""
    folder_url: str


class StartGenerationRequestV1(BaseModel):
    brand: str = ""


class ClipHistoryResponseV1(BaseModel):
    clip: ClipV1
    revisions: list[ClipRevisionV1] = Field(default_factory=list)
    assets: list[dict[str, Any]] = Field(default_factory=list)
    provider_calls: list[ProviderCallV1] = Field(default_factory=list)
