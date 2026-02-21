"""Phase 4 video generation schemas (test-mode workflow + provenance)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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

Phase4ClipMode = Literal["a_roll", "b_roll", "animation_broll"]
Phase4ReviewDecision = Literal["approve", "needs_revision"]

Phase4AssetType = Literal[
    "image_bank_source",
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


class BrollCatalogItemMetadataV1(BaseModel):
    model_config = ConfigDict(extra="allow")

    library_item_type: Literal["original_upload", "ai_generated"] = "original_upload"
    ai_generated: bool = False
    mode_hint: Literal["a_roll", "b_roll", "animation_broll", "unknown"] = "unknown"
    tags: list[str] = Field(default_factory=list)

    originating_video_run_id: str = ""
    originating_scene_line_id: str = ""
    originating_clip_id: str = ""
    source_image_asset_id: str = ""
    source_image_filename: str = ""
    source_pool: str = ""

    prompt_model_provider: str = ""
    prompt_model_id: str = ""
    prompt_model_label: str = ""
    image_edit_model_id: str = ""
    image_edit_model_label: str = ""
    edit_provider: str = ""
    edit_prompt: str = ""

    assignment_score: int = 0
    assignment_status: str = ""

    usage_count: int = 0
    last_used_at: str = ""
    auto_saved_at: str = ""
    indexing_status: Literal["ready", "failed", "unindexed"] = "unindexed"
    indexing_error: str = ""
    indexed_at: str = ""
    indexing_provider: str = ""
    indexing_model_id: str = ""
    indexing_input_checksum: str = ""


class BrollCatalogFileV1(BaseModel):
    model_config = ConfigDict(extra="allow")

    file_name: str
    size_bytes: int = 0
    added_at: str = ""
    thumbnail_url: str = ""
    original_url: str = ""
    display_type: str = "broll"
    filter_kind: Literal[
        "all",
        "original",
        "ai_modified",
        "a_roll",
        "broll",
        "animation_broll",
    ] = "all"
    metadata: BrollCatalogItemMetadataV1 = Field(default_factory=BrollCatalogItemMetadataV1)


class BrollCatalogListResponseV1(BaseModel):
    folder_path: str
    folder_label: str = ""
    file_count: int = 0
    files: list[BrollCatalogFileV1] = Field(default_factory=list)


class BrollCatalogDeleteRequestV1(BaseModel):
    brand: str = ""
    file_names: list[str] = Field(default_factory=list)


class BrollCatalogRenameRequestV1(BaseModel):
    brand: str = ""
    file_name: str
    new_file_name: str


class BrollCatalogUpdateMetadataRequestV1(BaseModel):
    brand: str = ""
    file_name: str
    tags: list[str] = Field(default_factory=list)


class StartGenerationRequestV1(BaseModel):
    brand: str = ""


class StoryboardBootstrapRequestV1(BaseModel):
    brand: str = ""
    phase3_run_id: str
    voice_preset_id: str = ""


class StoryboardBootstrapResponseV1(BaseModel):
    video_run_id: str
    reused_existing_run: bool = False
    workflow_state: str = "draft"
    clip_count: int = 0


class StoryboardAssignStartRequestV1(BaseModel):
    brand: str = ""
    folder_url: str = ""
    edit_threshold: int = 5
    low_flag_threshold: int = 6
    image_edit_model: str = ""
    prompt_model: str = ""
    selected_a_roll_files: list[str] = Field(default_factory=list)
    selected_b_roll_files: list[str] = Field(default_factory=list)


class StoryboardAssignControlRequestV1(BaseModel):
    brand: str = ""


class StoryboardSceneRedoRequestV1(BaseModel):
    brand: str = ""
    guidance: str = ""
    strategy: Literal["auto", "reedit_current", "reedit_original", "new_image"] = "auto"
    clip_id: str = ""
    mode: str = ""
    source_image_filename: str = ""


class StoryboardSourceSelectionRequestV1(BaseModel):
    brand: str = ""
    selected_a_roll_files: list[str] = Field(default_factory=list)
    selected_b_roll_files: list[str] = Field(default_factory=list)


class StoryboardSourceSelectionResponseV1(BaseModel):
    video_run_id: str
    selected_a_roll_files: list[str] = Field(default_factory=list)
    selected_b_roll_files: list[str] = Field(default_factory=list)
    selectable_a_roll_count: int = 0
    selectable_b_roll_count: int = 0
    updated_at: str = ""


class StoryboardSceneAssignmentV1(BaseModel):
    scene_line_id: str
    clip_id: str = ""
    script_line_id: str = ""
    mode: Phase4ClipMode = "b_roll"
    assignment_status: Literal[
        "pending",
        "analyzing",
        "assigned",
        "assigned_needs_review",
        "failed",
    ] = "pending"
    assignment_score: int = 0
    assignment_note: str = ""
    low_confidence: bool = False
    start_frame_url: str = ""
    start_frame_filename: str = ""
    source_image_asset_id: str = ""
    source_image_filename: str = ""
    edited: bool = False
    edit_prompt: str = ""
    edit_model_id: str = ""
    edit_provider: str = ""
    updated_at: str = ""


class StoryboardAssignStatusV1(BaseModel):
    video_run_id: str
    job_id: str = ""
    status: Literal["idle", "running", "completed", "failed", "aborted"] = "idle"
    started_at: str = ""
    updated_at: str = ""
    totals: dict[str, int] = Field(default_factory=dict)
    by_scene_line_id: dict[str, StoryboardSceneAssignmentV1] = Field(default_factory=dict)
    error: str = ""


class StoryboardSaveVersionRequestV1(BaseModel):
    brand: str = ""
    label: str = ""


class StoryboardDeleteVersionRequestV1(BaseModel):
    brand: str = ""
    version_id: str


class StoryboardRenameVersionRequestV1(BaseModel):
    brand: str = ""
    version_id: str
    label: str


class StoryboardSavedVersionClipV1(BaseModel):
    clip_id: str = ""
    scene_line_id: str = ""
    script_line_id: str = ""
    mode: str = ""
    narration_line: str = ""
    scene_description: str = ""
    start_frame_url: str = ""
    start_frame_filename: str = ""
    assignment_status: str = ""
    assignment_score: int = 0
    assignment_note: str = ""
    transform_prompt: str = ""
    preview_url: str = ""


class StoryboardSavedVersionV1(BaseModel):
    version_id: str
    created_at: str
    label: str = ""
    image_edit_model_id: str = ""
    image_edit_model_label: str = ""
    prompt_model_provider: str = ""
    prompt_model_id: str = ""
    prompt_model_label: str = ""
    totals: dict[str, int] = Field(default_factory=dict)
    clips: list[StoryboardSavedVersionClipV1] = Field(default_factory=list)


class ClipHistoryResponseV1(BaseModel):
    clip: ClipV1
    revisions: list[ClipRevisionV1] = Field(default_factory=list)
    assets: list[dict[str, Any]] = Field(default_factory=list)
    provider_calls: list[ProviderCallV1] = Field(default_factory=list)
