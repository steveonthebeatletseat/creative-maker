"""Phase 4 deterministic engine helpers (pure-ish functions for orchestration)."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from schemas.phase4_video import (
    ClipInputSnapshotV1,
    ClipProvenanceV1,
    ClipQcReportV1,
    DriveAssetV1,
    DriveValidationItemV1,
    DriveValidationReportV1,
    Phase4ClipMode,
    SceneLineMappingRowV1,
    StartFrameBriefItemV1,
    StartFrameBriefV1,
)


def now_iso() -> str:
    return datetime.now().isoformat()


def safe_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "na"
    text = re.sub(r"[^A-Za-z0-9_-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "na"


def sha256_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def sha256_json(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return sha256_text(raw)


def deterministic_start_frame_filename(
    *,
    brief_unit_id: str,
    hook_id: str,
    script_line_id: str,
    mode: Phase4ClipMode,
    ext: str = "png",
) -> str:
    return (
        f"sf__{safe_token(brief_unit_id)}__{safe_token(hook_id)}__"
        f"{safe_token(script_line_id)}__{safe_token(mode)}.{safe_token(ext).lower()}"
    )


def clip_id_from_scene_line(scene_line_id: str) -> str:
    return f"clip__{safe_token(scene_line_id)}"


def normalize_phase4_clip_mode(value: Any) -> Phase4ClipMode:
    mode = str(value or "").strip().lower()
    if mode == "a_roll":
        return "a_roll"
    if mode == "animation_broll":
        return "animation_broll"
    return "b_roll"


def is_phase4_a_roll_mode(value: Any) -> bool:
    return normalize_phase4_clip_mode(value) == "a_roll"


def is_phase4_b_roll_mode(value: Any) -> bool:
    return normalize_phase4_clip_mode(value) in {"b_roll", "animation_broll"}


def build_script_text_lookup(phase3_run_detail: dict[str, Any]) -> dict[tuple[str, str, str], str]:
    lookup: dict[tuple[str, str, str], str] = {}
    drafts_by_arm = phase3_run_detail.get("drafts_by_arm")
    if not isinstance(drafts_by_arm, dict):
        return lookup
    for arm, drafts in drafts_by_arm.items():
        arm_name = str(arm or "").strip()
        if not isinstance(drafts, list):
            continue
        for draft in drafts:
            if not isinstance(draft, dict):
                continue
            brief_unit_id = str(draft.get("brief_unit_id") or "").strip()
            if not brief_unit_id:
                continue
            lines = draft.get("lines")
            if not isinstance(lines, list):
                continue
            for line in lines:
                if not isinstance(line, dict):
                    continue
                line_id = str(line.get("line_id") or "").strip()
                text = str(line.get("text") or "").strip()
                if line_id and text:
                    lookup[(brief_unit_id, arm_name, line_id)] = text
    return lookup


def build_scene_line_mapping(
    *,
    production_handoff_packet: dict[str, Any],
    script_text_lookup: dict[tuple[str, str, str], str],
) -> list[SceneLineMappingRowV1]:
    rows: list[SceneLineMappingRowV1] = []
    items = production_handoff_packet.get("items")
    if not isinstance(items, list):
        return rows

    line_index = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        scene_unit_id = str(item.get("scene_unit_id") or "").strip()
        brief_unit_id = str(item.get("brief_unit_id") or "").strip()
        hook_id = str(item.get("hook_id") or "").strip()
        arm = str(item.get("arm") or "").strip()
        lines = item.get("lines")
        if not isinstance(lines, list):
            continue
        for line in lines:
            if not isinstance(line, dict):
                continue
            scene_line_id = str(line.get("scene_line_id") or "").strip()
            script_line_id = str(line.get("script_line_id") or "").strip()
            mode: Phase4ClipMode = normalize_phase4_clip_mode(line.get("mode"))
            duration_seconds = float(line.get("duration_seconds") or 0.0) or 0.0
            narration_text = str(line.get("narration_line") or "").strip()
            if not narration_text:
                narration_text = script_text_lookup.get((brief_unit_id, arm, script_line_id), "")
            if not narration_text:
                narration_text = str(line.get("beat_text") or "").strip()
            if not narration_text:
                narration_text = str(line.get("on_screen_text") or "").strip()
            if not narration_text:
                narration_text = script_line_id

            rows.append(
                SceneLineMappingRowV1(
                    clip_id=clip_id_from_scene_line(scene_line_id),
                    scene_unit_id=scene_unit_id,
                    scene_line_id=scene_line_id,
                    brief_unit_id=brief_unit_id,
                    hook_id=hook_id,
                    arm=arm,
                    script_line_id=script_line_id,
                    mode=mode,
                    duration_seconds=max(0.1, duration_seconds),
                    narration_text=narration_text,
                    line_index=line_index,
                )
            )
            line_index += 1
    return rows


def generate_start_frame_brief(
    *,
    video_run_id: str,
    phase3_run_id: str,
    mapping_rows: list[SceneLineMappingRowV1],
) -> StartFrameBriefV1:
    required_items: list[StartFrameBriefItemV1] = []
    optional_items: list[StartFrameBriefItemV1] = []

    has_a_roll = any(row.mode == "a_roll" for row in mapping_rows)
    if has_a_roll:
        required_items.append(
            StartFrameBriefItemV1(
                brief_request_id="avatar_master",
                brief_unit_id="avatar_master",
                hook_id="global",
                script_line_id="global",
                scene_line_id="avatar_master",
                mode="a_roll",
                file_role="avatar_master",
                required=True,
                filename=deterministic_start_frame_filename(
                    brief_unit_id="avatar_master",
                    hook_id="global",
                    script_line_id="global",
                    mode="a_roll",
                    ext="png",
                ),
                rationale="Single master avatar frame reused for all A-roll talking-head lines.",
            )
        )

    for row in sorted(mapping_rows, key=lambda r: (r.line_index, r.scene_line_id)):
        filename = deterministic_start_frame_filename(
            brief_unit_id=row.brief_unit_id,
            hook_id=row.hook_id,
            script_line_id=row.script_line_id,
            mode=row.mode,
            ext="png",
        )
        if is_phase4_b_roll_mode(row.mode):
            required_items.append(
                StartFrameBriefItemV1(
                    brief_request_id=row.clip_id,
                    brief_unit_id=row.brief_unit_id,
                    hook_id=row.hook_id,
                    script_line_id=row.script_line_id,
                    scene_line_id=row.scene_line_id,
                    mode=normalize_phase4_clip_mode(row.mode),
                    file_role="line_start_frame",
                    required=True,
                    filename=filename,
                    rationale="Required line-level start frame for B-roll/animation B-roll generation.",
                )
            )
        else:
            optional_items.append(
                StartFrameBriefItemV1(
                    brief_request_id=f"{row.clip_id}__override",
                    brief_unit_id=row.brief_unit_id,
                    hook_id=row.hook_id,
                    script_line_id=row.script_line_id,
                    scene_line_id=row.scene_line_id,
                    mode="a_roll",
                    file_role="a_roll_override",
                    required=False,
                    filename=filename,
                    rationale="Optional A-roll override frame. If absent, avatar_master is used.",
                )
            )

    return StartFrameBriefV1(
        video_run_id=video_run_id,
        phase3_run_id=phase3_run_id,
        generated_at=now_iso(),
        required_items=required_items,
        optional_items=optional_items,
        naming_rules=[
            "Use exact file names from this brief; v1 ingest is exact-match only.",
            "One run-level avatar_master file is required when A-roll lines exist.",
            "One line-level start frame file is required for every B-roll or animation B-roll line.",
            "Upload only supported image MIME types (png/jpeg/webp).",
        ],
        notes=[
            "Deterministic naming enables strict validation and idempotent retries.",
            "Optional A-roll override files let you replace avatar_master per line when needed.",
        ],
    )


def _is_supported_image_mime(mime_type: str) -> bool:
    mime = str(mime_type or "").lower().strip()
    if not mime:
        return False
    return mime in {
        "image/png",
        "image/jpeg",
        "image/jpg",
        "image/webp",
    }


def validate_drive_assets(
    *,
    video_run_id: str,
    folder_url: str,
    brief: StartFrameBriefV1,
    drive_assets: list[dict[str, Any]],
) -> DriveValidationReportV1:
    by_name: dict[str, list[DriveAssetV1]] = {}
    for row in drive_assets:
        try:
            parsed = DriveAssetV1.model_validate(row)
        except Exception:
            continue
        by_name.setdefault(parsed.name, []).append(parsed)

    errors: list[str] = []
    items: list[DriveValidationItemV1] = []

    def _validate_item(item: StartFrameBriefItemV1) -> DriveValidationItemV1:
        matches = by_name.get(item.filename, [])
        if not matches:
            status = "missing" if item.required else "ok"
            issue_code = "missing_required" if item.required else ""
            message = (
                f"Missing required file: {item.filename}" if item.required else "Optional file not uploaded (ok)."
            )
            remediation = (
                f"Upload `{item.filename}` to the folder and re-run validation."
                if item.required
                else ""
            )
            if item.required:
                errors.append(message)
            return DriveValidationItemV1(
                filename=item.filename,
                file_role=item.file_role,
                required=item.required,
                status=status,
                issue_code=issue_code,
                message=message,
                remediation=remediation,
                matched_asset=None,
            )

        if len(matches) > 1:
            message = f"Duplicate ambiguous matches found for `{item.filename}`"
            errors.append(message)
            return DriveValidationItemV1(
                filename=item.filename,
                file_role=item.file_role,
                required=item.required,
                status="duplicate",
                issue_code="duplicate_ambiguous",
                message=message,
                remediation="Remove duplicates so only one exact filename match remains.",
                matched_asset=None,
            )

        asset = matches[0]
        if not _is_supported_image_mime(asset.mime_type):
            message = f"Unsupported MIME type for `{item.filename}`: {asset.mime_type or 'unknown'}"
            errors.append(message)
            return DriveValidationItemV1(
                filename=item.filename,
                file_role=item.file_role,
                required=item.required,
                status="invalid",
                issue_code="unsupported_mime",
                message=message,
                remediation="Convert to PNG/JPEG/WEBP and upload with the same filename.",
                matched_asset=asset,
            )

        if not bool(asset.readable):
            message = f"File is not readable by service account/local process: `{item.filename}`"
            errors.append(message)
            return DriveValidationItemV1(
                filename=item.filename,
                file_role=item.file_role,
                required=item.required,
                status="invalid",
                issue_code="permission_denied",
                message=message,
                remediation="Grant read access to the service account or upload readable file.",
                matched_asset=asset,
            )

        if int(asset.size_bytes or 0) <= 0:
            message = f"Zero-byte file uploaded: `{item.filename}`"
            errors.append(message)
            return DriveValidationItemV1(
                filename=item.filename,
                file_role=item.file_role,
                required=item.required,
                status="invalid",
                issue_code="zero_byte",
                message=message,
                remediation="Re-upload the source image so file size is greater than zero.",
                matched_asset=asset,
            )

        return DriveValidationItemV1(
            filename=item.filename,
            file_role=item.file_role,
            required=item.required,
            status="ok",
            issue_code="",
            message="ok",
            remediation="",
            matched_asset=asset,
        )

    for item in brief.required_items:
        items.append(_validate_item(item))
    for item in brief.optional_items:
        items.append(_validate_item(item))

    required_total = len(brief.required_items)
    required_ok = len([r for r in items if r.required and r.status == "ok"])
    optional_ok = len([r for r in items if not r.required and r.status == "ok"])

    status = "passed" if required_ok == required_total and not errors else "failed"
    report_id = f"vrpt_{int(time.time() * 1000)}"

    return DriveValidationReportV1(
        report_id=report_id,
        video_run_id=video_run_id,
        folder_url=folder_url,
        validated_at=now_iso(),
        status=status,
        required_total=required_total,
        required_ok=required_ok,
        optional_ok=optional_ok,
        errors=errors,
        items=items,
    )


def validation_asset_lookup(report: DriveValidationReportV1) -> dict[str, DriveAssetV1]:
    out: dict[str, DriveAssetV1] = {}
    for item in report.items:
        if item.status != "ok" or not item.matched_asset:
            continue
        out[item.filename] = item.matched_asset
    return out


def compute_idempotency_key(
    *,
    run_id: str,
    clip_id: str,
    revision_index: int,
    mode: str,
    start_frame_checksum: str,
    transform_hash: str,
    narration_text_hash: str,
    voice_preset_id: str,
    model_ids: dict[str, str],
    avatar_checksum: str,
) -> str:
    model_ids_raw = json.dumps(model_ids, sort_keys=True, separators=(",", ":"))
    raw = "|".join(
        [
            str(run_id or ""),
            str(clip_id or ""),
            str(int(revision_index or 0)),
            str(mode or ""),
            str(start_frame_checksum or ""),
            str(transform_hash or ""),
            str(narration_text_hash or ""),
            str(voice_preset_id or ""),
            model_ids_raw,
            str(avatar_checksum or ""),
        ]
    )
    return sha256_text(raw)


def build_clip_input_snapshot(
    *,
    mode: Phase4ClipMode,
    voice_preset_id: str,
    narration_text: str,
    planned_duration_seconds: float,
    start_frame_filename: str,
    start_frame_checksum: str,
    avatar_filename: str,
    avatar_checksum: str,
    transform_prompt: str,
    model_ids: dict[str, str],
) -> ClipInputSnapshotV1:
    narration_text_hash = sha256_text(narration_text)
    transform_hash = sha256_text(transform_prompt) if transform_prompt else ""
    return ClipInputSnapshotV1(
        mode=mode,
        voice_preset_id=voice_preset_id,
        narration_text=narration_text,
        narration_text_hash=narration_text_hash,
        planned_duration_seconds=float(planned_duration_seconds or 0.0),
        start_frame_filename=start_frame_filename,
        start_frame_checksum=start_frame_checksum,
        avatar_filename=avatar_filename,
        avatar_checksum=avatar_checksum,
        transform_prompt=transform_prompt,
        transform_hash=transform_hash,
        model_ids=model_ids,
    )


def calc_duration_match(planned: float, actual: float, tolerance_ratio: float = 0.35) -> bool:
    planned_v = max(0.0, float(planned or 0.0))
    actual_v = max(0.0, float(actual or 0.0))
    if planned_v <= 0.0:
        return actual_v > 0.0
    tolerance = max(0.4, planned_v * max(0.0, tolerance_ratio))
    return abs(actual_v - planned_v) <= tolerance


def compute_provenance_completeness(mode: Phase4ClipMode, provenance: ClipProvenanceV1) -> int:
    checks: list[bool] = [bool(provenance.idempotency_key), bool(provenance.provider_call_ids)]
    if mode == "a_roll":
        checks.extend(
            [
                bool(provenance.voice_preset_id),
                bool(provenance.tts_model),
                bool(provenance.audio_asset_id),
                bool(provenance.talking_head_asset_id),
                bool(provenance.start_frame_asset_id),
            ]
        )
    else:
        checks.extend(
            [
                bool(provenance.start_frame_asset_id),
                bool(provenance.broll_asset_id),
            ]
        )
    if not checks:
        return 0
    return int(round((sum(1 for ok in checks if ok) / len(checks)) * 100))


def build_a_roll_qc(
    *,
    planned_duration: float,
    narration_duration: float,
    audio_size: int,
    talking_head_duration: float,
    talking_head_size: int,
) -> ClipQcReportV1:
    narration_exists = audio_size > 0
    talking_head_exists = talking_head_size > 0
    narration_duration_ok = calc_duration_match(planned_duration, narration_duration)
    talking_head_duration_ok = calc_duration_match(narration_duration, talking_head_duration)
    pass_qc = (
        narration_exists
        and talking_head_exists
        and narration_duration_ok
        and talking_head_duration_ok
    )
    return ClipQcReportV1(
        narration_audio_exists=narration_exists,
        narration_audio_nonzero=narration_exists,
        narration_duration_seconds=float(narration_duration or 0.0),
        planned_duration_seconds=float(planned_duration or 0.0),
        narration_duration_within_tolerance=narration_duration_ok,
        talking_head_has_audio_stream=talking_head_exists,
        talking_head_duration_seconds=float(talking_head_duration or 0.0),
        talking_head_duration_match=talking_head_duration_ok,
        pass_qc=pass_qc,
    )


def build_b_roll_qc(*, duration_seconds: float, file_size: int, planned_duration: float) -> ClipQcReportV1:
    pass_qc = file_size > 0 and calc_duration_match(planned_duration, duration_seconds)
    return ClipQcReportV1(
        planned_duration_seconds=float(planned_duration or 0.0),
        broll_duration_seconds=float(duration_seconds or 0.0),
        pass_qc=pass_qc,
    )


def build_review_queue(clips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    for clip in sorted(clips, key=lambda r: int(r.get("line_index") or 0)):
        status = str(clip.get("status") or "")
        if status not in {"pending_review", "needs_revision", "failed"}:
            continue
        queue.append(
            {
                "clip_id": str(clip.get("clip_id") or ""),
                "scene_line_id": str(clip.get("scene_line_id") or ""),
                "script_line_id": str(clip.get("script_line_id") or ""),
                "mode": str(clip.get("mode") or ""),
                "status": status,
                "line_index": int(clip.get("line_index") or 0),
            }
        )
    return queue


def ensure_phase4_asset_dirs(run_dir: Path) -> dict[str, Path]:
    assets_root = run_dir / "assets"
    paths = {
        "assets_root": assets_root,
        "start_frames": assets_root / "start_frames",
        "transformed_frames": assets_root / "transformed_frames",
        "narration_audio": assets_root / "narration_audio",
        "talking_heads": assets_root / "talking_heads",
        "broll": assets_root / "broll",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths
