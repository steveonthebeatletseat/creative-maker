"""Creative Maker — Web Server.

FastAPI backend that serves the dashboard and exposes API routes
for running the pipeline, checking status, viewing outputs, and
browsing run history (SQLite-backed).

Usage:
    python server.py
    # Then open http://localhost:8000
"""

from __future__ import annotations

import asyncio
from collections import Counter, deque
import hashlib
import json
import logging
import mimetypes
import os
import queue as queue_mod
import re
import sqlite3
import shutil
import subprocess
import tempfile
import time
import traceback
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
try:
    from PIL import Image, UnidentifiedImageError
except Exception:  # pragma: no cover - optional dependency fallback
    Image = None
    UnidentifiedImageError = Exception

import config
from agents.agent_01a_foundation_research import Agent01AFoundationResearch
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from pipeline.phase1_engine import run_phase1_collectors_only
from pipeline.phase1_quality_gates import evaluate_quality_gates
from pipeline.phase1_synthesize_pillars import derive_emotional_inventory_from_collectors
from pipeline.phase3_v2_engine import (
    build_evidence_pack,
    compile_script_spec_v1,
    compute_ab_summary,
    expand_brief_units,
    run_phase3_v2_m1,
)
from pipeline.phase3_v2_hook_engine import run_phase3_v2_hooks
from pipeline.phase3_v2_scene_engine import (
    build_scene_items_from_handoff,
    run_phase3_v2_scenes,
)
from pipeline.phase4_video_engine import (
    build_a_roll_qc,
    build_b_roll_qc,
    build_clip_input_snapshot,
    build_review_queue,
    build_scene_line_mapping,
    build_script_text_lookup,
    compute_idempotency_key,
    compute_provenance_completeness,
    deterministic_start_frame_filename,
    ensure_phase4_asset_dirs,
    generate_start_frame_brief,
    is_phase4_b_roll_mode,
    now_iso,
    normalize_phase4_clip_mode,
    sha256_text,
    validation_asset_lookup,
    validate_drive_assets,
)
from pipeline.phase4_video_providers import (
    InProcessWorkflowBackend,
    build_drive_client_for_folder,
    build_generation_providers,
    build_vision_scene_provider,
)
from schemas.foundation_research import (
    AwarenessLevel,
    CrossPillarConsistencyReport,
    EvidenceItem,
    Pillar1ProspectProfile,
    Pillar2VocLanguageBank,
    Pillar3CompetitiveIntelligence,
    Pillar4ProductMechanismAnalysis,
    Pillar5AwarenessClassification,
    Pillar7ProofCredibilityInventory,
)
from schemas.phase4_video import (
    ApproveBriefRequestV1,
    BrollCatalogItemMetadataV1,
    ClipHistoryResponseV1,
    ClipProvenanceV1,
    CreateVideoRunRequestV1,
    BrollCatalogDeleteRequestV1,
    BrollCatalogFileV1,
    BrollCatalogListResponseV1,
    BrollCatalogRenameRequestV1,
    BrollCatalogUpdateMetadataRequestV1,
    DriveValidationReportV1,
    DriveValidateRequestV1,
    GenerateBriefRequestV1,
    Phase4RunManifestV1,
    ReviewDecisionRequestV1,
    ReviseClipRequestV1,
    StoryboardAssignControlRequestV1,
    StoryboardSceneRedoRequestV1,
    StoryboardDeleteVersionRequestV1,
    StoryboardRenameVersionRequestV1,
    StoryboardSaveVersionRequestV1,
    StoryboardSavedVersionV1,
    StoryboardSavedVersionClipV1,
    StoryboardAssignStartRequestV1,
    StoryboardAssignStatusV1,
    StoryboardBootstrapRequestV1,
    StoryboardBootstrapResponseV1,
    StoryboardSceneAssignmentV1,
    StoryboardSourceSelectionRequestV1,
    StoryboardSourceSelectionResponseV1,
    SceneLineMappingRowV1,
    StartFrameBriefApprovalV1,
    StartFrameBriefV1,
    StartGenerationRequestV1,
    VoicePresetV1,
)
from schemas.phase3_v2 import (
    ARollDirectionV1,
    BriefUnitDecisionV1,
    BriefUnitV1,
    BRollDirectionV1,
    CoreScriptGeneratedV1,
    CoreScriptLineV1,
    CoreScriptSectionsV1,
    EvidencePackV1,
    HumanQualityReviewV1,
    HookBundleV1,
    HookSelectionV1,
    HookStageManifestV1,
    Phase3V2ChatMessageV1,
    Phase3V2ChatReplyV1,
    Phase3V2DecisionProgressV1,
    Phase3V2FinalLockV1,
    ProductionHandoffPacketV1,
    ProductionHandoffUnitV1,
    SceneChatReplyV1,
    SceneHandoffPacketV1,
    SceneGateReportV1,
    SceneLinePlanV1,
    ScenePlanV1,
    SceneStageManifestV1,
)

# Keep this aligned with Phase 3 v2 script/hook deterministic meta-term guards.
_P3V2_META_COPY_TERM_RE = re.compile(
    r"("
    r"\bpattern[\s_-]*interr?upt\b|"
    r"\bscroll[\s_-]*stop(?:per|ping)?\b|"
    r"\bmyth[\s_-]*bust\b|"
    r"\bidentity[\s_-]*callout\b|"
    r"\bcta\b|"
    r"\bcall\s+to\s+action\b"
    r")",
    re.IGNORECASE,
)
_P3V2_META_SUMMARY_LEADIN_RE = re.compile(
    r"^\s*(?:"
    r"calls?\s+out|"
    r"confronts?|"
    r"opens?\s+with|"
    r"highlights?|"
    r"identifies?|"
    r"signals?|"
    r"frames?|"
    r"positions?|"
    r"targets?|"
    r"addresses?|"
    r"emphasizes?|"
    r"explains?|"
    r"describes?|"
    r"shows?|"
    r"demonstrates?|"
    r"reveals?|"
    r"introduces?|"
    r"presents?|"
    r"outlines?"
    r")\b",
    re.IGNORECASE,
)
_P3V2_META_SUMMARY_PHRASE_RE = re.compile(
    r"(\bimmediately\s+signaling\b|\bsignaling\s+this\s+is\b|\bthis\s+is\s+a\s+different\s+kind\s+of\s+fix\b)",
    re.IGNORECASE,
)


def _phase3_v2_contains_meta_copy_terms(text: str) -> bool:
    value = str(text or "")
    return bool(
        _P3V2_META_COPY_TERM_RE.search(value)
        or _P3V2_META_SUMMARY_LEADIN_RE.search(value)
        or _P3V2_META_SUMMARY_PHRASE_RE.search(value)
    )

# ---------------------------------------------------------------------------
# Branch storage (brand-scoped)
# ---------------------------------------------------------------------------

def _brand_output_dir(brand_slug: str) -> Path:
    """Return the output directory for a brand."""
    d = config.OUTPUT_DIR / brand_slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _brand_branches_dir(brand_slug: str) -> Path:
    """Return the branches directory for a brand."""
    return _brand_output_dir(brand_slug) / "branches"


def _brand_branches_manifest(brand_slug: str) -> Path:
    return _brand_branches_dir(brand_slug) / "manifest.json"


def _load_branches(brand_slug: str | None = None) -> list[dict]:
    """Load all branches from a brand's manifest file."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    if not brand_slug:
        return []
    manifest = _brand_branches_manifest(brand_slug)
    if manifest.exists():
        try:
            branches = json.loads(manifest.read_text("utf-8"))
            if not isinstance(branches, list):
                return []
            for branch in branches:
                if not isinstance(branch, dict):
                    continue
                for key in ("available_agents", "completed_agents", "failed_agents"):
                    values = branch.get(key)
                    if isinstance(values, list):
                        branch[key] = _normalize_slug_list(values)
            return branches
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_branches(brand_slug: str, branches: list[dict]):
    """Save branches to a brand's manifest file."""
    bdir = _brand_branches_dir(brand_slug)
    bdir.mkdir(parents=True, exist_ok=True)
    _brand_branches_manifest(brand_slug).write_text(json.dumps(branches, indent=2), "utf-8")


def _get_branch(branch_id: str, brand_slug: str | None = None) -> dict | None:
    """Get a branch by ID."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    for b in _load_branches(brand_slug):
        if b["id"] == branch_id:
            return b
    return None


def _update_branch(branch_id: str, updates: dict, brand_slug: str | None = None):
    """Update a branch's fields and save."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    branches = _load_branches(brand_slug)
    for b in branches:
        if b["id"] == branch_id:
            b.update(updates)
            break
    _save_branches(brand_slug, branches)


def _clear_all_branches(brand_slug: str):
    """Remove all branches and their output directories. Called when Phase 1 starts (new pipeline)."""
    for b in _load_branches(brand_slug):
        bdir = _brand_branches_dir(brand_slug) / b["id"]
        if bdir.exists():
            shutil.rmtree(bdir, ignore_errors=True)
    _save_branches(brand_slug, [])
    logger.info("Cleared all branches for brand %s", brand_slug)


def _branch_output_dir(brand_slug: str, branch_id: str) -> Path:
    """Return the output directory for a branch within a brand."""
    return _brand_branches_dir(brand_slug) / branch_id


def _load_branch_output(brand_slug: str, branch_id: str, slug: str) -> dict | None:
    """Load an agent output from a specific branch directory."""
    base = _branch_output_dir(brand_slug, branch_id)
    return _load_output_from_base(base, slug)


def _phase3_v2_runs_dir(brand_slug: str, branch_id: str) -> Path:
    """Return Phase 3 v2 run root for a branch."""
    root = _branch_output_dir(brand_slug, branch_id) / "phase3_v2_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase3_v2_runs_manifest_path(brand_slug: str, branch_id: str) -> Path:
    return _phase3_v2_runs_dir(brand_slug, branch_id) / "manifest.json"


def _load_phase3_v2_runs_manifest(brand_slug: str, branch_id: str) -> list[dict[str, Any]]:
    path = _phase3_v2_runs_manifest_path(brand_slug, branch_id)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text("utf-8"))
        if not isinstance(raw, list):
            return []
        return [row for row in raw if isinstance(row, dict)]
    except (OSError, json.JSONDecodeError):
        return []


def _save_phase3_v2_runs_manifest(brand_slug: str, branch_id: str, runs: list[dict[str, Any]]) -> None:
    path = _phase3_v2_runs_manifest_path(brand_slug, branch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(runs, indent=2), "utf-8")


def _phase3_v2_run_dir(brand_slug: str, branch_id: str, run_id: str) -> Path:
    run_dir = _phase3_v2_runs_dir(brand_slug, branch_id) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _phase3_v2_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _phase3_v2_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _phase3_v2_decisions_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "decisions.json"


def _phase3_v2_chat_threads_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "chat_threads.json"


def _phase3_v2_hook_chat_threads_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "hook_chat_threads.json"


def _phase3_v2_scene_chat_threads_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "scene_chat_threads.json"


def _phase3_v2_final_lock_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "final_lock.json"


def _phase3_v2_pair_key(brief_unit_id: str, arm: str) -> str:
    return f"{str(brief_unit_id).strip()}::{str(arm).strip()}"


def _phase3_v2_hook_pair_key(brief_unit_id: str, arm: str, hook_id: str) -> str:
    return f"{_phase3_v2_pair_key(brief_unit_id, arm)}::{str(hook_id).strip()}"


def _phase3_v2_scene_pair_key(brief_unit_id: str, arm: str, hook_id: str) -> str:
    return f"{_phase3_v2_pair_key(brief_unit_id, arm)}::{str(hook_id).strip()}"


def _phase3_v2_load_decisions(brand_slug: str, branch_id: str, run_id: str) -> list[BriefUnitDecisionV1]:
    raw = _phase3_v2_read_json(_phase3_v2_decisions_path(brand_slug, branch_id, run_id), [])
    out: list[BriefUnitDecisionV1] = []
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            try:
                out.append(BriefUnitDecisionV1.model_validate(row))
            except Exception:
                continue
    return out


def _phase3_v2_save_decisions(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    decisions: list[BriefUnitDecisionV1],
) -> None:
    _phase3_v2_write_json(
        _phase3_v2_decisions_path(brand_slug, branch_id, run_id),
        [d.model_dump() for d in decisions],
    )


def _phase3_v2_load_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
) -> dict[str, list[Phase3V2ChatMessageV1]]:
    raw = _phase3_v2_read_json(_phase3_v2_chat_threads_path(brand_slug, branch_id, run_id), {})
    out: dict[str, list[Phase3V2ChatMessageV1]] = {}
    if not isinstance(raw, dict):
        return out
    for key, rows in raw.items():
        if not isinstance(rows, list):
            continue
        parsed_rows: list[Phase3V2ChatMessageV1] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                parsed_rows.append(Phase3V2ChatMessageV1.model_validate(row))
            except Exception:
                continue
        out[str(key)] = parsed_rows
    return out


def _phase3_v2_save_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    threads: dict[str, list[Phase3V2ChatMessageV1]],
) -> None:
    payload: dict[str, Any] = {}
    for key, rows in threads.items():
        payload[str(key)] = [r.model_dump() for r in rows]
    _phase3_v2_write_json(_phase3_v2_chat_threads_path(brand_slug, branch_id, run_id), payload)


def _phase3_v2_load_hook_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
) -> dict[str, list[Phase3V2ChatMessageV1]]:
    raw = _phase3_v2_read_json(_phase3_v2_hook_chat_threads_path(brand_slug, branch_id, run_id), {})
    out: dict[str, list[Phase3V2ChatMessageV1]] = {}
    if not isinstance(raw, dict):
        return out
    for key, rows in raw.items():
        if not isinstance(rows, list):
            continue
        parsed_rows: list[Phase3V2ChatMessageV1] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                parsed_rows.append(Phase3V2ChatMessageV1.model_validate(row))
            except Exception:
                continue
        out[str(key)] = parsed_rows
    return out


def _phase3_v2_save_hook_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    threads: dict[str, list[Phase3V2ChatMessageV1]],
) -> None:
    payload: dict[str, Any] = {}
    for key, rows in threads.items():
        payload[str(key)] = [r.model_dump() for r in rows]
    _phase3_v2_write_json(_phase3_v2_hook_chat_threads_path(brand_slug, branch_id, run_id), payload)


def _phase3_v2_load_scene_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
) -> dict[str, list[Phase3V2ChatMessageV1]]:
    raw = _phase3_v2_read_json(_phase3_v2_scene_chat_threads_path(brand_slug, branch_id, run_id), {})
    out: dict[str, list[Phase3V2ChatMessageV1]] = {}
    if not isinstance(raw, dict):
        return out
    for key, rows in raw.items():
        if not isinstance(rows, list):
            continue
        parsed_rows: list[Phase3V2ChatMessageV1] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                parsed_rows.append(Phase3V2ChatMessageV1.model_validate(row))
            except Exception:
                continue
        out[str(key)] = parsed_rows
    return out


def _phase3_v2_save_scene_chat_threads(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    threads: dict[str, list[Phase3V2ChatMessageV1]],
) -> None:
    payload: dict[str, Any] = {}
    for key, rows in threads.items():
        payload[str(key)] = [r.model_dump() for r in rows]
    _phase3_v2_write_json(_phase3_v2_scene_chat_threads_path(brand_slug, branch_id, run_id), payload)


def _phase3_v2_default_final_lock(run_id: str) -> Phase3V2FinalLockV1:
    return Phase3V2FinalLockV1(run_id=run_id, locked=False, locked_at="", locked_by_role="")


def _phase3_v2_load_final_lock(brand_slug: str, branch_id: str, run_id: str) -> Phase3V2FinalLockV1:
    raw = _phase3_v2_read_json(_phase3_v2_final_lock_path(brand_slug, branch_id, run_id), {})
    if isinstance(raw, dict):
        try:
            return Phase3V2FinalLockV1.model_validate(raw)
        except Exception:
            pass
    return _phase3_v2_default_final_lock(run_id)


def _phase3_v2_save_final_lock(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    lock_state: Phase3V2FinalLockV1,
) -> None:
    _phase3_v2_write_json(_phase3_v2_final_lock_path(brand_slug, branch_id, run_id), lock_state.model_dump())


def _phase3_v2_hook_stage_manifest_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "hook_stage_manifest.json"


def _phase3_v2_hook_candidates_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_hook_candidates.json"


def _phase3_v2_hook_gate_reports_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_hook_gate_reports.json"


def _phase3_v2_hook_bundles_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_hook_bundles.json"


def _phase3_v2_hook_scores_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_hook_scores.json"


def _phase3_v2_hook_selections_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "hook_selections.json"


def _phase3_v2_scene_handoff_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "scene_handoff_packet.json"


def _phase3_v2_scene_stage_manifest_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "scene_stage_manifest.json"


def _phase3_v2_scene_plans_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_scene_plans.json"


def _phase3_v2_scene_gate_reports_path(brand_slug: str, branch_id: str, run_id: str, arm: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / f"arm_{arm}_scene_gate_reports.json"


def _phase3_v2_production_handoff_path(brand_slug: str, branch_id: str, run_id: str) -> Path:
    return _phase3_v2_run_dir(brand_slug, branch_id, run_id) / "production_handoff_packet.json"


def _phase4_v1_runs_dir(brand_slug: str, branch_id: str) -> Path:
    root = _branch_output_dir(brand_slug, branch_id) / "phase4_video_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase4_v1_run_dir(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    run_dir = _phase4_v1_runs_dir(brand_slug, branch_id) / video_run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _phase4_v1_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _phase4_v1_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _phase4_v1_manifest_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "manifest.json"


def _phase4_v1_start_frame_brief_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "start_frame_brief.json"


def _phase4_v1_start_frame_brief_approval_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "start_frame_brief_approval.json"


def _phase4_v1_drive_validation_report_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "drive_validation_report.json"


def _phase4_v1_scene_line_mapping_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "scene_line_mapping.json"


def _phase4_v1_review_queue_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "review_queue.json"


def _phase4_v1_audit_pack_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "audit_pack.json"


def _phase4_v1_storyboard_assignment_report_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "storyboard_assignment_report.json"


def _phase4_v1_storyboard_saved_versions_path(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    return _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "storyboard_saved_versions.json"


def _phase4_v1_brand_broll_library_dir(brand_slug: str) -> Path:
    root = _brand_output_dir(brand_slug) / "phase4_broll_library"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase4_v1_brand_broll_library_manifest_path(brand_slug: str) -> Path:
    return _phase4_v1_brand_broll_library_dir(brand_slug) / "manifest.json"


def _phase4_v1_legacy_branch_broll_library_dir(brand_slug: str, branch_id: str) -> Path:
    return _branch_output_dir(brand_slug, branch_id) / "phase4_broll_library"


def _phase4_v1_legacy_branch_broll_library_manifest_path(brand_slug: str, branch_id: str) -> Path:
    return _phase4_v1_legacy_branch_broll_library_dir(brand_slug, branch_id) / "manifest.json"


def _phase4_v1_broll_unique_name(file_name: str, used_names: set[str]) -> str:
    original = str(file_name or "").strip()
    stem = Path(original).stem or "image"
    suffix = Path(original).suffix
    candidate = original or f"{stem}{suffix}"
    lower = candidate.lower()
    if lower not in used_names:
        return candidate
    idx = 2
    while True:
        candidate = f"{stem}__dup{idx}{suffix}"
        lower = candidate.lower()
        if lower not in used_names:
            return candidate
        idx += 1


def _phase4_v1_normalize_broll_mode_hint(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"aroll", "a_roll", "a"}:
        return "a_roll"
    if raw in {"broll", "b_roll", "b"}:
        return "b_roll"
    if raw in {"animation", "animation_broll", "animation_b_roll"}:
        return "animation_broll"
    if raw in {"unknown", ""}:
        return "unknown"
    return "unknown"


def _phase4_v1_normalize_broll_tags(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        tag = str(item or "").strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def _phase4_v1_normalize_broll_metadata(metadata: Any) -> dict[str, Any]:
    raw = metadata if isinstance(metadata, dict) else {}
    try:
        parsed = BrollCatalogItemMetadataV1.model_validate(raw).model_dump()
    except Exception:
        parsed = BrollCatalogItemMetadataV1().model_dump()
        for key, value in raw.items():
            if key not in parsed:
                parsed[key] = value

    mode_hint = _phase4_v1_normalize_broll_mode_hint(parsed.get("mode_hint"))
    ai_generated = bool(parsed.get("ai_generated"))
    library_item_type = str(parsed.get("library_item_type") or "").strip().lower()
    if library_item_type not in {"original_upload", "ai_generated"}:
        library_item_type = "ai_generated" if ai_generated else "original_upload"
    if library_item_type == "ai_generated":
        ai_generated = True
    tags = _phase4_v1_normalize_broll_tags(parsed.get("tags"))
    usage_count = max(0, int(parsed.get("usage_count") or 0))
    assignment_score = _phase4_v1_storyboard_score(parsed.get("assignment_score") or 0)
    indexing_status = str(parsed.get("indexing_status") or "").strip().lower()
    if indexing_status not in {"ready", "failed", "unindexed"}:
        analysis = parsed.get("analysis")
        indexing_status = "ready" if isinstance(analysis, dict) and bool(analysis) else "unindexed"

    parsed["mode_hint"] = mode_hint
    parsed["ai_generated"] = ai_generated
    parsed["library_item_type"] = library_item_type
    parsed["tags"] = tags
    parsed["usage_count"] = usage_count
    parsed["assignment_score"] = assignment_score
    parsed["assignment_status"] = str(parsed.get("assignment_status") or "").strip()
    parsed["last_used_at"] = str(parsed.get("last_used_at") or "").strip()
    parsed["auto_saved_at"] = str(parsed.get("auto_saved_at") or "").strip()
    parsed["indexing_status"] = indexing_status
    parsed["indexing_error"] = str(parsed.get("indexing_error") or "").strip()
    parsed["indexed_at"] = str(parsed.get("indexed_at") or "").strip()
    parsed["indexing_provider"] = str(parsed.get("indexing_provider") or "").strip()
    parsed["indexing_model_id"] = str(parsed.get("indexing_model_id") or "").strip()
    parsed["indexing_input_checksum"] = str(parsed.get("indexing_input_checksum") or "").strip().lower()
    if not _phase4_v1_is_sha256_hex(parsed["indexing_input_checksum"]):
        parsed["indexing_input_checksum"] = ""
    return parsed


def _phase4_v1_broll_display_type(metadata: dict[str, Any]) -> str:
    mode_hint = _phase4_v1_normalize_broll_mode_hint((metadata or {}).get("mode_hint"))
    ai_generated = bool((metadata or {}).get("ai_generated"))
    if ai_generated and mode_hint in {"b_roll", "animation_broll"}:
        return "ai modified broll"
    if mode_hint == "a_roll":
        return "a roll"
    if mode_hint == "animation_broll":
        return "animation broll"
    return "broll"


_PHASE4_BROLL_THUMB_WIDTH = 360
_PHASE4_BROLL_THUMB_HEIGHT = 640
_PHASE4_BROLL_THUMB_QUALITY = 82
_storyboard_image_bank_counters: Counter[str] = Counter()
_storyboard_thumb_warning_seen: set[str] = set()


def _storyboard_image_bank_counter_inc(counter_name: str, amount: int = 1) -> None:
    key = str(counter_name or "").strip()
    if not key:
        return
    _storyboard_image_bank_counters[key] += int(amount or 1)
    logger.info("%s=%d", key, int(_storyboard_image_bank_counters[key]))


def _phase4_v1_is_sha256_hex(value: Any) -> bool:
    raw = str(value or "").strip().lower()
    return bool(raw and re.fullmatch(r"[0-9a-f]{64}", raw))


def _phase4_v1_broll_filter_kind(metadata: dict[str, Any]) -> str:
    mode_hint = _phase4_v1_normalize_broll_mode_hint((metadata or {}).get("mode_hint"))
    ai_generated = bool((metadata or {}).get("ai_generated"))
    library_item_type = str((metadata or {}).get("library_item_type") or "").strip().lower()
    if mode_hint == "a_roll":
        return "a_roll"
    if ai_generated and mode_hint in {"b_roll", "animation_broll"}:
        return "ai_modified"
    if mode_hint == "animation_broll":
        return "animation_broll"
    if mode_hint in {"b_roll", "unknown"}:
        return "broll"
    if library_item_type == "original_upload":
        return "original"
    _storyboard_image_bank_counter_inc("image_bank_filter_kind_unknown")
    return "broll"


def _phase4_v1_broll_thumbs_dir(brand_slug: str, branch_id: str) -> Path:
    _ = branch_id
    root = _phase4_v1_brand_broll_library_dir(brand_slug) / ".thumbs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase4_v1_broll_thumbs_manifest_path(brand_slug: str, branch_id: str) -> Path:
    return _phase4_v1_broll_thumbs_dir(brand_slug, branch_id) / "manifest.json"


def _phase4_v1_load_broll_thumbs_manifest(brand_slug: str, branch_id: str) -> dict[str, Any]:
    raw = _phase4_v1_read_json(_phase4_v1_broll_thumbs_manifest_path(brand_slug, branch_id), {})
    checksums_raw = raw.get("checksums") if isinstance(raw, dict) else {}
    checksums: dict[str, dict[str, str]] = {}
    if isinstance(checksums_raw, dict):
        for checksum, entry in checksums_raw.items():
            key = str(checksum or "").strip().lower()
            if not _phase4_v1_is_sha256_hex(key):
                continue
            row = entry if isinstance(entry, dict) else {}
            thumb_name = Path(str(row.get("thumb_file_name") or "").strip()).name
            if not thumb_name:
                continue
            checksums[key] = {
                "thumb_file_name": thumb_name,
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
    return {"checksums": checksums}


def _phase4_v1_save_broll_thumbs_manifest(brand_slug: str, branch_id: str, payload: dict[str, Any]) -> None:
    checksums = payload.get("checksums") if isinstance(payload, dict) else {}
    normalized: dict[str, dict[str, str]] = {}
    if isinstance(checksums, dict):
        for checksum, entry in checksums.items():
            key = str(checksum or "").strip().lower()
            if not _phase4_v1_is_sha256_hex(key):
                continue
            row = entry if isinstance(entry, dict) else {}
            thumb_name = Path(str(row.get("thumb_file_name") or "").strip()).name
            if not thumb_name:
                continue
            normalized[key] = {
                "thumb_file_name": thumb_name,
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
    _phase4_v1_write_json(
        _phase4_v1_broll_thumbs_manifest_path(brand_slug, branch_id),
        {"checksums": normalized},
    )


def _phase4_v1_broll_checksum_for_row(
    *,
    row: dict[str, Any],
    file_path: Path,
) -> tuple[str, bool, dict[str, Any]]:
    metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
    existing = str(metadata.get("source_checksum_sha256") or "").strip().lower()
    if _phase4_v1_is_sha256_hex(existing):
        return existing, False, metadata
    if not file_path.exists() or not file_path.is_file():
        return "", False, metadata
    try:
        checksum = _phase4_v1_broll_checksum_sha256(file_path)
    except Exception:
        return "", False, metadata
    if not _phase4_v1_is_sha256_hex(checksum):
        return "", False, metadata
    metadata["source_checksum_sha256"] = checksum
    return checksum, True, metadata


def _phase4_v1_broll_thumb_file_name(checksum: str) -> str:
    return f"{checksum.lower()}.jpg"


def _phase4_v1_broll_render_thumbnail(source_path: Path, thumb_path: Path) -> bool:
    if Image is None:
        return False
    try:
        with Image.open(source_path) as image:
            image = image.convert("RGB")
            source_w, source_h = image.size
            if source_w <= 0 or source_h <= 0:
                return False
            target_ratio = _PHASE4_BROLL_THUMB_WIDTH / _PHASE4_BROLL_THUMB_HEIGHT
            source_ratio = source_w / source_h
            if source_ratio > target_ratio:
                crop_w = int(source_h * target_ratio)
                crop_h = source_h
                left = max(0, (source_w - crop_w) // 2)
                top = 0
            else:
                crop_w = source_w
                crop_h = int(source_w / target_ratio)
                left = 0
                top = max(0, (source_h - crop_h) // 2)
            crop_box = (left, top, left + crop_w, top + crop_h)
            resized = image.crop(crop_box).resize(
                (_PHASE4_BROLL_THUMB_WIDTH, _PHASE4_BROLL_THUMB_HEIGHT),
                resample=(Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS),
            )
            thumb_path.parent.mkdir(parents=True, exist_ok=True)
            resized.save(
                thumb_path,
                format="JPEG",
                quality=int(_PHASE4_BROLL_THUMB_QUALITY),
                optimize=True,
            )
        return True
    except (UnidentifiedImageError, OSError, ValueError):
        return False


def _phase4_v1_broll_resolve_thumbnail_url(
    *,
    brand_slug: str,
    branch_id: str,
    row: dict[str, Any],
    file_path: Path,
    thumbs_manifest: dict[str, Any],
) -> tuple[str, bool, str, bool, dict[str, Any]]:
    checksum, checksum_persisted, metadata = _phase4_v1_broll_checksum_for_row(
        row=row,
        file_path=file_path,
    )
    if not _phase4_v1_is_sha256_hex(checksum):
        _storyboard_image_bank_counter_inc("image_bank_thumb_fallback_original")
        return "", False, "", checksum_persisted, metadata

    checksums = thumbs_manifest.setdefault("checksums", {})
    if not isinstance(checksums, dict):
        checksums = {}
        thumbs_manifest["checksums"] = checksums

    thumb_file_name = _phase4_v1_broll_thumb_file_name(checksum)
    thumbs_dir = _phase4_v1_broll_thumbs_dir(brand_slug, branch_id)
    thumb_path = thumbs_dir / thumb_file_name
    manifest_changed = False
    entry = checksums.get(checksum)
    if (
        isinstance(entry, dict)
        and str(entry.get("thumb_file_name") or "").strip() == thumb_file_name
        and thumb_path.exists()
        and thumb_path.is_file()
    ):
        _storyboard_image_bank_counter_inc("image_bank_thumb_cache_hit")
        return _phase4_v1_storage_path_to_outputs_url(str(thumb_path)), False, checksum, checksum_persisted, metadata

    if thumb_path.exists() and thumb_path.is_file():
        checksums[checksum] = {"thumb_file_name": thumb_file_name, "updated_at": now_iso()}
        manifest_changed = True
        _storyboard_image_bank_counter_inc("image_bank_thumb_cache_hit")
        return _phase4_v1_storage_path_to_outputs_url(str(thumb_path)), manifest_changed, checksum, checksum_persisted, metadata

    if _phase4_v1_broll_render_thumbnail(file_path, thumb_path):
        checksums[checksum] = {"thumb_file_name": thumb_file_name, "updated_at": now_iso()}
        _storyboard_image_bank_counter_inc("image_bank_thumb_generated")
        manifest_changed = True
        return _phase4_v1_storage_path_to_outputs_url(str(thumb_path)), manifest_changed, checksum, checksum_persisted, metadata

    warn_key = f"{brand_slug}:{checksum}"
    if warn_key not in _storyboard_thumb_warning_seen:
        _storyboard_thumb_warning_seen.add(warn_key)
        logger.warning(
            "Image bank thumbnail fallback to original (brand=%s file=%s checksum=%s).",
            brand_slug,
            str(row.get("file_name") or ""),
            checksum,
        )
    _storyboard_image_bank_counter_inc("image_bank_thumb_fallback_original")
    return "", manifest_changed, checksum, checksum_persisted, metadata


def _phase4_v1_broll_remove_unused_thumbnails(
    *,
    brand_slug: str,
    branch_id: str,
    active_rows: list[dict[str, Any]],
) -> None:
    manifest = _phase4_v1_load_broll_thumbs_manifest(brand_slug, branch_id)
    checksums = manifest.get("checksums") if isinstance(manifest, dict) else {}
    if not isinstance(checksums, dict) or not checksums:
        return
    active_checksums = {
        str(((row.get("metadata") if isinstance(row, dict) else {}) or {}).get("source_checksum_sha256") or "").strip().lower()
        for row in active_rows
    }
    active_checksums = {value for value in active_checksums if _phase4_v1_is_sha256_hex(value)}
    thumbs_dir = _phase4_v1_broll_thumbs_dir(brand_slug, branch_id)
    changed = False
    for checksum in list(checksums.keys()):
        if checksum in active_checksums:
            continue
        row = checksums.pop(checksum, {})
        changed = True
        thumb_name = Path(str((row if isinstance(row, dict) else {}).get("thumb_file_name") or "").strip()).name
        if thumb_name:
            target = thumbs_dir / thumb_name
            if target.exists():
                try:
                    target.unlink(missing_ok=True)
                except TypeError:
                    if target.exists():
                        target.unlink()
    if changed:
        _phase4_v1_save_broll_thumbs_manifest(brand_slug, branch_id, manifest)


def _phase4_v1_normalize_broll_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for item in rows:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        lower = file_name.lower()
        if lower in seen_names:
            continue
        row["file_name"] = file_name
        row["size_bytes"] = int(row.get("size_bytes") or 0)
        row["added_at"] = str(row.get("added_at") or "").strip()
        row["metadata"] = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
        out.append(row)
        seen_names.add(lower)
    out.sort(key=lambda row: str(row.get("file_name") or "").lower())
    return out


def _phase4_v1_migrate_branch_broll_library_to_brand_scope(brand_slug: str, branch_id: str) -> None:
    brand_dir = _phase4_v1_brand_broll_library_dir(brand_slug)
    brand_manifest = _phase4_v1_brand_broll_library_manifest_path(brand_slug)
    legacy_dir = _phase4_v1_legacy_branch_broll_library_dir(brand_slug, branch_id)
    legacy_manifest = _phase4_v1_legacy_branch_broll_library_manifest_path(brand_slug, branch_id)
    if not legacy_dir.exists() or not legacy_dir.is_dir():
        return

    brand_rows = _phase4_v1_normalize_broll_rows(_phase4_v1_read_json(brand_manifest, []))
    row_index_by_lower = {
        str(row.get("file_name") or "").strip().lower(): idx
        for idx, row in enumerate(brand_rows)
        if str(row.get("file_name") or "").strip()
    }
    used_names = set(row_index_by_lower.keys())
    changed = False

    legacy_rows = _phase4_v1_normalize_broll_rows(_phase4_v1_read_json(legacy_manifest, []))
    for row in legacy_rows:
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        source_path = legacy_dir / file_name
        if not source_path.exists() or not source_path.is_file():
            continue
        lower = file_name.lower()
        if lower in row_index_by_lower:
            idx = row_index_by_lower[lower]
            existing_row = brand_rows[idx]
            merged = False
            for key, value in row.items():
                if key not in existing_row or existing_row.get(key) in ("", None, [], {}):
                    existing_row[key] = value
                    merged = True
            target_path = brand_dir / file_name
            if not target_path.exists():
                shutil.copy2(source_path, target_path)
                merged = True
            if merged:
                brand_rows[idx] = existing_row
                changed = True
            continue

        target_name = file_name
        if target_name.lower() in used_names:
            target_name = _phase4_v1_broll_unique_name(target_name, used_names)
        target_path = brand_dir / target_name
        if not target_path.exists():
            shutil.copy2(source_path, target_path)
        new_row = dict(row)
        new_row["file_name"] = target_name
        new_row["size_bytes"] = int(new_row.get("size_bytes") or target_path.stat().st_size)
        if not str(new_row.get("added_at") or "").strip():
            new_row["added_at"] = now_iso()
        new_row["metadata"] = _phase4_v1_normalize_broll_metadata(new_row.get("metadata"))
        brand_rows.append(new_row)
        used_names.add(target_name.lower())
        row_index_by_lower[target_name.lower()] = len(brand_rows) - 1
        changed = True

    for source_path in sorted(legacy_dir.iterdir(), key=lambda p: p.name.lower()):
        if (
            not source_path.is_file()
            or source_path.name == "manifest.json"
            or not _phase4_v1_storyboard_supported_image(source_path.name)
        ):
            continue
        lower = source_path.name.lower()
        if lower in row_index_by_lower:
            target_path = brand_dir / source_path.name
            if not target_path.exists():
                shutil.copy2(source_path, target_path)
                changed = True
            continue
        target_name = source_path.name
        if target_name.lower() in used_names:
            target_name = _phase4_v1_broll_unique_name(target_name, used_names)
        target_path = brand_dir / target_name
        if not target_path.exists():
            shutil.copy2(source_path, target_path)
        brand_rows.append(
            {
                "file_name": target_name,
                "size_bytes": int(source_path.stat().st_size),
                "added_at": now_iso(),
                "metadata": BrollCatalogItemMetadataV1().model_dump(),
            }
        )
        used_names.add(target_name.lower())
        row_index_by_lower[target_name.lower()] = len(brand_rows) - 1
        changed = True

    if changed:
        _phase4_v1_write_json(brand_manifest, _phase4_v1_normalize_broll_rows(brand_rows))


def _phase4_v1_broll_library_dir(brand_slug: str, branch_id: str) -> Path:
    _phase4_v1_migrate_branch_broll_library_to_brand_scope(brand_slug, branch_id)
    return _phase4_v1_brand_broll_library_dir(brand_slug)


def _phase4_v1_broll_library_manifest_path(brand_slug: str, branch_id: str) -> Path:
    _phase4_v1_migrate_branch_broll_library_to_brand_scope(brand_slug, branch_id)
    return _phase4_v1_brand_broll_library_manifest_path(brand_slug)


def _phase4_v1_load_broll_library(brand_slug: str, branch_id: str) -> list[dict[str, Any]]:
    raw = _phase4_v1_read_json(_phase4_v1_broll_library_manifest_path(brand_slug, branch_id), [])
    return _phase4_v1_normalize_broll_rows(raw)


def _phase4_v1_save_broll_library(brand_slug: str, branch_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    _ = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    normalized = _phase4_v1_normalize_broll_rows(rows)
    _phase4_v1_write_json(
        _phase4_v1_broll_library_manifest_path(brand_slug, branch_id),
        normalized,
    )
    return normalized


def _phase4_v1_clean_broll_library(
    brand_slug: str,
    branch_id: str,
) -> list[dict[str, Any]]:
    existing = _phase4_v1_load_broll_library(brand_slug, branch_id)
    if not existing:
        return []
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    cleaned: list[dict[str, Any]] = []
    missing_count = 0
    for row in existing:
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        if (library_dir / file_name).exists():
            cleaned.append(row)
        else:
            missing_count += 1
    if missing_count:
        cleaned = _phase4_v1_save_broll_library(brand_slug, branch_id, cleaned)
        _phase4_v1_broll_remove_unused_thumbnails(
            brand_slug=brand_slug,
            branch_id=branch_id,
            active_rows=cleaned,
        )
    return cleaned


def _phase4_v1_broll_backfill_state_path(brand_slug: str, branch_id: str) -> Path:
    _ = branch_id
    return _phase4_v1_brand_broll_library_dir(brand_slug) / "backfill_state.json"


def _phase4_v1_broll_build_row_index(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for idx, row in enumerate(rows):
        name = str(row.get("file_name") or "").strip()
        if not name:
            continue
        out[name.lower()] = idx
    return out


def _phase4_v1_broll_checksum_sha256(file_path: Path) -> str:
    return hashlib.sha256(file_path.read_bytes()).hexdigest().lower()


def _phase4_v1_broll_build_checksum_index(
    *,
    library_dir: Path,
    rows: list[dict[str, Any]],
) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in rows:
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        file_path = library_dir / file_name
        if not file_path.exists() or not file_path.is_file():
            continue
        try:
            checksum = _phase4_v1_broll_checksum_sha256(file_path)
        except Exception:
            continue
        if checksum and checksum not in index:
            index[checksum] = file_name
    return index


_PHASE4_V1_STORYBOARD_FIXED_PROMPT_PROVIDER = "openai"
_PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID = "gpt-5.2"
_PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_LABEL = "GPT 5.2"
_PHASE4_V1_STORYBOARD_INDEX_MAX_DIM = 1600
_PHASE4_V1_STORYBOARD_INDEX_MAX_BYTES = 4_500_000
_PHASE4_V1_STORYBOARD_INDEX_INITIAL_QUALITY = 86
_PHASE4_V1_STORYBOARD_INDEX_MIN_QUALITY = 52


def _phase4_v1_storyboard_normalize_selected_files(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        name = Path(str(item or "").strip()).name
        if not name:
            continue
        lower = name.lower()
        if lower in seen:
            continue
        seen.add(lower)
        out.append(name)
    return out


def _phase4_v1_storyboard_is_ready_library_row(row: dict[str, Any]) -> bool:
    metadata = _phase4_v1_normalize_broll_metadata((row if isinstance(row, dict) else {}).get("metadata"))
    if str(metadata.get("indexing_status") or "").strip().lower() != "ready":
        return False
    analysis = metadata.get("analysis")
    return isinstance(analysis, dict) and bool(analysis)


def _phase4_v1_storyboard_split_selectable_files(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    a_roll_map: dict[str, str] = {}
    b_roll_map: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
        if str(metadata.get("indexing_status") or "").strip().lower() != "ready":
            continue
        analysis = metadata.get("analysis")
        if not isinstance(analysis, dict) or not analysis:
            continue
        mode_hint = _phase4_v1_normalize_broll_mode_hint(metadata.get("mode_hint"))
        lower = file_name.lower()
        if mode_hint == "a_roll":
            a_roll_map[lower] = file_name
        else:
            b_roll_map[lower] = file_name
    return {
        "a_roll_map": a_roll_map,
        "b_roll_map": b_roll_map,
    }


def _phase4_v1_storyboard_resolve_source_selection(
    *,
    run_row: dict[str, Any],
    rows: list[dict[str, Any]],
    requested_a_roll: Any = None,
    requested_b_roll: Any = None,
) -> dict[str, Any]:
    metrics = run_row.get("metrics") if isinstance(run_row.get("metrics"), dict) else {}
    split = _phase4_v1_storyboard_split_selectable_files(rows)
    a_roll_map: dict[str, str] = split.get("a_roll_map") if isinstance(split.get("a_roll_map"), dict) else {}
    b_roll_map: dict[str, str] = split.get("b_roll_map") if isinstance(split.get("b_roll_map"), dict) else {}

    has_requested_a = bool(requested_a_roll is not None)
    has_requested_b = bool(requested_b_roll is not None)
    metric_has_a = "storyboard_selected_a_roll_files" in metrics
    metric_has_b = "storyboard_selected_b_roll_files" in metrics

    requested_a = _phase4_v1_storyboard_normalize_selected_files(
        requested_a_roll if has_requested_a else metrics.get("storyboard_selected_a_roll_files")
    )
    requested_b = _phase4_v1_storyboard_normalize_selected_files(
        requested_b_roll if has_requested_b else metrics.get("storyboard_selected_b_roll_files")
    )

    if has_requested_a or metric_has_a:
        selected_a_roll_files = [a_roll_map[name.lower()] for name in requested_a if name.lower() in a_roll_map]
    else:
        selected_a_roll_files = sorted(a_roll_map.values(), key=lambda name: name.lower())
    if has_requested_b or metric_has_b:
        selected_b_roll_files = [b_roll_map[name.lower()] for name in requested_b if name.lower() in b_roll_map]
    else:
        selected_b_roll_files = sorted(b_roll_map.values(), key=lambda name: name.lower())

    return {
        "selected_a_roll_files": selected_a_roll_files,
        "selected_b_roll_files": selected_b_roll_files,
        "selectable_a_roll_count": len(a_roll_map),
        "selectable_b_roll_count": len(b_roll_map),
        "updated_at": str(metrics.get("storyboard_selected_updated_at") or "").strip(),
    }


def _phase4_v1_storyboard_write_source_selection_metrics(
    *,
    video_run_id: str,
    selected_a_roll_files: list[str],
    selected_b_roll_files: list[str],
) -> dict[str, Any]:
    updated_at = now_iso()
    updates = {
        "storyboard_selected_a_roll_files": _phase4_v1_storyboard_normalize_selected_files(selected_a_roll_files),
        "storyboard_selected_b_roll_files": _phase4_v1_storyboard_normalize_selected_files(selected_b_roll_files),
        "storyboard_selected_updated_at": updated_at,
    }
    _phase4_v1_storyboard_update_metrics(video_run_id=video_run_id, updates=updates)
    return updates


def _phase4_v1_storyboard_build_ready_analysis_cache(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    cache: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
        if str(metadata.get("indexing_status") or "").strip().lower() != "ready":
            continue
        analysis = metadata.get("analysis")
        if not isinstance(analysis, dict) or not analysis:
            continue
        checksum = str(metadata.get("source_checksum_sha256") or "").strip().lower()
        if not _phase4_v1_is_sha256_hex(checksum):
            continue
        cache[checksum] = {
            "analysis": analysis,
            "indexing_provider": str(metadata.get("indexing_provider") or "").strip(),
            "indexing_model_id": str(metadata.get("indexing_model_id") or "").strip(),
            "indexing_input_checksum": str(metadata.get("indexing_input_checksum") or "").strip().lower(),
        }
    return cache


def _phase4_v1_storyboard_prepare_index_image(source_path: Path) -> tuple[Path, bool]:
    if Image is None:
        return source_path, False
    try:
        with Image.open(source_path) as opened:
            image = opened.convert("RGB")
            max_edge = max(image.size) if image.size else 0
            if max_edge > int(_PHASE4_V1_STORYBOARD_INDEX_MAX_DIM):
                ratio = float(_PHASE4_V1_STORYBOARD_INDEX_MAX_DIM) / float(max_edge)
                target_w = max(1, int(image.width * ratio))
                target_h = max(1, int(image.height * ratio))
                image = image.resize(
                    (target_w, target_h),
                    resample=(Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS),
                )

            quality = int(_PHASE4_V1_STORYBOARD_INDEX_INITIAL_QUALITY)
            while True:
                fd, tmp_name = tempfile.mkstemp(prefix="storyboard_index_", suffix=".jpg")
                os.close(fd)
                tmp_path = Path(tmp_name)
                image.save(tmp_path, format="JPEG", quality=quality, optimize=True)
                if tmp_path.stat().st_size <= int(_PHASE4_V1_STORYBOARD_INDEX_MAX_BYTES):
                    return tmp_path, True
                try:
                    tmp_path.unlink(missing_ok=True)
                except TypeError:
                    if tmp_path.exists():
                        tmp_path.unlink()
                if quality > int(_PHASE4_V1_STORYBOARD_INDEX_MIN_QUALITY):
                    quality = max(int(_PHASE4_V1_STORYBOARD_INDEX_MIN_QUALITY), quality - 8)
                    continue
                if max(image.size) <= 900:
                    fd, tmp_name = tempfile.mkstemp(prefix="storyboard_index_", suffix=".jpg")
                    os.close(fd)
                    tmp_path = Path(tmp_name)
                    image.save(
                        tmp_path,
                        format="JPEG",
                        quality=int(_PHASE4_V1_STORYBOARD_INDEX_MIN_QUALITY),
                        optimize=True,
                    )
                    return tmp_path, True
                image = image.resize(
                    (max(1, int(image.width * 0.86)), max(1, int(image.height * 0.86))),
                    resample=(Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS),
                )
                quality = int(_PHASE4_V1_STORYBOARD_INDEX_INITIAL_QUALITY)
    except Exception:
        return source_path, False


def _phase4_v1_storyboard_index_image_analysis(
    *,
    vision_provider: Any,
    source_path: Path,
    model_id: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared_path, should_cleanup = _phase4_v1_storyboard_prepare_index_image(source_path)
    prepared_checksum = ""
    try:
        prepared_bytes = prepared_path.read_bytes()
        prepared_checksum = hashlib.sha256(prepared_bytes).hexdigest().lower()
    except Exception:
        prepared_checksum = ""

    try:
        payload = vision_provider.analyze_image(
            image_path=prepared_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )
        analysis = payload if isinstance(payload, dict) else {}
        provider_name = str(analysis.get("provider") or "").strip()
        model_name = str(analysis.get("model_id") or analysis.get("model") or model_id).strip()
        return analysis, {
            "ok": bool(analysis),
            "error": "",
            "indexing_provider": provider_name,
            "indexing_model_id": model_name,
            "indexing_input_checksum": prepared_checksum if _phase4_v1_is_sha256_hex(prepared_checksum) else "",
        }
    except Exception as exc:
        return {}, {
            "ok": False,
            "error": str(exc),
            "indexing_provider": "",
            "indexing_model_id": str(model_id or "").strip(),
            "indexing_input_checksum": prepared_checksum if _phase4_v1_is_sha256_hex(prepared_checksum) else "",
        }
    finally:
        if should_cleanup:
            try:
                prepared_path.unlink(missing_ok=True)
            except TypeError:
                if prepared_path.exists():
                    prepared_path.unlink()


def _phase4_v1_storyboard_apply_indexing_metadata(
    *,
    metadata: dict[str, Any],
    analysis: dict[str, Any],
    indexing_ok: bool,
    indexing_error: str,
    indexing_provider: str,
    indexing_model_id: str,
    indexing_input_checksum: str,
) -> dict[str, Any]:
    next_meta = _phase4_v1_normalize_broll_metadata(metadata)
    if indexing_ok and isinstance(analysis, dict) and analysis:
        next_meta["analysis"] = analysis
        next_meta["indexing_status"] = "ready"
        next_meta["indexing_error"] = ""
    else:
        next_meta["indexing_status"] = "failed"
        next_meta["indexing_error"] = str(indexing_error or "").strip() or "Image indexing failed."
    next_meta["indexed_at"] = now_iso()
    next_meta["indexing_provider"] = str(indexing_provider or "").strip()
    next_meta["indexing_model_id"] = str(indexing_model_id or "").strip()
    checksum = str(indexing_input_checksum or "").strip().lower()
    next_meta["indexing_input_checksum"] = checksum if _phase4_v1_is_sha256_hex(checksum) else ""
    return _phase4_v1_normalize_broll_metadata(next_meta)


def _phase4_v1_broll_thumbnail_url(
    *,
    library_dir: Path,
    file_name: str,
) -> str:
    name = str(file_name or "").strip()
    if not name:
        return ""
    file_path = library_dir / name
    if not file_path.exists() or not file_path.is_file():
        return ""
    return _phase4_v1_storage_path_to_outputs_url(str(file_path))


def _phase4_v1_broll_enrich_rows_for_response(
    *,
    brand_slug: str,
    branch_id: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    thumb_manifest = _phase4_v1_load_broll_thumbs_manifest(brand_slug, branch_id)
    manifest_changed = False
    row_checksum_persisted = False
    normalized_rows = _phase4_v1_normalize_broll_rows(rows)
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(normalized_rows):
        normalized = dict(row)
        metadata = _phase4_v1_normalize_broll_metadata(normalized.get("metadata"))
        file_name = str(normalized.get("file_name") or "").strip()
        file_path = library_dir / file_name
        thumbnail_url = _phase4_v1_broll_thumbnail_url(
            library_dir=library_dir,
            file_name=file_name,
        )
        original_url = thumbnail_url
        if file_path.exists() and file_path.is_file():
            (
                generated_thumb_url,
                generated_manifest_changed,
                _checksum,
                checksum_persisted,
                enriched_metadata,
            ) = _phase4_v1_broll_resolve_thumbnail_url(
                brand_slug=brand_slug,
                branch_id=branch_id,
                row=normalized,
                file_path=file_path,
                thumbs_manifest=thumb_manifest,
            )
            metadata = enriched_metadata
            if generated_thumb_url:
                thumbnail_url = generated_thumb_url
            manifest_changed = manifest_changed or generated_manifest_changed
            if checksum_persisted:
                row_checksum_persisted = True
                updated_row = dict(normalized_rows[idx])
                updated_row["metadata"] = metadata
                normalized_rows[idx] = updated_row

        normalized["metadata"] = metadata
        normalized["display_type"] = _phase4_v1_broll_display_type(metadata)
        normalized["filter_kind"] = _phase4_v1_broll_filter_kind(metadata)
        normalized["thumbnail_url"] = thumbnail_url
        normalized["original_url"] = original_url
        out.append(normalized)
    if row_checksum_persisted:
        _phase4_v1_save_broll_library(brand_slug, branch_id, normalized_rows)
    if manifest_changed:
        _phase4_v1_save_broll_thumbs_manifest(brand_slug, branch_id, thumb_manifest)
    return out


def _phase4_v1_broll_upsert_from_source(
    *,
    brand_slug: str,
    branch_id: str,
    source_path: Path,
    preferred_file_name: str,
    metadata_updates: dict[str, Any] | None = None,
    rows: list[dict[str, Any]],
    row_index: dict[str, int],
    checksum_index: dict[str, str],
    increment_usage_count: bool = True,
) -> dict[str, Any]:
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    source_name = Path(str(preferred_file_name or source_path.name)).name
    if not source_name:
        source_name = source_path.name
    if not _phase4_v1_storyboard_supported_image(source_name):
        raise ValueError("Unsupported image extension for B-roll library upsert.")
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"Source image not found: {source_path}")

    source_bytes = source_path.read_bytes()
    source_checksum = hashlib.sha256(source_bytes).hexdigest().lower()
    existing_name = checksum_index.get(source_checksum, "")
    now_value = now_iso()
    updates = dict(metadata_updates or {})
    changed = False
    dedup_hit = False

    if existing_name:
        idx = row_index.get(existing_name.lower(), -1)
        if idx >= 0:
            current_row = dict(rows[idx])
            current_meta = _phase4_v1_normalize_broll_metadata(current_row.get("metadata"))
            merged_meta = dict(current_meta)
            for key, value in updates.items():
                if key in {"usage_count", "last_used_at", "auto_saved_at", "source_checksum_sha256"}:
                    continue
                if key == "tags":
                    merged_meta["tags"] = _phase4_v1_normalize_broll_tags(value)
                    continue
                text_like = isinstance(value, str)
                if value is None or value == "" or value == [] or value == {}:
                    continue
                if text_like and not str(value).strip():
                    continue
                merged_meta[key] = value
            if increment_usage_count:
                merged_meta["usage_count"] = max(0, int(merged_meta.get("usage_count") or 0)) + 1
                merged_meta["last_used_at"] = now_value
            merged_meta["auto_saved_at"] = str(merged_meta.get("auto_saved_at") or now_value)
            merged_meta["source_checksum_sha256"] = source_checksum
            current_row["metadata"] = _phase4_v1_normalize_broll_metadata(merged_meta)
            rows[idx] = current_row
            dedup_hit = True
            changed = True
            return {
                "rows": rows,
                "row_index": row_index,
                "checksum_index": checksum_index,
                "changed": changed,
                "dedup_hit": dedup_hit,
                "file_name": existing_name,
                "row": current_row,
                "checksum": source_checksum,
            }

    used_names = set(row_index.keys())
    target_name = source_name
    if target_name.lower() in used_names:
        target_name = _phase4_v1_broll_unique_name(target_name, used_names)
    target_path = library_dir / target_name
    target_path.write_bytes(source_bytes)

    payload_meta = _phase4_v1_normalize_broll_metadata(updates)
    if increment_usage_count:
        payload_meta["usage_count"] = max(1, int(payload_meta.get("usage_count") or 0))
        payload_meta["last_used_at"] = now_value
    payload_meta["auto_saved_at"] = str(payload_meta.get("auto_saved_at") or now_value)
    payload_meta["source_checksum_sha256"] = source_checksum

    row = {
        "file_name": target_name,
        "size_bytes": len(source_bytes),
        "added_at": now_value,
        "metadata": payload_meta,
    }
    rows.append(row)
    rows[:] = _phase4_v1_normalize_broll_rows(rows)
    row_index.clear()
    row_index.update(_phase4_v1_broll_build_row_index(rows))
    checksum_index[source_checksum] = target_name
    changed = True
    final_idx = row_index.get(target_name.lower(), -1)
    final_row = rows[final_idx] if final_idx >= 0 else row
    return {
        "rows": rows,
        "row_index": row_index,
        "checksum_index": checksum_index,
        "changed": changed,
        "dedup_hit": dedup_hit,
        "file_name": target_name,
        "row": final_row,
        "checksum": source_checksum,
    }


def _phase4_v1_broll_sanitize_file_name(file_name: str) -> str:
    return Path(str(file_name or "").strip()).name


def _phase4_v1_broll_resolve_rename_target(file_name: str, new_file_name: str) -> str:
    current = _phase4_v1_broll_sanitize_file_name(file_name)
    target = _phase4_v1_broll_sanitize_file_name(new_file_name)
    if not current:
        raise ValueError("Current file name is required.")
    if not target:
        raise ValueError("New file name is required.")
    current_suffix = Path(current).suffix
    target_path = Path(target)
    if not target_path.suffix and current_suffix:
        target_path = Path(f"{target_path.name}{current_suffix}")
    resolved = target_path.name
    if not resolved:
        raise ValueError("New file name is required.")
    if not _phase4_v1_storyboard_supported_image(resolved):
        raise ValueError("Renamed file must be PNG/JPG/JPEG/WEBP.")
    return resolved


def _phase4_v1_assets_root(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    root = _phase4_v1_run_dir(brand_slug, branch_id, video_run_id) / "assets"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase4_v1_local_uploads_root(brand_slug: str, branch_id: str, video_run_id: str) -> Path:
    root = _phase4_v1_assets_root(brand_slug, branch_id, video_run_id) / "local_folder_uploads"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _phase4_v1_storage_path_to_outputs_url(storage_path: str) -> str:
    raw = str(storage_path or "").strip()
    if not raw:
        return ""
    try:
        path = Path(raw).expanduser().resolve()
        rel = path.relative_to(config.OUTPUT_DIR.resolve())
        rel_url = quote(rel.as_posix(), safe="/._-")
        return f"/outputs/{rel_url}"
    except Exception:
        marker = "/outputs/"
        idx = raw.find(marker)
        if idx < 0:
            return ""
        rel = raw[idx + len(marker):].lstrip("/")
        if not rel:
            return ""
        return f"/outputs/{quote(rel, safe='/._-')}"


def _phase4_v1_load_manifest(brand_slug: str, branch_id: str, video_run_id: str) -> dict[str, Any]:
    return _phase4_v1_read_json(_phase4_v1_manifest_path(brand_slug, branch_id, video_run_id), {})


def _phase4_v1_save_manifest(brand_slug: str, branch_id: str, video_run_id: str, payload: dict[str, Any]) -> None:
    _phase4_v1_write_json(_phase4_v1_manifest_path(brand_slug, branch_id, video_run_id), payload)


def _phase4_v1_voice_presets() -> list[VoicePresetV1]:
    out: list[VoicePresetV1] = []
    for row in config.PHASE4_V1_VOICE_PRESETS:
        if not isinstance(row, dict):
            continue
        try:
            out.append(VoicePresetV1.model_validate(row))
        except Exception:
            continue
    return out


def _phase4_v1_voice_preset_by_id(voice_preset_id: str) -> VoicePresetV1 | None:
    target = str(voice_preset_id or "").strip()
    for preset in _phase4_v1_voice_presets():
        if preset.voice_preset_id == target:
            return preset
    return None


def _phase4_v1_default_voice_preset_id() -> str:
    presets = _phase4_v1_voice_presets()
    if presets:
        first = str(presets[0].voice_preset_id or "").strip()
        if first:
            return first
    return "calm_female_en_us_v1"


def _phase4_v1_storyboard_task_key(brand_slug: str, branch_id: str, video_run_id: str) -> str:
    return f"{_phase4_v1_run_key(brand_slug, branch_id, video_run_id)}:storyboard_assign"


def _phase4_v1_refresh_review_queue_artifact(brand_slug: str, branch_id: str, video_run_id: str) -> list[dict[str, Any]]:
    clips = list_video_clips(video_run_id)
    queue = build_review_queue(clips)
    _phase4_v1_write_json(_phase4_v1_review_queue_path(brand_slug, branch_id, video_run_id), queue)
    return queue


def _phase4_v1_get_current_revision_row(clip_row: dict[str, Any]) -> dict[str, Any] | None:
    clip_id = str(clip_row.get("clip_id") or "").strip()
    revision_index = int(clip_row.get("current_revision_index") or 1)
    if not clip_id:
        return None
    row = get_video_clip_revision_by_index(clip_id, revision_index)
    if row:
        return row
    return get_latest_video_clip_revision(clip_id)

def _phase3_v2_default_hook_stage_manifest(run_id: str) -> HookStageManifestV1:
    return HookStageManifestV1(
        run_id=run_id,
        hook_run_id="",
        status="idle",
        created_at="",
        started_at="",
        completed_at="",
        error="",
        eligible_count=0,
        processed_count=0,
        failed_count=0,
        skipped_count=0,
        candidate_target_per_unit=int(config.PHASE3_V2_HOOK_CANDIDATES_PER_UNIT),
        final_variants_per_unit=int(config.PHASE3_V2_HOOK_FINAL_VARIANTS_PER_UNIT),
        max_parallel=int(config.PHASE3_V2_HOOK_MAX_PARALLEL),
        max_repair_rounds=int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS),
        model_registry={},
        metrics={},
    )


def _phase3_v2_default_scene_stage_manifest(run_id: str) -> SceneStageManifestV1:
    return SceneStageManifestV1(
        run_id=run_id,
        scene_run_id="",
        status="idle",
        created_at="",
        started_at="",
        completed_at="",
        error="",
        eligible_count=0,
        processed_count=0,
        failed_count=0,
        skipped_count=0,
        stale_count=0,
        max_parallel=int(config.PHASE3_V2_SCENE_MAX_PARALLEL),
        max_repair_rounds=int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
        max_consecutive_mode=int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
        min_a_roll_lines=int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
        model_registry={},
        metrics={},
    )


def _phase3_v2_load_hook_stage_manifest(brand_slug: str, branch_id: str, run_id: str) -> HookStageManifestV1:
    raw = _phase3_v2_read_json(_phase3_v2_hook_stage_manifest_path(brand_slug, branch_id, run_id), {})
    if isinstance(raw, dict) and raw:
        try:
            return HookStageManifestV1.model_validate(raw)
        except Exception:
            pass
    return _phase3_v2_default_hook_stage_manifest(run_id)


def _phase3_v2_save_hook_stage_manifest(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    manifest: HookStageManifestV1,
) -> None:
    _phase3_v2_write_json(
        _phase3_v2_hook_stage_manifest_path(brand_slug, branch_id, run_id),
        manifest.model_dump(),
    )


def _phase3_v2_load_scene_stage_manifest(brand_slug: str, branch_id: str, run_id: str) -> SceneStageManifestV1:
    raw = _phase3_v2_read_json(_phase3_v2_scene_stage_manifest_path(brand_slug, branch_id, run_id), {})
    if isinstance(raw, dict) and raw:
        try:
            return SceneStageManifestV1.model_validate(raw)
        except Exception:
            pass
    return _phase3_v2_default_scene_stage_manifest(run_id)


def _phase3_v2_save_scene_stage_manifest(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    manifest: SceneStageManifestV1,
) -> None:
    _phase3_v2_write_json(
        _phase3_v2_scene_stage_manifest_path(brand_slug, branch_id, run_id),
        manifest.model_dump(),
    )


def _phase3_v2_load_scene_plans_by_arm(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arms: list[str],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for arm in arms:
        rows = _phase3_v2_read_json(_phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, arm), [])
        normalized_rows: list[dict[str, Any]] = []
        for row in (rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            brief_unit_id = str(row.get("brief_unit_id") or "").strip()
            hook_id = str(row.get("hook_id") or "").strip()
            if not brief_unit_id or not hook_id:
                normalized_rows.append(dict(row))
                continue
            canonical_lines: list[SceneLinePlanV1] = []
            for line_row in (row.get("lines", []) if isinstance(row.get("lines"), list) else []):
                if not isinstance(line_row, dict):
                    continue
                try:
                    payload = _phase3_v2_scene_payload_from_row_dict(line_row)
                    canonical_lines.append(
                        to_canonical_scene_line(
                            brief_unit_id=brief_unit_id,
                            hook_id=hook_id,
                            row=payload,
                        )
                    )
                except Exception:
                    continue
            canonical_lines = _phase3_v2_enforce_no_adjacent_a_roll(canonical_lines)
            total_duration, a_roll_count, b_roll_count, max_consecutive = _phase3_v2_scene_sequence_metrics(canonical_lines)
            normalized_rows.append(
                {
                    **row,
                    "lines": [line.model_dump() for line in canonical_lines],
                    "total_duration_seconds": total_duration,
                    "a_roll_line_count": a_roll_count,
                    "b_roll_line_count": b_roll_count,
                    "max_consecutive_mode": max_consecutive,
                }
            )
        out[arm] = normalized_rows
    return out


def _phase3_v2_load_scene_gate_reports_by_arm(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arms: list[str],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for arm in arms:
        rows = _phase3_v2_read_json(_phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, arm), [])
        out[arm] = rows if isinstance(rows, list) else []
    return out


def _phase3_v2_load_production_handoff_packet(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    *,
    scene_run_id: str = "",
) -> ProductionHandoffPacketV1:
    raw = _phase3_v2_read_json(_phase3_v2_production_handoff_path(brand_slug, branch_id, run_id), {})
    if isinstance(raw, dict) and raw:
        try:
            return ProductionHandoffPacketV1.model_validate(raw)
        except Exception:
            pass
    return ProductionHandoffPacketV1(
        run_id=run_id,
        scene_run_id=scene_run_id,
        ready=False,
        ready_count=0,
        total_required=0,
        generated_at="",
        items=[],
        metrics={},
    )


def _phase3_v2_load_hook_selections(brand_slug: str, branch_id: str, run_id: str) -> list[HookSelectionV1]:
    raw = _phase3_v2_read_json(_phase3_v2_hook_selections_path(brand_slug, branch_id, run_id), [])
    out: list[HookSelectionV1] = []
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            try:
                out.append(HookSelectionV1.model_validate(row))
            except Exception:
                continue
    return out


def _phase3_v2_save_hook_selections(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    selections: list[HookSelectionV1],
) -> None:
    _phase3_v2_write_json(
        _phase3_v2_hook_selections_path(brand_slug, branch_id, run_id),
        [row.model_dump() for row in selections],
    )


def _phase3_v2_is_manual_skip_decision(value: str) -> bool:
    decision = str(value or "").strip().lower()
    return decision in {"revise", "reject"}


def _phase3_v2_is_auto_skipped_status(status: str) -> bool:
    value = str(status or "").strip().lower()
    return value in {"blocked", "error", "missing"}


def _phase3_v2_decision_index(decisions: list[BriefUnitDecisionV1]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in decisions:
        out[_phase3_v2_pair_key(row.brief_unit_id, row.arm)] = str(row.decision or "").strip().lower()
    return out


def _phase3_v2_mark_hook_selections_stale_for_unit(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    reason: str,
) -> int:
    selections = _phase3_v2_load_hook_selections(brand_slug, branch_id, run_id)
    if not selections:
        return 0
    changed = 0
    for row in selections:
        if row.brief_unit_id != brief_unit_id or row.arm != arm:
            continue
        row.stale = True
        row.stale_reason = str(reason or "script_updated")
        row.updated_at = datetime.now().isoformat()
        changed += 1
    if changed:
        _phase3_v2_save_hook_selections(brand_slug, branch_id, run_id, selections)
    return changed


def _phase3_v2_mark_scene_plans_stale_for_unit(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    reason: str,
) -> int:
    changed = 0
    arms = [str(arm or "").strip()] if str(arm or "").strip() else []
    if not arms:
        detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
        arms = _phase3_v2_get_run_arms(detail or {}) if isinstance(detail, dict) else []
    for arm_name in arms:
        path = _phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, arm_name)
        rows = _phase3_v2_read_json(path, [])
        if not isinstance(rows, list):
            continue
        updated = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("brief_unit_id") or "").strip() != brief_unit_id:
                continue
            row["stale"] = True
            row["stale_reason"] = str(reason or "stale")
            row["status"] = "stale"
            updated = True
            changed += 1
        if updated:
            _phase3_v2_write_json(path, rows)
    return changed


def _phase3_v2_mark_scene_plans_stale_for_hook(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    hook_id: str,
    reason: str,
) -> int:
    arm_name = str(arm or "").strip()
    if not arm_name:
        return 0
    path = _phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, arm_name)
    rows = _phase3_v2_read_json(path, [])
    if not isinstance(rows, list):
        return 0
    changed = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("brief_unit_id") or "").strip() != brief_unit_id:
            continue
        if str(row.get("hook_id") or "").strip() != str(hook_id or "").strip():
            continue
        row["stale"] = True
        row["stale_reason"] = str(reason or "stale")
        row["status"] = "stale"
        changed += 1
    if changed:
        _phase3_v2_write_json(path, rows)
    return changed


def _phase3_v2_build_hook_eligibility(detail: dict[str, Any]) -> dict[str, Any]:
    units = detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else []
    drafts_by_arm = detail.get("drafts_by_arm", {}) if isinstance(detail.get("drafts_by_arm"), dict) else {}
    decisions: list[BriefUnitDecisionV1] = []
    for row in (detail.get("decisions", []) if isinstance(detail.get("decisions"), list) else []):
        if not isinstance(row, dict):
            continue
        try:
            decisions.append(BriefUnitDecisionV1.model_validate(row))
        except Exception:
            continue
    decision_map = _phase3_v2_decision_index(decisions)
    evidence_by_unit = {
        str(row.get("brief_unit_id") or "").strip(): row
        for row in (detail.get("evidence_packs", []) if isinstance(detail.get("evidence_packs"), list) else [])
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }
    run_arms = _phase3_v2_get_run_arms(detail)

    eligible: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for unit in units:
        if not isinstance(unit, dict):
            continue
        unit_id = str(unit.get("brief_unit_id") or "").strip()
        if not unit_id:
            continue
        evidence_pack = evidence_by_unit.get(unit_id)
        coverage = evidence_pack.get("coverage_report", {}) if isinstance(evidence_pack, dict) else {}
        evidence_blocked = bool(isinstance(coverage, dict) and coverage.get("blocked_evidence_insufficient"))
        for arm in run_arms:
            draft_rows = drafts_by_arm.get(arm, [])
            draft = next(
                (
                    row for row in (draft_rows if isinstance(draft_rows, list) else [])
                    if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == unit_id
                ),
                None,
            )
            status = str((draft or {}).get("status") or "missing")
            decision = decision_map.get(_phase3_v2_pair_key(unit_id, arm), "")
            reason = ""
            if evidence_blocked:
                reason = "blocked_evidence_insufficient"
            elif _phase3_v2_is_auto_skipped_status(status):
                reason = f"script_{status}"
            elif _phase3_v2_is_manual_skip_decision(decision):
                reason = f"manual_skip_{decision}"

            row = {
                "brief_unit_id": unit_id,
                "arm": arm,
                "awareness_level": str(unit.get("awareness_level") or "").strip(),
                "emotion_key": str(unit.get("emotion_key") or "").strip(),
                "emotion_label": str(unit.get("emotion_label") or "").strip(),
                "reason": reason,
            }
            if reason:
                skipped.append(row)
            else:
                eligible.append(row)

    return {
        "eligible": eligible,
        "skipped": skipped,
        "eligible_count": len(eligible),
        "skipped_count": len(skipped),
    }


def _phase3_v2_load_hook_bundles_by_arm(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arms: list[str],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for arm in arms:
        rows = _phase3_v2_read_json(
            _phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, arm),
            [],
        )
        out[arm] = rows if isinstance(rows, list) else []
    return out


def _phase3_v2_compute_hook_selection_progress(
    *,
    hook_eligibility: dict[str, Any],
    selections: list[HookSelectionV1],
) -> dict[str, Any]:
    eligible_pairs = {
        _phase3_v2_pair_key(str(row.get("brief_unit_id") or ""), str(row.get("arm") or ""))
        for row in (hook_eligibility.get("eligible", []) if isinstance(hook_eligibility.get("eligible"), list) else [])
        if str(row.get("brief_unit_id") or "").strip() and str(row.get("arm") or "").strip()
    }
    selected_pairs = 0
    skipped_pairs = 0
    stale_pairs = 0
    for row in selections:
        key = _phase3_v2_pair_key(row.brief_unit_id, row.arm)
        if key not in eligible_pairs:
            continue
        if row.skip:
            skipped_pairs += 1
            continue
        if row.stale:
            stale_pairs += 1
            continue
        selected_ids = [str(v).strip() for v in (row.selected_hook_ids or []) if str(v or "").strip()]
        if not selected_ids and str(row.selected_hook_id or "").strip():
            selected_ids = [str(row.selected_hook_id).strip()]
        if selected_ids:
            selected_pairs += 1

    total = len(eligible_pairs)
    ready = total > 0 and (selected_pairs + skipped_pairs) == total and stale_pairs == 0
    pending = max(0, total - (selected_pairs + skipped_pairs + stale_pairs))
    return {
        "total_required": total,
        "selected": selected_pairs,
        "skipped": skipped_pairs,
        "stale": stale_pairs,
        "pending": pending,
        "ready": ready,
    }


def _phase3_v2_required_scene_units(scene_handoff_packet: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    items = scene_handoff_packet.get("items", []) if isinstance(scene_handoff_packet, dict) else []
    for row in (items if isinstance(items, list) else []):
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip().lower() != "ready":
            continue
        brief_unit_id = str(row.get("brief_unit_id") or "").strip()
        arm = str(row.get("arm") or "").strip()
        if not brief_unit_id or not arm:
            continue
        selected_ids = [
            str(v).strip()
            for v in (row.get("selected_hook_ids", []) if isinstance(row.get("selected_hook_ids"), list) else [])
            if str(v or "").strip()
        ]
        if not selected_ids:
            legacy = str(row.get("selected_hook_id") or "").strip()
            if legacy:
                selected_ids = [legacy]
        primary_hook_id = selected_ids[0] if selected_ids else ""
        out.append(
            {
                "brief_unit_id": brief_unit_id,
                "arm": arm,
                "hook_id": primary_hook_id,
                "selected_hook_ids": selected_ids,
                "selected_hook_id": primary_hook_id,
            }
        )
    return out


def _phase3_v2_build_production_handoff_from_scene_state(
    *,
    run_id: str,
    scene_run_id: str,
    scene_handoff_packet: dict[str, Any],
    scene_plans_by_arm: dict[str, list[dict[str, Any]]],
    scene_gate_reports_by_arm: dict[str, list[dict[str, Any]]],
) -> ProductionHandoffPacketV1:
    required_units = _phase3_v2_required_scene_units(scene_handoff_packet)
    plan_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    gate_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    plan_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    gate_by_pair: dict[tuple[str, str], dict[str, Any]] = {}

    for arm, rows in (scene_plans_by_arm or {}).items():
        for row in (rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("brief_unit_id") or "").strip(),
                str(arm or "").strip(),
                str(row.get("hook_id") or "").strip(),
            )
            if all(key):
                plan_by_key[key] = row
            pair_key = (str(row.get("brief_unit_id") or "").strip(), str(arm or "").strip())
            if all(pair_key):
                plan_by_pair[pair_key] = row
    for arm, rows in (scene_gate_reports_by_arm or {}).items():
        for row in (rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("brief_unit_id") or "").strip(),
                str(arm or "").strip(),
                str(row.get("hook_id") or "").strip(),
            )
            if all(key):
                gate_by_key[key] = row
            pair_key = (str(row.get("brief_unit_id") or "").strip(), str(arm or "").strip())
            if all(pair_key):
                gate_by_pair[pair_key] = row

    units: list[ProductionHandoffUnitV1] = []
    ready_count = 0
    for row in required_units:
        brief_unit_id = row["brief_unit_id"]
        arm = row["arm"]
        hook_id = str(row.get("hook_id") or "").strip()
        selected_hook_ids = [
            str(v).strip()
            for v in (row.get("selected_hook_ids", []) if isinstance(row.get("selected_hook_ids"), list) else [])
            if str(v or "").strip()
        ]
        if not selected_hook_ids and hook_id:
            selected_hook_ids = [hook_id]
        key = (brief_unit_id, arm, hook_id)
        plan_raw = plan_by_key.get(key) or plan_by_pair.get((brief_unit_id, arm))
        gate_raw = gate_by_key.get(key) or gate_by_pair.get((brief_unit_id, arm))
        resolved_hook_id = str((plan_raw or {}).get("hook_id") or hook_id).strip()
        if resolved_hook_id and resolved_hook_id not in selected_hook_ids:
            selected_hook_ids = [resolved_hook_id, *selected_hook_ids]
        scene_unit_id = f"su_{brief_unit_id}_{resolved_hook_id or hook_id}"
        if not isinstance(plan_raw, dict):
            units.append(
                ProductionHandoffUnitV1(
                    scene_unit_id=scene_unit_id,
                    run_id=run_id,
                    brief_unit_id=brief_unit_id,
                    arm=arm,  # validated upstream
                    hook_id=resolved_hook_id,
                    selected_hook_ids=selected_hook_ids,
                    selected_hook_id=selected_hook_ids[0] if selected_hook_ids else "",
                    status="missing",
                )
            )
            continue

        stale = bool(plan_raw.get("stale"))
        gate_pass = bool((gate_raw or {}).get("overall_pass"))
        status = "ready" if gate_pass and not stale else ("stale" if stale else "failed")
        if status == "ready":
            ready_count += 1
        lines: list[SceneLinePlanV1] = []
        for line_row in (plan_raw.get("lines", []) if isinstance(plan_raw.get("lines"), list) else []):
            if not isinstance(line_row, dict):
                continue
            try:
                lines.append(SceneLinePlanV1.model_validate(line_row))
            except Exception:
                continue

        gate_report = None
        if isinstance(gate_raw, dict):
            try:
                gate_report = SceneGateReportV1.model_validate(gate_raw)
            except Exception:
                gate_report = None

        units.append(
            ProductionHandoffUnitV1(
                scene_unit_id=scene_unit_id,
                scene_plan_id=str(plan_raw.get("scene_plan_id") or ""),
                run_id=run_id,
                brief_unit_id=brief_unit_id,
                arm=arm,  # validated upstream
                hook_id=resolved_hook_id,
                selected_hook_ids=selected_hook_ids,
                selected_hook_id=selected_hook_ids[0] if selected_hook_ids else "",
                status=status,  # validated against literal values above
                stale=stale,
                stale_reason=str(plan_raw.get("stale_reason") or ""),
                lines=lines,
                gate_report=gate_report,
            )
        )

    total_required = len(required_units)
    return ProductionHandoffPacketV1(
        run_id=run_id,
        scene_run_id=scene_run_id,
        ready=(total_required > 0 and ready_count == total_required),
        ready_count=ready_count,
        total_required=total_required,
        generated_at=datetime.now().isoformat(),
        items=units,
        metrics={
            "failed": sum(1 for unit in units if unit.status == "failed"),
            "stale": sum(1 for unit in units if unit.status == "stale"),
            "missing": sum(1 for unit in units if unit.status == "missing"),
        },
    )


def _phase3_v2_compute_scene_progress(
    *,
    scene_handoff_packet: dict[str, Any],
    scene_plans_by_arm: dict[str, list[dict[str, Any]]],
    scene_gate_reports_by_arm: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    packet = _phase3_v2_build_production_handoff_from_scene_state(
        run_id=str(scene_handoff_packet.get("run_id") or ""),
        scene_run_id=str(scene_handoff_packet.get("scene_run_id") or ""),
        scene_handoff_packet=scene_handoff_packet,
        scene_plans_by_arm=scene_plans_by_arm,
        scene_gate_reports_by_arm=scene_gate_reports_by_arm,
    )
    generated = sum(1 for row in packet.items if row.status in {"ready", "failed", "stale"})
    failed = sum(1 for row in packet.items if row.status == "failed")
    stale = sum(1 for row in packet.items if row.status == "stale")
    missing = sum(1 for row in packet.items if row.status == "missing")
    return {
        "total_required": int(packet.total_required),
        "generated": int(generated),
        "ready": int(packet.ready_count),
        "failed": int(failed),
        "stale": int(stale),
        "missing": int(missing),
        "ready_for_handoff": bool(packet.ready),
    }


def _phase3_v2_compute_decision_progress(
    brief_units: list[dict[str, Any]],
    arms: list[str],
    decisions: list[BriefUnitDecisionV1],
) -> Phase3V2DecisionProgressV1:
    required_pairs: set[str] = set()
    for unit in brief_units:
        if not isinstance(unit, dict):
            continue
        unit_id = str(unit.get("brief_unit_id") or "").strip()
        if not unit_id:
            continue
        for arm in arms:
            arm_name = str(arm or "").strip()
            if not arm_name:
                continue
            required_pairs.add(_phase3_v2_pair_key(unit_id, arm_name))

    decision_map: dict[str, str] = {}
    for row in decisions:
        pair_key = _phase3_v2_pair_key(row.brief_unit_id, row.arm)
        decision_map[pair_key] = str(row.decision).strip().lower()

    approved = 0
    revise = 0
    reject = 0
    for pair_key in required_pairs:
        value = decision_map.get(pair_key, "")
        if value == "approve":
            approved += 1
        elif value == "revise":
            revise += 1
        elif value == "reject":
            reject += 1

    total_required = len(required_pairs)
    pending = max(0, total_required - (approved + revise + reject))
    return Phase3V2DecisionProgressV1(
        total_required=total_required,
        approved=approved,
        revise=revise,
        reject=reject,
        pending=pending,
        all_approved=(total_required > 0 and approved == total_required),
    )


def _phase3_v2_is_locked(brand_slug: str, branch_id: str, run_id: str) -> bool:
    return bool(_phase3_v2_load_final_lock(brand_slug, branch_id, run_id).locked)


def _migrate_flat_outputs_to_brand():
    """One-time migration: move flat output files from outputs/ into a brand directory.

    Detects flat *_output.json in the root outputs/ dir (old layout) and
    moves them into outputs/{brand-slug}/ (new layout). Idempotent.
    """
    flat_files = [
        p for p in config.OUTPUT_DIR.glob("*_output.json")
        if p.is_file()
    ]
    if not flat_files:
        return  # Nothing to migrate

    # Try to determine brand from Foundation Research output
    brand_name = "legacy"
    for candidate in ("foundation_research_output.json", "agent_01a_output.json"):
        foundation_path = config.OUTPUT_DIR / candidate
        if not foundation_path.exists():
            continue
        try:
            data = json.loads(foundation_path.read_text("utf-8"))
            brand_name = data.get("brand_name", "legacy") or "legacy"
            break
        except Exception:
            continue

    brand_slug = _slugify(brand_name)
    brand_dir = config.OUTPUT_DIR / brand_slug
    brand_dir.mkdir(parents=True, exist_ok=True)

    # Move flat output files
    moved = 0
    for f in flat_files:
        dest = brand_dir / f.name
        if not dest.exists():
            shutil.move(str(f), str(dest))
            moved += 1

    # Move branches directory if it exists at root level
    old_branches = config.OUTPUT_DIR / "branches"
    new_branches = brand_dir / "branches"
    if old_branches.exists() and not new_branches.exists():
        shutil.move(str(old_branches), str(new_branches))

    # Create the brand record in SQLite
    from pipeline.storage import get_brand as _get_brand_db, create_brand as _create_brand_db
    if not _get_brand_db(brand_slug):
        _create_brand_db(brand_name, "", {})

    logger.info("Migrated %d flat outputs to brand directory: %s", moved, brand_slug)


from pipeline.llm import reset_usage, get_usage_summary, get_usage_log, set_usage_context, clear_usage_context
from pipeline.scraper import scrape_website
from pipeline.storage import (
    init_db,
    create_run,
    complete_run,
    fail_run,
    update_run_cost,
    save_agent_output,
    list_runs,
    get_run,
    get_latest_run_cost,
    update_run_label,
    delete_run,
    # Brand system
    get_or_create_brand,
    get_brand,
    list_brands,
    touch_brand,
    delete_brand as storage_delete_brand,
    _slugify,
    # Phase 4 video storage
    create_video_asset,
    create_video_clip,
    create_video_clip_revision,
    create_video_operator_action,
    create_video_run,
    create_or_get_video_provider_call,
    find_video_asset_by_filename,
    get_latest_video_clip_revision,
    get_latest_video_validation_report,
    get_video_asset,
    get_video_clip,
    get_video_clip_revision_by_index,
    get_video_clip_revision,
    get_video_run,
    list_video_assets,
    list_video_clip_revisions,
    list_video_clips,
    list_video_operator_actions,
    list_video_provider_calls,
    list_active_video_runs,
    list_video_runs_for_branch,
    list_video_validation_items,
    save_video_validation_report,
    update_video_clip,
    update_video_clip_revision,
    update_video_provider_call,
    update_video_run,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _check_api_keys() -> list[str]:
    """Check which LLM provider API keys are configured. Returns list of warnings."""
    warnings = []
    if not config.OPENAI_API_KEY:
        warnings.append("OPENAI_API_KEY is not set")
    if not config.ANTHROPIC_API_KEY:
        warnings.append("ANTHROPIC_API_KEY is not set")
    if not config.GOOGLE_API_KEY:
        warnings.append("GOOGLE_API_KEY is not set")

    # Check if the default provider has a key
    provider = config.DEFAULT_PROVIDER
    key_map = {
        "openai": config.OPENAI_API_KEY,
        "anthropic": config.ANTHROPIC_API_KEY,
        "google": config.GOOGLE_API_KEY,
    }
    if not key_map.get(provider):
        warnings.insert(0, f"DEFAULT_PROVIDER is '{provider}' but {provider.upper()}_API_KEY is not set — pipeline will fail!")

    return warnings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    _migrate_flat_outputs_to_brand()
    key_warnings = _check_api_keys()
    if key_warnings:
        logger.warning("=" * 60)
        logger.warning("API KEY WARNINGS:")
        for w in key_warnings:
            logger.warning("  • %s", w)
        logger.warning("Copy .env.example to .env and add your keys:")
        logger.warning("  cp .env.example .env")
        logger.warning("=" * 60)
    else:
        logger.info("API keys: all providers configured")

    # Install WebSocket log handler so server logs stream to the frontend
    ws_handler = _WSLogHandler()
    ws_handler.setLevel(logging.INFO)
    ws_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(ws_handler)

    # Start background log broadcaster
    broadcaster_task = asyncio.create_task(_log_broadcaster())

    yield

    # Shutdown
    broadcaster_task.cancel()
    logging.getLogger().removeHandler(ws_handler)


app = FastAPI(title="Creative Maker Pipeline", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

pipeline_state: dict[str, Any] = {
    "running": False,
    "abort_requested": False,
    "abort_generation": 0,  # monotonic abort token for cancelling background thread work
    "pipeline_task": None,  # asyncio.Task reference for cancellation
    "current_phase": None,
    "current_agent": None,
    "completed_agents": [],
    "failed_agents": [],
    "start_time": None,
    "log": [],
    "run_id": None,  # current SQLite run_id
    "phase_gate": None,  # asyncio.Event — set when user approves next phase
    "waiting_for_approval": False,  # True while paused between phases
    "gate_info": None,  # latest gate payload for state sync/polling
    "selected_concepts": [],  # user-selected video concepts from Phase 2
    "active_branch": None,  # currently running branch ID (None = main pipeline)
    "active_brand_slug": None,  # current brand slug (scopes outputs + branches)
    "copywriter_failed_jobs": [],  # failed per-concept jobs from parallel Copywriter
    "copywriter_parallel_context": None,  # context for rewriting only failed jobs
    "copywriter_rewrite_in_progress": False,  # guard to block Continue during rewrite
}

ws_clients: list[WebSocket] = []
phase3_v2_tasks: dict[str, asyncio.Task] = {}
phase3_v2_hook_tasks: dict[str, asyncio.Task] = {}
phase3_v2_scene_tasks: dict[str, asyncio.Task] = {}
phase4_v1_generation_tasks: dict[str, asyncio.Task] = {}
phase4_v1_storyboard_assign_tasks: dict[str, asyncio.Task] = {}
phase4_v1_storyboard_assign_state: dict[str, dict[str, Any]] = {}
phase4_v1_workflow_backend = InProcessWorkflowBackend()


_ZERO_COST_SUMMARY: dict[str, Any] = {
    "total_input_tokens": 0,
    "total_output_tokens": 0,
    "total_tokens": 0,
    "total_cost": 0.0,
    "calls": 0,
}


def _normalize_cost_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    base = dict(_ZERO_COST_SUMMARY)
    if not isinstance(summary, dict):
        return base
    for key in ("total_input_tokens", "total_output_tokens", "total_tokens", "calls"):
        try:
            base[key] = int(summary.get(key, base[key]) or 0)
        except Exception:
            base[key] = int(base[key])
    try:
        base["total_cost"] = round(float(summary.get("total_cost", 0.0) or 0.0), 4)
    except Exception:
        base["total_cost"] = 0.0
    return base


def _running_cost_summary() -> dict[str, Any]:
    return _normalize_cost_summary(get_usage_summary())


def _current_cost_summary_for_status(brand_slug: str = "") -> dict[str, Any]:
    if pipeline_state.get("running"):
        return _running_cost_summary()

    brand = str(brand_slug or "").strip()
    if brand:
        saved = get_latest_run_cost(brand_slug=brand)
    else:
        current = _running_cost_summary()
        if current["total_cost"] > 0:
            return current
        saved = get_latest_run_cost(brand_slug="")

    if saved is None:
        return dict(_ZERO_COST_SUMMARY)
    out = dict(_ZERO_COST_SUMMARY)
    out["total_cost"] = round(float(saved or 0.0), 4)
    out["persisted"] = True
    return out


def _persist_run_cost_snapshot(run_id: int | None) -> dict[str, Any]:
    summary = _running_cost_summary()
    if run_id:
        try:
            update_run_cost(run_id, float(summary.get("total_cost", 0.0) or 0.0))
        except Exception:
            logger.exception("Failed persisting run cost for run_id=%s", run_id)
    return summary


# ---------------------------------------------------------------------------
# WebSocket broadcast
# ---------------------------------------------------------------------------

async def broadcast(msg: dict):
    """Send a message to all connected WebSocket clients."""
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)


def _add_log(message: str, level: str = "info"):
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "level": level,
        "message": message,
    }
    pipeline_state["log"].append(entry)
    if len(pipeline_state["log"]) > 200:
        pipeline_state["log"] = pipeline_state["log"][-200:]


# ---------------------------------------------------------------------------
# Live server-log streaming (captures Python logs → WebSocket)
# ---------------------------------------------------------------------------

_log_queue: queue_mod.Queue = queue_mod.Queue(maxsize=1000)
_recent_server_logs: deque[str] = deque(maxlen=500)


class _WSLogHandler(logging.Handler):
    """Captures pipeline log messages and queues them for WebSocket streaming."""

    _SKIP_LOGGERS = frozenset({
        "uvicorn.access", "uvicorn.error", "uvicorn", "websockets",
        "websockets.server", "websockets.protocol",
    })

    def emit(self, record: logging.LogRecord):
        try:
            if record.name in self._SKIP_LOGGERS:
                return
            msg = self.format(record)
            # Skip HTTP access lines and WS connection noise
            if ("HTTP/1.1" in msg and '- "' in msg):
                return
            if any(skip in msg for skip in ("WebSocket /ws", "connection open", "connection closed")):
                return
            _recent_server_logs.append(msg)
            _log_queue.put_nowait(msg)
        except (queue_mod.Full, Exception):
            pass


async def _log_broadcaster():
    """Background task: drain log queue and broadcast to WS clients."""
    while True:
        batch: list[str] = []
        try:
            while len(batch) < 30:
                batch.append(_log_queue.get_nowait())
        except queue_mod.Empty:
            pass

        if batch and ws_clients:
            await broadcast({"type": "server_log", "lines": batch})

        await asyncio.sleep(0.5)


def _reset_server_log_stream():
    """Clear buffered server log tail/queue so each run starts with a clean terminal."""
    _recent_server_logs.clear()
    try:
        while True:
            _log_queue.get_nowait()
    except queue_mod.Empty:
        pass


# ---------------------------------------------------------------------------
# Agent runner helpers (sync, called in thread)
# ---------------------------------------------------------------------------

AGENT_CLASSES = {
    "foundation_research": Agent01AFoundationResearch,
    "creative_engine": Agent02IdeaGenerator,
    "copywriter": Agent04Copywriter,
    "hook_specialist": Agent05HookSpecialist,
}

AGENT_META = {
    "foundation_research": {"name": "Foundation Research", "phase": 1, "icon": "🔬"},
    "creative_engine": {"name": "Matrix Planner", "phase": 2, "icon": "🧩"},
    "copywriter": {"name": "Copywriter", "phase": 3, "icon": "✍️"},
    "hook_specialist": {"name": "Hook Specialist", "phase": 3, "icon": "🎣"},
}

LEGACY_TO_CANONICAL_SLUG = {
    "agent_01a": "foundation_research",
    "agent_02": "creative_engine",
    "agent_04": "copywriter",
    "agent_05": "hook_specialist",
}
CANONICAL_TO_LEGACY_SLUG = {v: k for k, v in LEGACY_TO_CANONICAL_SLUG.items()}

MODEL_LABELS = {
    "gpt-5.2": "GPT 5.2",
    "gpt-5.2-mini": "GPT 5.2 Mini",
    "gemini-3.0-pro": "Gemini 3.0 Pro",
    "gemini-2.5-pro": "Gemini 2.5 Pro",
    "gemini-2.5-flash": "Gemini 2.5 Flash",
    "claude-opus-4-6": "Claude Opus 4.6",
}

MATRIX_AWARENESS_LEVELS = [level.value for level in AwarenessLevel]
MATRIX_CELL_MAX_BRIEFS = 50


def _friendly_model_label(model: str) -> str:
    return MODEL_LABELS.get(model, model)


def _canonical_slug(slug: str | None) -> str:
    text = str(slug or "").strip()
    if not text:
        return ""
    return LEGACY_TO_CANONICAL_SLUG.get(text, text)


def _normalize_slug_list(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        slug = _canonical_slug(str(value or ""))
        if not slug or slug in seen:
            continue
        seen.add(slug)
        normalized.append(slug)
    return normalized


def _normalize_model_overrides(overrides: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(overrides, dict):
        return {}
    normalized: dict[str, Any] = {}
    for slug, payload in overrides.items():
        canonical = _canonical_slug(slug)
        if not canonical:
            continue
        normalized[canonical] = payload
    return normalized


def _slug_variants(slug: str) -> list[str]:
    canonical = _canonical_slug(slug)
    if not canonical:
        return []
    variants = [canonical]
    legacy = CANONICAL_TO_LEGACY_SLUG.get(canonical)
    if legacy and legacy not in variants:
        variants.append(legacy)
    raw = str(slug or "").strip()
    if raw and raw not in variants:
        variants.append(raw)
    return variants


def _load_output_from_base(base: Path, slug: str) -> dict | None:
    for candidate in _slug_variants(slug):
        path = base / f"{candidate}_output.json"
        if path.exists():
            return json.loads(path.read_text("utf-8"))
    return None


def _output_write_path(base: Path, slug: str) -> Path:
    canonical = _canonical_slug(slug)
    return base / f"{canonical}_output.json"


def _output_exists(base: Path, slug: str) -> bool:
    for candidate in _slug_variants(slug):
        if (base / f"{candidate}_output.json").exists():
            return True
    return False


def _load_output(slug: str, brand_slug: str | None = None) -> dict | None:
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    if brand_slug:
        base = _brand_output_dir(brand_slug)
    else:
        base = config.OUTPUT_DIR
    return _load_output_from_base(base, slug)


def _phase1_gate_label(gate_id: str) -> str:
    labels = {
        "global_evidence_coverage": "Global Evidence Coverage",
        "source_contradiction_audit": "Source Contradiction Audit",
        "pillar_1_profile_completeness": "Pillar 1 Profile Completeness",
        "pillar_2_voc_depth": "Pillar 2 VOC Depth",
        "pillar_2_segment_alignment": "Pillar 2 Segment Alignment",
        "pillar_3_competitive_depth": "Pillar 3 Competitive Depth",
        "pillar_4_mechanism_strength": "Pillar 4 Mechanism Strength",
        "pillar_5_awareness_validity": "Pillar 5 Awareness Validity",
        "pillar_6_emotion_dominance": "Pillar 6 Emotion Dominance",
        "pillar_7_proof_coverage": "Pillar 7 Proof Coverage",
        "cross_pillar_consistency": "Cross-Pillar Consistency",
    }
    return labels.get(gate_id, gate_id.replace("_", " ").strip().title())


def _emit_phase1_quality_logs(quality_report: dict) -> None:
    if not isinstance(quality_report, dict):
        return
    failed_ids = quality_report.get("failed_gate_ids", [])
    if not isinstance(failed_ids, list):
        failed_ids = []
    checks = quality_report.get("checks", [])
    if not isinstance(checks, list):
        checks = []
    retries = int(quality_report.get("retry_rounds_used", 0) or 0)
    overall_pass = bool(quality_report.get("overall_pass", False))

    if overall_pass:
        _add_log(f"Phase 1 quality gates passed (retries used: {retries})", "success")
        return

    failed_labels = ", ".join(_phase1_gate_label(g) for g in failed_ids) or "Unknown gate(s)"
    _add_log(
        f"Phase 1 quality gates failed ({len(failed_ids)}): {failed_labels}",
        "warning",
    )
    for check in checks:
        if not isinstance(check, dict):
            continue
        if bool(check.get("passed", False)):
            continue
        gate_id = str(check.get("gate_id", "unknown"))
        required = str(check.get("required", "")).strip()
        actual = str(check.get("actual", "")).strip()
        details = str(check.get("details", "")).strip()
        msg = (
            f"Gate detail — {_phase1_gate_label(gate_id)} | "
            f"required: {required or 'n/a'} | actual: {actual or 'n/a'}"
        )
        if details:
            msg += f" | details: {details[:220]}"
        _add_log(msg, "warning")


def _run_agent_sync(
    slug: str,
    inputs: dict,
    provider: str | None = None,
    model: str | None = None,
    skip_deep_research: bool = False,
    output_dir: Path | None = None,
    temperature: float | None = None,
    abort_check=None,
) -> dict | None:
    """Run a single agent synchronously. Returns the output dict or None."""
    # Matrix-only Phase 2: synthesize a validated MatrixPlan artifact locally.
    if slug == "creative_engine" and config.PHASE2_MATRIX_ONLY_MODE:
        agent_inputs = dict(inputs)
        if skip_deep_research:
            agent_inputs["_skip_deep_research"] = True
        matrix_plan = _build_matrix_plan(agent_inputs)
        target_dir = output_dir or config.OUTPUT_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = _output_write_path(target_dir, slug)
        out_path.write_text(json.dumps(matrix_plan, indent=2), encoding="utf-8")
        logger.info("Matrix Planner output saved: %s", out_path)
        return matrix_plan

    cls = AGENT_CLASSES.get(slug)
    if not cls:
        return None
    agent = cls(provider=provider, model=model, output_dir=output_dir, temperature=temperature)
    # Use a shallow copy so per-agent flags don't leak to other agents
    agent_inputs = dict(inputs)
    if skip_deep_research:
        agent_inputs["_skip_deep_research"] = True
    if abort_check:
        agent_inputs["_abort_check"] = abort_check
    result = agent.run(agent_inputs)
    return json.loads(result.model_dump_json())


def _auto_load_upstream(inputs: dict, needed: list[str], sync_foundation_identity: bool = False, brand_slug: str | None = None):
    """Load upstream agent outputs from disk into the inputs dict.

    If ``sync_foundation_identity`` is True, brand/product are only hydrated
    from Foundation Research when those fields are currently missing.
    """
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    mapping = {
        "foundation_brief": "foundation_research",
        "idea_brief": "creative_engine",
        "copywriter_brief": "copywriter",
        "hook_brief": "hook_specialist",
    }
    for key in needed:
        if key not in inputs or inputs[key] is None:
            slug = mapping.get(key)
            if slug:
                data = _load_output(slug, brand_slug=brand_slug)
                if data:
                    inputs[key] = data

                    # Optional identity hydration from Foundation Research.
                    # Never overwrite explicit values from the current brief.
                    if sync_foundation_identity and key == "foundation_brief":
                        if not inputs.get("brand_name") and data.get("brand_name"):
                            inputs["brand_name"] = data["brand_name"]
                        if not inputs.get("product_name") and data.get("product_name"):
                            inputs["product_name"] = data["product_name"]


def _norm_identity_value(value: Any) -> str:
    """Normalise text for stable brand/product identity comparison."""
    return " ".join(str(value or "").strip().lower().split())


def _validate_foundation_context(inputs: dict[str, Any]) -> str | None:
    """Validate that Foundation Research exists and matches current brief identity."""
    foundation = inputs.get("foundation_brief")
    if not isinstance(foundation, dict):
        return (
            "Creative Engine requires Foundation Research output. "
            "Run Foundation Research (Phase 1) first."
        )

    schema_version = str(foundation.get("schema_version") or "").strip()
    if schema_version != "2.0":
        return (
            "Saved Foundation Research is stale or legacy (missing schema_version=2.0). "
            "Rerun Foundation Research (Phase 1)."
        )

    foundation_brand = str(foundation.get("brand_name") or "").strip()
    foundation_product = str(foundation.get("product_name") or "").strip()
    if not foundation_brand or not foundation_product:
        return (
            "Saved Foundation Research is missing brand/product identity. "
            "Rerun Foundation Research (Phase 1)."
        )

    request_brand = str(inputs.get("brand_name") or "").strip()
    request_product = str(inputs.get("product_name") or "").strip()

    if request_brand and _norm_identity_value(request_brand) != _norm_identity_value(foundation_brand):
        return (
            f"Foundation Research is for brand '{foundation_brand}', but the current brief brand "
            f"is '{request_brand}'. Run Foundation Research again for this brief."
        )
    if request_product and _norm_identity_value(request_product) != _norm_identity_value(foundation_product):
        return (
            f"Foundation Research is for product '{foundation_product}', but the current brief product "
            f"is '{request_product}'. Run Foundation Research again for this brief."
        )

    # Fill missing identity from Foundation Research only when blank.
    if not request_brand:
        inputs["brand_name"] = foundation_brand
    if not request_product:
        inputs["product_name"] = foundation_product

    return None


def _ensure_foundation_for_creative_engine(inputs: dict[str, Any], brand_slug: str | None = None) -> str | None:
    """Load and validate Foundation Research context before Creative Engine runs."""
    _auto_load_upstream(inputs, ["foundation_brief"], sync_foundation_identity=False, brand_slug=brand_slug)
    return _validate_foundation_context(inputs)


def _phase2_disabled_error() -> str | None:
    """Return the migration lock message when Phase 2 is disabled."""
    if config.PHASE2_MATRIX_ONLY_MODE:
        return None
    if config.PHASE2_TEMPORARILY_DISABLED:
        return config.PHASE2_DISABLED_MESSAGE
    return None


def _normalize_emotion_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def _normalize_segment_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _empty_audience_context() -> dict[str, Any]:
    return {
        "segment_name": "",
        "goals": [],
        "pains": [],
        "triggers": [],
        "objections": [],
        "information_sources": [],
    }


def _extract_pillar1_audience_options(foundation: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(foundation, dict):
        return []
    pillar_1 = foundation.get("pillar_1_prospect_profile", {})
    if not isinstance(pillar_1, dict):
        return []
    profiles = pillar_1.get("segment_profiles", [])
    if not isinstance(profiles, list):
        return []

    options: list[dict[str, Any]] = []
    seen: set[str] = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        segment_name = str(profile.get("segment_name") or "").strip()
        if not segment_name:
            continue
        key = _normalize_segment_name(segment_name)
        if not key or key in seen:
            continue
        seen.add(key)
        options.append(
            {
                "segment_name": segment_name,
                "goals": _string_list(profile.get("goals")),
                "pains": _string_list(profile.get("pains")),
                "triggers": _string_list(profile.get("triggers")),
                "objections": _string_list(profile.get("objections")),
                "information_sources": _string_list(profile.get("information_sources")),
            }
        )
    return options


def _extract_collector_reports_from_snapshot(snapshot: dict[str, Any]) -> list[str]:
    reports: list[str] = []
    if not isinstance(snapshot, dict):
        return reports

    labeled = snapshot.get("collector_reports", [])
    if isinstance(labeled, list):
        for row in labeled:
            if not isinstance(row, dict):
                continue
            text = str(row.get("report_preview") or row.get("report") or "").strip()
            if text:
                reports.append(text)

    # Backward compatibility with older snapshots that only stored plain previews.
    if not reports:
        previews = snapshot.get("collector_report_previews", [])
        if isinstance(previews, list):
            for row in previews:
                text = str(row or "").strip()
                if text:
                    reports.append(text)

    deduped: list[str] = []
    seen: set[str] = set()
    for report in reports:
        key = hashlib.sha1(report.encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(report)
    return deduped


def _load_step1_collector_reports(base: Path) -> list[str]:
    reports: list[str] = []
    snapshot_path = base / "foundation_research_collectors_snapshot.json"
    if snapshot_path.exists():
        try:
            payload = json.loads(snapshot_path.read_text("utf-8"))
            reports.extend(_extract_collector_reports_from_snapshot(payload))
        except Exception:
            logger.warning("Unable to parse collectors snapshot at %s", snapshot_path)

    checkpoint_path = base / "phase1_collector_checkpoint.json"
    if checkpoint_path.exists():
        try:
            payload = json.loads(checkpoint_path.read_text("utf-8"))
            rows = payload.get("collector_reports", []) if isinstance(payload, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    text = str(row or "").strip()
                    if text:
                        reports.append(text)
        except Exception:
            logger.warning("Unable to parse collector checkpoint at %s", checkpoint_path)

    deduped: list[str] = []
    seen: set[str] = set()
    for report in reports:
        key = hashlib.sha1(report.encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(report)
    return deduped


def _resolve_audience_context(
    foundation: dict[str, Any],
    selected_segment: str,
) -> tuple[dict[str, Any], str]:
    audience_options = _extract_pillar1_audience_options(foundation)
    selected_raw = str(selected_segment or "").strip()
    if not selected_raw:
        return _empty_audience_context(), ""

    selected_key = _normalize_segment_name(selected_raw)
    for option in audience_options:
        option_name = str(option.get("segment_name") or "").strip()
        if _normalize_segment_name(option_name) == selected_key:
            canonical_name = option_name
            return {
                "segment_name": canonical_name,
                "goals": _string_list(option.get("goals")),
                "pains": _string_list(option.get("pains")),
                "triggers": _string_list(option.get("triggers")),
                "objections": _string_list(option.get("objections")),
                "information_sources": _string_list(option.get("information_sources")),
            }, canonical_name

    raise RuntimeError(
        f"Selected audience '{selected_raw}' was not found in Pillar 1 segment_profiles."
    )


def _extract_pillar6_emotion_rows(foundation: dict[str, Any]) -> list[dict[str, Any]]:
    dominant = (
        foundation.get("pillar_6_emotional_driver_inventory", {}).get("dominant_emotions", [])
        if isinstance(foundation, dict)
        else []
    )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if not isinstance(dominant, list):
        return rows

    for idx, item in enumerate(dominant):
        if not isinstance(item, dict):
            continue
        label = str(item.get("emotion") or item.get("emotion_label") or "").strip()
        if not label:
            continue
        emotion_key = _normalize_emotion_key(label) or f"emotion_{idx + 1}"
        if emotion_key in seen:
            continue
        seen.add(emotion_key)
        sample_quote_ids = item.get("sample_quote_ids", [])
        if not isinstance(sample_quote_ids, list):
            sample_quote_ids = []
        rows.append(
            {
                "emotion_key": emotion_key,
                "emotion_label": label,
                "tagged_quote_count": int(item.get("tagged_quote_count", 0) or 0),
                "share_of_voc": float(item.get("share_of_voc", 0.0) or 0.0),
                "sample_quote_ids": [
                    str(v).strip() for v in sample_quote_ids if str(v or "").strip()
                ],
            }
        )
    return rows


def _extract_pillar6_lf8_rows_for_segment(
    foundation: dict[str, Any],
    segment_name: str,
) -> list[dict[str, Any]]:
    if not isinstance(foundation, dict):
        return []
    p6 = foundation.get("pillar_6_emotional_driver_inventory", {})
    if not isinstance(p6, dict):
        return []
    by_segment = p6.get("lf8_rows_by_segment", {})
    if not isinstance(by_segment, dict):
        return []

    selected_key = _normalize_segment_name(segment_name)
    rows_raw: list[Any] = []
    for key, value in by_segment.items():
        if _normalize_segment_name(key) == selected_key and isinstance(value, list):
            rows_raw = value
            break

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in rows_raw:
        if not isinstance(item, dict):
            continue
        lf8_code = str(item.get("lf8_code") or "").strip().lower()
        if not lf8_code:
            continue
        emotion_key = _normalize_emotion_key(lf8_code) or lf8_code
        if emotion_key in seen:
            continue
        seen.add(emotion_key)

        lf8_label = str(item.get("lf8_label") or lf8_code.upper()).strip()
        emotion_angle = str(item.get("emotion_angle") or "").strip()
        sample_quote_ids = item.get("sample_quote_ids", [])
        support_evidence_ids = item.get("support_evidence_ids", [])
        if not isinstance(sample_quote_ids, list):
            sample_quote_ids = []
        if not isinstance(support_evidence_ids, list):
            support_evidence_ids = []

        rows.append(
            {
                "emotion_key": emotion_key,
                "emotion_label": lf8_label,
                "lf8_code": lf8_code,
                "lf8_label": lf8_label,
                "emotion_angle": emotion_angle,
                "segment_name": str(item.get("segment_name") or segment_name).strip(),
                "tagged_quote_count": int(item.get("tagged_quote_count", 0) or 0),
                "share_of_voc": float(item.get("share_of_segment_voc", 0.0) or 0.0),
                "share_of_segment_voc": float(item.get("share_of_segment_voc", 0.0) or 0.0),
                "unique_domains": int(item.get("unique_domains", 0) or 0),
                "sample_quote_ids": [str(v).strip() for v in sample_quote_ids if str(v or "").strip()],
                "support_evidence_ids": [str(v).strip() for v in support_evidence_ids if str(v or "").strip()],
                "blocking_objection": str(item.get("blocking_objection") or "").strip(),
                "required_proof": str(item.get("required_proof") or "").strip(),
                "contradiction_risk": str(item.get("contradiction_risk") or "low").strip().lower(),
                "confidence": float(item.get("confidence", 0.0) or 0.0),
                "buying_power_score": float(item.get("buying_power_score", 0.0) or 0.0),
            }
        )

    rows.sort(
        key=lambda row: (
            -float(row.get("buying_power_score", 0.0) or 0.0),
            -int(row.get("tagged_quote_count", 0) or 0),
            str(row.get("lf8_code") or ""),
        )
    )
    return rows


def _extract_matrix_axes(
    foundation: dict[str, Any],
    *,
    selected_audience_segment: str = "",
    require_audience_selection: bool = False,
    allow_global_legacy: bool = False,
) -> tuple[list[str], list[dict[str, Any]], str, bool, str]:
    awareness_levels = list(MATRIX_AWARENESS_LEVELS)
    pillar6_rows = _extract_pillar6_emotion_rows(foundation if isinstance(foundation, dict) else {})
    selected_raw = str(selected_audience_segment or "").strip()

    if not selected_raw:
        if allow_global_legacy:
            return awareness_levels, pillar6_rows, "global_legacy", False, ""
        if require_audience_selection:
            return (
                awareness_levels,
                [],
                "lf8_empty",
                True,
                "Select one Pillar 1 audience to load LF8 emotion rows.",
            )
        return awareness_levels, pillar6_rows, "global_legacy", False, ""

    _, canonical_segment = _resolve_audience_context(foundation, selected_raw)
    lf8_rows = _extract_pillar6_lf8_rows_for_segment(foundation, canonical_segment)
    if not lf8_rows:
        return (
            awareness_levels,
            [],
            "lf8_empty",
            False,
            (
                f"No LF8 rows passed evidence gates for '{canonical_segment}'. "
                "Rebuild Pillar 6 or rerun Foundation Research."
            ),
        )
    return awareness_levels, lf8_rows, "lf8_audience_scoped", False, ""


def _normalize_matrix_cells(
    raw_cells: Any,
    awareness_levels: list[str],
    emotion_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, dict[str, int], dict[str, int]]:
    awareness_set = set(awareness_levels)
    emotion_keys = [str(row.get("emotion_key") or "") for row in emotion_rows]
    emotion_set = {k for k in emotion_keys if k}

    cell_counts: dict[tuple[str, str], int] = {
        (awareness, emotion_key): 0
        for emotion_key in emotion_keys
        for awareness in awareness_levels
    }

    if isinstance(raw_cells, list):
        for cell in raw_cells:
            if not isinstance(cell, dict):
                continue
            awareness_level = str(cell.get("awareness_level") or "").strip().lower()
            emotion_key = _normalize_emotion_key(
                cell.get("emotion_key") or cell.get("emotion") or cell.get("emotion_label")
            )
            if awareness_level not in awareness_set or emotion_key not in emotion_set:
                continue
            try:
                brief_count = int(cell.get("brief_count", 0) or 0)
            except (TypeError, ValueError):
                raise RuntimeError(
                    f"Invalid brief_count for cell ({awareness_level}, {emotion_key}). Must be an integer."
                ) from None
            if brief_count < 0:
                raise RuntimeError(
                    f"Invalid brief_count for cell ({awareness_level}, {emotion_key}). Must be >= 0."
                )
            if brief_count > MATRIX_CELL_MAX_BRIEFS:
                raise RuntimeError(
                    f"Invalid brief_count for cell ({awareness_level}, {emotion_key}). Max is {MATRIX_CELL_MAX_BRIEFS}."
                )
            cell_counts[(awareness_level, emotion_key)] = brief_count

    cells: list[dict[str, Any]] = []
    totals_by_awareness = {level: 0 for level in awareness_levels}
    totals_by_emotion = {key: 0 for key in emotion_keys}
    total_briefs = 0

    for emotion_row in emotion_rows:
        emotion_key = str(emotion_row.get("emotion_key") or "")
        for awareness_level in awareness_levels:
            brief_count = int(cell_counts.get((awareness_level, emotion_key), 0))
            cells.append(
                {
                    "awareness_level": awareness_level,
                    "emotion_key": emotion_key,
                    "brief_count": brief_count,
                }
            )
            totals_by_awareness[awareness_level] += brief_count
            totals_by_emotion[emotion_key] += brief_count
            total_briefs += brief_count

    return cells, total_briefs, totals_by_awareness, totals_by_emotion


def _build_matrix_plan(inputs: dict[str, Any]) -> dict[str, Any]:
    foundation = inputs.get("foundation_brief")
    if not isinstance(foundation, dict):
        raise RuntimeError(
            "Matrix Planner requires Foundation Research output. Run Phase 1 first."
        )

    selected_audience_segment = str(inputs.get("selected_audience_segment") or "").strip()
    audience_context, selected_canonical = _resolve_audience_context(
        foundation,
        selected_audience_segment,
    )
    awareness_levels, emotion_rows, emotion_source_mode, _, emotion_message = _extract_matrix_axes(
        foundation,
        selected_audience_segment=selected_canonical,
        allow_global_legacy=(not selected_canonical),
    )
    if not emotion_rows:
        if selected_canonical:
            raise RuntimeError(
                emotion_message
                or (
                    f"Matrix Planner could not build audience-scoped emotions for "
                    f"'{selected_canonical}'."
                )
            )
        raise RuntimeError(
            "Matrix Planner could not find emotional drivers in Phase 1 output. "
            "Rerun Foundation Research and verify pillar_6_emotional_driver_inventory."
        )

    cells, total_briefs, totals_by_awareness, totals_by_emotion = _normalize_matrix_cells(
        inputs.get("matrix_cells", []), awareness_levels, emotion_rows
    )

    if total_briefs <= 0:
        raise RuntimeError(
            "Matrix Planner requires at least one planned brief. Set one or more matrix cells above zero."
        )

    checks = [
        {
            "gate_id": "matrix_awareness_axis_integrity",
            "passed": len(awareness_levels) == 5,
            "required": "5 awareness levels",
            "actual": str(len(awareness_levels)),
        },
        {
            "gate_id": "matrix_emotion_axis_integrity",
            "passed": len(emotion_rows) > 0,
            "required": ">=1 emotional driver",
            "actual": str(len(emotion_rows)),
        },
        {
            "gate_id": "matrix_structural_integrity",
            "passed": len(cells) == len(awareness_levels) * len(emotion_rows),
            "required": "all matrix cells present",
            "actual": str(len(cells)),
        },
        {
            "gate_id": "matrix_non_zero_plan",
            "passed": total_briefs > 0,
            "required": ">=1 planned brief",
            "actual": str(total_briefs),
        },
    ]

    return {
        "schema_version": "matrix_plan_v1",
        "planning_mode": "matrix_only_phase2",
        "generated_date": date.today().isoformat(),
        "brand_name": str(inputs.get("brand_name") or foundation.get("brand_name") or ""),
        "product_name": str(inputs.get("product_name") or foundation.get("product_name") or ""),
        "selected_audience_segment": selected_canonical,
        "emotion_source_mode": emotion_source_mode,
        "audience": audience_context,
        "awareness_axis": {"axis": "x", "levels": awareness_levels},
        "emotion_axis": {"axis": "y", "rows": emotion_rows},
        "cells": cells,
        "totals": {
            "total_briefs": total_briefs,
            "by_awareness_level": totals_by_awareness,
            "by_emotion_key": totals_by_emotion,
        },
        "quality_gate_report": {
            "overall_pass": all(bool(check.get("passed")) for check in checks),
            "checks": checks,
        },
    }


def _phase3_disabled_error() -> str | None:
    """Return the rebuild lock message when Phase 3 is disabled."""
    if config.PHASE3_TEMPORARILY_DISABLED:
        return config.PHASE3_DISABLED_MESSAGE
    return None


def _phase3_v2_disabled_error() -> str | None:
    if not bool(config.PHASE3_V2_ENABLED):
        return "Phase 3 v2 pilot is disabled. Set PHASE3_V2_ENABLED=true to enable."
    return None


def _load_matrix_plan_for_branch(brand_slug: str, branch_id: str) -> tuple[dict[str, Any] | None, str | None]:
    data = _load_branch_output(brand_slug, branch_id, "creative_engine")
    if not isinstance(data, dict):
        return None, "No matrix plan found for this branch. Run Phase 2 first."
    schema_version = str(data.get("schema_version") or "").strip()
    if schema_version != "matrix_plan_v1":
        return None, "Branch Phase 2 output is not matrix_plan_v1. Rerun Phase 2."
    return data, None


def _matrix_planned_brief_total(matrix_plan: dict[str, Any]) -> int:
    if not isinstance(matrix_plan, dict):
        return 1
    totals = matrix_plan.get("totals")
    if isinstance(totals, dict):
        try:
            v = int(totals.get("total_briefs", 0) or 0)
        except (TypeError, ValueError):
            v = 0
        if v > 0:
            return v

    total = 0
    cells = matrix_plan.get("cells")
    if isinstance(cells, list):
        for cell in cells:
            if not isinstance(cell, dict):
                continue
            try:
                total += int(cell.get("brief_count", 0) or 0)
            except (TypeError, ValueError):
                continue
    return max(1, total)


def _normalize_phase3_v2_sdk_toggles(raw: Any) -> dict[str, bool]:
    # Claude SDK-only mode: force core script drafter on and other step toggles off.
    return {
        "core_script_drafter": True,
        "hook_generator": False,
        "scene_planner": False,
        "targeted_repair": False,
    }


def _normalize_phase3_v2_model_overrides(raw: Any) -> dict[str, dict[str, str]]:
    if not isinstance(raw, dict):
        return {}
    clean: dict[str, dict[str, str]] = {}
    payload = raw.get("claude_sdk")
    if isinstance(payload, dict):
        provider = str(payload.get("provider") or "").strip().lower()
        model = str(payload.get("model") or "").strip()
        if provider and model:
            clean["claude_sdk"] = {"provider": provider, "model": model}
        elif model:
            clean["claude_sdk"] = {"model": model}
    return clean


def _phase3_v2_arm_file_name(arm: str) -> str:
    if arm == "claude_sdk":
        return "arm_claude_sdk_core_scripts.json"
    return "arm_control_core_scripts.json"


def _phase3_v2_resolve_arms(ab_mode: bool, sdk_toggles: dict[str, bool]) -> list[str]:
    _ = ab_mode, sdk_toggles
    return ["claude_sdk"]


def _phase3_v2_input_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _slugify_job_key(raw: str, fallback: str) -> str:
    text = (raw or "").strip()
    if not text:
        text = fallback
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in text)
    cleaned = cleaned.strip("_")
    return cleaned[:80] or fallback


def _resolve_copywriter_jobs(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    """Resolve one copywriter job per selected concept (or first concept per angle)."""
    idea_brief = inputs.get("idea_brief")
    if not isinstance(idea_brief, dict):
        return []

    angles = idea_brief.get("angles", [])
    if not isinstance(angles, list):
        return []

    selected = inputs.get("selected_concepts", [])
    jobs: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _append_job(angle: dict[str, Any], concept: dict[str, Any], concept_index: int, fallback_idx: int):
        angle_id = str(angle.get("angle_id") or f"angle_{fallback_idx + 1}")
        base_key = _slugify_job_key(f"{angle_id}_{concept_index}", f"job_{fallback_idx + 1}")
        job_key = base_key
        suffix = 2
        while job_key in seen_keys:
            job_key = f"{base_key}_{suffix}"
            suffix += 1
        seen_keys.add(job_key)

        jobs.append({
            "job_index": len(jobs),
            "job_key": job_key,
            "angle_id": angle_id,
            "concept_index": concept_index,
            "angle": {k: v for k, v in angle.items() if k != "video_concepts"},
            "video_concept": concept,
        })

    if isinstance(selected, list) and selected:
        for i, sel in enumerate(selected):
            if not isinstance(sel, dict):
                continue
            angle_id = str(sel.get("angle_id") or "").strip()
            try:
                concept_index = int(sel.get("concept_index", 0))
            except (TypeError, ValueError):
                concept_index = 0

            for angle in angles:
                if not isinstance(angle, dict):
                    continue
                if str(angle.get("angle_id") or "") != angle_id:
                    continue
                concepts = angle.get("video_concepts", [])
                if isinstance(concepts, list) and 0 <= concept_index < len(concepts):
                    concept = concepts[concept_index]
                    if isinstance(concept, dict):
                        _append_job(angle, concept, concept_index, i)
                break
    else:
        for i, angle in enumerate(angles):
            if not isinstance(angle, dict):
                continue
            concepts = angle.get("video_concepts", [])
            if not isinstance(concepts, list) or not concepts:
                continue
            concept = concepts[0]
            if not isinstance(concept, dict):
                continue
            _append_job(angle, concept, 0, i)

    return jobs


def _build_copywriter_output(inputs: dict[str, Any], scripts: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the aggregated Agent 04 output from per-script results."""
    scripts_by_funnel: dict[str, int] = {}
    scripts_by_framework: dict[str, int] = {}
    wpm_values: list[float] = []
    quality_gate_failures: list[str] = []

    for script in scripts:
        if not isinstance(script, dict):
            continue
        funnel = str(script.get("funnel_stage", "unknown"))
        framework = str(script.get("copy_framework", "unknown"))
        scripts_by_funnel[funnel] = scripts_by_funnel.get(funnel, 0) + 1
        scripts_by_framework[framework] = scripts_by_framework.get(framework, 0) + 1

        wpm = script.get("words_per_minute")
        if isinstance(wpm, (int, float)):
            wpm_values.append(float(wpm))

        gates = script.get("quality_gates_passed")
        if isinstance(gates, dict):
            failed = [k for k, v in gates.items() if v is False]
            if failed:
                script_id = str(script.get("script_id") or script.get("concept_id") or "script")
                quality_gate_failures.append(f"{script_id}: {', '.join(failed)}")

    avg_wpm = round(sum(wpm_values) / len(wpm_values), 1) if wpm_values else 0.0

    return {
        "brand_name": str(inputs.get("brand_name") or ""),
        "product_name": str(inputs.get("product_name") or ""),
        "generated_date": date.today().isoformat(),
        "batch_id": str(inputs.get("batch_id") or ""),
        "scripts": scripts,
        "total_scripts": len(scripts),
        "scripts_by_funnel": scripts_by_funnel,
        "scripts_by_framework": scripts_by_framework,
        "average_wpm": avg_wpm,
        "quality_gate_failures": quality_gate_failures,
    }


async def _run_copywriter_jobs_parallel(
    jobs: list[dict[str, Any]],
    base_inputs: dict[str, Any],
    loop,
    provider: str,
    model: str,
    jobs_dir: Path,
    max_parallel: int = 4,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run one Agent 04 call per concept in parallel, with bounded concurrency."""
    jobs_dir.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(max_parallel)
    lock = asyncio.Lock()
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    done = 0
    total = len(jobs)

    async def _run_one(job: dict[str, Any]):
        nonlocal done
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        async with sem:
            if pipeline_state["abort_requested"]:
                raise PipelineAborted("Pipeline aborted by user")

            job_started = time.time()
            job_inputs = dict(base_inputs)
            angle = dict(job.get("angle") or {})
            angle["video_concepts"] = [job.get("video_concept", {})]
            job_inputs["idea_brief"] = {"angles": [angle]}
            job_inputs["selected_concepts"] = [{
                "angle_id": job.get("angle_id"),
                "concept_index": 0,
            }]

            run_tag = str(int(time.time() * 1000))
            job_dir = jobs_dir / f"{job.get('job_key', 'job')}_{run_tag}"
            job_dir.mkdir(parents=True, exist_ok=True)

            status_payload: dict[str, Any]
            try:
                result = await loop.run_in_executor(
                    None,
                    _run_agent_sync,
                    "copywriter",
                    job_inputs,
                    provider,
                    model,
                    False,
                    job_dir,
                    None,
                )
                scripts = result.get("scripts") if isinstance(result, dict) else None
                if not isinstance(scripts, list) or not scripts:
                    raise ValueError("Copywriter job returned no script")

                script = scripts[0]
                success = {
                    "job_index": int(job.get("job_index", 0)),
                    "job_key": str(job.get("job_key", "")),
                    "script": script,
                    "elapsed": round(time.time() - job_started, 1),
                }
                successes.append(success)
                status_payload = {
                    "status": "success",
                    "job": job,
                    "elapsed": success["elapsed"],
                    "script_id": script.get("script_id"),
                }
            except PipelineAborted:
                raise
            except Exception as exc:
                failure = {
                    "job_index": int(job.get("job_index", 0)),
                    "job_key": str(job.get("job_key", "")),
                    "angle_id": job.get("angle_id"),
                    "concept_index": job.get("concept_index"),
                    "angle": job.get("angle"),
                    "video_concept": job.get("video_concept"),
                    "error": str(exc),
                    "elapsed": round(time.time() - job_started, 1),
                }
                failures.append(failure)
                status_payload = {
                    "status": "failed",
                    "job": job,
                    "elapsed": failure["elapsed"],
                    "error": failure["error"],
                }

            try:
                (job_dir / "job_result.json").write_text(
                    json.dumps(status_payload, indent=2),
                    encoding="utf-8",
                )
            except OSError:
                pass

            async with lock:
                done += 1
                ok_count = len(successes)
                fail_count = len(failures)
                progress_msg = (
                    f"Parallel scripts {done}/{total} complete "
                    f"({ok_count} ok, {fail_count} failed)"
                )

            await broadcast({
                "type": "stream_progress",
                "slug": "copywriter",
                "message": progress_msg,
                "cost": _running_cost_summary(),
            })

    tasks = [asyncio.create_task(_run_one(job)) for job in jobs]
    await asyncio.gather(*tasks)

    successes.sort(key=lambda x: x["job_index"])
    failures.sort(key=lambda x: x["job_index"])
    return successes, failures


async def _run_copywriter_parallel_async(
    inputs: dict[str, Any],
    loop,
    run_id: int,
    provider: str | None = None,
    model: str | None = None,
    output_dir: Path | None = None,
) -> dict | None:
    """Run Agent 04 as one parallel job per selected concept (max concurrency 4)."""
    slug = "copywriter"
    meta = AGENT_META[slug]

    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    overrides = pipeline_state.get("model_overrides", {})
    agent_provider = provider
    agent_model = model
    if slug in overrides:
        agent_provider = overrides[slug].get("provider", provider)
        agent_model = overrides[slug].get("model", model)

    default_conf = config.get_agent_llm_config(slug)
    final_provider = agent_provider or default_conf["provider"]
    final_model = agent_model or default_conf["model"]
    model_label = _friendly_model_label(final_model)

    jobs = _resolve_copywriter_jobs(inputs)
    total_jobs = len(jobs)
    if total_jobs == 0:
        err = "No selected video concepts found for Copywriter."
        pipeline_state["failed_agents"].append(slug)
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        await broadcast({
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": 0.0,
        })
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=0.0,
            error=err,
        )
        return None

    max_parallel = 4
    base_output_dir = output_dir or config.OUTPUT_DIR
    jobs_dir = base_output_dir / "copywriter_jobs" / f"run_{run_id}"

    pipeline_state["current_agent"] = slug
    set_usage_context(agent_name=slug, phase=str(pipeline_state.get("current_phase", "")))
    _add_log(
        f"Starting {meta['icon']} {meta['name']} [{model_label}] — "
        f"{total_jobs} scripts in parallel (max {max_parallel})..."
    )
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
        "model": model_label,
        "provider": final_provider,
        "cost": _running_cost_summary(),
    })

    started = time.time()
    successes, failures = await _run_copywriter_jobs_parallel(
        jobs=jobs,
        base_inputs=inputs,
        loop=loop,
        provider=final_provider,
        model=final_model,
        jobs_dir=jobs_dir,
        max_parallel=max_parallel,
    )
    elapsed = time.time() - started

    success_scripts = [s["script"] for s in successes]
    fail_count = len(failures)
    ok_count = len(success_scripts)

    pipeline_state["copywriter_failed_jobs"] = failures
    pipeline_state["copywriter_parallel_context"] = {
        "run_id": run_id,
        "output_dir": str(base_output_dir),
        "jobs_dir": str(jobs_dir),
        "provider": final_provider,
        "model": final_model,
        "base_inputs": {
            "brand_name": inputs.get("brand_name"),
            "product_name": inputs.get("product_name"),
            "batch_id": inputs.get("batch_id"),
            "foundation_brief": inputs.get("foundation_brief"),
        },
    }

    try:
        (base_output_dir / "copywriter_parallel_meta.json").write_text(
            json.dumps({
                "generated_at": datetime.now().isoformat(),
                "run_id": run_id,
                "total_jobs": total_jobs,
                "succeeded_jobs": ok_count,
                "failed_jobs": fail_count,
                "failures": failures,
                "model": {"provider": final_provider, "model": final_model},
            }, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass

    if ok_count == 0:
        err = f"Copywriter failed for all {total_jobs} scripts."
        pipeline_state["failed_agents"].append(slug)
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        await broadcast({
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": round(elapsed, 1),
        })
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=elapsed,
            error=err,
        )
        return None

    result = _build_copywriter_output(inputs, success_scripts)
    output_path = _output_write_path(base_output_dir, slug)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("Output saved: %s", output_path)

    pipeline_state["completed_agents"].append(slug)
    cost_summary = _persist_run_cost_snapshot(run_id)
    cost_str = (
        f"${cost_summary['total_cost']:.2f}"
        if cost_summary["total_cost"] >= 0.01
        else f"${cost_summary['total_cost']:.4f}"
    )
    fail_suffix = f", {fail_count} failed" if fail_count else ""
    _add_log(
        f"Completed {meta['icon']} {meta['name']} in {elapsed:.1f}s — "
        f"{ok_count}/{total_jobs} scripts succeeded{fail_suffix} — running total: {cost_str}",
        "success" if fail_count == 0 else "warning",
    )
    await broadcast({
        "type": "agent_complete",
        "slug": slug,
        "name": meta["name"],
        "elapsed": round(elapsed, 1),
        "cost": cost_summary,
        "parallel_jobs": total_jobs,
        "failed_jobs": fail_count,
    })

    save_agent_output(
        run_id=run_id,
        agent_slug=slug,
        agent_name=meta["name"],
        output=result,
        elapsed=elapsed,
    )

    return result


# ---------------------------------------------------------------------------
# Pipeline execution (runs in background task)
# ---------------------------------------------------------------------------

class PipelineAborted(Exception):
    """Raised when the pipeline is aborted by the user."""
    pass


async def _run_single_agent_async(slug: str, inputs: dict, loop, run_id: int, provider: str | None = None, model: str | None = None, output_dir: Path | None = None, temperature: float | None = None) -> dict | None:
    """Run agent in thread pool and broadcast progress. Saves to SQLite."""
    # Check abort flag before starting this agent
    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    # Resolve per-agent model overrides (takes priority over global override)
    overrides = pipeline_state.get("model_overrides", {})
    agent_provider = provider
    agent_model = model
    skip_deep_research = False
    if slug in overrides:
        agent_provider = overrides[slug].get("provider", provider)
        agent_model = overrides[slug].get("model", model)
        # Agents 1A and 1B default to deep research — if user picked a
        # different model, skip deep research and use that model directly
        if slug == "foundation_research":
            skip_deep_research = True

    # Resolve the final model label for broadcast
    from config import get_agent_llm_config, AGENT_LLM_CONFIG
    default_conf = get_agent_llm_config(slug)
    # If agent_provider/model were explicitly set (override or global), use those;
    # otherwise fall back to the per-agent defaults from config.py
    final_provider = agent_provider or default_conf["provider"]
    final_model = agent_model or default_conf["model"]
    # For deep research agents, label them correctly
    if slug == "foundation_research" and not skip_deep_research:
        model_label = "Deep Research"
    else:
        model_label = _friendly_model_label(final_model)

    meta = AGENT_META[slug]
    pipeline_state["current_agent"] = slug
    set_usage_context(agent_name=slug, phase=str(pipeline_state.get("current_phase", "")))
    _add_log(f"Starting {meta['icon']} {meta['name']} [{model_label}]...")
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
        "model": model_label,
        "provider": final_provider,
        "cost": _running_cost_summary(),
    })

    # Set up streaming progress callback to broadcast to frontend
    from pipeline.llm import set_stream_progress_callback

    def _on_stream_progress(msg):
        """Called from LLM thread during streaming — fire-and-forget broadcast."""
        try:
            cost_snapshot = _running_cost_summary()
            asyncio.run_coroutine_threadsafe(
                broadcast(
                    {
                        "type": "stream_progress",
                        "slug": slug,
                        "message": msg,
                        "cost": cost_snapshot,
                    }
                ),
                loop,
            )
        except Exception:
            pass

    set_stream_progress_callback(_on_stream_progress)

    start = time.time()
    try:
        run_abort_generation = int(pipeline_state.get("abort_generation", 0))

        def _abort_check() -> bool:
            return bool(
                pipeline_state.get("abort_requested")
                or int(pipeline_state.get("abort_generation", 0)) != run_abort_generation
            )

        result = await loop.run_in_executor(
            None,
            _run_agent_sync,
            slug,
            inputs,
            agent_provider,
            agent_model,
            skip_deep_research,
            output_dir,
            temperature,
            _abort_check,
        )
        elapsed = time.time() - start
        pipeline_state["completed_agents"].append(slug)

        # Get running cost totals
        cost_summary = _persist_run_cost_snapshot(run_id)
        cost_str = f"${cost_summary['total_cost']:.2f}" if cost_summary['total_cost'] >= 0.01 else f"${cost_summary['total_cost']:.4f}"
        phase1_quality_report = None
        if slug == "foundation_research" and isinstance(result, dict):
            candidate = result.get("quality_gate_report")
            if isinstance(candidate, dict):
                phase1_quality_report = candidate
                _emit_phase1_quality_logs(candidate)

        _add_log(f"Completed {meta['icon']} {meta['name']} in {elapsed:.1f}s — running total: {cost_str}", "success")
        payload = {
            "type": "agent_complete",
            "slug": slug,
            "name": meta["name"],
            "elapsed": round(elapsed, 1),
            "cost": cost_summary,
        }
        if phase1_quality_report is not None:
            payload["quality_gate_report"] = phase1_quality_report
        await broadcast(payload)

        # Save to SQLite
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=result,
            elapsed=elapsed,
        )

        return result
    except Exception as e:
        elapsed = time.time() - start
        pipeline_state["failed_agents"].append(slug)
        err = str(e)
        phase1_quality_report = None
        if slug == "foundation_research":
            try:
                base_output_dir = output_dir or config.OUTPUT_DIR
                q_path = base_output_dir / "foundation_research_quality_report.json"
                if q_path.exists():
                    phase1_quality_report = json.loads(q_path.read_text("utf-8"))
                    if isinstance(phase1_quality_report, dict):
                        _emit_phase1_quality_logs(phase1_quality_report)
            except Exception:
                phase1_quality_report = None
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        logger.exception("Agent %s failed", slug)
        payload = {
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": round(elapsed, 1),
        }
        if phase1_quality_report is not None:
            payload["quality_gate_report"] = phase1_quality_report
        await broadcast(payload)

        # Save failure to SQLite
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=elapsed,
            error=err,
        )

        return None
    finally:
        set_stream_progress_callback(None)


async def _run_foundation_collectors_step_async(
    inputs: dict,
    loop,
    run_id: int,
    provider: str | None = None,
    model: str | None = None,
    output_dir: Path | None = None,
    temperature: float | None = None,
) -> dict | None:
    """Run Phase 1 collectors only (step 1/2) and persist preview output."""
    slug = "foundation_research"
    meta = AGENT_META[slug]

    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    overrides = pipeline_state.get("model_overrides", {})
    agent_provider = provider
    agent_model = model
    if slug in overrides:
        agent_provider = overrides[slug].get("provider", provider)
        agent_model = overrides[slug].get("model", model)

    default_conf = config.get_agent_llm_config(slug)
    final_provider = agent_provider or default_conf["provider"]
    final_model = agent_model or default_conf["model"]
    model_label = "Collectors (Gemini + Claude)"

    pipeline_state["current_agent"] = slug
    set_usage_context(agent_name=slug, phase=str(pipeline_state.get("current_phase", "")))
    _add_log(f"Starting {meta['icon']} {meta['name']} Step 1/2 [{model_label}]...")
    await broadcast(
        {
            "type": "agent_start",
            "slug": slug,
            "name": f"{meta['name']} (Step 1/2)",
            "model": model_label,
            "provider": final_provider,
            "cost": _running_cost_summary(),
        }
    )

    started = time.time()
    try:
        target_dir = output_dir or config.OUTPUT_DIR
        snapshot = await loop.run_in_executor(
            None,
            lambda: run_phase1_collectors_only(
                inputs=inputs,
                provider=final_provider,
                model=final_model,
                temperature=temperature if temperature is not None else default_conf.get("temperature", 0.4),
                max_tokens=int(default_conf.get("max_tokens", 50000)),
                output_dir=target_dir,
            ),
        )
        elapsed = time.time() - started

        # Persist snapshot into the canonical Foundation output slot so the card preview works at the gate.
        path = _output_write_path(target_dir, slug)
        path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

        if slug not in pipeline_state["completed_agents"]:
            pipeline_state["completed_agents"].append(slug)

        cost_summary = _persist_run_cost_snapshot(run_id)
        evidence_count = int(snapshot.get("evidence_count", 0))
        collector_count = int(snapshot.get("collector_count", 0))
        _add_log(
            f"Completed {meta['icon']} {meta['name']} Step 1/2 in {elapsed:.1f}s — "
            f"{collector_count} collectors, {evidence_count} evidence rows",
            "success",
        )
        await broadcast(
            {
                "type": "agent_complete",
                "slug": slug,
                "name": f"{meta['name']} (Step 1/2)",
                "elapsed": round(elapsed, 1),
                "cost": cost_summary,
                "phase1_step": "collectors_complete",
                "collector_summary": snapshot.get("collector_summary", []),
                "evidence_count": evidence_count,
                "evidence_summary": snapshot.get("evidence_summary", {}),
            }
        )

        return snapshot
    except Exception as exc:
        elapsed = time.time() - started
        pipeline_state["failed_agents"].append(slug)
        err = f"Foundation collectors step failed: {exc}"
        _add_log(f"Failed {meta['icon']} {meta['name']} Step 1/2: {exc}", "error")
        logger.exception("Foundation collectors step failed")
        await broadcast(
            {
                "type": "agent_error",
                "slug": slug,
                "name": f"{meta['name']} (Step 1/2)",
                "error": err,
                "elapsed": round(elapsed, 1),
            }
        )
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=f"{meta['name']} (Step 1/2)",
            output=None,
            elapsed=elapsed,
            error=err,
        )
        return None


async def _wait_for_agent_gate(
    completed_slug: str,
    next_slug: str,
    next_name: str,
    show_concept_selection: bool = False,
    phase: int = 0,
    gate_mode: str = "standard",
    message: str = "",
    extra_payload: dict[str, Any] | None = None,
):
    """Emit a manual review gate and wait for user approval."""
    gate_msg = message.strip() or f"{AGENT_META.get(completed_slug, {'name': completed_slug})['name']} complete"
    if not message:
        if show_concept_selection:
            gate_msg += " — select concepts and choose model for Copywriter."
        else:
            gate_msg += f" — review, choose model for {next_name}, then continue."
    _add_log(gate_msg)
    # Once an agent has completed and we're at a manual gate, there is no
    # active running agent. Clearing this avoids stale "running" state after
    # browser refresh/state sync.
    pipeline_state["current_agent"] = None
    copywriter_failed_count = 0
    if completed_slug == "copywriter":
        copywriter_failed_count = len(pipeline_state.get("copywriter_failed_jobs", []))
    pipeline_state["waiting_for_approval"] = True
    pipeline_state["phase_gate"] = asyncio.Event()
    gate_payload = {
        "type": "phase_gate",
        "completed_agent": completed_slug,
        "next_agent": next_slug,
        "next_agent_name": next_name,
        "phase": phase,
        "show_concept_selection": show_concept_selection,
        "copywriter_failed_count": copywriter_failed_count,
        "gate_mode": gate_mode,
        "message": gate_msg,
        "cost": _running_cost_summary(),
    }
    if extra_payload:
        gate_payload.update(extra_payload)
    pipeline_state["gate_info"] = gate_payload
    await broadcast(gate_payload)
    await pipeline_state["phase_gate"].wait()
    pipeline_state["waiting_for_approval"] = False
    pipeline_state["gate_info"] = None

    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    # Apply any per-agent model override the user picked at the gate
    override = pipeline_state.pop("next_agent_override", None)
    if override and isinstance(override, dict) and override.get("provider"):
        pipeline_state["model_overrides"][next_slug] = override
        logger.info("User selected model override for %s: %s", next_slug, override)

    await broadcast({"type": "phase_gate_cleared"})


async def run_pipeline_phases(
    phases: list[int],
    inputs: dict,
    provider: str | None = None,
    model: str | None = None,
    model_overrides: dict | None = None,
    brand_slug: str | None = None,
    phase1_step_review: bool = True,
):
    """Execute requested pipeline phases sequentially, gating between every agent."""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["abort_requested"] = False
    pipeline_state["model_overrides"] = _normalize_model_overrides(model_overrides)
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []
    pipeline_state["copywriter_failed_jobs"] = []
    pipeline_state["copywriter_parallel_context"] = None
    pipeline_state["copywriter_rewrite_in_progress"] = False
    pipeline_state["selected_concepts"] = []
    pipeline_state["gate_info"] = None
    pipeline_state["waiting_for_approval"] = False
    pipeline_state["phase_gate"] = None
    pipeline_state["active_brand_slug"] = brand_slug
    pipeline_state["gate_info"] = None
    pipeline_state["waiting_for_approval"] = False
    pipeline_state["phase_gate"] = None

    # Brand-scoped output directory
    output_dir = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR

    # Keep live terminal scoped to this run only.
    _reset_server_log_stream()

    # Reset the LLM cost tracker for this run
    reset_usage()
    clear_usage_context()

    # New pipeline (Phase 1 included) → clear stale branches from previous runs
    if 1 in phases and brand_slug:
        _clear_all_branches(brand_slug)

    # Reuse existing run if continuing later phases (e.g. Phase 3 after Phase 1+2)
    # Only create a new run if Phase 1 is included or no prior run exists
    existing_run_id = pipeline_state.get("run_id")
    if 1 not in phases and existing_run_id:
        run_id = existing_run_id
        logger.info("Continuing existing run #%d with phases %s", run_id, phases)
    elif 1 not in phases:
        # Server may have restarted — find the most recent run from DB
        recent = list_runs(limit=1)
        if recent:
            run_id = recent[0]["id"]
            pipeline_state["run_id"] = run_id
            logger.info("Resuming most recent run #%d with phases %s", run_id, phases)
        else:
            run_id = create_run(phases, inputs, brand_slug=brand_slug or "")
            pipeline_state["run_id"] = run_id
    else:
        run_id = create_run(phases, inputs, brand_slug=brand_slug or "")
        pipeline_state["run_id"] = run_id

    start_cost = _persist_run_cost_snapshot(run_id)
    await broadcast({
        "type": "pipeline_start",
        "phases": phases,
        "run_id": run_id,
        "brand_slug": brand_slug or "",
        "cost": start_cost,
    })

    try:
        # Pre-step: Scrape website if URL provided
        website_url = inputs.get("website_url")
        if website_url and not pipeline_state["abort_requested"]:
            _add_log(f"🌐 Scraping website: {website_url}")
            await broadcast({"type": "phase_start", "phase": 0})
            try:
                scrape_result = await loop.run_in_executor(
                    None, scrape_website, website_url, provider or "openai", model,
                )
                inputs["website_intel"] = scrape_result

                # Count what we extracted for the log
                n_testimonials = len(scrape_result.get("testimonials", []))
                n_benefits = len(scrape_result.get("key_benefits", []))
                n_claims = len(scrape_result.get("claims_made", []))
                headline = scrape_result.get("hero_headline", "")
                parts = []
                if headline:
                    parts.append(f"headline found")
                if n_testimonials:
                    parts.append(f"{n_testimonials} testimonials")
                if n_benefits:
                    parts.append(f"{n_benefits} benefits")
                if n_claims:
                    parts.append(f"{n_claims} claims")
                summary = ", ".join(parts) if parts else "basic info extracted"
                _add_log(f"✅ Website scraped — {summary}", "success")
            except Exception as e:
                _add_log(f"⚠️ Website scrape failed: {e} — continuing without it", "warning")
                logger.warning("Website scrape failed for %s: %s", website_url, e)

        # ===================================================================
        # Phase 1 — Research (Foundation Research)
        # ===================================================================
        if 1 in phases:
            pipeline_state["current_phase"] = 1
            _add_log("═══ PHASE 1 — RESEARCH ═══")
            _add_log("Foundation Research v2 started (7-pillar pipeline)", "info")
            logger.info("Foundation Research v2 started (7-pillar pipeline)")
            await broadcast({"type": "phase_start", "phase": 1})

            _add_log("Phase 1 Step 1/2 — Collecting research sources", "info")
            collectors_snapshot = await _run_foundation_collectors_step_async(
                inputs,
                loop,
                run_id,
                provider,
                model,
                output_dir=output_dir,
            )
            if not collectors_snapshot:
                error_detail = "Phase 1 collectors step failed"
                for entry in reversed(pipeline_state["log"]):
                    if "Foundation Research" in entry.get("message", "") and entry.get("level") == "error":
                        error_detail = entry["message"]
                        break
                _add_log("Phase 1 failed at collectors step", "error")
                await broadcast({"type": "pipeline_error", "message": error_detail})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            collector_summary = collectors_snapshot.get("collector_summary", [])
            if phase1_step_review:
                await _wait_for_agent_gate(
                    completed_slug="foundation_research",
                    next_slug="foundation_research",
                    next_name="Foundation Research Step 2",
                    phase=1,
                    gate_mode="phase1_collectors_review",
                    message=(
                        "Phase 1 Step 1/2 complete — review collector outputs, then continue to "
                        "cleaning, contradiction audit, synthesis, and quality gates."
                    ),
                    extra_payload={
                        "continue_label": "Continue to Phase 1 Step 2",
                        "collector_summary": collector_summary,
                        "evidence_count": int(collectors_snapshot.get("evidence_count", 0)),
                        "evidence_summary": collectors_snapshot.get("evidence_summary", {}),
                        "snapshot_available": True,
                    },
                )

                if pipeline_state["abort_requested"]:
                    raise PipelineAborted("Pipeline aborted by user")
            else:
                _add_log(
                    "Phase 1 Step 1/2 complete — auto-continuing to Step 2/2 (synthesis + QA)",
                    "info",
                )

            _add_log("Phase 1 Step 2/2 — Running synthesis, contradiction checks, and quality gates", "info")

            r1a = await _run_single_agent_async(
                "foundation_research",
                inputs,
                loop,
                run_id,
                provider,
                model,
                output_dir=output_dir,
            )

            if not r1a:
                error_detail = "Foundation Research failed (unknown reason)"
                for entry in reversed(pipeline_state["log"]):
                    if "Foundation Research" in entry.get("message", "") and entry.get("level") == "error":
                        error_detail = entry["message"]
                        break
                _add_log("Phase 1 failed — Foundation Research is required", "error")
                await broadcast({"type": "pipeline_error", "message": error_detail})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            inputs["foundation_brief"] = r1a

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Foundation Research → before Creative Engine ---
        if 1 in phases and 2 in phases:
            await _wait_for_agent_gate("foundation_research", "creative_engine", "Creative Engine", phase=1)

        # ===================================================================
        # Phase 2 — Ideation (Creative Engine)
        # ===================================================================
        if 2 in phases:
            phase2_disabled = _phase2_disabled_error()
            if phase2_disabled:
                _add_log(f"Phase 2 blocked — {phase2_disabled}", "error")
                await broadcast({"type": "pipeline_error", "message": phase2_disabled})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            pipeline_state["current_phase"] = 2
            _add_log("═══ PHASE 2 — IDEATION ═══")
            await broadcast({"type": "phase_start", "phase": 2})

            foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
            if foundation_err:
                _add_log(f"Phase 2 blocked — {foundation_err}", "error")
                await broadcast({"type": "pipeline_error", "message": foundation_err})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            r02 = await _run_single_agent_async("creative_engine", inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r02:
                _add_log("Phase 2 failed — Creative Engine is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["idea_brief"] = r02

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Creative Engine → before Copywriter (with concept selection) ---
        if 2 in phases and 3 in phases:
            await _wait_for_agent_gate("creative_engine", "copywriter", "Copywriter", show_concept_selection=True, phase=2)

        # ===================================================================
        # Phase 3 — Scripting (one agent at a time: 04 → 05 → 07)
        # ===================================================================
        if 3 in phases:
            phase3_disabled = _phase3_disabled_error()
            if phase3_disabled:
                _add_log(f"Phase 3 blocked — {phase3_disabled}", "error")
                await broadcast({"type": "pipeline_error", "message": phase3_disabled})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3})

            _auto_load_upstream(
                inputs,
                ["foundation_brief", "idea_brief"],
                sync_foundation_identity=True,
                brand_slug=brand_slug,
            )

            # Apply user's concept selections (from the Phase 2→3 gate)
            selected = pipeline_state.get("selected_concepts", [])
            if selected and inputs.get("idea_brief"):
                inputs["selected_concepts"] = selected
                _add_log(f"User selected {len(selected)} video concepts")

            # --- Copywriter ---
            r04 = await _run_copywriter_parallel_async(inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["copywriter_brief"] = r04

            # --- GATE: After Copywriter → before Hook Specialist ---
            await _wait_for_agent_gate("copywriter", "hook_specialist", "Hook Specialist", phase=3)

            # --- Hook Specialist ---
            r05 = await _run_single_agent_async("hook_specialist", inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["hook_brief"] = r05

        total = time.time() - pipeline_state["start_time"]
        complete_run(run_id, total)
        final_cost = _persist_run_cost_snapshot(run_id)
        cost_str = f"${final_cost['total_cost']:.2f}" if final_cost['total_cost'] >= 0.01 else f"${final_cost['total_cost']:.4f}"
        _add_log(f"Pipeline complete in {total:.1f}s — total cost: {cost_str}", "success")
        await broadcast({
            "type": "pipeline_complete",
            "elapsed": round(total, 1),
            "run_id": run_id,
            "cost": final_cost,
            "brand_slug": brand_slug or "",
        })

    except (PipelineAborted, asyncio.CancelledError):
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        abort_cost = _persist_run_cost_snapshot(run_id)
        _add_log(f"🛑 Pipeline aborted by user — cost so far: ${abort_cost['total_cost']:.4f}", "warning")
        logger.info("Pipeline aborted by user after %.1fs", total)
        # Shield the broadcast so it sends even though the task is cancelled
        try:
            await asyncio.shield(broadcast({
                "type": "pipeline_error",
                "message": "Pipeline aborted by user",
                "aborted": True,
                "cost": abort_cost,
            }))
        except asyncio.CancelledError:
            pass  # broadcast already sent or task fully cancelled
    except Exception as e:
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        err_cost = _persist_run_cost_snapshot(run_id)
        _add_log(f"Pipeline error: {e}", "error")
        logger.exception("Pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost})
    finally:
        clear_usage_context()
        pipeline_state["running"] = False
        pipeline_state["abort_requested"] = False
        pipeline_state["pipeline_task"] = None
        pipeline_state["current_phase"] = None
        pipeline_state["current_agent"] = None
        pipeline_state["run_id"] = None
        pipeline_state["copywriter_rewrite_in_progress"] = False
        pipeline_state["gate_info"] = None
        pipeline_state["waiting_for_approval"] = False
        pipeline_state["phase_gate"] = None


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    phases: list[int] = [1, 2, 3]
    inputs: dict = {}
    quick_mode: bool = False  # Skip web research in Phase 1 (fast testing)
    model_overrides: dict = {}  # Per-agent: {"foundation_research": {"provider": "openai", "model": "gpt-5.2"}}
    phase1_step_review: bool = False  # If true, pause after collectors-only step for manual review


@app.post("/api/run")
async def api_run(req: RunRequest):
    """Kick off pipeline phases."""
    if pipeline_state["running"]:
        return JSONResponse(
            {"error": "Pipeline is already running"}, status_code=409
        )

    if 2 in req.phases:
        phase2_disabled = _phase2_disabled_error()
        if phase2_disabled:
            return JSONResponse({"error": phase2_disabled}, status_code=400)
    if 3 in req.phases:
        phase3_disabled = _phase3_disabled_error()
        if phase3_disabled:
            return JSONResponse({"error": phase3_disabled}, status_code=400)

    inputs = {k: v for k, v in req.inputs.items() if v}

    # Brand name is required only when starting Phase 1 from brief.
    # Phase 2/3 can hydrate identity from saved Foundation Research.
    needs_brand = 1 in req.phases
    if needs_brand and not inputs.get("brand_name"):
        return JSONResponse(
            {"error": "Brand name is required"}, status_code=400
        )

    # Create or update the brand
    brand_slug = ""
    if inputs.get("brand_name"):
        brand_slug = get_or_create_brand(
            inputs["brand_name"],
            inputs.get("product_name", ""),
            inputs,
        )
        # Save brief.json to the brand's output directory
        brand_dir = _brand_output_dir(brand_slug)
        (brand_dir / "brief.json").write_text(json.dumps(inputs, indent=2, default=str), "utf-8")

    # Starting at Phase 2 without Phase 1 requires valid saved Foundation Research.
    if 2 in req.phases and 1 not in req.phases:
        preflight_inputs = dict(inputs)
        foundation_err = _ensure_foundation_for_creative_engine(preflight_inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Quick mode — use Gemini 2.5 Flash (fast, cheap, 65K output tokens)
    # and skip web research in Agent 1B
    override_provider = None
    override_model = None
    if req.quick_mode:
        inputs["_quick_mode"] = True
        override_provider = "google"
        override_model = "gemini-2.5-flash"

    model_overrides = _normalize_model_overrides(req.model_overrides if not req.quick_mode else {})
    task = asyncio.create_task(
        run_pipeline_phases(
            req.phases,
            inputs,
            override_provider,
            override_model,
            model_overrides,
            brand_slug=brand_slug,
            phase1_step_review=req.phase1_step_review,
        )
    )
    pipeline_state["pipeline_task"] = task
    return {"status": "started", "phases": req.phases, "quick_mode": req.quick_mode, "brand_slug": brand_slug}


@app.post("/api/abort")
async def api_abort():
    """Abort the currently running pipeline immediately."""
    if not pipeline_state["running"]:
        return JSONResponse({"error": "No pipeline is running"}, status_code=409)

    pipeline_state["abort_generation"] = int(pipeline_state.get("abort_generation", 0)) + 1
    pipeline_state["abort_requested"] = True
    _add_log("🛑 Abort requested — stopping pipeline now...", "warning")
    await broadcast({
        "type": "pipeline_aborting",
        "message": "Stopping pipeline...",
    })

    # Cancel the asyncio task — this interrupts the current await immediately
    task = pipeline_state.get("pipeline_task")
    if task and not task.done():
        task.cancel()

    return {"status": "aborting"}


class RerunRequest(BaseModel):
    slug: str
    inputs: dict = {}
    quick_mode: bool = False
    provider: Optional[str] = None   # Optional model override for rerun
    model: Optional[str] = None


@app.post("/api/rerun")
async def api_rerun(req: RerunRequest):
    """Rerun a single agent independently.

    Auto-loads upstream outputs from disk so the agent has the context it needs.
    Useful for retrying a failed agent without restarting the entire pipeline.
    """
    req.slug = _canonical_slug(req.slug)
    if req.slug not in AGENT_CLASSES:
        return JSONResponse(
            {"error": f"Unknown agent: {req.slug}"}, status_code=400
        )

    if req.slug == "creative_engine":
        phase2_disabled = _phase2_disabled_error()
        if phase2_disabled:
            return JSONResponse({"error": phase2_disabled}, status_code=400)
    if req.slug in {"copywriter", "hook_specialist"}:
        phase3_disabled = _phase3_disabled_error()
        if phase3_disabled:
            return JSONResponse({"error": phase3_disabled}, status_code=400)

    inputs = {k: v for k, v in req.inputs.items() if v}

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Model overrides: explicit provider/model > quick_mode > defaults
    override_provider = req.provider
    override_model = req.model
    skip_deep_research = False
    if req.quick_mode and not override_provider:
        inputs["_quick_mode"] = True
        override_provider = "google"
        override_model = "gemini-2.5-flash"
    # If user explicitly picked a non-default model for deep research agents,
    # skip deep research and use the model directly
    if override_provider and req.slug == "foundation_research":
        skip_deep_research = True

    # Auto-load upstream outputs from disk (brand-scoped)
    brand_slug = pipeline_state.get("active_brand_slug") or ""
    needed = ["foundation_brief", "idea_brief",
              "copywriter_brief", "hook_brief"]
    _auto_load_upstream(inputs, needed, sync_foundation_identity=True, brand_slug=brand_slug)

    if req.slug == "creative_engine":
        foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    # Run the single agent in a thread pool
    rerun_output_dir = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    loop = asyncio.get_event_loop()
    start = time.time()

    try:
        result = await loop.run_in_executor(
            None,
            lambda: _run_agent_sync(
                req.slug, inputs, override_provider, override_model,
                skip_deep_research, output_dir=rerun_output_dir,
            ),
        )
        elapsed = round(time.time() - start, 1)

        if result is None:
            return JSONResponse(
                {"error": f"Agent {req.slug} returned no output"}, status_code=500
            )

        # Get cost data
        cost = _running_cost_summary()

        # Save to SQLite so it persists across refreshes
        # Use current pipeline run_id, or fall back to most recent run
        run_id = pipeline_state.get("run_id")
        if not run_id:
            recent_runs = list_runs(limit=1)
            if recent_runs:
                run_id = recent_runs[0]["id"]
        if run_id:
            meta = AGENT_META.get(req.slug, {})
            save_agent_output(
                run_id=run_id,
                agent_slug=req.slug,
                agent_name=meta.get("name", req.slug),
                output=result,
                elapsed=elapsed,
            )
            update_run_cost(run_id, float(cost.get("total_cost", 0.0) or 0.0))
            logger.info("Rerun output saved to SQLite (run_id=%d)", run_id)

        return {
            "status": "completed",
            "slug": req.slug,
            "elapsed": elapsed,
            "cost": cost,
        }
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.exception("Rerun failed for %s", req.slug)
        return JSONResponse(
            {"error": str(e), "elapsed": elapsed}, status_code=500
        )


# ---------------------------------------------------------------------------
# Chat with agent output
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str    # "user" or "assistant"
    content: str

class ChatRequest(BaseModel):
    slug: str
    message: str
    history: list[ChatMessage] = []
    provider: Optional[str] = None
    model: Optional[str] = None

_CHAT_SYSTEM_TEMPLATE = """You are an AI assistant helping a user analyze and refine the output of a creative advertising pipeline agent called "{agent_name}".

## Current Agent Output (JSON)
```json
{output_json}
```

## Your Role
1. **Answer questions** about this output — summarize, explain, highlight key points.
2. **Make changes** when the user asks — add, remove, or modify content in the output.

## When Making Changes
When the user asks you to change the output, do BOTH:
- Briefly explain what you changed
- Return the FULL modified JSON wrapped in <modified_output> and </modified_output> tags

IMPORTANT: The JSON inside <modified_output> must be the COMPLETE valid output — not a partial diff. Only include these tags when the user explicitly asks for changes.

## Style
Be concise and direct. No fluff."""


@app.post("/api/chat")
async def api_chat(req: ChatRequest):
    """Chat with an agent's output — ask questions or request modifications."""
    from pipeline.llm import call_llm

    req.slug = _canonical_slug(req.slug)

    # Load the agent's output
    output = _load_output(req.slug)
    if not output:
        return JSONResponse({"error": f"No output found for {req.slug}"}, status_code=404)

    meta = AGENT_META.get(req.slug, {"name": req.slug})
    output_json = json.dumps(output, indent=2)

    # Truncate if extremely large (keep under ~80K chars for context)
    if len(output_json) > 80000:
        output_json = output_json[:80000] + "\n... (truncated)"

    system_prompt = _CHAT_SYSTEM_TEMPLATE.format(
        agent_name=meta["name"],
        output_json=output_json,
    )

    # Build conversation into user prompt (since call_llm only takes system + user)
    conversation_parts = []
    for msg in req.history[-20:]:  # Keep last 20 messages for context
        role_label = "User" if msg.role == "user" else "Assistant"
        conversation_parts.append(f"{role_label}: {msg.content}")
    conversation_parts.append(f"User: {req.message}")
    user_prompt = "\n\n".join(conversation_parts)

    # Default to Gemini 3.0 Pro for chat (1M context + strong reasoning)
    provider = req.provider or "google"
    model = req.model or "gemini-3.0-pro"

    loop = asyncio.get_event_loop()
    try:
        response_text = await loop.run_in_executor(
            None,
            lambda: call_llm(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                provider=provider,
                model=model,
                temperature=0.5,
                max_tokens=16_000,
            ),
        )
    except Exception as e:
        logger.exception("Chat LLM call failed for %s", req.slug)
        return JSONResponse({"error": str(e)}, status_code=500)

    # Check if the response contains a modified output
    modified_output = None
    display_text = response_text

    if "<modified_output>" in response_text and "</modified_output>" in response_text:
        # Extract the modified JSON
        start = response_text.index("<modified_output>") + len("<modified_output>")
        end = response_text.index("</modified_output>")
        json_str = response_text[start:end].strip()

        # Strip markdown fences if present
        if json_str.startswith("```"):
            first_nl = json_str.index("\n")
            json_str = json_str[first_nl + 1:]
        if json_str.endswith("```"):
            json_str = json_str[:-3].strip()

        try:
            modified_output = json.loads(json_str)
            # Remove the raw JSON block from the display text
            display_text = response_text[:response_text.index("<modified_output>")].strip()
            if not display_text:
                display_text = "Changes applied."
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse modified output JSON: %s", e)
            # Leave the full response as-is if parsing fails
            display_text = response_text

    result = {
        "response": display_text,
        "has_changes": modified_output is not None,
    }

    if modified_output is not None:
        result["modified_output"] = modified_output

    return result


class ChatApplyRequest(BaseModel):
    slug: str
    output: dict


@app.post("/api/chat/apply")
async def api_chat_apply(req: ChatApplyRequest):
    """Apply a modified output from a chat session — saves to disk."""
    req.slug = _canonical_slug(req.slug)
    if req.slug not in AGENT_META:
        return JSONResponse({"error": f"Unknown agent: {req.slug}"}, status_code=400)
    if not req.output:
        return JSONResponse({"error": "No output provided"}, status_code=400)

    brand_slug = pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    path = _output_write_path(base, req.slug)
    path.write_text(json.dumps(req.output, indent=2), encoding="utf-8")
    logger.info("Chat: applied modified output for %s (%d chars, brand=%s)", req.slug, len(json.dumps(req.output)), brand_slug)

    return {"ok": True, "slug": req.slug}


class ConceptSelectionRequest(BaseModel):
    """Selected video concepts to pass to the Copywriter."""
    selected: list[dict] = []  # [{angle_id, concept_index}, ...]
    model_override: dict = {}  # {"provider": "openai", "model": "gpt-5.2"}


@app.post("/api/select-concepts")
async def api_select_concepts(req: ConceptSelectionRequest):
    """Save user's video concept selections and continue pipeline."""
    pipeline_state["selected_concepts"] = req.selected
    logger.info("User selected %d video concepts", len(req.selected))

    # Store model override for the Copywriter (next agent)
    if req.model_override:
        pipeline_state["next_agent_override"] = req.model_override
        logger.info("Copywriter model override: %s", req.model_override)

    # Also trigger the phase gate continue
    if not pipeline_state.get("waiting_for_approval"):
        return JSONResponse(
            {"error": "Pipeline is not waiting for approval"}, status_code=409
        )

    gate = pipeline_state.get("phase_gate")
    if gate:
        gate.set()
    return {"status": "continued", "selected": len(req.selected)}


# ---------------------------------------------------------------------------
# Branch API Routes
# ---------------------------------------------------------------------------

class CreateBranchRequest(BaseModel):
    label: str = ""
    tof_count: int = 10
    mof_count: int = 5
    bof_count: int = 2
    matrix_cells: list[dict[str, Any]] = []
    selected_audience_segment: str = ""
    temperature: Optional[float] = None  # Custom temperature for Creative Engine
    model_overrides: dict = {}
    brand: str = ""


@app.get("/api/branches")
async def api_list_branches(brand: str = ""):
    """List all branches with their status."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    branches = _load_branches(brand_slug)
    # Enrich each branch with output availability
    for b in branches:
        bdir = _branch_output_dir(brand_slug, b["id"])
        b["available_agents"] = [
            slug for slug in ["creative_engine", "copywriter", "hook_specialist"]
            if _output_exists(bdir, slug)
        ]
        b["completed_agents"] = _normalize_slug_list(b.get("completed_agents", []))
        b["failed_agents"] = _normalize_slug_list(b.get("failed_agents", []))
    return branches


@app.post("/api/branches")
async def api_create_branch(req: CreateBranchRequest, brand: str = ""):
    """Create a new creative branch (Phase 2+ direction)."""
    brand_slug = (brand or req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)

    preflight_inputs: dict[str, Any] = {}
    foundation_err = _ensure_foundation_for_creative_engine(preflight_inputs, brand_slug=brand_slug)
    if foundation_err:
        return JSONResponse({"error": foundation_err}, status_code=400)
    foundation = preflight_inputs.get("foundation_brief", {})
    if not isinstance(foundation, dict):
        return JSONResponse({"error": "Foundation Research output not found"}, status_code=400)

    selected_audience_raw = str(req.selected_audience_segment or "").strip()
    if not selected_audience_raw:
        return JSONResponse(
            {"error": "Select one Pillar 1 audience before creating a branch."},
            status_code=400,
        )

    try:
        _, selected_audience_canonical = _resolve_audience_context(
            foundation,
            selected_audience_raw,
        )
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    _, scoped_emotion_rows, _, _, scoped_message = _extract_matrix_axes(
        foundation,
        selected_audience_segment=selected_audience_canonical,
        allow_global_legacy=False,
    )
    if not scoped_emotion_rows:
        return JSONResponse(
            {
                "error": (
                    scoped_message
                    or (
                        f"No audience-scoped emotional drivers found for "
                        f"'{selected_audience_canonical}'."
                    )
                )
            },
            status_code=400,
        )

    branches = _load_branches(brand_slug)
    branch_num = len(branches) + 1
    branch_id = f"b{branch_num}_{int(time.time())}"

    label = req.label.strip() or f"Branch {branch_num}"

    branch = {
        "id": branch_id,
        "label": label,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "inputs": {
            "tof_count": req.tof_count,
            "mof_count": req.mof_count,
            "bof_count": req.bof_count,
            "matrix_cells": req.matrix_cells if isinstance(req.matrix_cells, list) else [],
            "selected_audience_segment": selected_audience_canonical,
        },
        "temperature": req.temperature,
        "model_overrides": req.model_overrides,
        "status": "pending",
        "completed_agents": [],
        "failed_agents": [],
    }
    branches.append(branch)
    _save_branches(brand_slug, branches)
    _branch_output_dir(brand_slug, branch_id).mkdir(parents=True, exist_ok=True)

    logger.info("Created branch %s: %s (brand: %s)", branch_id, label, brand_slug)
    await broadcast({"type": "branch_created", "branch": branch})
    return branch


@app.delete("/api/branches/{branch_id}")
async def api_delete_branch(branch_id: str, brand: str = ""):
    """Delete a branch and its outputs."""
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branches = _load_branches(brand_slug)
    found = False
    branches = [b for b in branches if b["id"] != branch_id or not (found := True)]
    if not found:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    _save_branches(brand_slug, branches)

    # Remove output directory
    import shutil
    bdir = _branch_output_dir(brand_slug, branch_id)
    if bdir.exists():
        shutil.rmtree(bdir, ignore_errors=True)

    logger.info("Deleted branch %s (brand: %s)", branch_id, brand_slug)
    await broadcast({"type": "branch_deleted", "branch_id": branch_id})
    return {"ok": True, "deleted": branch_id}


class RenameBranchRequest(BaseModel):
    label: str
    brand: str = ""


@app.patch("/api/branches/{branch_id}")
async def api_rename_branch(branch_id: str, body: RenameBranchRequest, brand: str = ""):
    """Rename a branch."""
    brand_slug = (brand or body.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    _update_branch(branch_id, {"label": body.label.strip()}, brand_slug)
    return {"ok": True, "branch_id": branch_id, "label": body.label.strip()}


@app.get("/api/branches/{branch_id}/outputs/{slug}")
async def api_get_branch_output(branch_id: str, slug: str, brand: str = ""):
    """Get a specific agent's output from a branch."""
    slug = _canonical_slug(slug)
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    data = _load_branch_output(brand_slug, branch_id, slug)
    if not data:
        return JSONResponse({"error": f"No output for {slug} in branch {branch_id}"}, status_code=404)
    meta = AGENT_META.get(slug, {"name": slug, "phase": 0, "icon": ""})
    return {
        "slug": slug,
        "name": meta["name"],
        "phase": meta["phase"],
        "data": data,
        "branch_id": branch_id,
    }


class RunBranchRequest(BaseModel):
    phases: list[int] = [2]  # Which phases to run (2, or [2, 3])
    inputs: dict = {}  # Brief inputs (brand, product, etc.)
    model_overrides: dict = {}
    brand: str = ""


@app.post("/api/branches/{branch_id}/run")
async def api_run_branch(branch_id: str, req: RunBranchRequest, brand: str = ""):
    """Run Phase 2+ for a specific branch."""
    if pipeline_state["running"]:
        return JSONResponse(
            {"error": "Pipeline is already running"}, status_code=409
        )

    brand_slug = (brand or req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    inputs = {k: v for k, v in req.inputs.items() if v}

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Merge branch-level funnel counts into inputs
    branch_inputs = branch.get("inputs", {})
    inputs["tof_count"] = branch_inputs.get("tof_count", 10)
    inputs["mof_count"] = branch_inputs.get("mof_count", 5)
    inputs["bof_count"] = branch_inputs.get("bof_count", 2)
    inputs["matrix_cells"] = branch_inputs.get("matrix_cells", [])
    inputs["selected_audience_segment"] = str(branch_inputs.get("selected_audience_segment") or "").strip()

    if 2 in req.phases:
        phase2_disabled = _phase2_disabled_error()
        if phase2_disabled:
            return JSONResponse({"error": phase2_disabled}, status_code=400)

        preflight_inputs = dict(inputs)
        foundation_err = _ensure_foundation_for_creative_engine(preflight_inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    if 3 in req.phases:
        phase3_disabled = _phase3_disabled_error()
        if phase3_disabled:
            return JSONResponse({"error": phase3_disabled}, status_code=400)

    # Model overrides: request-level > branch-level > none
    model_overrides = _normalize_model_overrides(req.model_overrides or branch.get("model_overrides", {}))

    phases = req.phases

    task = asyncio.create_task(
        run_branch_pipeline(branch_id, phases, inputs, model_overrides, brand_slug=brand_slug)
    )
    pipeline_state["pipeline_task"] = task
    return {"status": "started", "branch_id": branch_id, "phases": phases}


class Phase3V2RunRequest(BaseModel):
    brand: str = ""
    pilot_size: int = config.PHASE3_V2_DEFAULT_PILOT_SIZE
    selected_brief_unit_ids: list[str] = Field(default_factory=list)
    ab_mode: bool = False
    sdk_toggles: dict[str, Any] = Field(default_factory=dict)
    reviewer_role: str = config.PHASE3_V2_REVIEWER_ROLE_DEFAULT
    model_overrides: dict[str, Any] = Field(default_factory=dict)


class Phase3V2ReviewPayload(BaseModel):
    brief_unit_id: str
    arm: str
    reviewer_id: str = ""
    quality_score_1_10: int
    decision: str
    notes: str = ""


class Phase3V2ReviewRequest(BaseModel):
    run_id: str
    brand: str = ""
    reviewer_role: str = ""
    reviews: list[Phase3V2ReviewPayload] = Field(default_factory=list)


class Phase3V2DecisionPayload(BaseModel):
    brief_unit_id: str
    arm: str
    decision: str
    reviewer_id: str = ""


class Phase3V2DecisionRequest(BaseModel):
    run_id: str
    brand: str = ""
    reviewer_role: str = ""
    decisions: list[Phase3V2DecisionPayload] = Field(default_factory=list)


class Phase3V2DraftLinePayload(BaseModel):
    line_id: str = ""
    text: str
    evidence_ids: list[str] = Field(default_factory=list)


class Phase3V2DraftUpdateRequest(BaseModel):
    brand: str = ""
    sections: CoreScriptSectionsV1 | None = None
    lines: list[Phase3V2DraftLinePayload] = Field(default_factory=list)
    source: Literal["manual", "chat_apply"] = "manual"


class Phase3V2ChatRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    message: str


class Phase3V2ChatApplyRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    proposed_draft: CoreScriptGeneratedV1


class Phase3V2FinalLockRequest(BaseModel):
    brand: str = ""
    reviewer_role: str = ""


class Phase3V2HookRunRequest(BaseModel):
    brand: str = ""
    selected_brief_unit_ids: list[str] = Field(default_factory=list)
    candidate_target_per_unit: int | None = None
    final_variants_per_unit: int | None = None
    model_overrides: dict[str, Any] = Field(default_factory=dict)


class Phase3V2HookSelectionPayload(BaseModel):
    brief_unit_id: str
    arm: str
    selected_hook_ids: list[str] = Field(default_factory=list)
    selected_hook_id: str = ""
    skip: bool = False


class Phase3V2HookSelectionRequest(BaseModel):
    brand: str = ""
    selections: list[Phase3V2HookSelectionPayload] = Field(default_factory=list)


class Phase3V2HookUpdateRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    verbal_open: str
    visual_pattern_interrupt: str = ""
    on_screen_text: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    source: Literal["manual", "chat_apply"] = "manual"


class Phase3V2HookProposedPayload(BaseModel):
    verbal_open: str
    visual_pattern_interrupt: str = ""
    on_screen_text: str = ""
    evidence_ids: list[str] = Field(default_factory=list)


class Phase3V2HookChatRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    message: str


class Phase3V2HookChatReply(BaseModel):
    assistant_message: str
    proposed_hook: Phase3V2HookProposedPayload | None = None


class Phase3V2HookChatApplyRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    proposed_hook: Phase3V2HookProposedPayload


class Phase3V2SceneRunRequest(BaseModel):
    brand: str = ""
    selected_brief_unit_ids: list[str] = Field(default_factory=list)
    model_overrides: dict[str, Any] = Field(default_factory=dict)


class Phase3V2SceneLinePayload(BaseModel):
    scene_line_id: str = ""
    script_line_id: str
    source_script_line_id: str = ""
    beat_index: int = 1
    beat_text: str = ""
    mode: Literal["a_roll", "b_roll", "animation_broll"]
    narration_line: str = ""
    scene_description: str = ""
    # Legacy compatibility fields (read path only; non-authoritative).
    a_roll: dict[str, Any] = Field(default_factory=dict)
    b_roll: dict[str, Any] = Field(default_factory=dict)
    on_screen_text: str = ""
    duration_seconds: float = 2.0
    evidence_ids: list[str] = Field(default_factory=list)
    difficulty_1_10: int = 5


class Phase3V2SceneUpdateRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    lines: list[Phase3V2SceneLinePayload] = Field(default_factory=list)
    source: Literal["manual", "chat_apply"] = "manual"


class Phase3V2SceneChatRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    message: str


class Phase3V2SceneChatApplyRequest(BaseModel):
    brand: str = ""
    brief_unit_id: str
    arm: str
    hook_id: str
    proposed_scene_plan: ScenePlanV1


def _phase3_v2_upsert_manifest_entry(brand_slug: str, branch_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    runs = _load_phase3_v2_runs_manifest(brand_slug, branch_id)
    run_id = str(entry.get("run_id") or "")
    if not run_id:
        raise ValueError("Manifest entry requires run_id")
    updated = False
    for idx, row in enumerate(runs):
        if str(row.get("run_id") or "") == run_id:
            merged = dict(row)
            merged.update(entry)
            runs[idx] = merged
            entry = merged
            updated = True
            break
    if not updated:
        runs.append(entry)
    runs.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    _save_phase3_v2_runs_manifest(brand_slug, branch_id, runs)
    return entry


def _phase3_v2_reconcile_orphaned_running_entry(
    brand_slug: str,
    branch_id: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    """Mark orphaned `running` entries failed after process/task interruptions."""
    if not isinstance(entry, dict):
        return entry
    if str(entry.get("status") or "").strip().lower() != "running":
        return entry

    run_id = str(entry.get("run_id") or "").strip()
    if not run_id:
        return entry
    task_key = f"{brand_slug}:{branch_id}:{run_id}"
    task = phase3_v2_tasks.get(task_key)
    if task is not None and not task.done():
        return entry

    patched = dict(entry)
    patched["status"] = "failed"
    patched.setdefault("completed_at", datetime.now().isoformat())
    patched.setdefault("error", "Run interrupted before completion.")
    return _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, patched)


def _phase3_v2_reconcile_orphaned_hook_stage(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    run_row: dict[str, Any],
) -> HookStageManifestV1:
    manifest = _phase3_v2_load_hook_stage_manifest(brand_slug, branch_id, run_id)
    if manifest.status != "running":
        return manifest

    task_key = f"{brand_slug}:{branch_id}:{run_id}:hooks"
    task = phase3_v2_hook_tasks.get(task_key)
    if task is not None and not task.done():
        return manifest

    patched = manifest.model_copy()
    patched.status = "failed"
    if not patched.error:
        patched.error = "Hook stage interrupted before completion."
    if not patched.completed_at:
        patched.completed_at = datetime.now().isoformat()
    _phase3_v2_save_hook_stage_manifest(brand_slug, branch_id, run_id, patched)
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "hook_stage_status": patched.status,
            "hook_stage_error": patched.error,
            "hook_run_id": patched.hook_run_id,
        },
    )
    return patched


def _phase3_v2_reconcile_orphaned_scene_stage(
    brand_slug: str,
    branch_id: str,
    run_id: str,
    run_row: dict[str, Any],
) -> SceneStageManifestV1:
    manifest = _phase3_v2_load_scene_stage_manifest(brand_slug, branch_id, run_id)
    if manifest.status != "running":
        return manifest

    task_key = f"{brand_slug}:{branch_id}:{run_id}:scenes"
    task = phase3_v2_scene_tasks.get(task_key)
    if task is not None and not task.done():
        return manifest

    patched = manifest.model_copy()
    patched.status = "failed"
    if not patched.error:
        patched.error = "Scene stage interrupted before completion."
    if not patched.completed_at:
        patched.completed_at = datetime.now().isoformat()
    _phase3_v2_save_scene_stage_manifest(brand_slug, branch_id, run_id, patched)
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "scene_stage_status": patched.status,
            "scene_stage_error": patched.error,
            "scene_run_id": patched.scene_run_id,
        },
    )
    return patched


def _phase3_v2_collect_run_detail(
    brand_slug: str,
    branch_id: str,
    run_id: str,
) -> dict[str, Any] | None:
    runs = _load_phase3_v2_runs_manifest(brand_slug, branch_id)
    run_row = next((r for r in runs if str(r.get("run_id") or "") == run_id), None)
    if not isinstance(run_row, dict):
        return None
    run_row = _phase3_v2_reconcile_orphaned_running_entry(brand_slug, branch_id, run_row)
    run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
    brief_units = _phase3_v2_read_json(run_dir / "brief_units.json", [])
    evidence_packs = _phase3_v2_read_json(run_dir / "evidence_packs.json", [])
    reviews = _phase3_v2_read_json(run_dir / "reviews.json", [])
    summary = _phase3_v2_read_json(run_dir / "summary.json", {})
    drafts_by_arm: dict[str, Any] = {}
    arms = run_row.get("arms", [])
    if isinstance(arms, list):
        for arm in arms:
            arm_name = str(arm)
            drafts_by_arm[arm_name] = _phase3_v2_read_json(
                run_dir / _phase3_v2_arm_file_name(arm_name), []
            )
    if not isinstance(arms, list) or not arms:
        arms = list(drafts_by_arm.keys())
    decisions = _phase3_v2_load_decisions(brand_slug, branch_id, run_id)
    final_lock = _phase3_v2_load_final_lock(brand_slug, branch_id, run_id)
    progress = _phase3_v2_compute_decision_progress(
        brief_units if isinstance(brief_units, list) else [],
        [str(a) for a in arms],
        decisions,
    )
    hook_stage = _phase3_v2_reconcile_orphaned_hook_stage(brand_slug, branch_id, run_id, run_row)
    scene_stage = _phase3_v2_reconcile_orphaned_scene_stage(brand_slug, branch_id, run_id, run_row)
    hook_candidates_by_arm: dict[str, list[dict[str, Any]]] = {}
    hook_gate_reports_by_arm: dict[str, list[dict[str, Any]]] = {}
    hook_scores_by_arm: dict[str, list[dict[str, Any]]] = {}
    for arm in [str(a) for a in arms]:
        hook_candidates_by_arm[arm] = _phase3_v2_read_json(
            _phase3_v2_hook_candidates_path(brand_slug, branch_id, run_id, arm),
            [],
        )
        hook_gate_reports_by_arm[arm] = _phase3_v2_read_json(
            _phase3_v2_hook_gate_reports_path(brand_slug, branch_id, run_id, arm),
            [],
        )
        hook_scores_by_arm[arm] = _phase3_v2_read_json(
            _phase3_v2_hook_scores_path(brand_slug, branch_id, run_id, arm),
            [],
        )
    hook_bundles_by_arm = _phase3_v2_load_hook_bundles_by_arm(
        brand_slug,
        branch_id,
        run_id,
        [str(a) for a in arms],
    )
    hook_selections = _phase3_v2_load_hook_selections(brand_slug, branch_id, run_id)
    hook_eligibility = _phase3_v2_build_hook_eligibility(
        {
            "brief_units": brief_units,
            "drafts_by_arm": drafts_by_arm,
            "decisions": [d.model_dump() for d in decisions],
            "evidence_packs": evidence_packs,
            "run": {"arms": [str(a) for a in arms]},
        }
    )
    hook_selection_progress = _phase3_v2_compute_hook_selection_progress(
        hook_eligibility=hook_eligibility,
        selections=hook_selections,
    )
    scene_handoff_raw = _phase3_v2_read_json(
        _phase3_v2_scene_handoff_path(brand_slug, branch_id, run_id),
        {},
    )
    try:
        scene_handoff = SceneHandoffPacketV1.model_validate(scene_handoff_raw).model_dump()
    except Exception:
        scene_handoff = SceneHandoffPacketV1(
            run_id=run_id,
            hook_run_id=hook_stage.hook_run_id,
            ready=False,
            ready_count=0,
            total_required=hook_selection_progress.get("total_required", 0),
            generated_at="",
            items=[],
        ).model_dump()
    scene_plans_by_arm = _phase3_v2_load_scene_plans_by_arm(
        brand_slug,
        branch_id,
        run_id,
        [str(a) for a in arms],
    )
    scene_gate_reports_by_arm = _phase3_v2_load_scene_gate_reports_by_arm(
        brand_slug,
        branch_id,
        run_id,
        [str(a) for a in arms],
    )
    normalized_scene_handoff = {
        **(scene_handoff if isinstance(scene_handoff, dict) else {}),
        "run_id": run_id,
        "scene_run_id": scene_stage.scene_run_id,
    }
    production_handoff_packet = _phase3_v2_build_production_handoff_from_scene_state(
        run_id=run_id,
        scene_run_id=scene_stage.scene_run_id,
        scene_handoff_packet=normalized_scene_handoff,
        scene_plans_by_arm=scene_plans_by_arm,
        scene_gate_reports_by_arm=scene_gate_reports_by_arm,
    )
    scene_progress = _phase3_v2_compute_scene_progress(
        scene_handoff_packet=normalized_scene_handoff,
        scene_plans_by_arm=scene_plans_by_arm,
        scene_gate_reports_by_arm=scene_gate_reports_by_arm,
    )
    return {
        "run": run_row,
        "brief_units": brief_units,
        "evidence_packs": evidence_packs,
        "drafts_by_arm": drafts_by_arm,
        "reviews": reviews,
        "summary": summary,
        "decisions": [d.model_dump() for d in decisions],
        "decision_progress": progress.model_dump(),
        "final_lock": final_lock.model_dump(),
        "hook_stage": hook_stage.model_dump(),
        "scene_stage": scene_stage.model_dump(),
        "hook_eligibility": hook_eligibility,
        "hook_candidates_by_arm": hook_candidates_by_arm,
        "hook_gate_reports_by_arm": hook_gate_reports_by_arm,
        "hook_scores_by_arm": hook_scores_by_arm,
        "hook_bundles_by_arm": hook_bundles_by_arm,
        "hook_selections": [row.model_dump() for row in hook_selections],
        "hook_selection_progress": hook_selection_progress,
        "scene_plans_by_arm": scene_plans_by_arm,
        "scene_gate_reports_by_arm": scene_gate_reports_by_arm,
        "scene_progress": scene_progress,
        "scene_handoff_packet": scene_handoff,
        "scene_handoff_ready": bool(hook_selection_progress.get("ready")),
        "production_handoff_packet": production_handoff_packet.model_dump(),
        "production_handoff_ready": bool(production_handoff_packet.ready),
    }


def _phase3_v2_get_run_arms(detail: dict[str, Any]) -> list[str]:
    arms_raw = detail.get("run", {}).get("arms", []) if isinstance(detail.get("run"), dict) else []
    arms: list[str] = []
    if isinstance(arms_raw, list):
        arms = [str(v).strip() for v in arms_raw if str(v or "").strip()]
    if not arms:
        drafts_by_arm = detail.get("drafts_by_arm", {})
        if isinstance(drafts_by_arm, dict):
            arms = [str(v).strip() for v in drafts_by_arm.keys() if str(v or "").strip()]
    return arms


def _phase3_v2_mutation_locked_response(run_id: str) -> JSONResponse:
    return JSONResponse(
        {"error": f"Run {run_id} is final locked and read-only."},
        status_code=409,
    )


def _phase3_v2_find_draft(detail: dict[str, Any], arm: str, brief_unit_id: str) -> dict[str, Any] | None:
    drafts_by_arm = detail.get("drafts_by_arm", {})
    if not isinstance(drafts_by_arm, dict):
        return None
    rows = drafts_by_arm.get(arm, [])
    if not isinstance(rows, list):
        return None
    return next(
        (
            row for row in rows
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        None,
    )


def _phase3_v2_find_brief_unit(detail: dict[str, Any], brief_unit_id: str) -> dict[str, Any] | None:
    rows = detail.get("brief_units", [])
    if not isinstance(rows, list):
        return None
    return next(
        (
            row for row in rows
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        None,
    )


def _phase3_v2_find_evidence_pack(detail: dict[str, Any], brief_unit_id: str) -> dict[str, Any] | None:
    rows = detail.get("evidence_packs", [])
    if not isinstance(rows, list):
        return None
    return next(
        (
            row for row in rows
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        None,
    )


def _phase3_v2_find_hook_bundle(detail: dict[str, Any], arm: str, brief_unit_id: str) -> dict[str, Any] | None:
    bundles_by_arm = detail.get("hook_bundles_by_arm", {})
    if not isinstance(bundles_by_arm, dict):
        return None
    rows = bundles_by_arm.get(arm, [])
    if not isinstance(rows, list):
        return None
    return next(
        (
            row for row in rows
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        None,
    )


def _phase3_v2_find_hook_variant(
    detail: dict[str, Any],
    arm: str,
    brief_unit_id: str,
    hook_id: str,
) -> dict[str, Any] | None:
    bundle = _phase3_v2_find_hook_bundle(detail, arm, brief_unit_id)
    if not isinstance(bundle, dict):
        return None
    variants = bundle.get("variants", [])
    if not isinstance(variants, list):
        return None
    target_hook_id = str(hook_id or "").strip()
    return next(
        (
            row for row in variants
            if isinstance(row, dict) and str(row.get("hook_id") or "").strip() == target_hook_id
        ),
        None,
    )


def _phase3_v2_find_scene_plan(
    detail: dict[str, Any],
    arm: str,
    brief_unit_id: str,
    hook_id: str,
) -> dict[str, Any] | None:
    rows_by_arm = detail.get("scene_plans_by_arm", {})
    if not isinstance(rows_by_arm, dict):
        return None
    rows = rows_by_arm.get(arm, [])
    if not isinstance(rows, list):
        return None
    target_hook = str(hook_id or "").strip()
    return next(
        (
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
            and str(row.get("hook_id") or "").strip() == target_hook
        ),
        None,
    )


def _phase3_v2_sections_from_lines(lines: list[CoreScriptLineV1]) -> CoreScriptSectionsV1:
    texts = [str(line.text or "").strip() for line in lines if str(line.text or "").strip()]
    if not texts:
        texts = ["Updated script line"]

    def _pick(*indices: int) -> str:
        for idx in indices:
            if 0 <= idx < len(texts):
                return texts[idx]
        return texts[-1]

    return CoreScriptSectionsV1(
        hook=_pick(0),
        problem=_pick(1, 0),
        mechanism=_pick(2, 1, 0),
        proof=_pick(3, 2, 1, 0),
        cta=_pick(len(texts) - 1, 3, 2, 1, 0),
    )


def _phase3_v2_normalize_lines(lines: list[Phase3V2DraftLinePayload]) -> list[CoreScriptLineV1]:
    normalized: list[CoreScriptLineV1] = []
    for row in lines:
        text = str(row.text or "").strip()
        if not text:
            continue
        evidence_ids: list[str] = []
        for raw_id in row.evidence_ids or []:
            eid = str(raw_id or "").strip()
            if eid:
                evidence_ids.append(eid)
        normalized.append(
            CoreScriptLineV1(
                line_id=f"L{len(normalized) + 1:02d}",
                text=text,
                evidence_ids=evidence_ids,
            )
        )
    return normalized


def _phase3_v2_update_draft_for_unit(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arm: str,
    brief_unit_id: str,
    sections: CoreScriptSectionsV1,
    lines: list[CoreScriptLineV1],
    source: str,
) -> dict[str, Any] | None:
    run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
    arm_file = run_dir / _phase3_v2_arm_file_name(arm)
    rows = _phase3_v2_read_json(arm_file, [])
    if not isinstance(rows, list):
        rows = []

    idx = next(
        (
            i for i, row in enumerate(rows)
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        -1,
    )
    existing: dict[str, Any] = {}
    if idx >= 0 and isinstance(rows[idx], dict):
        existing = dict(rows[idx])

    model_metadata = dict(existing.get("model_metadata") or {})
    model_metadata["edited_source"] = source
    model_metadata["edited_at"] = datetime.now().isoformat()
    model_metadata["sdk_used"] = True
    if not model_metadata.get("provider"):
        model_metadata["provider"] = "anthropic"
    if not model_metadata.get("model"):
        model_metadata["model"] = config.ANTHROPIC_FRONTIER

    updated_row: dict[str, Any] = {
        "script_id": str(existing.get("script_id") or f"script_{brief_unit_id}_{arm}"),
        "brief_unit_id": brief_unit_id,
        "arm": arm,
        "sections": sections.model_dump(),
        "lines": [line.model_dump() for line in lines],
        "model_metadata": model_metadata,
        "gate_report": {
            "overall_pass": True,
            "checks": [],
            "edited_source": source,
        },
        "status": "ok",
        "error": "",
        "latency_seconds": float(existing.get("latency_seconds", 0.0) or 0.0),
        "cost_usd": float(existing.get("cost_usd", 0.0) or 0.0),
    }
    if idx >= 0:
        rows[idx] = updated_row
    else:
        rows.append(updated_row)
    _phase3_v2_write_json(arm_file, rows)
    _phase3_v2_mark_hook_selections_stale_for_unit(
        brand_slug,
        branch_id,
        run_id,
        brief_unit_id,
        arm,
        "script_updated_after_hook_run",
    )
    _phase3_v2_mark_scene_plans_stale_for_unit(
        brand_slug,
        branch_id,
        run_id,
        brief_unit_id,
        arm,
        "script_updated_after_scene_run",
    )
    return updated_row


def _phase3_v2_normalize_hook_evidence_ids(raw_ids: list[str] | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw_id in raw_ids or []:
        value = str(raw_id or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _phase3_v2_safe_scene_token(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_")
    return cleaned or "L00"


_PHASE3_V2_BEAT_LINE_RE = re.compile(r"^(?P<source>[A-Za-z0-9_]+)\.(?P<index>\d+)$")


def _phase3_v2_scene_line_lineage(script_line_id: str) -> tuple[str, int]:
    raw = str(script_line_id or "").strip()
    if not raw:
        return "", 1
    match = _PHASE3_V2_BEAT_LINE_RE.match(raw)
    if not match:
        return raw, 1
    source = str(match.group("source") or "").strip() or raw
    try:
        beat_index = max(1, int(match.group("index") or 1))
    except Exception:
        beat_index = 1
    return source, beat_index


def _phase3_v2_normalize_scene_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    if mode == "a_roll":
        return "a_roll"
    if mode == "animation_broll":
        return "animation_broll"
    return "b_roll"


def normalize_scene_mode(value: Any) -> str:
    return _phase3_v2_normalize_scene_mode(value)


def _phase3_v2_is_a_roll_mode(value: Any) -> bool:
    return _phase3_v2_normalize_scene_mode(value) == "a_roll"


def _phase3_v2_is_b_roll_mode(value: Any) -> bool:
    return _phase3_v2_normalize_scene_mode(value) in {"b_roll", "animation_broll"}


def _phase3_v2_scene_line_id(brief_unit_id: str, hook_id: str, script_line_id: str) -> str:
    return f"sl_{brief_unit_id}_{hook_id}_{_phase3_v2_safe_scene_token(script_line_id)}"


def _phase3_v2_scene_payload_from_row_dict(row: dict[str, Any]) -> Phase3V2SceneLinePayload:
    payload = row if isinstance(row, dict) else {}
    return Phase3V2SceneLinePayload(
        scene_line_id=str(payload.get("scene_line_id") or "").strip(),
        script_line_id=str(payload.get("script_line_id") or "").strip(),
        source_script_line_id=str(payload.get("source_script_line_id") or "").strip(),
        beat_index=max(1, int(payload.get("beat_index") or 1)),
        beat_text=str(payload.get("beat_text") or "").strip(),
        mode=normalize_scene_mode(payload.get("mode")),
        narration_line=str(payload.get("narration_line") or "").strip(),
        scene_description=str(payload.get("scene_description") or "").strip(),
        a_roll=payload.get("a_roll") if isinstance(payload.get("a_roll"), dict) else {},
        b_roll=payload.get("b_roll") if isinstance(payload.get("b_roll"), dict) else {},
        on_screen_text=str(payload.get("on_screen_text") or "").strip(),
        duration_seconds=max(0.1, min(30.0, float(payload.get("duration_seconds") or 2.0))),
        evidence_ids=[
            str(v).strip() for v in (payload.get("evidence_ids") if isinstance(payload.get("evidence_ids"), list) else [])
            if str(v or "").strip()
        ],
        difficulty_1_10=max(1, min(10, int(payload.get("difficulty_1_10") or 5))),
    )


def _phase3_v2_first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _phase3_v2_default_scene_description(mode: Any, narration_line: str) -> str:
    normalized_mode = normalize_scene_mode(mode)
    narration = str(narration_line or "").strip()
    if normalized_mode == "a_roll":
        return f"On-camera delivery: {narration or 'Speak directly to camera with clear intent.'}"
    if normalized_mode == "animation_broll":
        return f"Animation direction: {narration or 'Use animation to visualize this beat clearly.'}"
    return f"B-roll direction: {narration or 'Show a practical visual action that matches this beat.'}"


def derive_scene_description(mode: Any, legacy_fields: dict[str, Any] | None = None) -> str:
    normalized_mode = normalize_scene_mode(mode)
    fields = legacy_fields if isinstance(legacy_fields, dict) else {}
    explicit = _phase3_v2_first_non_empty(fields.get("scene_description"))
    if explicit:
        return explicit
    a_roll = fields.get("a_roll") if isinstance(fields.get("a_roll"), dict) else {}
    b_roll = fields.get("b_roll") if isinstance(fields.get("b_roll"), dict) else {}
    if normalized_mode == "a_roll":
        return _phase3_v2_first_non_empty(
            a_roll.get("creator_action"),
            a_roll.get("framing"),
            a_roll.get("performance_direction"),
            a_roll.get("product_interaction"),
            fields.get("on_screen_text"),
        )
    return _phase3_v2_first_non_empty(
        b_roll.get("shot_description"),
        b_roll.get("subject_action"),
        a_roll.get("creator_action"),
        fields.get("on_screen_text"),
    )


def _phase3_v2_compat_direction_blocks(
    mode: Any,
    scene_description: str,
) -> tuple[ARollDirectionV1 | None, BRollDirectionV1 | None]:
    normalized_mode = normalize_scene_mode(mode)
    description = str(scene_description or "").strip()
    if normalized_mode == "a_roll":
        return (
            ARollDirectionV1(
                framing="Talking-head medium close-up",
                creator_action=description,
                performance_direction="Conversational and grounded.",
                product_interaction="",
                location="",
            ),
            None,
        )
    return (
        None,
        BRollDirectionV1(
            shot_description=description,
            subject_action="",
        ),
    )


def to_canonical_scene_line(
    *,
    brief_unit_id: str,
    hook_id: str,
    row: Phase3V2SceneLinePayload,
) -> SceneLinePlanV1:
    script_line_id = str(row.script_line_id or "").strip()
    inferred_source_id, inferred_beat_index = _phase3_v2_scene_line_lineage(script_line_id)
    source_script_line_id = str(row.source_script_line_id or "").strip() or inferred_source_id
    if "." in script_line_id:
        source_script_line_id = inferred_source_id
    try:
        beat_index = max(1, int(row.beat_index or inferred_beat_index))
    except Exception:
        beat_index = inferred_beat_index
    if "." in script_line_id:
        beat_index = inferred_beat_index
    beat_text = str(row.beat_text or "").strip()
    mode = normalize_scene_mode(row.mode)
    evidence_ids = _phase3_v2_normalize_hook_evidence_ids(list(row.evidence_ids or []))
    narration_line = _phase3_v2_first_non_empty(
        row.narration_line,
        beat_text,
    )
    scene_description = _phase3_v2_first_non_empty(
        row.scene_description,
        derive_scene_description(
            mode,
            {
                "a_roll": row.a_roll,
                "b_roll": row.b_roll,
                "on_screen_text": row.on_screen_text,
            },
        ),
        _phase3_v2_default_scene_description(mode, narration_line),
    )
    a_roll = None
    b_roll = None
    if _phase3_v2_is_a_roll_mode(mode):
        try:
            a_roll = ARollDirectionV1.model_validate(row.a_roll or {})
        except Exception:
            a_roll = None
        if not a_roll:
            a_roll, _ = _phase3_v2_compat_direction_blocks(mode, scene_description)
    else:
        try:
            b_roll = BRollDirectionV1.model_validate(row.b_roll or {})
        except Exception:
            b_roll = None
        if not b_roll:
            _, b_roll = _phase3_v2_compat_direction_blocks(mode, scene_description)

    return SceneLinePlanV1(
        scene_line_id=_phase3_v2_scene_line_id(brief_unit_id, hook_id, script_line_id),
        script_line_id=script_line_id,
        source_script_line_id=source_script_line_id,
        beat_index=beat_index,
        beat_text=beat_text,
        mode=mode,
        narration_line=narration_line,
        scene_description=scene_description,
        a_roll=a_roll,
        b_roll=b_roll,
        on_screen_text=str(row.on_screen_text or "").strip(),
        duration_seconds=max(0.1, min(30.0, float(row.duration_seconds or 2.0))),
        evidence_ids=evidence_ids,
        difficulty_1_10=max(1, min(10, int(row.difficulty_1_10 or 5))),
    )


def _phase3_v2_enforce_no_adjacent_a_roll(lines: list[SceneLinePlanV1]) -> list[SceneLinePlanV1]:
    out: list[SceneLinePlanV1] = []
    previous_mode = ""
    for line in lines:
        current = line
        normalized_mode = normalize_scene_mode(current.mode)
        if previous_mode == "a_roll" and normalized_mode == "a_roll":
            forced_mode = "b_roll"
            forced_description = _phase3_v2_default_scene_description(
                forced_mode,
                _phase3_v2_first_non_empty(current.narration_line, current.beat_text),
            )
            _, forced_b_roll = _phase3_v2_compat_direction_blocks(forced_mode, forced_description)
            current = current.model_copy(
                update={
                    "mode": forced_mode,
                    "scene_description": forced_description,
                    "a_roll": None,
                    "b_roll": forced_b_roll,
                }
            )
            normalized_mode = forced_mode
        out.append(current)
        previous_mode = normalized_mode
    return out


def _phase3_v2_normalize_scene_lines(
    *,
    brief_unit_id: str,
    hook_id: str,
    lines: list[Phase3V2SceneLinePayload],
) -> list[SceneLinePlanV1]:
    normalized: list[SceneLinePlanV1] = []
    for row in lines:
        script_line_id = str(row.script_line_id or "").strip()
        if not script_line_id:
            continue
        normalized.append(
            to_canonical_scene_line(
                brief_unit_id=brief_unit_id,
                hook_id=hook_id,
                row=row,
            )
        )
    return _phase3_v2_enforce_no_adjacent_a_roll(normalized)


def _phase3_v2_scene_sequence_metrics(lines: list[SceneLinePlanV1]) -> tuple[float, int, int, int]:
    total_duration = round(sum(float(row.duration_seconds or 0.0) for row in lines), 3)
    a_roll_count = sum(1 for row in lines if _phase3_v2_is_a_roll_mode(row.mode))
    b_roll_count = sum(1 for row in lines if _phase3_v2_is_b_roll_mode(row.mode))
    max_consecutive = 0
    streak = 0
    last_mode = ""
    for row in lines:
        mode = "a_roll" if _phase3_v2_is_a_roll_mode(row.mode) else "b_roll"
        if mode == last_mode:
            streak += 1
        else:
            streak = 1
            last_mode = mode
        if streak > max_consecutive:
            max_consecutive = streak
    return total_duration, a_roll_count, b_roll_count, max_consecutive


def _phase3_v2_update_scene_plan_for_unit(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arm: str,
    brief_unit_id: str,
    hook_id: str,
    lines: list[SceneLinePlanV1],
    source: str,
) -> dict[str, Any] | None:
    path = _phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, arm)
    rows = _phase3_v2_read_json(path, [])
    if not isinstance(rows, list):
        rows = []

    idx = next(
        (
            i
            for i, row in enumerate(rows)
            if isinstance(row, dict)
            and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
            and str(row.get("hook_id") or "").strip() == hook_id
        ),
        -1,
    )
    if idx < 0:
        return None
    existing = rows[idx] if isinstance(rows[idx], dict) else {}
    normalized_lines = _phase3_v2_enforce_no_adjacent_a_roll(list(lines or []))
    total_duration, a_roll_count, b_roll_count, max_consecutive = _phase3_v2_scene_sequence_metrics(normalized_lines)
    updated = dict(existing)
    updated["scene_plan_id"] = str(existing.get("scene_plan_id") or f"sp_{brief_unit_id}_{hook_id}_{arm}")
    updated["run_id"] = str(existing.get("run_id") or run_id)
    updated["brief_unit_id"] = brief_unit_id
    updated["arm"] = arm
    updated["hook_id"] = hook_id
    updated["lines"] = [line.model_dump() for line in normalized_lines]
    updated["total_duration_seconds"] = total_duration
    updated["a_roll_line_count"] = a_roll_count
    updated["b_roll_line_count"] = b_roll_count
    updated["max_consecutive_mode"] = max_consecutive
    updated["status"] = "ok"
    updated["stale"] = False
    updated["stale_reason"] = ""
    updated["error"] = ""
    updated["generated_at"] = datetime.now().isoformat()
    updated["edited_source"] = source
    updated["edited_at"] = datetime.now().isoformat()
    rows[idx] = updated
    _phase3_v2_write_json(path, rows)
    return updated


def _phase3_v2_manual_scene_gate_report(plan: dict[str, Any]) -> dict[str, Any]:
    lines = plan.get("lines", []) if isinstance(plan.get("lines"), list) else []
    mode_pass = all(
        isinstance(row, dict)
        and str(row.get("mode") or "").strip() in {"a_roll", "b_roll", "animation_broll"}
        and str(row.get("script_line_id") or "").strip()
        and str(row.get("scene_description") or "").strip()
        for row in lines
    )
    duration_sanity_pass = all(
        isinstance(row, dict) and float(row.get("duration_seconds") or 0.0) >= 0.1
        for row in lines
    )
    pacing_pass = True
    failing_line_ids: list[str] = []
    previous_mode = ""
    for row in lines:
        if not isinstance(row, dict):
            continue
        mode = normalize_scene_mode(row.get("mode"))
        script_line_id = str(row.get("script_line_id") or "").strip()
        if previous_mode == "a_roll" and mode == "a_roll":
            pacing_pass = False
            if script_line_id:
                failing_line_ids.append(script_line_id)
        previous_mode = mode
    line_coverage_pass = bool(lines)
    overall_pass = bool(line_coverage_pass and mode_pass and pacing_pass and duration_sanity_pass)
    failure_reasons: list[str] = []
    if not line_coverage_pass:
        failure_reasons.append("line_coverage_failed")
    if not mode_pass:
        failure_reasons.append("mode_invalid_or_missing_scene_description")
    if not pacing_pass:
        failure_reasons.append("adjacent_a_roll_failed")
    if not duration_sanity_pass:
        failure_reasons.append("duration_sanity_failed")
    return {
        "scene_plan_id": str(plan.get("scene_plan_id") or ""),
        "scene_unit_id": f"su_{str(plan.get('brief_unit_id') or '')}_{str(plan.get('hook_id') or '')}",
        "run_id": str(plan.get("run_id") or ""),
        "brief_unit_id": str(plan.get("brief_unit_id") or ""),
        "arm": str(plan.get("arm") or "claude_sdk"),
        "hook_id": str(plan.get("hook_id") or ""),
        "line_coverage_pass": line_coverage_pass,
        "mode_pass": mode_pass,
        "ugc_pass": True,
        "evidence_pass": True,
        "claim_safety_pass": True,
        "pacing_pass": pacing_pass,
        "post_polish_pass": True,
        "overall_pass": overall_pass,
        "failure_reasons": failure_reasons,
        "failing_line_ids": list(dict.fromkeys([str(v).strip() for v in failing_line_ids if str(v or "").strip()])),
        "repair_rounds_used": 0,
        "evaluated_at": datetime.now().isoformat(),
        "evaluator_metadata": {"source": "manual_edit"},
    }


def _phase3_v2_upsert_scene_gate_report_for_unit(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arm: str,
    brief_unit_id: str,
    hook_id: str,
    gate_report: dict[str, Any],
) -> dict[str, Any]:
    path = _phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, arm)
    rows = _phase3_v2_read_json(path, [])
    if not isinstance(rows, list):
        rows = []
    idx = next(
        (
            i
            for i, row in enumerate(rows)
            if isinstance(row, dict)
            and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
            and str(row.get("hook_id") or "").strip() == hook_id
        ),
        -1,
    )
    if idx >= 0:
        rows[idx] = gate_report
    else:
        rows.append(gate_report)
    _phase3_v2_write_json(path, rows)
    return gate_report


def _phase3_v2_update_hook_variant_for_unit(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    arm: str,
    brief_unit_id: str,
    hook_id: str,
    verbal_open: str,
    visual_pattern_interrupt: str,
    on_screen_text: str,
    evidence_ids: list[str],
    source: str,
) -> dict[str, Any] | None:
    bundles_file = _phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, arm)
    bundles_raw = _phase3_v2_read_json(bundles_file, [])
    if not isinstance(bundles_raw, list):
        bundles_raw = []

    bundle_idx = next(
        (
            idx for idx, row in enumerate(bundles_raw)
            if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip() == brief_unit_id
        ),
        -1,
    )
    if bundle_idx < 0:
        return None

    bundle = bundles_raw[bundle_idx]
    variants = bundle.get("variants", []) if isinstance(bundle, dict) else []
    if not isinstance(variants, list):
        variants = []
    variant_idx = next(
        (
            idx for idx, row in enumerate(variants)
            if isinstance(row, dict) and str(row.get("hook_id") or "").strip() == hook_id
        ),
        -1,
    )
    if variant_idx < 0:
        return None

    existing = variants[variant_idx] if isinstance(variants[variant_idx], dict) else {}
    updated_variant = dict(existing)
    updated_variant["hook_id"] = str(existing.get("hook_id") or hook_id).strip()
    updated_variant["brief_unit_id"] = brief_unit_id
    updated_variant["arm"] = arm
    if _phase3_v2_contains_meta_copy_terms(verbal_open):
        return None
    updated_variant["verbal_open"] = verbal_open
    updated_variant["visual_pattern_interrupt"] = visual_pattern_interrupt
    updated_variant["on_screen_text"] = on_screen_text
    updated_variant["evidence_ids"] = evidence_ids
    updated_variant["edited_source"] = source
    updated_variant["edited_at"] = datetime.now().isoformat()
    variants[variant_idx] = updated_variant

    if isinstance(bundle, dict):
        bundle["variants"] = variants
        bundles_raw[bundle_idx] = bundle
    _phase3_v2_write_json(bundles_file, bundles_raw)
    _phase3_v2_mark_scene_plans_stale_for_hook(
        brand_slug,
        branch_id,
        run_id,
        brief_unit_id,
        arm,
        hook_id,
        "hook_updated_after_scene_run",
    )
    return updated_variant


def _phase3_v2_build_hook_items_from_detail(
    detail: dict[str, Any],
    *,
    selected_brief_unit_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    hook_eligibility = _phase3_v2_build_hook_eligibility(detail)
    selected = {str(v).strip() for v in (selected_brief_unit_ids or []) if str(v or "").strip()}

    units = detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else []
    evidence_rows = detail.get("evidence_packs", []) if isinstance(detail.get("evidence_packs"), list) else []
    drafts_by_arm = detail.get("drafts_by_arm", {}) if isinstance(detail.get("drafts_by_arm"), dict) else {}

    unit_map = {
        str(row.get("brief_unit_id") or "").strip(): row
        for row in units
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }
    evidence_map = {
        str(row.get("brief_unit_id") or "").strip(): row
        for row in evidence_rows
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }

    items: list[dict[str, Any]] = []
    for row in hook_eligibility.get("eligible", []):
        if not isinstance(row, dict):
            continue
        unit_id = str(row.get("brief_unit_id") or "").strip()
        arm = str(row.get("arm") or "").strip()
        if not unit_id or not arm:
            continue
        if selected and unit_id not in selected:
            continue
        unit = unit_map.get(unit_id)
        evidence_pack = evidence_map.get(unit_id)
        drafts = drafts_by_arm.get(arm, [])
        draft = next(
            (
                d for d in (drafts if isinstance(drafts, list) else [])
                if isinstance(d, dict) and str(d.get("brief_unit_id") or "").strip() == unit_id
            ),
            None,
        )
        if not isinstance(unit, dict) or not isinstance(evidence_pack, dict) or not isinstance(draft, dict):
            continue
        items.append(
            {
                "brief_unit_id": unit_id,
                "arm": arm,
                "brief_unit": unit,
                "evidence_pack": evidence_pack,
                "draft": draft,
            }
        )
    return items, hook_eligibility


def _phase3_v2_build_scene_items_from_detail(
    detail: dict[str, Any],
    *,
    selected_brief_unit_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    run = detail.get("run", {}) if isinstance(detail.get("run"), dict) else {}
    run_id = str(run.get("run_id") or "").strip()
    return build_scene_items_from_handoff(
        run_id=run_id,
        scene_handoff_packet=detail.get("scene_handoff_packet", {}) if isinstance(detail.get("scene_handoff_packet"), dict) else {},
        drafts_by_arm=detail.get("drafts_by_arm", {}) if isinstance(detail.get("drafts_by_arm"), dict) else {},
        evidence_packs=detail.get("evidence_packs", []) if isinstance(detail.get("evidence_packs"), list) else [],
        brief_units=detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else [],
        selected_brief_unit_ids=selected_brief_unit_ids,
    )


def _phase3_v2_build_scene_handoff_packet(
    *,
    run_id: str,
    hook_run_id: str,
    hook_eligibility: dict[str, Any],
    hook_bundles_by_arm: dict[str, list[dict[str, Any]]],
    selections: list[HookSelectionV1],
) -> SceneHandoffPacketV1:
    bundle_lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for arm, bundles in (hook_bundles_by_arm or {}).items():
        if not isinstance(bundles, list):
            continue
        arm_map: dict[str, dict[str, Any]] = {}
        for bundle in bundles:
            if not isinstance(bundle, dict):
                continue
            unit_id = str(bundle.get("brief_unit_id") or "").strip()
            if unit_id:
                arm_map[unit_id] = bundle
        bundle_lookup[str(arm)] = arm_map

    selection_map = {
        _phase3_v2_pair_key(row.brief_unit_id, row.arm): row
        for row in selections
    }

    items: list[dict[str, Any]] = []
    ready_count = 0
    total_required = 0

    eligible_rows = hook_eligibility.get("eligible", []) if isinstance(hook_eligibility.get("eligible"), list) else []
    for row in eligible_rows:
        if not isinstance(row, dict):
            continue
        unit_id = str(row.get("brief_unit_id") or "").strip()
        arm = str(row.get("arm") or "").strip()
        if not unit_id or not arm:
            continue
        total_required += 1
        key = _phase3_v2_pair_key(unit_id, arm)
        selection = selection_map.get(key)
        bundle = bundle_lookup.get(arm, {}).get(unit_id, {})
        script_id = ""
        selected_hook_id = ""
        selected_hook_ids: list[str] = []
        selected_hook = None
        selected_hooks: list[dict[str, Any]] = []
        stale = False
        status = "missing_selection"

        variants = bundle.get("variants", []) if isinstance(bundle, dict) else []
        variant_map = {
            str(v.get("hook_id") or "").strip(): v
            for v in (variants if isinstance(variants, list) else [])
            if isinstance(v, dict) and str(v.get("hook_id") or "").strip()
        }
        if selection:
            selected_hook_ids = [str(v).strip() for v in (selection.selected_hook_ids or []) if str(v or "").strip()]
            if not selected_hook_ids and str(selection.selected_hook_id or "").strip():
                selected_hook_ids = [str(selection.selected_hook_id).strip()]
            selected_hook_id = selected_hook_ids[0] if selected_hook_ids else ""
            stale = bool(selection.stale)
            if selection.skip:
                status = "skipped"
                ready_count += 1
            elif stale:
                status = "stale"
            elif selected_hook_ids:
                missing_ids = [hid for hid in selected_hook_ids if hid not in variant_map]
                selected_hooks = [variant_map[hid] for hid in selected_hook_ids if hid in variant_map]
                if missing_ids:
                    status = "stale"
                    stale = True
                elif selected_hooks:
                    selected_hook = selected_hooks[0]
                    status = "ready"
                    ready_count += 1
                else:
                    status = "missing_selection"
            else:
                status = "missing_selection"

        if isinstance(bundle, dict):
            script_id = str(bundle.get("script_id") or "")

        items.append(
            {
                "brief_unit_id": unit_id,
                "arm": arm,
                "script_id": script_id,
                "selected_hook_ids": selected_hook_ids,
                "selected_hooks": selected_hooks,
                "selected_hook_id": selected_hook_id,
                "selected_hook": selected_hook,
                "stale": stale,
                "status": status,
            }
        )

    ready = total_required > 0 and ready_count == total_required
    return SceneHandoffPacketV1(
        run_id=run_id,
        hook_run_id=hook_run_id,
        ready=ready,
        ready_count=ready_count,
        total_required=total_required,
        generated_at=datetime.now().isoformat(),
        items=items,
    )


def _phase3_v2_refresh_scene_handoff(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
) -> dict[str, Any]:
    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not isinstance(detail, dict):
        return {
            "hook_selection_progress": {},
            "scene_handoff_ready": False,
            "scene_handoff_packet": {},
        }
    hook_stage_raw = detail.get("hook_stage", {})
    try:
        hook_stage = HookStageManifestV1.model_validate(hook_stage_raw)
    except Exception:
        hook_stage = _phase3_v2_default_hook_stage_manifest(run_id)
    scene_stage_raw = detail.get("scene_stage", {})
    try:
        scene_stage = SceneStageManifestV1.model_validate(scene_stage_raw)
    except Exception:
        scene_stage = _phase3_v2_default_scene_stage_manifest(run_id)

    hook_eligibility = detail.get("hook_eligibility", {})
    if not isinstance(hook_eligibility, dict):
        hook_eligibility = _phase3_v2_build_hook_eligibility(detail)
    hook_bundles_by_arm = detail.get("hook_bundles_by_arm", {})
    if not isinstance(hook_bundles_by_arm, dict):
        hook_bundles_by_arm = {}
    selections = _phase3_v2_load_hook_selections(brand_slug, branch_id, run_id)
    progress = _phase3_v2_compute_hook_selection_progress(
        hook_eligibility=hook_eligibility,
        selections=selections,
    )
    scene_packet = _phase3_v2_build_scene_handoff_packet(
        run_id=run_id,
        hook_run_id=hook_stage.hook_run_id,
        hook_eligibility=hook_eligibility,
        hook_bundles_by_arm=hook_bundles_by_arm,
        selections=selections,
    )
    _phase3_v2_write_json(
        _phase3_v2_scene_handoff_path(brand_slug, branch_id, run_id),
        scene_packet.model_dump(),
    )
    scene_plans_by_arm = detail.get("scene_plans_by_arm", {})
    if not isinstance(scene_plans_by_arm, dict):
        scene_plans_by_arm = {}
    scene_gate_reports_by_arm = detail.get("scene_gate_reports_by_arm", {})
    if not isinstance(scene_gate_reports_by_arm, dict):
        scene_gate_reports_by_arm = {}
    production_packet = _phase3_v2_build_production_handoff_from_scene_state(
        run_id=run_id,
        scene_run_id=scene_stage.scene_run_id,
        scene_handoff_packet={**scene_packet.model_dump(), "scene_run_id": scene_stage.scene_run_id},
        scene_plans_by_arm=scene_plans_by_arm,
        scene_gate_reports_by_arm=scene_gate_reports_by_arm,
    )
    _phase3_v2_write_json(
        _phase3_v2_production_handoff_path(brand_slug, branch_id, run_id),
        production_packet.model_dump(),
    )
    scene_progress = _phase3_v2_compute_scene_progress(
        scene_handoff_packet={**scene_packet.model_dump(), "scene_run_id": scene_stage.scene_run_id},
        scene_plans_by_arm=scene_plans_by_arm,
        scene_gate_reports_by_arm=scene_gate_reports_by_arm,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "hook_scene_handoff_ready": bool(scene_packet.ready),
            "hook_selection_selected": int(progress.get("selected", 0)),
            "hook_selection_pending": int(progress.get("pending", 0)),
            "hook_selection_stale": int(progress.get("stale", 0)),
            "production_handoff_ready": bool(production_packet.ready),
        },
    )
    return {
        "hook_selection_progress": progress,
        "scene_handoff_ready": bool(scene_packet.ready),
        "scene_handoff_packet": scene_packet.model_dump(),
        "scene_progress": scene_progress,
        "production_handoff_packet": production_packet.model_dump(),
        "production_handoff_ready": bool(production_packet.ready),
    }


async def _phase3_v2_execute_hooks(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    hook_run_id: str,
    selected_brief_unit_ids: list[str],
    candidate_target_per_unit: int,
    final_variants_per_unit: int,
    model_overrides: dict[str, Any],
) -> None:
    task_key = f"{brand_slug}:{branch_id}:{run_id}:hooks"
    started = time.time()
    try:
        detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
        if not detail:
            raise RuntimeError(f"Run not found: {run_id}")
        hook_items, hook_eligibility = _phase3_v2_build_hook_items_from_detail(
            detail,
            selected_brief_unit_ids=selected_brief_unit_ids,
        )
        hook_manifest = HookStageManifestV1(
            run_id=run_id,
            hook_run_id=hook_run_id,
            status="running",
            created_at=datetime.now().isoformat(),
            started_at=datetime.now().isoformat(),
            completed_at="",
            error="",
            eligible_count=len(hook_items),
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            candidate_target_per_unit=int(candidate_target_per_unit),
            final_variants_per_unit=int(final_variants_per_unit),
            max_parallel=int(config.PHASE3_V2_HOOK_MAX_PARALLEL),
            max_repair_rounds=int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS),
            model_registry={},
            metrics={},
        )
        _phase3_v2_save_hook_stage_manifest(brand_slug, branch_id, run_id, hook_manifest)
        _phase3_v2_upsert_manifest_entry(
            brand_slug,
            branch_id,
            {
                "run_id": run_id,
                "updated_at": datetime.now().isoformat(),
                "hook_stage_status": "running",
                "hook_run_id": hook_run_id,
            },
        )

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_phase3_v2_hooks(
                run_id=run_id,
                hook_run_id=hook_run_id,
                hook_items=hook_items,
                candidate_target_per_unit=candidate_target_per_unit,
                final_variants_per_unit=final_variants_per_unit,
                model_overrides=model_overrides,
            ),
        )

        hook_stage_raw = result.get("hook_stage_manifest", {})
        hook_stage = HookStageManifestV1.model_validate(hook_stage_raw)
        _phase3_v2_save_hook_stage_manifest(brand_slug, branch_id, run_id, hook_stage)

        hook_candidates_by_arm = result.get("hook_candidates_by_arm", {})
        hook_gate_reports_by_arm = result.get("hook_gate_reports_by_arm", {})
        hook_bundles_by_arm = result.get("hook_bundles_by_arm", {})
        hook_scores_by_arm = result.get("hook_scores_by_arm", {})
        run_arms = _phase3_v2_get_run_arms(detail)
        for arm in run_arms:
            _phase3_v2_write_json(
                _phase3_v2_hook_candidates_path(brand_slug, branch_id, run_id, arm),
                hook_candidates_by_arm.get(arm, []),
            )
            _phase3_v2_write_json(
                _phase3_v2_hook_gate_reports_path(brand_slug, branch_id, run_id, arm),
                hook_gate_reports_by_arm.get(arm, []),
            )
            _phase3_v2_write_json(
                _phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, arm),
                hook_bundles_by_arm.get(arm, []),
            )
            _phase3_v2_write_json(
                _phase3_v2_hook_scores_path(brand_slug, branch_id, run_id, arm),
                hook_scores_by_arm.get(arm, []),
            )

        selections = _phase3_v2_load_hook_selections(brand_slug, branch_id, run_id)
        if selections:
            selected_hook_ids_by_pair = {}
            for arm, bundles in hook_bundles_by_arm.items():
                for bundle in (bundles if isinstance(bundles, list) else []):
                    if not isinstance(bundle, dict):
                        continue
                    unit_id = str(bundle.get("brief_unit_id") or "").strip()
                    if not unit_id:
                        continue
                    valid_ids = {
                        str(v.get("hook_id") or "").strip()
                        for v in (bundle.get("variants", []) if isinstance(bundle.get("variants"), list) else [])
                        if isinstance(v, dict) and str(v.get("hook_id") or "").strip()
                    }
                    selected_hook_ids_by_pair[_phase3_v2_pair_key(unit_id, arm)] = valid_ids
            for selection in selections:
                key = _phase3_v2_pair_key(selection.brief_unit_id, selection.arm)
                valid_ids = selected_hook_ids_by_pair.get(key, set())
                if selection.skip:
                    selection.stale = False
                    selection.stale_reason = ""
                    continue
                selected_ids = [str(v).strip() for v in (selection.selected_hook_ids or []) if str(v or "").strip()]
                if not selected_ids and selection.selected_hook_id:
                    selected_ids = [str(selection.selected_hook_id).strip()]
                if selected_ids and all(hid in valid_ids for hid in selected_ids):
                    selection.stale = False
                    selection.stale_reason = ""
                elif selected_ids:
                    selection.stale = True
                    selection.stale_reason = "hook_selection_invalid_after_rerun"
            _phase3_v2_save_hook_selections(brand_slug, branch_id, run_id, selections)

        scene_packet = _phase3_v2_build_scene_handoff_packet(
            run_id=run_id,
            hook_run_id=hook_run_id,
            hook_eligibility=hook_eligibility,
            hook_bundles_by_arm=hook_bundles_by_arm if isinstance(hook_bundles_by_arm, dict) else {},
            selections=_phase3_v2_load_hook_selections(brand_slug, branch_id, run_id),
        )
        _phase3_v2_write_json(
            _phase3_v2_scene_handoff_path(brand_slug, branch_id, run_id),
            scene_packet.model_dump(),
        )

        run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
        manifest = _phase3_v2_read_json(run_dir / "manifest.json", {})
        manifest["updated_at"] = datetime.now().isoformat()
        manifest["hook_run_id"] = hook_run_id
        manifest["hook_stage_status"] = hook_stage.status
        manifest["hook_metrics"] = hook_stage.metrics
        manifest["hook_scene_handoff_ready"] = scene_packet.ready
        manifest["hook_elapsed_seconds"] = round(time.time() - started, 3)
        _phase3_v2_write_json(run_dir / "manifest.json", manifest)
        _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, manifest)
    except Exception as exc:
        logger.exception("Phase3 v2 hook stage failed for run %s", run_id)
        failed_manifest = HookStageManifestV1(
            run_id=run_id,
            hook_run_id=hook_run_id,
            status="failed",
            created_at=datetime.now().isoformat(),
            started_at=datetime.now().isoformat(),
            completed_at=datetime.now().isoformat(),
            error=str(exc),
            eligible_count=0,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            candidate_target_per_unit=int(candidate_target_per_unit),
            final_variants_per_unit=int(final_variants_per_unit),
            max_parallel=int(config.PHASE3_V2_HOOK_MAX_PARALLEL),
            max_repair_rounds=int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS),
            model_registry={},
            metrics={"elapsed_seconds": round(time.time() - started, 3)},
        )
        _phase3_v2_save_hook_stage_manifest(brand_slug, branch_id, run_id, failed_manifest)
        _phase3_v2_upsert_manifest_entry(
            brand_slug,
            branch_id,
            {
                "run_id": run_id,
                "updated_at": datetime.now().isoformat(),
                "hook_stage_status": "failed",
                "hook_stage_error": str(exc),
                "hook_run_id": hook_run_id,
            },
        )
    finally:
        phase3_v2_hook_tasks.pop(task_key, None)


async def _phase3_v2_execute_scenes(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    scene_run_id: str,
    selected_brief_unit_ids: list[str],
    model_overrides: dict[str, Any],
) -> None:
    task_key = f"{brand_slug}:{branch_id}:{run_id}:scenes"
    started = time.time()
    try:
        detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
        if not detail:
            raise RuntimeError(f"Run not found: {run_id}")
        scene_items, scene_eligibility = _phase3_v2_build_scene_items_from_detail(
            detail,
            selected_brief_unit_ids=selected_brief_unit_ids,
        )
        scene_manifest = SceneStageManifestV1(
            run_id=run_id,
            scene_run_id=scene_run_id,
            status="running",
            created_at=datetime.now().isoformat(),
            started_at=datetime.now().isoformat(),
            completed_at="",
            error="",
            eligible_count=len(scene_items),
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            stale_count=0,
            max_parallel=int(config.PHASE3_V2_SCENE_MAX_PARALLEL),
            max_repair_rounds=int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
            max_consecutive_mode=int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
            min_a_roll_lines=int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
            model_registry={},
            metrics={},
        )
        _phase3_v2_save_scene_stage_manifest(brand_slug, branch_id, run_id, scene_manifest)
        _phase3_v2_upsert_manifest_entry(
            brand_slug,
            branch_id,
            {
                "run_id": run_id,
                "updated_at": datetime.now().isoformat(),
                "scene_stage_status": "running",
                "scene_run_id": scene_run_id,
                "scene_eligible_count": len(scene_items),
            },
        )

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_phase3_v2_scenes(
                run_id=run_id,
                scene_run_id=scene_run_id,
                scene_items=scene_items,
                model_overrides=model_overrides,
            ),
        )

        scene_stage_raw = result.get("scene_stage_manifest", {})
        scene_stage = SceneStageManifestV1.model_validate(scene_stage_raw)
        _phase3_v2_save_scene_stage_manifest(brand_slug, branch_id, run_id, scene_stage)

        scene_plans_by_arm = result.get("scene_plans_by_arm", {})
        scene_gate_reports_by_arm = result.get("scene_gate_reports_by_arm", {})
        production_handoff_packet = result.get("production_handoff_packet", {})

        run_arms = _phase3_v2_get_run_arms(detail)
        for arm in run_arms:
            _phase3_v2_write_json(
                _phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, arm),
                scene_plans_by_arm.get(arm, []),
            )
            _phase3_v2_write_json(
                _phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, arm),
                scene_gate_reports_by_arm.get(arm, []),
            )
        _phase3_v2_write_json(
            _phase3_v2_production_handoff_path(brand_slug, branch_id, run_id),
            production_handoff_packet if isinstance(production_handoff_packet, dict) else {},
        )

        run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
        manifest = _phase3_v2_read_json(run_dir / "manifest.json", {})
        manifest["updated_at"] = datetime.now().isoformat()
        manifest["scene_run_id"] = scene_run_id
        manifest["scene_stage_status"] = scene_stage.status
        manifest["scene_metrics"] = scene_stage.metrics
        manifest["production_handoff_ready"] = bool(
            isinstance(production_handoff_packet, dict) and production_handoff_packet.get("ready")
        )
        manifest["scene_elapsed_seconds"] = round(time.time() - started, 3)
        _phase3_v2_write_json(run_dir / "manifest.json", manifest)
        _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, manifest)
    except Exception as exc:
        logger.exception("Phase3 v2 scene stage failed for run %s", run_id)
        failed_manifest = SceneStageManifestV1(
            run_id=run_id,
            scene_run_id=scene_run_id,
            status="failed",
            created_at=datetime.now().isoformat(),
            started_at=datetime.now().isoformat(),
            completed_at=datetime.now().isoformat(),
            error=str(exc),
            eligible_count=0,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            stale_count=0,
            max_parallel=int(config.PHASE3_V2_SCENE_MAX_PARALLEL),
            max_repair_rounds=int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
            max_consecutive_mode=int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
            min_a_roll_lines=int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
            model_registry={},
            metrics={"elapsed_seconds": round(time.time() - started, 3)},
        )
        _phase3_v2_save_scene_stage_manifest(brand_slug, branch_id, run_id, failed_manifest)
        _phase3_v2_upsert_manifest_entry(
            brand_slug,
            branch_id,
            {
                "run_id": run_id,
                "updated_at": datetime.now().isoformat(),
                "scene_stage_status": "failed",
                "scene_stage_error": str(exc),
                "scene_run_id": scene_run_id,
            },
        )
    finally:
        phase3_v2_scene_tasks.pop(task_key, None)


async def _phase3_v2_execute_run(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    matrix_plan: dict[str, Any],
    foundation_brief: dict[str, Any],
    pilot_size: int,
    selected_brief_unit_ids: list[str],
    ab_mode: bool,
    sdk_toggles: dict[str, bool],
    reviewer_role: str,
    model_overrides: dict[str, dict[str, str]],
):
    run_key = f"{brand_slug}:{branch_id}:{run_id}"
    run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
    started = time.time()
    started_iso = datetime.now().isoformat()
    initial_manifest = _phase3_v2_read_json(run_dir / "manifest.json", {})
    created_at = str(initial_manifest.get("created_at") or started_iso)
    foundation_hash = _phase3_v2_input_hash(foundation_brief if isinstance(foundation_brief, dict) else {})
    matrix_hash = _phase3_v2_input_hash(matrix_plan if isinstance(matrix_plan, dict) else {})
    run_input_hash = _phase3_v2_input_hash(
        {
            "branch_id": branch_id,
            "brand_slug": brand_slug,
            "pilot_size": pilot_size,
            "selected_brief_unit_ids": selected_brief_unit_ids,
            "ab_mode": bool(ab_mode),
            "sdk_toggles": sdk_toggles,
            "reviewer_role": reviewer_role,
            "model_overrides": model_overrides,
            "foundation_hash": foundation_hash,
            "matrix_hash": matrix_hash,
        }
    )
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_phase3_v2_m1(
                matrix_plan=matrix_plan,
                foundation_brief=foundation_brief,
                branch_id=branch_id,
                brand_slug=brand_slug,
                pilot_size=pilot_size,
                selected_brief_unit_ids=selected_brief_unit_ids,
                ab_mode=False,
                sdk_toggles=sdk_toggles,
                reviewer_role=reviewer_role,
                model_overrides=model_overrides,
            ),
        )

        brief_units = result.get("brief_units", [])
        evidence_packs = result.get("evidence_packs", [])
        drafts_by_arm = result.get("drafts_by_arm", {})
        arms = result.get("arms", _phase3_v2_resolve_arms(ab_mode, sdk_toggles))

        _phase3_v2_write_json(run_dir / "brief_units.json", brief_units)
        _phase3_v2_write_json(run_dir / "evidence_packs.json", evidence_packs)
        _phase3_v2_write_json(run_dir / "reviews.json", [])
        for arm_name, drafts in drafts_by_arm.items():
            _phase3_v2_write_json(run_dir / _phase3_v2_arm_file_name(str(arm_name)), drafts)

        summary = compute_ab_summary(
            run_id=run_id,
            drafts_by_arm=drafts_by_arm if isinstance(drafts_by_arm, dict) else {},
            reviews=[],
        )
        _phase3_v2_write_json(run_dir / "summary.json", summary.model_dump())

        summary_by_arm = {row.arm: row for row in summary.arms}
        model_metadata_by_arm: dict[str, Any] = {}
        cost_usd_by_arm: dict[str, float] = {}
        latency_seconds_by_arm: dict[str, float] = {}
        for arm_name, drafts in (drafts_by_arm.items() if isinstance(drafts_by_arm, dict) else []):
            if not isinstance(drafts, list):
                continue
            total_cost = 0.0
            total_latency = 0.0
            metadata = {}
            for draft in drafts:
                if not isinstance(draft, dict):
                    continue
                total_cost += float(draft.get("cost_usd", 0.0) or 0.0)
                total_latency += float(draft.get("latency_seconds", 0.0) or 0.0)
                if not metadata and isinstance(draft.get("model_metadata"), dict):
                    metadata = dict(draft.get("model_metadata") or {})
            if metadata:
                model_metadata_by_arm[str(arm_name)] = metadata
            cost_usd_by_arm[str(arm_name)] = round(total_cost, 6)
            latency_seconds_by_arm[str(arm_name)] = round(total_latency, 4)

        manifest = {
            "run_id": run_id,
            "status": "completed",
            "created_at": created_at,
            "completed_at": datetime.now().isoformat(),
            "elapsed_seconds": round(time.time() - started, 2),
            "pilot_size": pilot_size,
            "selected_brief_unit_ids": selected_brief_unit_ids,
            "ab_mode": False,
            "reviewer_role": reviewer_role,
            "sdk_toggles": sdk_toggles,
            "model_overrides": model_overrides,
            "arms": arms,
            "brief_unit_count": len(brief_units) if isinstance(brief_units, list) else 0,
            "input_hash": run_input_hash,
            "source_hashes": {
                "matrix_plan_hash": matrix_hash,
                "foundation_brief_hash": foundation_hash,
            },
            "model_metadata_by_arm": model_metadata_by_arm,
            "cost_usd_total": round(sum(cost_usd_by_arm.values()), 6),
            "cost_usd_by_arm": cost_usd_by_arm,
            "latency_seconds_by_arm": latency_seconds_by_arm,
            "gate_pass_rate_by_arm": {
                arm_name: float(summary_by_arm[arm_name].gate_pass_rate)
                for arm_name in summary_by_arm
            },
            "winner": summary.winner,
        }
        _phase3_v2_write_json(run_dir / "manifest.json", manifest)
        _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, manifest)
    except Exception as exc:
        logger.exception("Phase 3 v2 run failed: %s", run_key)
        failure = {
            "run_id": run_id,
            "status": "failed",
            "created_at": created_at,
            "completed_at": datetime.now().isoformat(),
            "elapsed_seconds": round(time.time() - started, 2),
            "error": str(exc),
            "pilot_size": pilot_size,
            "selected_brief_unit_ids": selected_brief_unit_ids,
            "ab_mode": False,
            "reviewer_role": reviewer_role,
            "sdk_toggles": sdk_toggles,
            "model_overrides": model_overrides,
            "arms": _phase3_v2_resolve_arms(ab_mode, sdk_toggles),
            "input_hash": run_input_hash,
            "source_hashes": {
                "matrix_plan_hash": matrix_hash,
                "foundation_brief_hash": foundation_hash,
            },
        }
        _phase3_v2_write_json(run_dir / "manifest.json", failure)
        _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, failure)
    finally:
        phase3_v2_tasks.pop(run_key, None)


@app.get("/api/branches/{branch_id}/phase3-v2/prepare")
async def api_phase3_v2_prepare(branch_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    inputs: dict[str, Any] = {}
    foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
    if foundation_err:
        return JSONResponse({"error": foundation_err}, status_code=400)
    foundation = inputs.get("foundation_brief")
    if not isinstance(foundation, dict):
        return JSONResponse({"error": "Foundation Research output not found"}, status_code=400)

    matrix_plan, matrix_err = _load_matrix_plan_for_branch(brand_slug, branch_id)
    if matrix_err:
        return JSONResponse({"error": matrix_err}, status_code=400)
    if not isinstance(matrix_plan, dict):
        return JSONResponse({"error": "Matrix plan not available"}, status_code=400)

    resolved_pilot = _matrix_planned_brief_total(matrix_plan)
    brief_units = expand_brief_units(
        matrix_plan,
        branch_id=branch_id,
        brand_slug=brand_slug,
        pilot_size=resolved_pilot,
        selection_strategy="round_robin",
    )

    evidence_rows = []
    blocked_count = 0
    for unit in brief_units:
        pack = build_evidence_pack(unit, foundation)
        blocked = bool(pack.coverage_report.blocked_evidence_insufficient)
        if blocked:
            blocked_count += 1
        evidence_rows.append(
            {
                "brief_unit_id": unit.brief_unit_id,
                "matrix_cell_id": unit.matrix_cell_id,
                "awareness_level": unit.awareness_level,
                "emotion_key": unit.emotion_key,
                "emotion_label": unit.emotion_label,
                "blocked_evidence_insufficient": blocked,
                "coverage_report": pack.coverage_report.model_dump(),
            }
        )

    return {
        "branch_id": branch_id,
        "pilot_size": resolved_pilot,
        "planned_brief_units": resolved_pilot,
        "candidate_count": len(brief_units),
        "blocked_count": blocked_count,
        "brief_units": [unit.model_dump() for unit in brief_units],
        "evidence_overview": evidence_rows,
    }


@app.post("/api/branches/{branch_id}/phase3-v2/run")
async def api_phase3_v2_run(branch_id: str, req: Phase3V2RunRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    if pipeline_state["running"]:
        return JSONResponse({"error": "Pipeline is already running"}, status_code=409)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    inputs: dict[str, Any] = {}
    foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
    if foundation_err:
        return JSONResponse({"error": foundation_err}, status_code=400)
    foundation = inputs.get("foundation_brief")
    if not isinstance(foundation, dict):
        return JSONResponse({"error": "Foundation Research output not found"}, status_code=400)

    matrix_plan, matrix_err = _load_matrix_plan_for_branch(brand_slug, branch_id)
    if matrix_err:
        return JSONResponse({"error": matrix_err}, status_code=400)
    if not isinstance(matrix_plan, dict):
        return JSONResponse({"error": "Matrix plan not available"}, status_code=400)

    resolved_pilot = _matrix_planned_brief_total(matrix_plan)
    sdk_toggles = _normalize_phase3_v2_sdk_toggles(req.sdk_toggles)
    model_overrides = _normalize_phase3_v2_model_overrides(req.model_overrides)
    arms = _phase3_v2_resolve_arms(bool(req.ab_mode), sdk_toggles)
    reviewer_role = (
        str(req.reviewer_role or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT).strip().lower()
        or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT
    )

    run_id = f"p3v2_{int(time.time() * 1000)}"
    run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
    selected_ids = [str(v).strip() for v in req.selected_brief_unit_ids if str(v or "").strip()]

    initial_manifest = {
        "run_id": run_id,
        "status": "running",
        "created_at": datetime.now().isoformat(),
        "pilot_size": resolved_pilot,
        "planned_brief_units": resolved_pilot,
        "selected_brief_unit_ids": selected_ids,
        "ab_mode": False,
        "reviewer_role": reviewer_role,
        "sdk_toggles": sdk_toggles,
        "model_overrides": model_overrides,
        "arms": arms,
    }
    _phase3_v2_write_json(run_dir / "manifest.json", initial_manifest)
    _phase3_v2_write_json(run_dir / "decisions.json", [])
    _phase3_v2_write_json(run_dir / "chat_threads.json", {})
    _phase3_v2_write_json(run_dir / "hook_chat_threads.json", {})
    _phase3_v2_write_json(run_dir / "scene_chat_threads.json", {})
    _phase3_v2_write_json(run_dir / "final_lock.json", _phase3_v2_default_final_lock(run_id).model_dump())
    _phase3_v2_write_json(run_dir / "hook_selections.json", [])
    _phase3_v2_write_json(
        run_dir / "hook_stage_manifest.json",
        _phase3_v2_default_hook_stage_manifest(run_id).model_dump(),
    )
    _phase3_v2_write_json(
        run_dir / "scene_handoff_packet.json",
        SceneHandoffPacketV1(
            run_id=run_id,
            hook_run_id="",
            ready=False,
            ready_count=0,
            total_required=0,
            generated_at="",
            items=[],
        ).model_dump(),
    )
    _phase3_v2_write_json(
        run_dir / "scene_stage_manifest.json",
        _phase3_v2_default_scene_stage_manifest(run_id).model_dump(),
    )
    for arm_name in arms:
        _phase3_v2_write_json(_phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, str(arm_name)), [])
        _phase3_v2_write_json(_phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, str(arm_name)), [])
    _phase3_v2_write_json(
        run_dir / "production_handoff_packet.json",
        ProductionHandoffPacketV1(
            run_id=run_id,
            scene_run_id="",
            ready=False,
            ready_count=0,
            total_required=0,
            generated_at="",
            items=[],
            metrics={},
        ).model_dump(),
    )
    _phase3_v2_upsert_manifest_entry(brand_slug, branch_id, initial_manifest)

    task = asyncio.create_task(
        _phase3_v2_execute_run(
            brand_slug=brand_slug,
            branch_id=branch_id,
            run_id=run_id,
            matrix_plan=matrix_plan,
            foundation_brief=foundation,
            pilot_size=resolved_pilot,
            selected_brief_unit_ids=selected_ids,
            ab_mode=False,
            sdk_toggles=sdk_toggles,
            reviewer_role=reviewer_role,
            model_overrides=model_overrides,
        )
    )
    phase3_v2_tasks[f"{brand_slug}:{branch_id}:{run_id}"] = task

    return {
        "status": "started",
        "run_id": run_id,
        "branch_id": branch_id,
        "pilot_size": resolved_pilot,
        "planned_brief_units": resolved_pilot,
        "arms": initial_manifest["arms"],
        "arm_ids": initial_manifest["arms"],
    }


@app.get("/api/branches/{branch_id}/phase3-v2/runs")
async def api_phase3_v2_runs(branch_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    runs = _load_phase3_v2_runs_manifest(brand_slug, branch_id)
    return [
        _phase3_v2_reconcile_orphaned_running_entry(brand_slug, branch_id, row)
        for row in runs
        if isinstance(row, dict)
    ]


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}")
async def api_phase3_v2_run_detail(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    return detail


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/prepare")
async def api_phase3_v2_hooks_prepare(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    hook_eligibility = _phase3_v2_build_hook_eligibility(detail)
    return {
        "run_id": run_id,
        "hook_stage": detail.get("hook_stage", {}),
        "eligible_count": int(hook_eligibility.get("eligible_count", 0)),
        "skipped_count": int(hook_eligibility.get("skipped_count", 0)),
        "eligible_units": hook_eligibility.get("eligible", []),
        "skipped_units": hook_eligibility.get("skipped", []),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/run")
async def api_phase3_v2_hooks_run(branch_id: str, run_id: str, req: Phase3V2HookRunRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    hook_stage = _phase3_v2_load_hook_stage_manifest(brand_slug, branch_id, run_id)
    task_key = f"{brand_slug}:{branch_id}:{run_id}:hooks"
    task = phase3_v2_hook_tasks.get(task_key)
    if hook_stage.status == "running" and task is not None and not task.done():
        return JSONResponse({"error": "Hook stage is already running for this run."}, status_code=409)

    selected_ids = [str(v).strip() for v in req.selected_brief_unit_ids if str(v or "").strip()]
    hook_items, hook_eligibility = _phase3_v2_build_hook_items_from_detail(
        detail,
        selected_brief_unit_ids=selected_ids,
    )
    if not hook_items:
        return JSONResponse(
            {
                "error": "No eligible Brief Units to run hooks.",
                "eligible_count": int(hook_eligibility.get("eligible_count", 0)),
                "skipped_count": int(hook_eligibility.get("skipped_count", 0)),
            },
            status_code=400,
        )

    candidate_target = int(req.candidate_target_per_unit or config.PHASE3_V2_HOOK_CANDIDATES_PER_UNIT)
    final_variants = int(req.final_variants_per_unit or config.PHASE3_V2_HOOK_FINAL_VARIANTS_PER_UNIT)
    candidate_target = max(1, candidate_target)
    final_variants = max(int(config.PHASE3_V2_HOOK_MIN_NEW_VARIANTS), final_variants)
    hook_run_id = f"hkv2_{int(time.time() * 1000)}"

    stage_manifest = HookStageManifestV1(
        run_id=run_id,
        hook_run_id=hook_run_id,
        status="running",
        created_at=datetime.now().isoformat(),
        started_at=datetime.now().isoformat(),
        completed_at="",
        error="",
        eligible_count=len(hook_items),
        processed_count=0,
        failed_count=0,
        skipped_count=0,
        candidate_target_per_unit=candidate_target,
        final_variants_per_unit=final_variants,
        max_parallel=int(config.PHASE3_V2_HOOK_MAX_PARALLEL),
        max_repair_rounds=int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS),
        model_registry={},
        metrics={},
    )
    _phase3_v2_save_hook_stage_manifest(brand_slug, branch_id, run_id, stage_manifest)
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "hook_stage_status": "running",
            "hook_run_id": hook_run_id,
        },
    )

    task = asyncio.create_task(
        _phase3_v2_execute_hooks(
            brand_slug=brand_slug,
            branch_id=branch_id,
            run_id=run_id,
            hook_run_id=hook_run_id,
            selected_brief_unit_ids=selected_ids,
            candidate_target_per_unit=candidate_target,
            final_variants_per_unit=final_variants,
            model_overrides=dict(req.model_overrides or {}),
        )
    )
    phase3_v2_hook_tasks[task_key] = task

    return {
        "status": "started",
        "run_id": run_id,
        "hook_run_id": hook_run_id,
        "eligible_count": len(hook_items),
        "candidate_target_per_unit": candidate_target,
        "final_variants_per_unit": final_variants,
    }


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks")
async def api_phase3_v2_hooks_status(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    return {
        "run_id": run_id,
        "hook_stage": detail.get("hook_stage", {}),
        "hook_eligibility": detail.get("hook_eligibility", {}),
        "hook_bundles_by_arm": detail.get("hook_bundles_by_arm", {}),
        "hook_gate_reports_by_arm": detail.get("hook_gate_reports_by_arm", {}),
        "hook_scores_by_arm": detail.get("hook_scores_by_arm", {}),
        "hook_selection_progress": detail.get("hook_selection_progress", {}),
        "hook_selections": detail.get("hook_selections", []),
        "scene_handoff_ready": bool(detail.get("scene_handoff_ready")),
        "scene_handoff_packet": detail.get("scene_handoff_packet", {}),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/selections")
async def api_phase3_v2_hooks_selections(
    branch_id: str,
    run_id: str,
    req: Phase3V2HookSelectionRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)
    if not req.selections:
        return JSONResponse({"error": "No hook selections provided."}, status_code=400)

    hook_eligibility = _phase3_v2_build_hook_eligibility(detail)
    eligible_pairs = {
        _phase3_v2_pair_key(str(row.get("brief_unit_id") or ""), str(row.get("arm") or ""))
        for row in (hook_eligibility.get("eligible", []) if isinstance(hook_eligibility.get("eligible"), list) else [])
        if str(row.get("brief_unit_id") or "").strip() and str(row.get("arm") or "").strip()
    }
    bundles_by_arm = detail.get("hook_bundles_by_arm", {}) if isinstance(detail.get("hook_bundles_by_arm"), dict) else {}
    valid_hook_ids_by_pair: dict[str, set[str]] = {}
    for arm, bundles in bundles_by_arm.items():
        for bundle in (bundles if isinstance(bundles, list) else []):
            if not isinstance(bundle, dict):
                continue
            unit_id = str(bundle.get("brief_unit_id") or "").strip()
            if not unit_id:
                continue
            pair_key = _phase3_v2_pair_key(unit_id, str(arm))
            valid_hook_ids_by_pair[pair_key] = {
                str(row.get("hook_id") or "").strip()
                for row in (bundle.get("variants", []) if isinstance(bundle.get("variants"), list) else [])
                if isinstance(row, dict) and str(row.get("hook_id") or "").strip()
            }

    hook_stage = _phase3_v2_load_hook_stage_manifest(brand_slug, branch_id, run_id)
    existing = _phase3_v2_load_hook_selections(brand_slug, branch_id, run_id)
    upsert_map: dict[tuple[str, str], HookSelectionV1] = {(row.brief_unit_id, row.arm): row for row in existing}
    changed_pairs: set[tuple[str, str]] = set()

    for payload in req.selections:
        unit_id = str(payload.brief_unit_id or "").strip()
        arm = str(payload.arm or "").strip()
        pair_key = _phase3_v2_pair_key(unit_id, arm)
        if pair_key not in eligible_pairs:
            return JSONResponse({"error": f"Brief Unit/arm is not eligible for hooks: {unit_id} ({arm})"}, status_code=400)
        selected_hook_ids = [str(v).strip() for v in (payload.selected_hook_ids or []) if str(v or "").strip()]
        if not selected_hook_ids:
            legacy_id = str(payload.selected_hook_id or "").strip()
            if legacy_id:
                selected_hook_ids = [legacy_id]
        selected_hook_ids = list(dict.fromkeys(selected_hook_ids))
        skip = bool(payload.skip)
        clear_selection = (not skip) and (not selected_hook_ids)
        valid_ids = valid_hook_ids_by_pair.get(pair_key, set())
        previous_row = upsert_map.get((unit_id, arm))
        prev_ids = []
        prev_skip = False
        if previous_row:
            prev_ids = [str(v).strip() for v in (previous_row.selected_hook_ids or []) if str(v or "").strip()]
            if not prev_ids and str(previous_row.selected_hook_id or "").strip():
                prev_ids = [str(previous_row.selected_hook_id).strip()]
            prev_skip = bool(previous_row.skip)
        if not skip and not clear_selection:
            unknown_ids = [hid for hid in selected_hook_ids if hid not in valid_ids]
            if unknown_ids:
                return JSONResponse(
                    {"error": f"Unknown selected_hook_id for {unit_id} ({arm}): {', '.join(unknown_ids)}"},
                    status_code=400,
                )
        if clear_selection:
            if previous_row:
                changed_pairs.add((unit_id, arm))
            upsert_map.pop((unit_id, arm), None)
            continue
        row = HookSelectionV1(
            run_id=run_id,
            hook_run_id=hook_stage.hook_run_id,
            brief_unit_id=unit_id,
            arm=arm,  # validated above
            selected_hook_ids=[] if skip else selected_hook_ids,
            selected_hook_id="" if skip else (selected_hook_ids[0] if selected_hook_ids else ""),
            skip=skip,
            stale=False,
            stale_reason="",
            updated_at=datetime.now().isoformat(),
        )
        upsert_map[(unit_id, arm)] = row
        if prev_skip != skip or sorted(prev_ids) != sorted(selected_hook_ids):
            changed_pairs.add((unit_id, arm))

    merged = list(upsert_map.values())
    merged.sort(key=lambda row: (row.brief_unit_id, row.arm))
    _phase3_v2_save_hook_selections(brand_slug, branch_id, run_id, merged)
    for unit_id, arm_name in changed_pairs:
        _phase3_v2_mark_scene_plans_stale_for_unit(
            brand_slug,
            branch_id,
            run_id,
            unit_id,
            arm_name,
            "hook_selection_changed",
        )

    progress = _phase3_v2_compute_hook_selection_progress(
        hook_eligibility=hook_eligibility,
        selections=merged,
    )
    scene_packet = _phase3_v2_build_scene_handoff_packet(
        run_id=run_id,
        hook_run_id=hook_stage.hook_run_id,
        hook_eligibility=hook_eligibility,
        hook_bundles_by_arm=bundles_by_arm,
        selections=merged,
    )
    _phase3_v2_write_json(
        _phase3_v2_scene_handoff_path(brand_slug, branch_id, run_id),
        scene_packet.model_dump(),
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "hook_scene_handoff_ready": bool(scene_packet.ready),
            "hook_selection_selected": int(progress.get("selected", 0)),
            "hook_selection_pending": int(progress.get("pending", 0)),
            "hook_selection_stale": int(progress.get("stale", 0)),
        },
    )

    return {
        "ok": True,
        "hook_selection_progress": progress,
        "scene_handoff_ready": bool(scene_packet.ready),
        "scene_handoff_packet": scene_packet.model_dump(),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/update")
async def api_phase3_v2_hooks_update(
    branch_id: str,
    run_id: str,
    req: Phase3V2HookUpdateRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    hook_id = str(req.hook_id or "").strip()
    if not unit_id:
        return JSONResponse({"error": "brief_unit_id is required."}, status_code=400)
    if not hook_id:
        return JSONResponse({"error": "hook_id is required."}, status_code=400)

    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)
    if not _phase3_v2_find_hook_variant(detail, arm_name, unit_id, hook_id):
        return JSONResponse({"error": f"Unknown hook_id for this Brief Unit/arm: {hook_id}"}, status_code=404)

    verbal_open = str(req.verbal_open or "").strip()
    visual_pattern_interrupt = str(req.visual_pattern_interrupt or "").strip()
    on_screen_text = str(req.on_screen_text or "").strip()
    evidence_ids = _phase3_v2_normalize_hook_evidence_ids(req.evidence_ids)
    if not verbal_open:
        return JSONResponse({"error": "verbal_open is required."}, status_code=400)
    if _phase3_v2_contains_meta_copy_terms(verbal_open):
        return JSONResponse(
            {"error": "verbal_open contains framework/meta wording. Use direct spoken copy."},
            status_code=400,
        )
    updated = _phase3_v2_update_hook_variant_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=hook_id,
        verbal_open=verbal_open,
        visual_pattern_interrupt=visual_pattern_interrupt,
        on_screen_text=on_screen_text,
        evidence_ids=evidence_ids,
        source=str(req.source or "manual"),
    )
    if not updated:
        return JSONResponse({"error": f"Could not update hook variant: {hook_id}"}, status_code=404)

    refreshed = _phase3_v2_refresh_scene_handoff(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": str(req.source or "manual"),
            "last_hook_edit": {
                "brief_unit_id": unit_id,
                "arm": arm_name,
                "hook_id": hook_id,
            },
        },
    )
    return {
        "ok": True,
        "hook": updated,
        **refreshed,
    }


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/chat")
async def api_phase3_v2_hooks_chat_get(
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    hook_id: str,
    brand: str = "",
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(brief_unit_id or "").strip()
    arm_name = str(arm or "").strip()
    variant_id = str(hook_id or "").strip()
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)
    if not _phase3_v2_find_hook_variant(detail, arm_name, unit_id, variant_id):
        return JSONResponse({"error": f"Unknown hook_id for this Brief Unit/arm: {variant_id}"}, status_code=404)

    threads = _phase3_v2_load_hook_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_hook_pair_key(unit_id, arm_name, variant_id)
    rows = threads.get(key, [])
    return {
        "run_id": run_id,
        "brief_unit_id": unit_id,
        "arm": arm_name,
        "hook_id": variant_id,
        "messages": [row.model_dump() for row in rows],
        "locked": _phase3_v2_is_locked(brand_slug, branch_id, run_id),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/chat")
async def api_phase3_v2_hooks_chat_post(
    branch_id: str,
    run_id: str,
    req: Phase3V2HookChatRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    variant_id = str(req.hook_id or "").strip()
    prompt = str(req.message or "").strip()
    if not prompt:
        return JSONResponse({"error": "Message is required."}, status_code=400)
    if not variant_id:
        return JSONResponse({"error": "hook_id is required."}, status_code=400)

    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)

    brief_unit = _phase3_v2_find_brief_unit(detail, unit_id)
    if not brief_unit:
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)
    draft = _phase3_v2_find_draft(detail, arm_name, unit_id)
    if not draft:
        return JSONResponse({"error": "Draft not found for this Brief Unit/arm."}, status_code=404)
    hook_variant = _phase3_v2_find_hook_variant(detail, arm_name, unit_id, variant_id)
    if not hook_variant:
        return JSONResponse({"error": f"Unknown hook_id for this Brief Unit/arm: {variant_id}"}, status_code=404)
    evidence_pack = _phase3_v2_find_evidence_pack(detail, unit_id)

    threads = _phase3_v2_load_hook_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_hook_pair_key(unit_id, arm_name, variant_id)
    prior_rows = list(threads.get(key, []))
    prior_history_payload = [
        {
            "role": row.role,
            "content": str(row.content or "").strip(),
            "created_at": row.created_at,
        }
        for row in prior_rows[-20:]
        if str(row.content or "").strip()
    ]

    system_prompt = (
        "You are an elite direct-response hook editor.\n"
        "Goals:\n"
        "1) Improve hook quality for scroll-stop and conversion intent.\n"
        "2) Keep awareness + emotion alignment.\n"
        "3) Keep copy natural. Never use meta/framework terms in verbal copy "
        "(e.g. pattern interrupt, hook type, lane, framework).\n"
        "4) Keep evidence_ids grounded in provided evidence context.\n"
        "5) Treat prior chat turns as authoritative context for references like option letters.\n"
        "Output rules:\n"
        "- Always return assistant_message.\n"
        "- Return proposed_hook only when the user asks for a rewrite/change.\n"
        "- If proposed_hook is returned, include only verbal_open and evidence_ids.\n"
        "- Do not include visual_pattern_interrupt or on_screen_text."
    )
    context_payload = {
        "brief_unit": brief_unit,
        "arm": arm_name,
        "hook_id": variant_id,
        "current_hook": hook_variant,
        "current_script": draft,
        "evidence_pack": evidence_pack or {},
    }
    chat_model = "claude-opus-4-6"
    history_json = json.dumps(prior_history_payload, ensure_ascii=True)
    user_prompt = (
        f"Context JSON:\n{json.dumps(context_payload, ensure_ascii=True)}\n\n"
        f"Prior chat turns (oldest to newest):\n{history_json}\n\n"
        f"Latest user request:\n{prompt}"
    )

    from pipeline.llm import call_llm_structured

    loop = asyncio.get_event_loop()
    try:
        reply = await loop.run_in_executor(
            None,
            lambda: call_llm_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=Phase3V2HookChatReply,
                provider="anthropic",
                model=chat_model,
                temperature=0.35,
                max_tokens=8_000,
            ),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    rows = list(prior_rows)
    rows.append(
        Phase3V2ChatMessageV1(
            role="user",
            content=prompt,
            created_at=datetime.now().isoformat(),
            provider="",
            model="",
            has_proposed_draft=False,
        )
    )
    rows.append(
        Phase3V2ChatMessageV1(
            role="assistant",
            content=str(reply.assistant_message or "").strip(),
            created_at=datetime.now().isoformat(),
            provider="anthropic",
            model=chat_model,
            has_proposed_draft=bool(reply.proposed_hook),
        )
    )
    threads[key] = rows
    _phase3_v2_save_hook_chat_threads(brand_slug, branch_id, run_id, threads)

    return {
        "assistant_message": reply.assistant_message,
        "has_proposed_hook": bool(reply.proposed_hook),
        "proposed_hook": reply.proposed_hook.model_dump() if reply.proposed_hook else None,
        "messages": [row.model_dump() for row in rows],
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/hooks/chat/apply")
async def api_phase3_v2_hooks_chat_apply(
    branch_id: str,
    run_id: str,
    req: Phase3V2HookChatApplyRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_HOOKS_ENABLED):
        return JSONResponse({"error": "Hook Generator is disabled. Set PHASE3_V2_HOOKS_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    variant_id = str(req.hook_id or "").strip()
    if not unit_id:
        return JSONResponse({"error": "brief_unit_id is required."}, status_code=400)
    if not variant_id:
        return JSONResponse({"error": "hook_id is required."}, status_code=400)

    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)
    if not _phase3_v2_find_hook_variant(detail, arm_name, unit_id, variant_id):
        return JSONResponse({"error": f"Unknown hook_id for this Brief Unit/arm: {variant_id}"}, status_code=404)

    verbal_open = str(req.proposed_hook.verbal_open or "").strip()
    visual_pattern_interrupt = str(req.proposed_hook.visual_pattern_interrupt or "").strip()
    on_screen_text = str(req.proposed_hook.on_screen_text or "").strip()
    evidence_ids = _phase3_v2_normalize_hook_evidence_ids(req.proposed_hook.evidence_ids)
    if not verbal_open:
        return JSONResponse({"error": "proposed_hook.verbal_open is required."}, status_code=400)
    if _phase3_v2_contains_meta_copy_terms(verbal_open):
        return JSONResponse(
            {"error": "proposed_hook.verbal_open contains framework/meta wording. Use direct spoken copy."},
            status_code=400,
        )
    updated = _phase3_v2_update_hook_variant_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=variant_id,
        verbal_open=verbal_open,
        visual_pattern_interrupt=visual_pattern_interrupt,
        on_screen_text=on_screen_text,
        evidence_ids=evidence_ids,
        source="chat_apply",
    )
    if not updated:
        return JSONResponse({"error": f"Could not update hook variant: {variant_id}"}, status_code=404)

    refreshed = _phase3_v2_refresh_scene_handoff(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": "hook_chat_apply",
            "last_hook_edit": {
                "brief_unit_id": unit_id,
                "arm": arm_name,
                "hook_id": variant_id,
            },
        },
    )
    return {
        "ok": True,
        "hook": updated,
        **refreshed,
    }


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/prepare")
async def api_phase3_v2_scenes_prepare(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    scene_items, scene_eligibility = _phase3_v2_build_scene_items_from_detail(detail)
    return {
        "run_id": run_id,
        "scene_stage": detail.get("scene_stage", {}),
        "scene_handoff_ready": bool(detail.get("scene_handoff_ready")),
        "eligible_count": len(scene_items),
        "skipped_count": int(scene_eligibility.get("skipped_count", 0)),
        "eligible_units": scene_eligibility.get("eligible", []),
        "skipped_units": scene_eligibility.get("skipped", []),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/run")
async def api_phase3_v2_scenes_run(branch_id: str, run_id: str, req: Phase3V2SceneRunRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if not bool(detail.get("scene_handoff_ready")):
        return JSONResponse(
            {"error": "Scene handoff is not ready. Complete hook selections first."},
            status_code=400,
        )

    scene_stage = _phase3_v2_load_scene_stage_manifest(brand_slug, branch_id, run_id)
    task_key = f"{brand_slug}:{branch_id}:{run_id}:scenes"
    task = phase3_v2_scene_tasks.get(task_key)
    if scene_stage.status == "running" and task is not None and not task.done():
        return JSONResponse({"error": "Scene stage is already running for this run."}, status_code=409)

    selected_ids = [str(v).strip() for v in (req.selected_brief_unit_ids or []) if str(v or "").strip()]
    scene_items, scene_eligibility = _phase3_v2_build_scene_items_from_detail(
        detail,
        selected_brief_unit_ids=selected_ids,
    )
    if not scene_items:
        return JSONResponse(
            {
                "error": "No eligible Scene Units to run scenes.",
                "eligible_count": int(scene_eligibility.get("eligible_count", 0)),
                "skipped_count": int(scene_eligibility.get("skipped_count", 0)),
            },
            status_code=400,
        )

    scene_run_id = f"scv2_{int(time.time() * 1000)}"
    stage_manifest = SceneStageManifestV1(
        run_id=run_id,
        scene_run_id=scene_run_id,
        status="running",
        created_at=datetime.now().isoformat(),
        started_at=datetime.now().isoformat(),
        completed_at="",
        error="",
        eligible_count=len(scene_items),
        processed_count=0,
        failed_count=0,
        skipped_count=0,
        stale_count=0,
        max_parallel=int(config.PHASE3_V2_SCENE_MAX_PARALLEL),
        max_repair_rounds=int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
        max_consecutive_mode=int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
        min_a_roll_lines=int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
        model_registry={},
        metrics={},
    )
    _phase3_v2_save_scene_stage_manifest(brand_slug, branch_id, run_id, stage_manifest)
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "scene_stage_status": "running",
            "scene_run_id": scene_run_id,
        },
    )

    task = asyncio.create_task(
        _phase3_v2_execute_scenes(
            brand_slug=brand_slug,
            branch_id=branch_id,
            run_id=run_id,
            scene_run_id=scene_run_id,
            selected_brief_unit_ids=selected_ids,
            model_overrides=dict(req.model_overrides or {}),
        )
    )
    phase3_v2_scene_tasks[task_key] = task

    return {
        "status": "started",
        "run_id": run_id,
        "scene_run_id": scene_run_id,
        "eligible_count": len(scene_items),
    }


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes")
async def api_phase3_v2_scenes_status(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    return {
        "run_id": run_id,
        "scene_stage": detail.get("scene_stage", {}),
        "scene_progress": detail.get("scene_progress", {}),
        "scene_plans_by_arm": detail.get("scene_plans_by_arm", {}),
        "scene_gate_reports_by_arm": detail.get("scene_gate_reports_by_arm", {}),
        "production_handoff_packet": detail.get("production_handoff_packet", {}),
        "production_handoff_ready": bool(detail.get("production_handoff_ready")),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/update")
async def api_phase3_v2_scenes_update(
    branch_id: str,
    run_id: str,
    req: Phase3V2SceneUpdateRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    hook_id = str(req.hook_id or "").strip()
    if not unit_id:
        return JSONResponse({"error": "brief_unit_id is required."}, status_code=400)
    if not hook_id:
        return JSONResponse({"error": "hook_id is required."}, status_code=400)
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_scene_plan(detail, arm_name, unit_id, hook_id):
        return JSONResponse({"error": f"Scene plan not found for {unit_id} / {hook_id} / {arm_name}"}, status_code=404)

    normalized_lines = _phase3_v2_normalize_scene_lines(
        brief_unit_id=unit_id,
        hook_id=hook_id,
        lines=list(req.lines or []),
    )
    if not normalized_lines:
        return JSONResponse({"error": "At least one scene line is required."}, status_code=400)

    updated = _phase3_v2_update_scene_plan_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=hook_id,
        lines=normalized_lines,
        source=str(req.source or "manual"),
    )
    if not updated:
        return JSONResponse({"error": "Could not update scene plan."}, status_code=404)
    gate_report = _phase3_v2_manual_scene_gate_report(updated)
    _phase3_v2_upsert_scene_gate_report_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=hook_id,
        gate_report=gate_report,
    )

    refreshed = _phase3_v2_refresh_scene_handoff(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": str(req.source or "manual"),
            "last_scene_edit": {
                "brief_unit_id": unit_id,
                "arm": arm_name,
                "hook_id": hook_id,
            },
        },
    )
    return {"ok": True, "scene_plan": updated, "scene_gate_report": gate_report, **refreshed}


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/chat")
async def api_phase3_v2_scenes_chat_get(
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    hook_id: str,
    brand: str = "",
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(brief_unit_id or "").strip()
    arm_name = str(arm or "").strip()
    hook_variant_id = str(hook_id or "").strip()
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_scene_plan(detail, arm_name, unit_id, hook_variant_id):
        return JSONResponse({"error": f"Scene plan not found for {unit_id} / {hook_variant_id} / {arm_name}"}, status_code=404)

    threads = _phase3_v2_load_scene_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_scene_pair_key(unit_id, arm_name, hook_variant_id)
    rows = threads.get(key, [])
    return {
        "run_id": run_id,
        "brief_unit_id": unit_id,
        "arm": arm_name,
        "hook_id": hook_variant_id,
        "messages": [row.model_dump() for row in rows],
        "locked": _phase3_v2_is_locked(brand_slug, branch_id, run_id),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/chat")
async def api_phase3_v2_scenes_chat_post(
    branch_id: str,
    run_id: str,
    req: Phase3V2SceneChatRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    hook_variant_id = str(req.hook_id or "").strip()
    prompt = str(req.message or "").strip()
    if not prompt:
        return JSONResponse({"error": "Message is required."}, status_code=400)
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    scene_plan = _phase3_v2_find_scene_plan(detail, arm_name, unit_id, hook_variant_id)
    if not scene_plan:
        return JSONResponse({"error": f"Scene plan not found for {unit_id} / {hook_variant_id} / {arm_name}"}, status_code=404)

    draft = _phase3_v2_find_draft(detail, arm_name, unit_id)
    hook_variant = _phase3_v2_find_hook_variant(detail, arm_name, unit_id, hook_variant_id)
    evidence_pack = _phase3_v2_find_evidence_pack(detail, unit_id)
    brief_unit = _phase3_v2_find_brief_unit(detail, unit_id)

    threads = _phase3_v2_load_scene_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_scene_pair_key(unit_id, arm_name, hook_variant_id)
    prior_rows = list(threads.get(key, []))
    prior_history_payload = [
        {
            "role": row.role,
            "content": str(row.content or "").strip(),
            "created_at": row.created_at,
        }
        for row in prior_rows[-20:]
        if str(row.content or "").strip()
    ]

    system_prompt = (
        "You are an elite UGC scene direction editor.\\n"
        "Improve scene clarity and pacing while preserving script intent.\\n"
        "Keep each scene line simple: mode, narration_line, scene_description.\\n"
        "For a_roll describe what the creator does on camera.\\n"
        "For b_roll describe physical scene/action.\\n"
        "For animation_broll describe animation direction.\\n"
        "No adjacent a_roll lines in the final plan.\\n"
        "When user asks for changes, return complete proposed_scene_plan JSON."
    )
    context_payload = {
        "brief_unit": brief_unit or {},
        "arm": arm_name,
        "hook_id": hook_variant_id,
        "hook_variant": hook_variant or {},
        "script_draft": draft or {},
        "evidence_pack": evidence_pack or {},
        "current_scene_plan": scene_plan,
        "prior_chat": prior_history_payload,
    }
    chat_model = "claude-opus-4-6"
    user_prompt = (
        f"Context JSON:\\n{json.dumps(context_payload, ensure_ascii=True)}\\n\\n"
        f"Latest user request:\\n{prompt}"
    )

    from pipeline.llm import call_llm_structured

    loop = asyncio.get_event_loop()
    try:
        reply = await loop.run_in_executor(
            None,
            lambda: call_llm_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=SceneChatReplyV1,
                provider="anthropic",
                model=chat_model,
                temperature=0.35,
                max_tokens=14_000,
            ),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    rows = list(prior_rows)
    rows.append(
        Phase3V2ChatMessageV1(
            role="user",
            content=prompt,
            created_at=datetime.now().isoformat(),
            provider="",
            model="",
            has_proposed_draft=False,
        )
    )
    rows.append(
        Phase3V2ChatMessageV1(
            role="assistant",
            content=str(reply.assistant_message or "").strip(),
            created_at=datetime.now().isoformat(),
            provider="anthropic",
            model=chat_model,
            has_proposed_draft=bool(reply.proposed_scene_plan),
        )
    )
    threads[key] = rows
    _phase3_v2_save_scene_chat_threads(brand_slug, branch_id, run_id, threads)

    return {
        "assistant_message": reply.assistant_message,
        "has_proposed_scene_plan": bool(reply.proposed_scene_plan),
        "proposed_scene_plan": reply.proposed_scene_plan.model_dump() if reply.proposed_scene_plan else None,
        "messages": [row.model_dump() for row in rows],
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/scenes/chat/apply")
async def api_phase3_v2_scenes_chat_apply(
    branch_id: str,
    run_id: str,
    req: Phase3V2SceneChatApplyRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE3_V2_SCENES_ENABLED):
        return JSONResponse({"error": "Scene Writer is disabled. Set PHASE3_V2_SCENES_ENABLED=true."}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    hook_variant_id = str(req.hook_id or "").strip()
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_scene_plan(detail, arm_name, unit_id, hook_variant_id):
        return JSONResponse({"error": f"Scene plan not found for {unit_id} / {hook_variant_id} / {arm_name}"}, status_code=404)

    line_payloads = [
        Phase3V2SceneLinePayload(
            scene_line_id=str(line.scene_line_id or ""),
            script_line_id=str(line.script_line_id or ""),
            source_script_line_id=str(line.source_script_line_id or ""),
            beat_index=int(line.beat_index or 1),
            beat_text=str(line.beat_text or ""),
            mode=str(line.mode or "a_roll"),
            narration_line=str(line.narration_line or ""),
            scene_description=str(line.scene_description or ""),
            a_roll=line.a_roll.model_dump() if line.a_roll else {},
            b_roll=line.b_roll.model_dump() if line.b_roll else {},
            on_screen_text=str(line.on_screen_text or ""),
            duration_seconds=float(line.duration_seconds or 2.0),
            evidence_ids=list(line.evidence_ids or []),
            difficulty_1_10=int(line.difficulty_1_10 or 5),
        )
        for line in (req.proposed_scene_plan.lines or [])
    ]
    normalized_lines = _phase3_v2_normalize_scene_lines(
        brief_unit_id=unit_id,
        hook_id=hook_variant_id,
        lines=line_payloads,
    )
    if not normalized_lines:
        return JSONResponse({"error": "proposed_scene_plan must contain at least one line."}, status_code=400)

    updated = _phase3_v2_update_scene_plan_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=hook_variant_id,
        lines=normalized_lines,
        source="chat_apply",
    )
    if not updated:
        return JSONResponse({"error": "Could not update scene plan."}, status_code=404)
    gate_report = _phase3_v2_manual_scene_gate_report(updated)
    _phase3_v2_upsert_scene_gate_report_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        hook_id=hook_variant_id,
        gate_report=gate_report,
    )

    refreshed = _phase3_v2_refresh_scene_handoff(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": "scene_chat_apply",
            "last_scene_edit": {
                "brief_unit_id": unit_id,
                "arm": arm_name,
                "hook_id": hook_variant_id,
            },
        },
    )
    return {"ok": True, "scene_plan": updated, "scene_gate_report": gate_report, **refreshed}


@app.post("/api/branches/{branch_id}/phase3-v2/decisions")
async def api_phase3_v2_decisions(branch_id: str, req: Phase3V2DecisionRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, req.run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, req.run_id):
        return _phase3_v2_mutation_locked_response(req.run_id)

    run = detail.get("run", {})
    reviewer_role = (
        str(req.reviewer_role or run.get("reviewer_role") or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT)
        .strip()
        .lower()
        or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT
    )
    allowed_decisions = {"approve", "revise", "reject"}
    arms = set(_phase3_v2_get_run_arms(detail))
    brief_unit_ids = {
        str(row.get("brief_unit_id") or "").strip()
        for row in (detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else [])
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }

    existing = _phase3_v2_load_decisions(brand_slug, branch_id, req.run_id)
    upsert_map: dict[tuple[str, str], BriefUnitDecisionV1] = {}
    for row in existing:
        upsert_map[(row.brief_unit_id, row.arm)] = row

    if not req.decisions:
        return JSONResponse({"error": "No decisions provided."}, status_code=400)

    for payload in req.decisions:
        brief_unit_id = str(payload.brief_unit_id or "").strip()
        arm = str(payload.arm or "").strip()
        decision = str(payload.decision or "").strip().lower()
        if not brief_unit_id:
            return JSONResponse({"error": "brief_unit_id is required for each decision."}, status_code=400)
        if brief_unit_id not in brief_unit_ids:
            return JSONResponse({"error": f"Unknown brief_unit_id: {brief_unit_id}"}, status_code=400)
        if arm not in arms:
            return JSONResponse({"error": f"Unknown arm for this run: {arm}"}, status_code=400)
        if decision not in allowed_decisions:
            return JSONResponse({"error": f"Invalid decision '{decision}'. Use approve/revise/reject."}, status_code=400)

        parsed = BriefUnitDecisionV1(
            run_id=req.run_id,
            brief_unit_id=brief_unit_id,
            arm=arm,  # validated against run arms above
            reviewer_role=reviewer_role,
            reviewer_id=str(payload.reviewer_id or "").strip(),
            decision=decision,  # validated above
            updated_at=datetime.now().isoformat(),
        )
        upsert_map[(brief_unit_id, arm)] = parsed

    merged = list(upsert_map.values())
    merged.sort(key=lambda d: (d.brief_unit_id, d.arm))
    _phase3_v2_save_decisions(brand_slug, branch_id, req.run_id, merged)

    progress = _phase3_v2_compute_decision_progress(
        detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else [],
        list(arms),
        merged,
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": req.run_id,
            "updated_at": datetime.now().isoformat(),
            "decision_count": len(merged),
            "decision_approved": int(progress.approved),
            "decision_pending": int(progress.pending),
        },
    )
    return {
        "ok": True,
        "decision_count": len(merged),
        "decision_progress": progress.model_dump(),
        "final_lock": _phase3_v2_load_final_lock(brand_slug, branch_id, req.run_id).model_dump(),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/drafts/{arm}/{brief_unit_id}")
async def api_phase3_v2_update_draft(
    branch_id: str,
    run_id: str,
    arm: str,
    brief_unit_id: str,
    req: Phase3V2DraftUpdateRequest,
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    arm_name = str(arm or "").strip()
    unit_id = str(brief_unit_id or "").strip()
    if not unit_id:
        return JSONResponse({"error": "brief_unit_id is required."}, status_code=400)

    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)

    normalized_lines = _phase3_v2_normalize_lines(req.lines or [])
    if not normalized_lines:
        return JSONResponse({"error": "At least one non-empty line is required."}, status_code=400)

    draft = _phase3_v2_find_draft(detail, arm_name, unit_id)
    existing_sections = None
    if isinstance(draft, dict) and isinstance(draft.get("sections"), dict):
        try:
            existing_sections = CoreScriptSectionsV1.model_validate(draft.get("sections"))
        except Exception:
            existing_sections = None

    chosen_sections = req.sections or existing_sections or _phase3_v2_sections_from_lines(normalized_lines)
    required_sections = [
        str(chosen_sections.hook or "").strip(),
        str(chosen_sections.problem or "").strip(),
        str(chosen_sections.mechanism or "").strip(),
        str(chosen_sections.proof or "").strip(),
        str(chosen_sections.cta or "").strip(),
    ]
    if not all(required_sections):
        return JSONResponse({"error": "All script sections (hook/problem/mechanism/proof/cta) must be non-empty."}, status_code=400)

    updated = _phase3_v2_update_draft_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        sections=chosen_sections,
        lines=normalized_lines,
        source=str(req.source or "manual"),
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": str(req.source or "manual"),
        },
    )
    return {"ok": True, "draft": updated}


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/chat")
async def api_phase3_v2_chat_get(
    branch_id: str,
    run_id: str,
    brief_unit_id: str,
    arm: str,
    brand: str = "",
):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(brief_unit_id or "").strip()
    arm_name = str(arm or "").strip()
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)

    threads = _phase3_v2_load_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_pair_key(unit_id, arm_name)
    rows = threads.get(key, [])
    return {
        "run_id": run_id,
        "brief_unit_id": unit_id,
        "arm": arm_name,
        "messages": [row.model_dump() for row in rows],
        "locked": _phase3_v2_is_locked(brand_slug, branch_id, run_id),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/chat")
async def api_phase3_v2_chat_post(branch_id: str, run_id: str, req: Phase3V2ChatRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    prompt = str(req.message or "").strip()
    if not prompt:
        return JSONResponse({"error": "Message is required."}, status_code=400)

    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)

    brief_unit = _phase3_v2_find_brief_unit(detail, unit_id)
    if not brief_unit:
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)
    draft = _phase3_v2_find_draft(detail, arm_name, unit_id)
    if not draft:
        return JSONResponse({"error": "Draft not found for this Brief Unit/arm."}, status_code=404)
    evidence_pack = _phase3_v2_find_evidence_pack(detail, unit_id)

    spec_payload: dict[str, Any] = {}
    if isinstance(brief_unit, dict) and isinstance(evidence_pack, dict):
        try:
            spec_payload = compile_script_spec_v1(
                BriefUnitV1.model_validate(brief_unit),
                EvidencePackV1.model_validate(evidence_pack),
            ).model_dump()
        except Exception:
            spec_payload = {}

    threads = _phase3_v2_load_chat_threads(brand_slug, branch_id, run_id)
    key = _phase3_v2_pair_key(unit_id, arm_name)
    prior_rows = list(threads.get(key, []))
    prior_history_payload = [
        {
            "role": row.role,
            "content": str(row.content or "").strip(),
            "created_at": row.created_at,
        }
        for row in prior_rows[-16:]
        if str(row.content or "").strip()
    ]

    system_prompt = (
        "You are an elite direct-response script editor for Phase 3 Brief Units.\n"
        "Your goals:\n"
        "1) Help the user improve the script quality.\n"
        "2) When asked for edits, return a complete proposed draft payload.\n"
        "3) Keep citations/evidence_ids realistic and line-level.\n"
        "4) Keep awareness + emotion alignment.\n"
        "5) Treat prior chat turns as authoritative context for references like option letters.\n"
        "Output format rules:\n"
        "- Always return assistant_message.\n"
        "- Return proposed_draft only when the user is requesting a rewrite/change.\n"
        "- If proposed_draft is provided, include complete sections + full lines array."
    )
    context_payload = {
        "brief_unit": brief_unit,
        "arm": arm_name,
        "current_draft": draft,
        "evidence_pack": evidence_pack or {},
        "script_spec": spec_payload,
    }
    chat_model = "claude-opus-4-6"
    history_json = json.dumps(prior_history_payload, ensure_ascii=True)
    user_prompt = (
        f"Context JSON:\n{json.dumps(context_payload, ensure_ascii=True)}\n\n"
        f"Prior chat turns (oldest to newest):\n{history_json}\n\n"
        f"Latest user request:\n{prompt}"
    )

    from pipeline.llm import call_llm_structured

    loop = asyncio.get_event_loop()
    try:
        reply = await loop.run_in_executor(
            None,
            lambda: call_llm_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=Phase3V2ChatReplyV1,
                provider="anthropic",
                model=chat_model,
                temperature=0.35,
                max_tokens=12_000,
            ),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    rows = list(prior_rows)
    rows.append(
        Phase3V2ChatMessageV1(
            role="user",
            content=prompt,
            created_at=datetime.now().isoformat(),
            provider="",
            model="",
            has_proposed_draft=False,
        )
    )
    rows.append(
        Phase3V2ChatMessageV1(
            role="assistant",
            content=str(reply.assistant_message or "").strip(),
            created_at=datetime.now().isoformat(),
            provider="anthropic",
            model=chat_model,
            has_proposed_draft=bool(reply.proposed_draft),
        )
    )
    threads[key] = rows
    _phase3_v2_save_chat_threads(brand_slug, branch_id, run_id, threads)

    return {
        "assistant_message": reply.assistant_message,
        "has_proposed_draft": bool(reply.proposed_draft),
        "proposed_draft": reply.proposed_draft.model_dump() if reply.proposed_draft else None,
        "messages": [row.model_dump() for row in rows],
    }


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/chat/apply")
async def api_phase3_v2_chat_apply(branch_id: str, run_id: str, req: Phase3V2ChatApplyRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if _phase3_v2_is_locked(brand_slug, branch_id, run_id):
        return _phase3_v2_mutation_locked_response(run_id)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    unit_id = str(req.brief_unit_id or "").strip()
    arm_name = str(req.arm or "").strip()
    run_arms = set(_phase3_v2_get_run_arms(detail))
    if arm_name not in run_arms:
        return JSONResponse({"error": f"Unknown arm for this run: {arm_name}"}, status_code=400)
    if not _phase3_v2_find_brief_unit(detail, unit_id):
        return JSONResponse({"error": f"Unknown brief_unit_id: {unit_id}"}, status_code=400)

    line_payloads = [
        Phase3V2DraftLinePayload(
            line_id=str(line.line_id or ""),
            text=str(line.text or ""),
            evidence_ids=list(line.evidence_ids or []),
        )
        for line in (req.proposed_draft.lines or [])
    ]
    normalized_lines = _phase3_v2_normalize_lines(line_payloads)
    if not normalized_lines:
        return JSONResponse({"error": "Proposed draft must contain at least one non-empty line."}, status_code=400)

    sections = req.proposed_draft.sections or _phase3_v2_sections_from_lines(normalized_lines)
    required_sections = [
        str(sections.hook or "").strip(),
        str(sections.problem or "").strip(),
        str(sections.mechanism or "").strip(),
        str(sections.proof or "").strip(),
        str(sections.cta or "").strip(),
    ]
    if not all(required_sections):
        return JSONResponse({"error": "Proposed draft sections must be non-empty."}, status_code=400)

    updated = _phase3_v2_update_draft_for_unit(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
        arm=arm_name,
        brief_unit_id=unit_id,
        sections=sections,
        lines=normalized_lines,
        source="chat_apply",
    )
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "last_edit_source": "chat_apply",
        },
    )
    return {"ok": True, "draft": updated}


@app.post("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/final-lock")
async def api_phase3_v2_final_lock(branch_id: str, run_id: str, req: Phase3V2FinalLockRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    existing_lock = _phase3_v2_load_final_lock(brand_slug, branch_id, run_id)
    if existing_lock.locked:
        return {
            "ok": True,
            "already_locked": True,
            "final_lock": existing_lock.model_dump(),
            "decision_progress": detail.get("decision_progress", {}),
        }

    decisions = _phase3_v2_load_decisions(brand_slug, branch_id, run_id)
    progress = _phase3_v2_compute_decision_progress(
        detail.get("brief_units", []) if isinstance(detail.get("brief_units"), list) else [],
        _phase3_v2_get_run_arms(detail),
        decisions,
    )
    if not progress.all_approved:
        return JSONResponse(
            {
                "error": "All Brief Units must be approved before final lock.",
                "decision_progress": progress.model_dump(),
            },
            status_code=400,
        )

    run = detail.get("run", {})
    reviewer_role = (
        str(req.reviewer_role or run.get("reviewer_role") or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT)
        .strip()
        .lower()
        or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT
    )
    lock_state = Phase3V2FinalLockV1(
        run_id=run_id,
        locked=True,
        locked_at=datetime.now().isoformat(),
        locked_by_role=reviewer_role,
    )
    _phase3_v2_save_final_lock(brand_slug, branch_id, run_id, lock_state)
    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": run_id,
            "updated_at": datetime.now().isoformat(),
            "final_locked": True,
            "final_locked_at": lock_state.locked_at,
            "lock_state": "locked",
            "lock_reviewer_role": reviewer_role,
        },
    )
    return {
        "ok": True,
        "final_lock": lock_state.model_dump(),
        "decision_progress": progress.model_dump(),
    }


@app.post("/api/branches/{branch_id}/phase3-v2/reviews")
async def api_phase3_v2_reviews(branch_id: str, req: Phase3V2ReviewRequest):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, req.run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    run = detail.get("run", {})
    reviewer_role = (
        str(req.reviewer_role or run.get("reviewer_role") or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT)
        .strip()
        .lower()
        or config.PHASE3_V2_REVIEWER_ROLE_DEFAULT
    )

    existing_raw = detail.get("reviews", [])
    existing_reviews: list[HumanQualityReviewV1] = []
    if isinstance(existing_raw, list):
        for row in existing_raw:
            if not isinstance(row, dict):
                continue
            try:
                existing_reviews.append(HumanQualityReviewV1.model_validate(row))
            except Exception:
                continue

    upsert_map: dict[tuple[str, str, str, str], HumanQualityReviewV1] = {}
    for row in existing_reviews:
        key = (row.brief_unit_id, row.arm, row.reviewer_role, row.reviewer_id)
        upsert_map[key] = row

    for payload in req.reviews:
        try:
            parsed = HumanQualityReviewV1(
                run_id=req.run_id,
                brief_unit_id=str(payload.brief_unit_id).strip(),
                arm=str(payload.arm).strip(),  # validated by schema literal
                reviewer_role=reviewer_role,
                reviewer_id=str(payload.reviewer_id or "").strip(),
                quality_score_1_10=int(payload.quality_score_1_10),
                decision=str(payload.decision).strip(),
                notes=str(payload.notes or "").strip(),
            )
        except Exception as exc:
            return JSONResponse({"error": f"Invalid review payload: {exc}"}, status_code=400)
        key = (parsed.brief_unit_id, parsed.arm, parsed.reviewer_role, parsed.reviewer_id)
        upsert_map[key] = parsed

    merged_reviews = list(upsert_map.values())
    merged_reviews.sort(key=lambda r: (r.brief_unit_id, r.arm, r.reviewer_role, r.reviewer_id))

    run_dir = _phase3_v2_run_dir(brand_slug, branch_id, req.run_id)
    _phase3_v2_write_json(run_dir / "reviews.json", [r.model_dump() for r in merged_reviews])

    drafts_by_arm = detail.get("drafts_by_arm", {})
    if not isinstance(drafts_by_arm, dict):
        drafts_by_arm = {}
    summary = compute_ab_summary(
        run_id=req.run_id,
        drafts_by_arm=drafts_by_arm,
        reviews=merged_reviews,
    )
    _phase3_v2_write_json(run_dir / "summary.json", summary.model_dump())

    _phase3_v2_upsert_manifest_entry(
        brand_slug,
        branch_id,
        {
            "run_id": req.run_id,
            "updated_at": datetime.now().isoformat(),
            "review_count": len(merged_reviews),
            "winner": summary.winner,
        },
    )

    return {"ok": True, "summary": summary.model_dump(), "review_count": len(merged_reviews)}


@app.get("/api/branches/{branch_id}/phase3-v2/runs/{run_id}/summary")
async def api_phase3_v2_run_summary(branch_id: str, run_id: str, brand: str = ""):
    err = _phase3_v2_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, run_id)
    if not detail:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    summary = detail.get("summary", {})
    if not summary:
        reviews_raw = detail.get("reviews", [])
        parsed_reviews: list[HumanQualityReviewV1] = []
        if isinstance(reviews_raw, list):
            for row in reviews_raw:
                if not isinstance(row, dict):
                    continue
                try:
                    parsed_reviews.append(HumanQualityReviewV1.model_validate(row))
                except Exception:
                    continue
        drafts_by_arm = detail.get("drafts_by_arm", {})
        if not isinstance(drafts_by_arm, dict):
            drafts_by_arm = {}
        computed = compute_ab_summary(
            run_id=run_id,
            drafts_by_arm=drafts_by_arm,
            reviews=parsed_reviews,
        )
        summary = computed.model_dump()
        run_dir = _phase3_v2_run_dir(brand_slug, branch_id, run_id)
        _phase3_v2_write_json(run_dir / "summary.json", summary)
    return summary


def _phase4_v1_disabled_error() -> str | None:
    if not bool(config.PHASE4_V1_ENABLED):
        return "Phase 4 video generation is disabled. Set PHASE4_V1_ENABLED=true to enable."
    return None


def _phase4_v1_model_registry() -> dict[str, str]:
    return {
        "fal_broll": str(config.PHASE4_V1_FAL_BROLL_MODEL_ID),
        "fal_talking_head": str(config.PHASE4_V1_FAL_TALKING_HEAD_MODEL_ID),
        "gemini_image_edit": str(config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID),
        "openai_image_edit": str(config.PHASE4_V1_OPENAI_IMAGE_EDIT_MODEL_ID),
        "tts": str(config.PHASE4_V1_TTS_MODEL),
    }


def _phase4_v1_run_key(brand_slug: str, branch_id: str, video_run_id: str) -> str:
    return f"{brand_slug}:{branch_id}:{video_run_id}"


def _phase4_v1_load_brief(brand_slug: str, branch_id: str, video_run_id: str) -> StartFrameBriefV1 | None:
    raw = _phase4_v1_read_json(_phase4_v1_start_frame_brief_path(brand_slug, branch_id, video_run_id), {})
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        return StartFrameBriefV1.model_validate(raw)
    except Exception:
        return None


def _phase4_v1_load_mapping_rows(brand_slug: str, branch_id: str, video_run_id: str) -> list[SceneLineMappingRowV1]:
    raw = _phase4_v1_read_json(_phase4_v1_scene_line_mapping_path(brand_slug, branch_id, video_run_id), [])
    out: list[SceneLineMappingRowV1] = []
    if not isinstance(raw, list):
        return out
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            out.append(SceneLineMappingRowV1.model_validate(row))
        except Exception:
            continue
    return out


_PHASE4_STORYBOARD_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
_PHASE4_STORYBOARD_START_FRAME_WIDTH = 1080
_PHASE4_STORYBOARD_START_FRAME_HEIGHT = 1920
_PHASE4_STORYBOARD_ASSIGN_MAX_PARALLEL = 5
_PHASE4_STORYBOARD_SHORTLIST_SIZE = 12
_PHASE4_STORYBOARD_SHORTLIST_EXPAND_BATCH = 4
_PHASE4_STORYBOARD_SHORTLIST_MAX_SCORES = 20
_PHASE4_STORYBOARD_RECENT_FINGERPRINT_WINDOW = 3
_PHASE4_STORYBOARD_TOKEN_STOPWORDS = {
    "about",
    "after",
    "and",
    "are",
    "around",
    "because",
    "been",
    "before",
    "being",
    "between",
    "could",
    "does",
    "from",
    "into",
    "just",
    "like",
    "maybe",
    "more",
    "near",
    "over",
    "show",
    "that",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "with",
    "your",
}


def _phase4_v1_storyboard_supported_image(file_name: str) -> bool:
    suffix = Path(str(file_name or "").strip()).suffix.lower()
    return suffix in _PHASE4_STORYBOARD_IMAGE_EXTS


def _phase4_v1_storyboard_score(value: Any) -> int:
    try:
        score = int(float(value))
    except Exception:
        score = 0
    if score <= 0:
        return 0
    return max(1, min(10, score))


def _phase4_v1_storyboard_scene_description_from_line(line: dict[str, Any]) -> str:
    if not isinstance(line, dict):
        return ""
    explicit = str(line.get("scene_description") or "").strip()
    if explicit:
        return explicit
    mode = normalize_phase4_clip_mode(line.get("mode"))
    a_roll = line.get("a_roll") if isinstance(line.get("a_roll"), dict) else {}
    b_roll = line.get("b_roll") if isinstance(line.get("b_roll"), dict) else {}
    if mode == "a_roll":
        for key in ("creator_action", "framing", "performance_direction", "product_interaction", "location"):
            text = str(a_roll.get(key) or "").strip()
            if text:
                return text
    for key in ("shot_description", "subject_action", "camera_motion", "props_assets", "location", "transition_intent"):
        text = str(b_roll.get(key) or "").strip()
        if text:
            return text
    fallback = str(line.get("on_screen_text") or "").strip()
    return fallback


def _phase4_v1_storyboard_scene_lookup(
    *,
    brand_slug: str,
    branch_id: str,
    phase3_run_id: str,
) -> dict[str, dict[str, str]]:
    detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, phase3_run_id)
    production = detail.get("production_handoff_packet") if isinstance(detail, dict) else None
    needs_fallback = (
        not isinstance(production, dict)
        or not isinstance(production.get("items"), list)
        or not bool(production.get("items"))
    )
    if needs_fallback:
        raw = _phase3_v2_read_json(
            _phase3_v2_production_handoff_path(brand_slug, branch_id, phase3_run_id),
            {},
        )
        if isinstance(raw, dict) and isinstance(raw.get("items"), list) and bool(raw.get("items")):
            production = raw
    if not isinstance(production, dict):
        return {}
    items = production.get("items") if isinstance(production.get("items"), list) else []
    lookup: dict[str, dict[str, str]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        lines = item.get("lines") if isinstance(item.get("lines"), list) else []
        for line in lines:
            if not isinstance(line, dict):
                continue
            scene_line_id = str(line.get("scene_line_id") or "").strip()
            if not scene_line_id:
                continue
            lookup[scene_line_id] = {
                "narration_line": str(line.get("narration_line") or "").strip(),
                "scene_description": _phase4_v1_storyboard_scene_description_from_line(line),
            }
    return lookup


def _phase4_v1_storyboard_resolve_image_edit_model(selection: str) -> tuple[str, str]:
    raw = str(selection or "").strip()
    key = raw.lower()
    preset_map: dict[str, tuple[str, str]] = {
        "nano_banana_pro_1k_2k": ("gemini-2.5-flash-image", "Nano Banana Pro 1K/2K"),
        "nano_banana_pro_4k": ("gemini-2.5-flash-image", "Nano Banana Pro 4K"),
        "gemini_2_5_flash_image": ("gemini-2.5-flash-image", "Gemini 2.5 Flash Image"),
        "chatgpt_gpt_image_1_5": ("gpt-image-1.5", "ChatGPT GPT Image 1.5"),
    }
    if key in preset_map:
        return preset_map[key]
    if raw:
        return raw, raw
    default_model = str(config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID).strip() or "gemini-2.5-flash-image"
    return default_model, default_model


def _phase4_v1_storyboard_detect_prompt_provider(model_id: str) -> str:
    value = str(model_id or "").strip().lower()
    if not value:
        return "openai"
    if value.startswith("claude") or "anthropic" in value:
        return "anthropic"
    if value.startswith("gemini") or "google" in value:
        return "google"
    return "openai"


def _phase4_v1_storyboard_resolve_prompt_model(selection: str) -> tuple[str, str, str]:
    raw = str(selection or "").strip()
    key = raw.lower()
    preset_map: dict[str, tuple[str, str, str]] = {
        "claude_opus_4_6": ("anthropic", "claude-opus-4-6", "Claude Opus 4.6"),
        "claude_sonnet_4_6": ("anthropic", "claude-sonnet-4-6", "Claude Sonnet 4.6"),
        "gpt_5_2": ("openai", "gpt-5.2", "GPT 5.2"),
        "gemini_3_1": ("google", "gemini-3.1-pro-preview", "Gemini 3.1 Pro Preview"),
    }
    if key in preset_map:
        return preset_map[key]
    if "/" in raw:
        provider_part, model_part = raw.split("/", 1)
        provider = str(provider_part or "").strip().lower()
        model_id = str(model_part or "").strip()
        if provider in {"openai", "anthropic", "google"} and model_id:
            return provider, model_id, f"{provider}/{model_id}"
    if raw:
        provider = _phase4_v1_storyboard_detect_prompt_provider(raw)
        return provider, raw, raw
    default_model = str(config.PHASE4_V1_VISION_SCENE_MODEL_ID).strip() or "gpt-4o-mini"
    default_provider = _phase4_v1_storyboard_detect_prompt_provider(default_model)
    return default_provider, default_model, default_model


def _phase4_v1_storyboard_render_start_frame_9_16(*, source_path: Path, output_path: Path) -> None:
    if not source_path.exists() or not source_path.is_file():
        raise RuntimeError(f"Storyboard source image missing: {source_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(source_path),
        "-vf",
        (
            f"scale={_PHASE4_STORYBOARD_START_FRAME_WIDTH}:{_PHASE4_STORYBOARD_START_FRAME_HEIGHT}:"
            "force_original_aspect_ratio=increase,"
            f"crop={_PHASE4_STORYBOARD_START_FRAME_WIDTH}:{_PHASE4_STORYBOARD_START_FRAME_HEIGHT},"
            "setsar=1"
        ),
        "-frames:v",
        "1",
        "-c:v",
        "png",
        str(tmp_path),
    ]
    proc = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0 or not tmp_path.exists():
        stderr = str(proc.stderr or "").strip()
        force_mock = str(os.getenv("PHASE4_V1_FORCE_MOCK_GENERATION", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if force_mock:
            shutil.copy2(source_path, output_path)
            return
        raise RuntimeError(f"Failed to normalize start frame to 9:16 via ffmpeg: {stderr or 'unknown error'}")
    tmp_path.replace(output_path)


def _phase4_v1_storyboard_style_profile(analysis_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counters: dict[str, Counter[str]] = {
        "camera_angle": Counter(),
        "shot_type": Counter(),
        "lighting": Counter(),
        "mood": Counter(),
        "setting": Counter(),
    }
    style_tags: Counter[str] = Counter()
    for row in analysis_rows:
        if not isinstance(row, dict):
            continue
        for key in counters.keys():
            value = str(row.get(key) or "").strip().lower()
            if value:
                counters[key][value] += 1
        tags = row.get("style_tags")
        if isinstance(tags, list):
            for tag in tags:
                text = str(tag or "").strip().lower()
                if text:
                    style_tags[text] += 1

    profile: dict[str, Any] = {}
    for key, counter in counters.items():
        profile[key] = counter.most_common(1)[0][0] if counter else ""
    profile["style_tags"] = [tag for tag, _ in style_tags.most_common(8)]
    return profile


def _phase4_v1_storyboard_edit_prompt(
    *,
    scene_intent: dict[str, Any],
    style_profile: dict[str, Any],
) -> str:
    mode = str(scene_intent.get("mode") or "b_roll").strip()
    script_line_id = str(scene_intent.get("script_line_id") or "").strip()
    narration = str(scene_intent.get("narration_line") or "").strip()
    description = str(scene_intent.get("scene_description") or "").strip()
    style_chunks = []
    for key in ("shot_type", "camera_angle", "lighting", "mood", "setting"):
        value = str(style_profile.get(key) or "").strip()
        if value:
            style_chunks.append(f"{key}: {value}")
    tags = style_profile.get("style_tags") if isinstance(style_profile.get("style_tags"), list) else []
    if tags:
        style_chunks.append(f"style tags: {', '.join([str(v) for v in tags[:8]])}")
    style_text = "; ".join(style_chunks)
    return (
        "Refine this image into a stronger short-form video start frame.\n"
        f"Mode: {mode}\n"
        f"Script line ID: {script_line_id or 'unknown'}\n"
        f"Narration line: {narration}\n"
        f"Scene description: {description}\n"
        f"Style profile to match: {style_text or 'keep current visual style consistent'}.\n"
        "Create a visually distinct variation for this specific scene beat.\n"
        "Preserve identity/product consistency and 9:16 composition.\n"
        "STRICT: Never render any text, words, letters, numbers, captions, subtitles, watermarks, or logos anywhere in the image — including on screens, signs, clothing, or surfaces."
    )


def _phase4_v1_storyboard_totals(by_scene_line_id: dict[str, dict[str, Any]]) -> dict[str, int]:
    totals = {
        "total": len(by_scene_line_id),
        "pending": 0,
        "analyzing": 0,
        "assigned": 0,
        "assigned_needs_review": 0,
        "failed": 0,
        "completed": 0,
    }
    for row in by_scene_line_id.values():
        status = str(row.get("assignment_status") or "pending").strip()
        if status in totals:
            totals[status] += 1
        if status in {"assigned", "assigned_needs_review", "failed"}:
            totals["completed"] += 1
    return totals


def _phase4_v1_storyboard_blank_status(video_run_id: str) -> dict[str, Any]:
    return StoryboardAssignStatusV1(video_run_id=video_run_id).model_dump()


def _phase4_v1_storyboard_backfill_edit_fields(
    *,
    video_run_id: str,
    by_scene_line_id: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not by_scene_line_id:
        return by_scene_line_id
    clip_rows = list_video_clips(video_run_id)
    scene_to_clip: dict[str, str] = {}
    for clip in clip_rows:
        if not isinstance(clip, dict):
            continue
        scene_line_id = str(clip.get("scene_line_id") or "").strip()
        clip_id = str(clip.get("clip_id") or "").strip()
        if scene_line_id and clip_id:
            scene_to_clip[scene_line_id] = clip_id
    all_assets = list_video_assets(video_run_id)
    latest_start_frame_by_clip: dict[str, dict[str, Any]] = {}
    latest_transformed_by_clip: dict[str, dict[str, Any]] = {}
    for asset in all_assets:
        if not isinstance(asset, dict):
            continue
        clip_id = str(asset.get("clip_id") or "").strip()
        if not clip_id:
            continue
        asset_type = str(asset.get("asset_type") or "").strip().lower()
        if asset_type == "start_frame":
            latest_start_frame_by_clip[clip_id] = asset
        elif asset_type == "transformed_frame":
            latest_transformed_by_clip[clip_id] = asset
    for scene_line_id, row in by_scene_line_id.items():
        if not isinstance(row, dict):
            continue
        clip_id = str(row.get("clip_id") or "").strip() or scene_to_clip.get(scene_line_id, "")
        if clip_id:
            row["clip_id"] = clip_id
        if not clip_id:
            continue
        start_asset = latest_start_frame_by_clip.get(clip_id) or {}
        transformed_asset = latest_transformed_by_clip.get(clip_id) or {}
        start_meta = start_asset.get("metadata") if isinstance(start_asset.get("metadata"), dict) else {}
        transformed_meta = (
            transformed_asset.get("metadata") if isinstance(transformed_asset.get("metadata"), dict) else {}
        )
        if not str(row.get("edit_prompt") or "").strip():
            row["edit_prompt"] = str(
                start_meta.get("edit_prompt")
                or transformed_meta.get("prompt")
                or ""
            ).strip()
        if not str(row.get("edit_model_id") or "").strip():
            fallback_model_id = str(config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID) if str(row.get("edit_prompt") or "").strip() else ""
            row["edit_model_id"] = str(
                start_meta.get("edit_model_id")
                or transformed_meta.get("model_id")
                or fallback_model_id
            ).strip()
        if not str(row.get("edit_provider") or "").strip():
            row["edit_provider"] = str(
                start_meta.get("edit_provider")
                or transformed_meta.get("provider")
                or ""
            ).strip()
        if not bool(row.get("edited")):
            selection = str(start_meta.get("selection") or "").strip().lower()
            row["edited"] = bool(selection == "edited" or str(row.get("edit_prompt") or "").strip())
        if not str(row.get("start_frame_filename") or "").strip():
            row["start_frame_filename"] = str(start_asset.get("file_name") or "").strip()
    return by_scene_line_id


def _phase4_v1_storyboard_load_status(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
) -> dict[str, Any]:
    raw = _phase4_v1_read_json(
        _phase4_v1_storyboard_assignment_report_path(brand_slug, branch_id, video_run_id),
        {},
    )
    if not isinstance(raw, dict) or not raw:
        return _phase4_v1_storyboard_blank_status(video_run_id)
    raw["video_run_id"] = video_run_id
    by_scene = raw.get("by_scene_line_id") if isinstance(raw.get("by_scene_line_id"), dict) else {}
    normalized_by_scene: dict[str, dict[str, Any]] = {}
    for scene_line_id, row in by_scene.items():
        if not isinstance(row, dict):
            continue
        payload = dict(row)
        payload["scene_line_id"] = str(payload.get("scene_line_id") or scene_line_id)
        payload["assignment_score"] = _phase4_v1_storyboard_score(payload.get("assignment_score") or 0)
        payload["low_confidence"] = bool(payload.get("low_confidence"))
        payload["assignment_status"] = str(payload.get("assignment_status") or "pending")
        try:
            normalized = StoryboardSceneAssignmentV1.model_validate(payload).model_dump()
        except Exception:
            continue
        normalized_by_scene[str(normalized.get("scene_line_id") or scene_line_id)] = normalized
    normalized_by_scene = _phase4_v1_storyboard_backfill_edit_fields(
        video_run_id=video_run_id,
        by_scene_line_id=normalized_by_scene,
    )
    raw["by_scene_line_id"] = normalized_by_scene
    raw["totals"] = _phase4_v1_storyboard_totals(normalized_by_scene)
    try:
        return StoryboardAssignStatusV1.model_validate(raw).model_dump()
    except Exception:
        return _phase4_v1_storyboard_blank_status(video_run_id)


def _phase4_v1_storyboard_save_status(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    model = StoryboardAssignStatusV1.model_validate(payload)
    normalized = model.model_dump()
    normalized["totals"] = _phase4_v1_storyboard_totals(
        normalized.get("by_scene_line_id") if isinstance(normalized.get("by_scene_line_id"), dict) else {}
    )
    _phase4_v1_write_json(
        _phase4_v1_storyboard_assignment_report_path(brand_slug, branch_id, video_run_id),
        normalized,
    )
    return normalized


def _phase4_v1_storyboard_load_saved_versions(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
) -> list[dict[str, Any]]:
    raw = _phase4_v1_read_json(
        _phase4_v1_storyboard_saved_versions_path(brand_slug, branch_id, video_run_id),
        [],
    )
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            out.append(StoryboardSavedVersionV1.model_validate(row).model_dump())
        except Exception:
            continue
    out.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return out


def _phase4_v1_storyboard_save_saved_versions(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            normalized.append(StoryboardSavedVersionV1.model_validate(row).model_dump())
        except Exception:
            continue
    normalized.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    if len(normalized) > 50:
        normalized = normalized[:50]
    _phase4_v1_write_json(
        _phase4_v1_storyboard_saved_versions_path(brand_slug, branch_id, video_run_id),
        normalized,
    )
    return normalized


def _phase4_v1_storyboard_update_metrics(
    *,
    video_run_id: str,
    updates: dict[str, Any],
) -> None:
    run_row = get_video_run(video_run_id) or {}
    metrics = run_row.get("metrics") if isinstance(run_row.get("metrics"), dict) else {}
    merged = dict(metrics)
    merged.update({k: v for k, v in updates.items() if v is not None})
    update_video_run(video_run_id, metrics=merged)


def _phase4_v1_storyboard_int_dict(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        try:
            out[str(key)] = int(value)
        except Exception:
            continue
    return out


def _phase4_v1_storyboard_build_initial_status(
    *,
    video_run_id: str,
    clips: list[dict[str, Any]],
) -> dict[str, Any]:
    by_scene_line_id: dict[str, dict[str, Any]] = {}
    for clip in clips:
        if not isinstance(clip, dict):
            continue
        scene_line_id = str(clip.get("scene_line_id") or "").strip()
        if not scene_line_id:
            continue
        payload = StoryboardSceneAssignmentV1(
            scene_line_id=scene_line_id,
            clip_id=str(clip.get("clip_id") or ""),
            script_line_id=str(clip.get("script_line_id") or ""),
            mode=normalize_phase4_clip_mode(clip.get("mode")),
            assignment_status="pending",
            assignment_score=0,
            assignment_note="",
            low_confidence=False,
            start_frame_url="",
            start_frame_filename="",
            source_image_asset_id="",
            source_image_filename="",
            edited=False,
            updated_at=now_iso(),
        ).model_dump()
        by_scene_line_id[scene_line_id] = payload
    status = StoryboardAssignStatusV1(
        video_run_id=video_run_id,
        status="idle",
        started_at="",
        updated_at=now_iso(),
        by_scene_line_id=by_scene_line_id,
    ).model_dump()
    status["totals"] = _phase4_v1_storyboard_totals(by_scene_line_id)
    return status


def _phase4_v1_storyboard_find_reusable_run(
    *,
    brand_slug: str,
    branch_id: str,
    phase3_run_id: str,
) -> dict[str, Any] | None:
    rows = list_video_runs_for_branch(brand_slug, branch_id)
    target_phase3 = str(phase3_run_id or "").strip()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("phase3_run_id") or "").strip() != target_phase3:
            continue
        if str(row.get("status") or "").strip() != "active":
            continue
        return row
    return None


def _phase4_v1_storyboard_find_any_active_run() -> dict[str, Any] | None:
    rows = list_active_video_runs(limit=200)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip() != "active":
            continue
        video_run_id = str(row.get("video_run_id") or "").strip()
        if not video_run_id:
            continue
        return row
    return None


def _phase4_v1_storyboard_assigned_rows(status_payload: dict[str, Any]) -> list[dict[str, Any]]:
    by_scene = (
        status_payload.get("by_scene_line_id")
        if isinstance(status_payload.get("by_scene_line_id"), dict)
        else {}
    )
    out: list[dict[str, Any]] = []
    for row in by_scene.values():
        if not isinstance(row, dict):
            continue
        assignment_status = str(row.get("assignment_status") or "").strip().lower()
        start_frame_filename = str(row.get("start_frame_filename") or "").strip()
        if assignment_status != "assigned" or not start_frame_filename:
            continue
        out.append(row)
    return out


def _phase4_v1_storyboard_resolve_start_frame_path(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    start_frame_filename: str,
) -> Path | None:
    file_name = str(start_frame_filename or "").strip()
    if not file_name:
        return None
    asset = find_video_asset_by_filename(video_run_id, file_name)
    if isinstance(asset, dict):
        storage_path = Path(str(asset.get("storage_path") or "")).expanduser()
        if storage_path.exists() and storage_path.is_file():
            return storage_path
    fallback = _phase4_v1_assets_root(brand_slug, branch_id, video_run_id) / "start_frames" / file_name
    if fallback.exists() and fallback.is_file():
        return fallback
    return None


def _phase4_v1_storyboard_ai_library_metadata(
    *,
    mode_hint: str,
    source_pool: str,
    source_image_asset_id: str,
    source_image_filename: str,
    originating_video_run_id: str,
    originating_scene_line_id: str,
    originating_clip_id: str,
    assignment_score: int,
    assignment_status: str,
    prompt_model_provider: str,
    prompt_model_id: str,
    prompt_model_label: str,
    image_edit_model_id: str,
    image_edit_model_label: str,
    edit_provider: str,
    edit_prompt: str,
) -> dict[str, Any]:
    return _phase4_v1_normalize_broll_metadata(
        {
            "library_item_type": "ai_generated",
            "ai_generated": True,
            "mode_hint": _phase4_v1_normalize_broll_mode_hint(mode_hint),
            "tags": [],
            "originating_video_run_id": str(originating_video_run_id or "").strip(),
            "originating_scene_line_id": str(originating_scene_line_id or "").strip(),
            "originating_clip_id": str(originating_clip_id or "").strip(),
            "source_image_asset_id": str(source_image_asset_id or "").strip(),
            "source_image_filename": str(source_image_filename or "").strip(),
            "source_pool": str(source_pool or "").strip(),
            "prompt_model_provider": str(prompt_model_provider or "").strip(),
            "prompt_model_id": str(prompt_model_id or "").strip(),
            "prompt_model_label": str(prompt_model_label or "").strip(),
            "image_edit_model_id": str(image_edit_model_id or "").strip(),
            "image_edit_model_label": str(image_edit_model_label or "").strip(),
            "edit_provider": str(edit_provider or "").strip(),
            "edit_prompt": str(edit_prompt or "").strip(),
            "assignment_score": _phase4_v1_storyboard_score(assignment_score),
            "assignment_status": str(assignment_status or "").strip(),
            "usage_count": 0,
        }
    )


def _phase4_v1_storyboard_backfill_latest_assigned_outputs(
    *,
    brand_slug: str,
    branch_id: str,
) -> dict[str, Any]:
    runs = list_video_runs_for_branch(brand_slug, branch_id, limit=200)
    latest_run_id = ""
    latest_status: dict[str, Any] = {}
    latest_run_row: dict[str, Any] = {}
    for row in runs:
        if not isinstance(row, dict):
            continue
        run_id = str(row.get("video_run_id") or "").strip()
        if not run_id:
            continue
        status_payload = _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=run_id,
        )
        if _phase4_v1_storyboard_assigned_rows(status_payload):
            latest_run_id = run_id
            latest_status = status_payload
            latest_run_row = row
            break

    state_path = _phase4_v1_broll_backfill_state_path(brand_slug, branch_id)
    prev_state = _phase4_v1_read_json(state_path, {})
    previous_run_id = str(prev_state.get("latest_backfilled_run_id") or "").strip() if isinstance(prev_state, dict) else ""

    if not latest_run_id:
        return {
            "latest_backfilled_run_id": previous_run_id,
            "completed_at": str(prev_state.get("completed_at") or "") if isinstance(prev_state, dict) else "",
            "imported_count": 0,
            "dedup_count": 0,
        }

    if previous_run_id == latest_run_id:
        return {
            "latest_backfilled_run_id": latest_run_id,
            "completed_at": str(prev_state.get("completed_at") or "") if isinstance(prev_state, dict) else "",
            "imported_count": 0,
            "dedup_count": 0,
        }

    rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    row_index = _phase4_v1_broll_build_row_index(rows)
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    checksum_index = _phase4_v1_broll_build_checksum_index(library_dir=library_dir, rows=rows)

    assigned_rows = _phase4_v1_storyboard_assigned_rows(latest_status)
    imported_count = 0
    dedup_count = 0
    changed = False

    metrics = latest_run_row.get("metrics") if isinstance(latest_run_row.get("metrics"), dict) else {}
    prompt_model_provider = str(metrics.get("storyboard_prompt_model_provider") or "").strip()
    prompt_model_id = str(metrics.get("storyboard_prompt_model_id") or "").strip()
    prompt_model_label = str(metrics.get("storyboard_prompt_model_label") or "").strip()
    image_edit_model_id = str(metrics.get("storyboard_image_edit_model_id") or "").strip()
    image_edit_model_label = str(metrics.get("storyboard_image_edit_model_label") or "").strip()

    for row in assigned_rows:
        start_frame_filename = str(row.get("start_frame_filename") or "").strip()
        source_path = _phase4_v1_storyboard_resolve_start_frame_path(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=latest_run_id,
            start_frame_filename=start_frame_filename,
        )
        if not source_path:
            continue
        metadata = _phase4_v1_storyboard_ai_library_metadata(
            mode_hint=str(row.get("mode") or ""),
            source_pool=str(row.get("source_pool") or ""),
            source_image_asset_id=str(row.get("source_image_asset_id") or ""),
            source_image_filename=str(row.get("source_image_filename") or ""),
            originating_video_run_id=latest_run_id,
            originating_scene_line_id=str(row.get("scene_line_id") or ""),
            originating_clip_id=str(row.get("clip_id") or ""),
            assignment_score=int(row.get("assignment_score") or 0),
            assignment_status="assigned",
            prompt_model_provider=prompt_model_provider,
            prompt_model_id=prompt_model_id,
            prompt_model_label=prompt_model_label,
            image_edit_model_id=image_edit_model_id,
            image_edit_model_label=image_edit_model_label,
            edit_provider=str(row.get("edit_provider") or ""),
            edit_prompt=str(row.get("edit_prompt") or ""),
        )
        upsert_result = _phase4_v1_broll_upsert_from_source(
            brand_slug=brand_slug,
            branch_id=branch_id,
            source_path=source_path,
            preferred_file_name=start_frame_filename or source_path.name,
            metadata_updates=metadata,
            rows=rows,
            row_index=row_index,
            checksum_index=checksum_index,
            increment_usage_count=True,
        )
        rows = upsert_result.get("rows") if isinstance(upsert_result.get("rows"), list) else rows
        changed = bool(upsert_result.get("changed")) or changed
        if bool(upsert_result.get("dedup_hit")):
            dedup_count += 1
        else:
            imported_count += 1

    if changed:
        _phase4_v1_save_broll_library(brand_slug, branch_id, rows)

    state_payload = {
        "latest_backfilled_run_id": latest_run_id,
        "completed_at": now_iso(),
        "imported_count": imported_count,
        "dedup_count": dedup_count,
    }
    _phase4_v1_write_json(state_path, state_payload)
    logger.info(
        "storyboard_ai_library_backfill_imported_count=%s brand=%s branch=%s run=%s dedup=%s",
        imported_count,
        brand_slug,
        branch_id,
        latest_run_id,
        dedup_count,
    )
    return state_payload


def _phase4_v1_update_run_manifest_mirror(brand_slug: str, branch_id: str, video_run_id: str) -> dict[str, Any]:
    run_row = get_video_run(video_run_id)
    if not run_row:
        return {}
    clips = list_video_clips(video_run_id)
    payload = {
        "video_run_id": run_row.get("video_run_id"),
        "phase3_run_id": run_row.get("phase3_run_id"),
        "brand_slug": run_row.get("brand_slug"),
        "branch_id": run_row.get("branch_id"),
        "status": run_row.get("status"),
        "workflow_state": run_row.get("workflow_state"),
        "voice_preset_id": run_row.get("voice_preset_id"),
        "reviewer_role": run_row.get("reviewer_role"),
        "drive_folder_url": run_row.get("drive_folder_url"),
        "parallelism": run_row.get("parallelism"),
        "error": run_row.get("error", ""),
        "created_at": run_row.get("created_at"),
        "updated_at": run_row.get("updated_at"),
        "completed_at": run_row.get("completed_at", ""),
        "clip_count": len(clips),
        "approved_clip_count": len([c for c in clips if str(c.get("status")) == "approved"]),
        "failed_clip_count": len([c for c in clips if str(c.get("status")) == "failed"]),
        "pending_review_clip_count": len([c for c in clips if str(c.get("status")) == "pending_review"]),
        "metrics": run_row.get("metrics", {}),
    }
    _phase4_v1_save_manifest(brand_slug, branch_id, video_run_id, payload)
    return payload


def _phase4_v1_collect_run_detail(brand_slug: str, branch_id: str, video_run_id: str) -> dict[str, Any] | None:
    run_row = get_video_run(video_run_id)
    if not run_row:
        return None
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return None

    clips = list_video_clips(video_run_id)
    all_assets = list_video_assets(video_run_id)
    assets_by_clip: dict[str, list[dict[str, Any]]] = {}
    start_frame_assets_by_filename: dict[str, dict[str, Any]] = {}
    for asset in all_assets:
        file_name = str(asset.get("file_name") or "").strip()
        asset_type = str(asset.get("asset_type") or "").strip()
        if file_name and asset_type == "start_frame":
            # list_video_assets is ASC by created_at, so latest assignment wins.
            start_frame_assets_by_filename[file_name] = asset
        clip_id = str(asset.get("clip_id") or "").strip()
        if not clip_id:
            continue
        assets_by_clip.setdefault(clip_id, []).append(asset)

    drive_folder_path: Path | None = None
    raw_drive_folder = str(run_row.get("drive_folder_url") or "").strip()
    if raw_drive_folder:
        try:
            clean = raw_drive_folder[7:] if raw_drive_folder.startswith("file://") else raw_drive_folder
            candidate = Path(clean).expanduser().resolve()
            if candidate.exists() and candidate.is_dir():
                drive_folder_path = candidate
        except Exception:
            drive_folder_path = None

    def _resolve_start_frame_url(file_name: str) -> str:
        clean_name = str(file_name or "").strip()
        if not clean_name:
            return ""
        by_asset = start_frame_assets_by_filename.get(clean_name)
        if by_asset:
            url = _phase4_v1_storage_path_to_outputs_url(str(by_asset.get("storage_path") or ""))
            if url:
                return url
        if drive_folder_path:
            candidate = (drive_folder_path / clean_name).resolve()
            if candidate.exists() and candidate.is_file():
                url = _phase4_v1_storage_path_to_outputs_url(str(candidate))
                if url:
                    return url
        return ""

    storyboard_assignment = _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    phase3_run_id = str(run_row.get("phase3_run_id") or "").strip()
    storyboard_scene_lookup = _phase4_v1_storyboard_scene_lookup(
        brand_slug=brand_slug,
        branch_id=branch_id,
        phase3_run_id=phase3_run_id,
    ) if phase3_run_id else {}
    assignment_by_scene = (
        storyboard_assignment.get("by_scene_line_id")
        if isinstance(storyboard_assignment.get("by_scene_line_id"), dict)
        else {}
    )
    run_metrics = run_row.get("metrics") if isinstance(run_row.get("metrics"), dict) else {}
    run_prompt_model_provider = str(run_metrics.get("storyboard_prompt_model_provider") or "").strip()
    run_prompt_model_id = str(run_metrics.get("storyboard_prompt_model_id") or "").strip()
    run_prompt_model_label = str(run_metrics.get("storyboard_prompt_model_label") or "").strip()
    run_image_edit_model_id = str(run_metrics.get("storyboard_image_edit_model_id") or "").strip()
    run_image_edit_model_label = str(run_metrics.get("storyboard_image_edit_model_label") or "").strip()

    clips_with_revision: list[dict[str, Any]] = []
    for clip in clips:
        current_revision = _phase4_v1_get_current_revision_row(clip) or {}
        clip_id = str(clip.get("clip_id") or "").strip()
        mode = str(clip.get("mode") or "").strip()
        revision_id = str(current_revision.get("revision_id") or "").strip()
        input_snapshot = current_revision.get("input_snapshot") if isinstance(current_revision.get("input_snapshot"), dict) else {}
        model_ids = input_snapshot.get("model_ids") if isinstance(input_snapshot.get("model_ids"), dict) else {}
        narration_line = str(input_snapshot.get("narration_text") or clip.get("narration_text") or "").strip()
        snapshot_transform_prompt = str(input_snapshot.get("transform_prompt") or "").strip()
        if mode == "a_roll":
            generation_model = str(model_ids.get("fal_talking_head") or "")
            start_frame_filename = str(input_snapshot.get("avatar_filename") or input_snapshot.get("start_frame_filename") or "").strip()
        else:
            generation_model = str(model_ids.get("fal_broll") or "")
            start_frame_filename = str(input_snapshot.get("start_frame_filename") or "").strip()
        generation_prompt = narration_line
        start_frame_url = _resolve_start_frame_url(start_frame_filename)
        scene_line_id = str(clip.get("scene_line_id") or "").strip()
        assignment = assignment_by_scene.get(scene_line_id) if isinstance(assignment_by_scene, dict) else {}
        assignment_status = str(assignment.get("assignment_status") or "").strip() if isinstance(assignment, dict) else ""
        assignment_score = _phase4_v1_storyboard_score(assignment.get("assignment_score") or 0) if isinstance(assignment, dict) else 0
        assignment_note = str(assignment.get("assignment_note") or "").strip() if isinstance(assignment, dict) else ""
        assignment_edit_prompt = str(assignment.get("edit_prompt") or "").strip() if isinstance(assignment, dict) else ""
        assignment_edit_model_id = str(assignment.get("edit_model_id") or "").strip() if isinstance(assignment, dict) else ""
        assignment_edit_provider = str(assignment.get("edit_provider") or "").strip() if isinstance(assignment, dict) else ""
        assignment_source_image_filename = str(assignment.get("source_image_filename") or "").strip() if isinstance(assignment, dict) else ""
        transform_prompt = assignment_edit_prompt or snapshot_transform_prompt
        scene_lookup_row = (
            storyboard_scene_lookup.get(scene_line_id)
            if isinstance(storyboard_scene_lookup, dict)
            else {}
        )
        scene_description = str(
            (scene_lookup_row.get("scene_description") if isinstance(scene_lookup_row, dict) else "")
            or ""
        ).strip()
        if not start_frame_url and isinstance(assignment, dict):
            start_frame_url = str(assignment.get("start_frame_url") or "").strip()

        clip_assets = assets_by_clip.get(clip_id, [])
        preferred_types = ["talking_head", "broll"] if mode == "a_roll" else ["broll", "talking_head"]
        preview_asset: dict[str, Any] | None = None
        for asset_type in preferred_types:
            for asset in clip_assets:
                if str(asset.get("asset_type") or "") != asset_type:
                    continue
                if revision_id and str(asset.get("revision_id") or "") != revision_id:
                    continue
                preview_asset = asset
                break
            if preview_asset:
                break
        if not preview_asset:
            # Fallback: latest clip-level video asset regardless of revision.
            for asset in clip_assets:
                if str(asset.get("asset_type") or "") in {"talking_head", "broll"}:
                    preview_asset = asset
                    break

        preview_url = _phase4_v1_storage_path_to_outputs_url(str(preview_asset.get("storage_path") if preview_asset else ""))
        clips_with_revision.append(
            {
                **clip,
                "current_revision": current_revision,
                "narration_line": narration_line,
                "generation_prompt": generation_prompt,
                "generation_model": generation_model,
                "transform_prompt": transform_prompt,
                "start_frame_filename": start_frame_filename,
                "start_frame_url": start_frame_url,
                "scene_description": scene_description,
                "assignment_status": assignment_status or "pending",
                "assignment_score": assignment_score,
                "assignment_note": assignment_note,
                "edit_prompt": assignment_edit_prompt or transform_prompt,
                "edit_model_id": assignment_edit_model_id,
                "edit_provider": assignment_edit_provider,
                "source_image_filename": assignment_source_image_filename,
                "prompt_model_provider": run_prompt_model_provider,
                "prompt_model_id": run_prompt_model_id,
                "prompt_model_label": run_prompt_model_label,
                "image_edit_model_id": assignment_edit_model_id or run_image_edit_model_id,
                "image_edit_model_label": run_image_edit_model_label,
                "preview_url": preview_url,
                "preview_asset_type": str(preview_asset.get("asset_type") or "") if preview_asset else "",
                "preview_asset_id": str(preview_asset.get("asset_id") or "") if preview_asset else "",
            }
        )

    validation_report = get_latest_video_validation_report(video_run_id)
    validation_items: list[dict[str, Any]] = []
    if validation_report and validation_report.get("report_id"):
        validation_items = list_video_validation_items(str(validation_report.get("report_id")))
    review_queue = _phase4_v1_read_json(_phase4_v1_review_queue_path(brand_slug, branch_id, video_run_id), [])
    start_frame_brief = _phase4_v1_read_json(_phase4_v1_start_frame_brief_path(brand_slug, branch_id, video_run_id), {})
    start_frame_approval = _phase4_v1_read_json(
        _phase4_v1_start_frame_brief_approval_path(brand_slug, branch_id, video_run_id),
        {},
    )
    manifest = _phase4_v1_load_manifest(brand_slug, branch_id, video_run_id)
    if not manifest:
        manifest = _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)

    task_key = _phase4_v1_run_key(brand_slug, branch_id, video_run_id)
    task = phase4_v1_generation_tasks.get(task_key)
    generation_in_progress = bool(task and not task.done())
    library_rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    source_selection = _phase4_v1_storyboard_resolve_source_selection(
        run_row=run_row,
        rows=library_rows,
    )
    source_selection_payload = StoryboardSourceSelectionResponseV1(
        video_run_id=video_run_id,
        selected_a_roll_files=source_selection.get("selected_a_roll_files") or [],
        selected_b_roll_files=source_selection.get("selected_b_roll_files") or [],
        selectable_a_roll_count=int(source_selection.get("selectable_a_roll_count") or 0),
        selectable_b_roll_count=int(source_selection.get("selectable_b_roll_count") or 0),
        updated_at=str(source_selection.get("updated_at") or ""),
    ).model_dump()
    saved_versions = _phase4_v1_storyboard_load_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    return {
        "run": run_row,
        "manifest": manifest,
        "clips": clips_with_revision,
        "validation_report": validation_report,
        "validation_items": validation_items,
        "start_frame_brief": start_frame_brief,
        "start_frame_brief_approval": start_frame_approval,
        "storyboard_assignment": storyboard_assignment,
        "review_queue": review_queue if isinstance(review_queue, list) else [],
        "generation_in_progress": generation_in_progress,
        "storyboard_source_selection": source_selection_payload,
        "storyboard_saved_versions": saved_versions,
    }


def _phase4_v1_all_clips_approved(video_run_id: str) -> bool:
    clips = list_video_clips(video_run_id)
    if not clips:
        return False
    return all(str(c.get("status") or "") == "approved" for c in clips)


def _phase4_v1_all_latest_revisions_complete(video_run_id: str) -> bool:
    clips = list_video_clips(video_run_id)
    if not clips:
        return False
    for clip in clips:
        revision = _phase4_v1_get_current_revision_row(clip)
        if not revision:
            return False
        provenance = revision.get("provenance") if isinstance(revision.get("provenance"), dict) else {}
        completeness = int(provenance.get("completeness_pct") or 0)
        if completeness < 100:
            return False
    return True


def _phase4_v1_validation_report_model(video_run_id: str) -> DriveValidationReportV1 | None:
    saved = get_latest_video_validation_report(video_run_id)
    if not saved:
        return None
    report_id = str(saved.get("report_id") or "")
    items = list_video_validation_items(report_id) if report_id else []
    payload = dict(saved.get("summary") or {})
    if not isinstance(payload, dict):
        payload = {}
    payload["report_id"] = report_id
    payload["video_run_id"] = video_run_id
    payload["items"] = items
    payload["status"] = saved.get("status") or payload.get("status") or "failed"
    payload["folder_url"] = saved.get("folder_url") or payload.get("folder_url") or ""
    payload["validated_at"] = payload.get("validated_at") or saved.get("created_at") or ""
    try:
        return DriveValidationReportV1.model_validate(payload)
    except Exception:
        return None


def _phase4_v1_copy_start_frame_asset(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    matched_asset: dict[str, Any],
    drive_client: Any | None = None,
) -> dict[str, Any]:
    file_name = str(matched_asset.get("name") or "").strip()
    if not file_name:
        raise ValueError("Invalid validation asset: missing name")
    existing = find_video_asset_by_filename(video_run_id, file_name)
    if existing:
        return existing

    source_id = str(matched_asset.get("source_id") or "").strip()
    source_url = str(matched_asset.get("source_url") or "").strip()
    source_path = Path(source_id) if source_id else None
    target_path = _phase4_v1_assets_root(brand_slug, branch_id, video_run_id) / "start_frames" / file_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path and source_path.exists() and source_path.is_file():
        shutil.copy2(source_path, target_path)
        storage_path = str(target_path)
    elif drive_client is not None and hasattr(drive_client, "download_asset"):
        try:
            downloaded = drive_client.download_asset(source_id, target_path)
            storage_path = str(downloaded)
        except Exception:
            storage_path = source_url
    else:
        # Listing-only providers may not expose direct file paths in test mode.
        storage_path = source_url

    asset_row = create_video_asset(
        asset_id=f"asset_{uuid.uuid4().hex}",
        video_run_id=video_run_id,
        asset_type="start_frame",
        storage_path=storage_path,
        source_url=source_url,
        file_name=file_name,
        mime_type=str(matched_asset.get("mime_type") or ""),
        byte_size=int(matched_asset.get("size_bytes") or 0),
        checksum_sha256=str(matched_asset.get("checksum_sha256") or ""),
        metadata={
            "source_id": source_id,
            "ingested_from_validation": True,
        },
    )
    return asset_row


def _phase4_v1_storyboard_local_folder_path(folder_url: str) -> Path:
    raw = str(folder_url or "").strip()
    clean = raw[7:] if raw.startswith("file://") else raw
    path = Path(clean).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Storyboard folder not found: {path}")
    return path


def _phase4_v1_storyboard_clear_clip_start_frames(video_run_id: str) -> int:
    cleared = 0
    clips = list_video_clips(video_run_id)
    for clip in clips:
        revision = _phase4_v1_get_current_revision_row(clip)
        if not isinstance(revision, dict):
            continue
        revision_id = str(revision.get("revision_id") or "").strip()
        if not revision_id:
            continue
        snapshot = revision.get("input_snapshot") if isinstance(revision.get("input_snapshot"), dict) else {}
        updated_snapshot = dict(snapshot)
        changed = False
        for key in (
            "start_frame_filename",
            "start_frame_checksum",
            "avatar_filename",
            "avatar_checksum",
        ):
            if str(updated_snapshot.get(key) or "").strip():
                updated_snapshot[key] = ""
                changed = True
        if not changed:
            continue
        update_video_clip_revision(revision_id, input_snapshot=updated_snapshot)
        cleared += 1
    return cleared


def _phase4_v1_storyboard_write_runtime_status(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    task_key: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = _phase4_v1_storyboard_save_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        payload=payload,
    )
    phase4_v1_storyboard_assign_state[task_key] = normalized
    return normalized


def _phase4_v1_storyboard_update_scene_status(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    task_key: str,
    scene_line_id: str,
    updates: dict[str, Any],
) -> dict[str, Any]:
    base = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    by_scene = base.get("by_scene_line_id") if isinstance(base.get("by_scene_line_id"), dict) else {}
    scene = dict(by_scene.get(scene_line_id) or {"scene_line_id": scene_line_id})
    scene.update({k: v for k, v in updates.items() if v is not None})
    scene["scene_line_id"] = scene_line_id
    scene["updated_at"] = now_iso()
    try:
        by_scene[scene_line_id] = StoryboardSceneAssignmentV1.model_validate(scene).model_dump()
    except Exception:
        by_scene[scene_line_id] = scene
    base["by_scene_line_id"] = by_scene
    base["updated_at"] = now_iso()
    return _phase4_v1_storyboard_write_runtime_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        task_key=task_key,
        payload=base,
    )


def _phase4_v1_storyboard_generate_validation_report(
    *,
    video_run_id: str,
    folder_url: str,
    brief: StartFrameBriefV1,
    matched_assets_by_name: dict[str, dict[str, Any]],
) -> DriveValidationReportV1:
    required_ok = 0
    optional_ok = 0
    required_total = len(brief.required_items)
    items: list[dict[str, Any]] = []
    errors: list[str] = []

    def _item_row(item: Any, required: bool) -> dict[str, Any]:
        nonlocal required_ok, optional_ok
        name = str(getattr(item, "filename", "") or "").strip()
        role = str(getattr(item, "file_role", "") or "").strip()
        matched = matched_assets_by_name.get(name)
        if matched:
            if required:
                required_ok += 1
            else:
                optional_ok += 1
            return {
                "filename": name,
                "file_role": role,
                "required": required,
                "status": "ok",
                "issue_code": "",
                "message": "",
                "remediation": "",
                "matched_asset": {
                    "name": name,
                    "mime_type": str(matched.get("mime_type") or ""),
                    "size_bytes": int(matched.get("byte_size") or 0),
                    "checksum_sha256": str(matched.get("checksum_sha256") or ""),
                    "readable": True,
                    "source_id": str(matched.get("asset_id") or ""),
                    "source_url": str(matched.get("source_url") or ""),
                },
            }
        issue = "missing_required" if required else "missing_optional"
        if required:
            errors.append(f"Missing required storyboard asset: {name}")
        return {
            "filename": name,
            "file_role": role,
            "required": required,
            "status": "missing" if required else "invalid",
            "issue_code": issue,
            "message": f"{name} was not assigned by storyboard auto-match.",
            "remediation": "Assign a start frame manually and rerun storyboard assignment." if required else "",
            "matched_asset": None,
        }

    for item in brief.required_items:
        items.append(_item_row(item, True))
    for item in brief.optional_items:
        items.append(_item_row(item, False))

    status = "passed" if required_ok == required_total and required_total > 0 else "failed"
    return DriveValidationReportV1(
        report_id=f"val_storyboard_{uuid.uuid4().hex}",
        video_run_id=video_run_id,
        folder_url=str(folder_url or ""),
        validated_at=now_iso(),
        status=status,
        required_total=required_total,
        required_ok=required_ok,
        optional_ok=optional_ok,
        errors=errors,
        items=items,
    )


def _phase4_v1_storyboard_candidate_files(folder_path: Path) -> list[Path]:
    candidates: list[Path] = []
    for entry in sorted(folder_path.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_file():
            continue
        if _phase4_v1_storyboard_supported_image(entry.name):
            candidates.append(entry)
    return candidates


def _phase4_v1_storyboard_tokenize(value: Any) -> set[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    normalized = re.sub(r"[^a-z0-9]+", " ", raw)
    out: set[str] = set()
    for token in normalized.split():
        if len(token) < 3:
            continue
        if token.isdigit():
            continue
        if token in _PHASE4_STORYBOARD_TOKEN_STOPWORDS:
            continue
        out.add(token)
    return out


def _phase4_v1_storyboard_candidate_fingerprint(candidate: dict[str, Any]) -> str:
    if not isinstance(candidate, dict):
        return ""
    precomputed = str(candidate.get("_source_fingerprint") or "").strip()
    if precomputed:
        return precomputed
    candidate_asset = candidate.get("asset") if isinstance(candidate.get("asset"), dict) else {}
    source_checksum = str(candidate_asset.get("checksum_sha256") or "").strip().lower()
    if source_checksum:
        return source_checksum
    source_asset_id = str(candidate_asset.get("asset_id") or "").strip()
    if source_asset_id:
        return source_asset_id
    path_text = str(candidate.get("path") or "").strip()
    if not path_text:
        return ""
    try:
        return str(Path(path_text).resolve())
    except Exception:
        return path_text


def _phase4_v1_storyboard_candidate_keywords(candidate: dict[str, Any]) -> set[str]:
    if not isinstance(candidate, dict):
        return set()
    tokens: set[str] = set()
    path_value = candidate.get("path")
    if path_value:
        try:
            stem = Path(str(path_value)).stem
        except Exception:
            stem = str(path_value or "")
        tokens.update(_phase4_v1_storyboard_tokenize(stem))
    tags = candidate.get("library_tags")
    if isinstance(tags, list):
        for tag in tags:
            tokens.update(_phase4_v1_storyboard_tokenize(tag))
    analysis = candidate.get("analysis") if isinstance(candidate.get("analysis"), dict) else {}
    for key in ("caption", "setting", "camera_angle", "shot_type", "lighting", "mood"):
        tokens.update(_phase4_v1_storyboard_tokenize(analysis.get(key)))
    for key in ("subjects", "actions", "style_tags"):
        values = analysis.get(key)
        if isinstance(values, list):
            for value in values:
                tokens.update(_phase4_v1_storyboard_tokenize(value))
    return tokens


def _phase4_v1_storyboard_scene_keywords(scene_intent: dict[str, Any]) -> set[str]:
    if not isinstance(scene_intent, dict):
        return set()
    tokens: set[str] = set()
    tokens.update(_phase4_v1_storyboard_tokenize(scene_intent.get("narration_line")))
    tokens.update(_phase4_v1_storyboard_tokenize(scene_intent.get("scene_description")))
    tokens.update(_phase4_v1_storyboard_tokenize(scene_intent.get("script_line_id")))
    return tokens


def _phase4_v1_storyboard_candidate_mode_hint(candidate: dict[str, Any]) -> str:
    if not isinstance(candidate, dict):
        return "unknown"
    return _phase4_v1_normalize_broll_mode_hint(candidate.get("library_mode_hint"))


def _phase4_v1_storyboard_retrieval_score(
    *,
    candidate: dict[str, Any],
    mode: str,
    scene_keywords: set[str],
    recent_fingerprints: set[str],
) -> tuple[int, int, str]:
    usage_count = max(0, int(candidate.get("library_usage_count") or 0))
    mode_hint = _phase4_v1_storyboard_candidate_mode_hint(candidate)
    source_fingerprint = _phase4_v1_storyboard_candidate_fingerprint(candidate)
    keyword_hits = len(scene_keywords.intersection(_phase4_v1_storyboard_candidate_keywords(candidate)))

    score = 0
    if mode == "a_roll":
        if mode_hint == "a_roll":
            score += 16
        elif mode_hint == "unknown":
            score += 2
        else:
            score -= 10
    elif mode == "animation_broll":
        if mode_hint == "animation_broll":
            score += 16
        elif mode_hint == "b_roll":
            score += 10
        elif mode_hint == "unknown":
            score += 2
        else:
            score -= 10
    else:
        if mode_hint == "b_roll":
            score += 14
        elif mode_hint == "animation_broll":
            score += 9
        elif mode_hint == "unknown":
            score += 2
        elif mode_hint == "a_roll":
            score -= 10

    score += min(24, keyword_hits * 4)
    score -= min(12, usage_count)
    if source_fingerprint and source_fingerprint in recent_fingerprints:
        score -= 120

    return score, usage_count, source_fingerprint


async def _phase4_v1_execute_storyboard_assignment(
    *,
    brand_slug: str,
    branch_id: str,
    video_run_id: str,
    folder_url: str,
    edit_threshold: int,
    low_flag_threshold: int,
    image_edit_model_id: str = "",
    image_edit_model_label: str = "",
    prompt_model_provider: str = "",
    prompt_model_id: str = "",
    prompt_model_label: str = "",
    selected_a_roll_files: list[str] | None = None,
    selected_b_roll_files: list[str] | None = None,
    job_id: str = "",
):
    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    try:
        run_row = get_video_run(video_run_id)
        if not run_row:
            raise RuntimeError("Video run not found.")
        phase3_run_id = str(run_row.get("phase3_run_id") or "").strip()
        if not phase3_run_id:
            raise RuntimeError("Video run is missing phase3_run_id.")
        clips = list_video_clips(video_run_id)
        clips.sort(key=lambda c: int(c.get("line_index") or 0))
        if not clips:
            raise RuntimeError("No clips found for storyboard assignment.")

        current = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_build_initial_status(
            video_run_id=video_run_id,
            clips=clips,
        )
        current.update(
            {
                "video_run_id": video_run_id,
                "job_id": job_id,
                "status": "running",
                "started_at": str(current.get("started_at") or now_iso()),
                "updated_at": now_iso(),
                "error": "",
            }
        )
        _phase4_v1_storyboard_write_runtime_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            task_key=task_key,
            payload=current,
        )

        effective_folder_url = str(folder_url or _phase4_v1_broll_library_dir(brand_slug, branch_id))
        update_video_run(
            video_run_id,
            status="active",
            workflow_state="validating_assets",
            drive_folder_url=effective_folder_url,
            error="",
        )
        _phase4_v1_storyboard_update_metrics(
            video_run_id=video_run_id,
            updates={
                "storyboard_assignment_job_id": job_id,
                "assignment_completed_count": 0,
                "assignment_failed_count": 0,
                "storyboard_image_edit_model_id": str(image_edit_model_id or ""),
                "storyboard_image_edit_model_label": str(image_edit_model_label or ""),
                "storyboard_prompt_model_provider": str(prompt_model_provider or ""),
                "storyboard_prompt_model_id": str(prompt_model_id or ""),
                "storyboard_prompt_model_label": str(prompt_model_label or ""),
            },
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)

        vision_provider = build_vision_scene_provider(
            str(prompt_model_provider or _PHASE4_V1_STORYBOARD_FIXED_PROMPT_PROVIDER)
        )
        _, _, gemini_provider = build_generation_providers()
        run_dir = _phase4_v1_run_dir(brand_slug, branch_id, video_run_id)
        asset_dirs = ensure_phase4_asset_dirs(run_dir)
        library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
        library_rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
        if not library_rows:
            raise RuntimeError("No images saved in the image bank. Upload A-roll and B-roll images first.")

        library_row_index = _phase4_v1_broll_build_row_index(library_rows)
        library_checksum_index = _phase4_v1_broll_build_checksum_index(
            library_dir=library_dir,
            rows=library_rows,
        )
        library_saved_count = 0
        library_dedup_hit_count = 0
        library_reused_count = 0
        library_lock = asyncio.Lock()

        source_selection = _phase4_v1_storyboard_resolve_source_selection(
            run_row=run_row,
            rows=library_rows,
            requested_a_roll=selected_a_roll_files if selected_a_roll_files is not None else None,
            requested_b_roll=selected_b_roll_files if selected_b_roll_files is not None else None,
        )
        selected_a_roll_files = _phase4_v1_storyboard_normalize_selected_files(
            source_selection.get("selected_a_roll_files")
        )
        selected_b_roll_files = _phase4_v1_storyboard_normalize_selected_files(
            source_selection.get("selected_b_roll_files")
        )
        selected_a_roll_set = {name.lower() for name in selected_a_roll_files}
        selected_b_roll_set = {name.lower() for name in selected_b_roll_files}
        if not selected_b_roll_set:
            raise RuntimeError("No ready B-roll images selected. Select at least one indexed B-roll image.")

        analyzed_images: list[dict[str, Any]] = []
        a_roll_images: list[dict[str, Any]] = []
        b_roll_images: list[dict[str, Any]] = []
        for row in library_rows:
            if not isinstance(row, dict):
                continue
            file_name = str(row.get("file_name") or "").strip()
            if not file_name:
                continue
            lower_name = file_name.lower()
            if lower_name not in selected_a_roll_set and lower_name not in selected_b_roll_set:
                continue
            metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
            if str(metadata.get("indexing_status") or "").strip().lower() != "ready":
                continue
            analysis = metadata.get("analysis")
            if not isinstance(analysis, dict) or not analysis:
                continue
            image_path = library_dir / file_name
            if not image_path.exists() or not image_path.is_file():
                continue
            try:
                image_bytes = image_path.read_bytes()
            except Exception:
                continue
            checksum = str(metadata.get("source_checksum_sha256") or "").strip().lower()
            if not _phase4_v1_is_sha256_hex(checksum):
                checksum = hashlib.sha256(image_bytes).hexdigest()
            asset_row = create_video_asset(
                asset_id=f"asset_{uuid.uuid4().hex}",
                video_run_id=video_run_id,
                asset_type="image_bank_source",
                storage_path=str(image_path),
                source_url=str(image_path),
                file_name=file_name,
                mime_type=(mimetypes.guess_type(image_path.name)[0] or ""),
                byte_size=len(image_bytes),
                checksum_sha256=checksum,
                metadata={
                    "analysis": analysis if isinstance(analysis, dict) else {},
                    "folder_url": effective_folder_url,
                    "provider": str((analysis or {}).get("provider") if isinstance(analysis, dict) else ""),
                    "source_pool": "image_bank",
                },
            )
            candidate = {
                "path": image_path,
                "analysis": analysis if isinstance(analysis, dict) else {},
                "asset": asset_row,
                "source_pool": "image_bank",
                "library_file_name": file_name,
                "library_usage_count": int(metadata.get("usage_count") or 0),
                "library_mode_hint": str(metadata.get("mode_hint") or "unknown"),
                "library_tags": _phase4_v1_normalize_broll_tags(metadata.get("tags")),
            }
            analyzed_images.append(candidate)
            mode_hint = _phase4_v1_normalize_broll_mode_hint(metadata.get("mode_hint"))
            if lower_name in selected_a_roll_set and mode_hint == "a_roll":
                a_roll_images.append(candidate)
            if lower_name in selected_b_roll_set and mode_hint in {"b_roll", "animation_broll", "unknown"}:
                b_roll_images.append(candidate)

        if not analyzed_images:
            raise RuntimeError("No ready indexed images are available in the current source selection.")
        if not b_roll_images:
            raise RuntimeError("No ready indexed B-roll images selected. Select at least one B-roll image.")

        style_profile = _phase4_v1_storyboard_style_profile(
            [
                row.get("analysis")
                for row in analyzed_images
                if isinstance(row.get("analysis"), dict)
            ]
        )
        scene_lookup = _phase4_v1_storyboard_scene_lookup(
            brand_slug=brand_slug,
            branch_id=branch_id,
            phase3_run_id=phase3_run_id,
        )
        _phase4_v1_storyboard_update_metrics(
            video_run_id=video_run_id,
            updates={
                "image_bank_count": len(analyzed_images),
                "storyboard_selected_a_roll_count": len(selected_a_roll_files),
                "storyboard_selected_b_roll_count": len(selected_b_roll_files),
            },
        )

        edit_threshold = max(1, min(10, int(edit_threshold or 5)))
        low_flag_threshold = max(1, min(10, int(low_flag_threshold or 6)))
        completed_count = 0
        failed_count = 0
        recent_source_fingerprints: deque[str] = deque(maxlen=_PHASE4_STORYBOARD_RECENT_FINGERPRINT_WINDOW)
        assignment_plans: list[dict[str, Any]] = []

        for clip in clips:
            clip_id = str(clip.get("clip_id") or "").strip()
            scene_line_id = str(clip.get("scene_line_id") or "").strip()
            if not clip_id or not scene_line_id:
                continue

            mode = normalize_phase4_clip_mode(clip.get("mode"))
            a_roll_fallback_to_broll = False
            if mode == "a_roll":
                if a_roll_images:
                    candidate_pool = a_roll_images
                else:
                    candidate_pool = b_roll_images
                    a_roll_fallback_to_broll = bool(candidate_pool)
            else:
                candidate_pool = b_roll_images

            revision = _phase4_v1_get_current_revision_row(clip)
            revision_id = str(revision.get("revision_id") or "") if isinstance(revision, dict) else ""
            snapshot = (
                revision.get("input_snapshot")
                if isinstance(revision, dict) and isinstance(revision.get("input_snapshot"), dict)
                else {}
            )

            lookup = scene_lookup.get(scene_line_id, {})
            narration_line = str(
                snapshot.get("narration_text")
                or clip.get("narration_text")
                or lookup.get("narration_line")
                or ""
            ).strip()
            scene_description = str(lookup.get("scene_description") or "").strip()
            scene_intent = {
                "mode": mode,
                "script_line_id": str(clip.get("script_line_id") or ""),
                "narration_line": narration_line,
                "scene_description": scene_description,
            }
            scene_keywords = _phase4_v1_storyboard_scene_keywords(scene_intent)
            recent_fingerprint_set = {
                str(value or "").strip()
                for value in recent_source_fingerprints
                if str(value or "").strip()
            }

            ranked_candidates: list[dict[str, Any]] = []
            seen_ranked_fingerprints: set[str] = set()
            for candidate in candidate_pool:
                retrieval_score, usage_count, source_fingerprint = _phase4_v1_storyboard_retrieval_score(
                    candidate=candidate if isinstance(candidate, dict) else {},
                    mode=mode,
                    scene_keywords=scene_keywords,
                    recent_fingerprints=recent_fingerprint_set,
                )
                fingerprint_key = str(source_fingerprint or "").strip()
                if fingerprint_key and fingerprint_key in seen_ranked_fingerprints:
                    continue
                if fingerprint_key:
                    seen_ranked_fingerprints.add(fingerprint_key)
                ranked = dict(candidate) if isinstance(candidate, dict) else {}
                ranked["_retrieval_score"] = int(retrieval_score)
                ranked["_usage_count"] = int(usage_count)
                ranked["_source_fingerprint"] = fingerprint_key
                ranked_candidates.append(ranked)
            ranked_candidates.sort(
                key=lambda row: (
                    -int(row.get("_retrieval_score") or 0),
                    int(row.get("_usage_count") or 0),
                    str(row.get("_source_fingerprint") or ""),
                )
            )

            if not ranked_candidates:
                failed_count += 1
                _phase4_v1_storyboard_update_scene_status(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    video_run_id=video_run_id,
                    task_key=task_key,
                    scene_line_id=scene_line_id,
                    updates={
                        "assignment_status": "failed",
                        "assignment_score": 0,
                        "assignment_note": (
                            "No ready indexed A-roll images selected."
                            if mode == "a_roll" and not a_roll_fallback_to_broll
                            else "No candidate images available."
                        ),
                    },
                )
                _phase4_v1_storyboard_update_metrics(
                    video_run_id=video_run_id,
                    updates={
                        "image_bank_count": len(analyzed_images),
                        "assignment_completed_count": completed_count,
                        "assignment_failed_count": failed_count,
                    },
                )
                continue

            shortlist_target = min(len(ranked_candidates), max(1, int(_PHASE4_STORYBOARD_SHORTLIST_SIZE)))
            max_score_candidates = min(
                len(ranked_candidates),
                max(shortlist_target, int(_PHASE4_STORYBOARD_SHORTLIST_MAX_SCORES)),
            )
            if mode == "a_roll" and a_roll_fallback_to_broll:
                status_note = f"Scoring B-roll fallback shortlist ({shortlist_target}/{len(ranked_candidates)})..."
            else:
                status_note = f"Scoring image bank shortlist ({shortlist_target}/{len(ranked_candidates)})..."
            _phase4_v1_storyboard_update_scene_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
                task_key=task_key,
                scene_line_id=scene_line_id,
                updates={"assignment_status": "analyzing", "assignment_note": status_note},
            )

            scored_candidates: list[dict[str, Any]] = []
            score_cursor = 0
            initial_target = min(shortlist_target, max_score_candidates)
            score_parallel = max(1, int(_PHASE4_STORYBOARD_ASSIGN_MAX_PARALLEL))
            score_semaphore = asyncio.Semaphore(score_parallel)

            async def _score_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
                candidate_source_pool = str(candidate.get("source_pool") or "image_bank").strip().lower()
                image_path = Path(str(candidate.get("path") or ""))
                async with score_semaphore:
                    score_payload = await asyncio.to_thread(
                        vision_provider.score_scene_match,
                        image_path=image_path,
                        scene_intent=scene_intent,
                        style_profile=style_profile,
                        model_id=str(prompt_model_id or _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID),
                        idempotency_key=f"{video_run_id}:{scene_line_id}:{image_path.name}:{candidate_source_pool}:score",
                    )
                score_value = _phase4_v1_storyboard_score(
                    (score_payload or {}).get("score_1_to_10") if isinstance(score_payload, dict) else 0
                )
                reason = str((score_payload or {}).get("reason_short") if isinstance(score_payload, dict) else "").strip()
                candidate_asset = candidate.get("asset") if isinstance(candidate.get("asset"), dict) else {}
                source_asset_id = str(candidate_asset.get("asset_id") or "").strip()
                source_checksum = str(candidate_asset.get("checksum_sha256") or "").strip().lower()
                source_fingerprint = str(candidate.get("_source_fingerprint") or "").strip()
                if not source_fingerprint:
                    source_fingerprint = source_checksum or source_asset_id
                    if not source_fingerprint:
                        try:
                            source_fingerprint = str(image_path.resolve())
                        except Exception:
                            source_fingerprint = str(image_path)
                return {
                    "candidate": candidate,
                    "score": score_value,
                    "reason": reason,
                    "score_payload": score_payload if isinstance(score_payload, dict) else {},
                    "source_asset_id": source_asset_id,
                    "source_checksum": source_checksum,
                    "source_fingerprint": source_fingerprint,
                    "usage_count": max(
                        0,
                        int(candidate.get("_usage_count") or candidate.get("library_usage_count") or 0),
                    ),
                    "source_pool": candidate_source_pool,
                    "library_file_name": str(candidate.get("library_file_name") or "").strip(),
                }

            async def _score_candidate_slice(start_idx: int, end_idx: int) -> None:
                if end_idx <= start_idx:
                    return
                rows = await asyncio.gather(
                    *(_score_candidate(ranked_candidates[idx]) for idx in range(start_idx, end_idx))
                )
                scored_candidates.extend(rows)

            if initial_target > 0:
                await _score_candidate_slice(score_cursor, initial_target)
                score_cursor = initial_target

            while True:
                scored_candidates.sort(
                    key=lambda row: (
                        -int(row.get("score") or 0),
                        int(row.get("usage_count") or 0),
                        str(row.get("source_fingerprint") or ""),
                    )
                )
                non_repeat_candidates = [
                    row
                    for row in scored_candidates
                    if str(row.get("source_fingerprint") or "") not in recent_fingerprint_set
                ]
                best_non_repeat_score = int(non_repeat_candidates[0].get("score") or 0) if non_repeat_candidates else 0
                if score_cursor >= max_score_candidates:
                    break
                if score_cursor >= len(ranked_candidates):
                    break
                if non_repeat_candidates and best_non_repeat_score > low_flag_threshold:
                    break
                next_target = min(
                    max_score_candidates,
                    len(ranked_candidates),
                    score_cursor + max(1, int(_PHASE4_STORYBOARD_SHORTLIST_EXPAND_BATCH)),
                )
                if next_target <= score_cursor:
                    break
                await _score_candidate_slice(score_cursor, next_target)
                score_cursor = next_target

            scored_candidates.sort(
                key=lambda row: (
                    -int(row.get("score") or 0),
                    int(row.get("usage_count") or 0),
                    str(row.get("source_fingerprint") or ""),
                )
            )

            if not scored_candidates:
                failed_count += 1
                _phase4_v1_storyboard_update_scene_status(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    video_run_id=video_run_id,
                    task_key=task_key,
                    scene_line_id=scene_line_id,
                    updates={
                        "assignment_status": "failed",
                        "assignment_score": 0,
                        "assignment_note": (
                            "No ready indexed A-roll images selected."
                            if mode == "a_roll" and not a_roll_fallback_to_broll
                            else "No candidate images available."
                        ),
                    },
                )
                _phase4_v1_storyboard_update_metrics(
                    video_run_id=video_run_id,
                    updates={
                        "image_bank_count": len(analyzed_images),
                        "assignment_completed_count": completed_count,
                        "assignment_failed_count": failed_count,
                    },
                )
                continue

            non_repeat_candidates = [
                row
                for row in scored_candidates
                if str(row.get("source_fingerprint") or "") not in recent_fingerprint_set
            ]
            selected = non_repeat_candidates[0] if non_repeat_candidates else scored_candidates[0]
            selected_source_fingerprint = str(selected.get("source_fingerprint") or "")
            consecutive_reuse_forced = bool(recent_fingerprint_set) and bool(selected_source_fingerprint) and (
                selected_source_fingerprint in recent_fingerprint_set
            )
            if selected_source_fingerprint:
                recent_source_fingerprints.append(selected_source_fingerprint)
            selected_candidate = selected.get("candidate") if isinstance(selected.get("candidate"), dict) else {}
            selected_source_pool = str(selected.get("source_pool") or selected_candidate.get("source_pool") or "").strip()
            selected_library_file_name = str(
                selected.get("library_file_name") or selected_candidate.get("library_file_name") or ""
            ).strip()
            if selected_library_file_name:
                selected_idx = library_row_index.get(selected_library_file_name.lower(), -1)
                if selected_idx >= 0:
                    selected_row = dict(library_rows[selected_idx])
                    selected_meta = _phase4_v1_normalize_broll_metadata(selected_row.get("metadata"))
                    selected_meta["usage_count"] = max(0, int(selected_meta.get("usage_count") or 0)) + 1
                    selected_meta["last_used_at"] = now_iso()
                    selected_row["metadata"] = selected_meta
                    library_rows[selected_idx] = selected_row
                    selected_candidate["library_usage_count"] = int(selected_meta.get("usage_count") or 0)
                    library_reused_count += 1
            assignment_plans.append(
                {
                    "clip": clip,
                    "clip_id": clip_id,
                    "scene_line_id": scene_line_id,
                    "mode": mode,
                    "revision_id": revision_id,
                    "snapshot": snapshot,
                    "scene_intent": scene_intent,
                    "selected": selected,
                    "selected_candidate": selected_candidate,
                    "chosen_path": Path(str(selected_candidate.get("path") or "")),
                    "chosen_asset": selected_candidate.get("asset")
                    if isinstance(selected_candidate.get("asset"), dict)
                    else {},
                    "chosen_score": int(selected.get("score") or 1),
                    "chosen_reason": str(selected.get("reason") or "").strip(),
                    "consecutive_reuse_forced": consecutive_reuse_forced,
                    "chosen_source_pool": selected_source_pool,
                    "chosen_source_mode_hint": str(selected_candidate.get("library_mode_hint") or mode),
                    "a_roll_fallback_to_broll": a_roll_fallback_to_broll,
                }
            )

        if assignment_plans:
            library_rows = _phase4_v1_save_broll_library(brand_slug, branch_id, library_rows)
            library_row_index.clear()
            library_row_index.update(_phase4_v1_broll_build_row_index(library_rows))

        progress_lock = asyncio.Lock()
        assignment_semaphore = asyncio.Semaphore(max(1, int(_PHASE4_STORYBOARD_ASSIGN_MAX_PARALLEL)))

        async def _update_progress(*, completed_delta: int = 0, failed_delta: int = 0):
            nonlocal completed_count, failed_count
            async with progress_lock:
                completed_count += int(completed_delta or 0)
                failed_count += int(failed_delta or 0)
                _phase4_v1_storyboard_update_metrics(
                    video_run_id=video_run_id,
                    updates={
                        "image_bank_count": len(analyzed_images),
                        "assignment_completed_count": completed_count,
                        "assignment_failed_count": failed_count,
                    },
                )

        async def _process_assignment_plan(plan: dict[str, Any]):
            nonlocal library_rows, library_saved_count, library_dedup_hit_count
            clip = plan.get("clip") if isinstance(plan.get("clip"), dict) else {}
            clip_id = str(plan.get("clip_id") or "").strip()
            scene_line_id = str(plan.get("scene_line_id") or "").strip()
            mode = normalize_phase4_clip_mode(plan.get("mode"))
            revision_id = str(plan.get("revision_id") or "").strip()
            snapshot = plan.get("snapshot") if isinstance(plan.get("snapshot"), dict) else {}
            scene_intent = plan.get("scene_intent") if isinstance(plan.get("scene_intent"), dict) else {}
            selected = plan.get("selected") if isinstance(plan.get("selected"), dict) else {}
            chosen_candidate = (
                plan.get("selected_candidate") if isinstance(plan.get("selected_candidate"), dict) else {}
            )
            chosen_path = Path(str(plan.get("chosen_path") or ""))
            chosen_asset = plan.get("chosen_asset") if isinstance(plan.get("chosen_asset"), dict) else {}
            chosen_score = int(plan.get("chosen_score") or 1)
            chosen_reason = str(plan.get("chosen_reason") or "").strip()
            consecutive_reuse_forced = bool(plan.get("consecutive_reuse_forced"))
            chosen_source_pool = str(plan.get("chosen_source_pool") or chosen_candidate.get("source_pool") or "").strip()
            chosen_source_mode_hint = str(plan.get("chosen_source_mode_hint") or mode).strip()
            a_roll_fallback_to_broll = bool(plan.get("a_roll_fallback_to_broll"))
            if consecutive_reuse_forced:
                chosen_reason = (
                    f"{chosen_reason} Reused a recently used image because no alternative candidate was available."
                    if chosen_reason
                    else "Reused a recently used image because no alternative candidate was available."
                )

            async with assignment_semaphore:
                edited = False
                transformed_asset_id = ""
                edit_error = ""
                force_broll_edit = bool(is_phase4_b_roll_mode(mode))
                edit_prompt_text = ""
                edit_provider_name = ""
                edit_model_id = ""
                try:
                    should_attempt_edit = (
                        force_broll_edit
                        or mode == "a_roll"
                        or chosen_score <= edit_threshold
                        or chosen_score <= low_flag_threshold
                        or consecutive_reuse_forced
                    )
                    if should_attempt_edit:
                        try:
                            prompt_payload: dict[str, Any] = {}
                            compose_fn = getattr(vision_provider, "compose_transform_prompt", None)
                            if callable(compose_fn):
                                prompt_payload = await asyncio.to_thread(
                                    compose_fn,
                                    image_path=chosen_path,
                                    scene_intent=scene_intent,
                                    style_profile=style_profile,
                                    image_analysis=chosen_candidate.get("analysis")
                                    if isinstance(chosen_candidate.get("analysis"), dict)
                                    else {},
                                    model_id=str(prompt_model_id or config.PHASE4_V1_VISION_SCENE_MODEL_ID),
                                    idempotency_key=f"{video_run_id}:{scene_line_id}:compose_transform_prompt",
                                )
                            edit_prompt_text = str(
                                (prompt_payload or {}).get("edit_prompt")
                                if isinstance(prompt_payload, dict)
                                else ""
                            ).strip()
                            if not edit_prompt_text:
                                edit_prompt_text = _phase4_v1_storyboard_edit_prompt(
                                    scene_intent=scene_intent,
                                    style_profile=style_profile,
                                )
                            transformed_path = (
                                asset_dirs["transformed_frames"] / f"{clip_id}__storyboard__{int(time.time() * 1000)}.png"
                            )
                            transformed_result = await asyncio.to_thread(
                                gemini_provider.transform_image,
                                input_path=chosen_path,
                                prompt=edit_prompt_text,
                                output_path=transformed_path,
                                model_id=str(image_edit_model_id or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID),
                                idempotency_key=f"{video_run_id}:{scene_line_id}:storyboard_transform",
                            )
                            edit_provider_name = str(transformed_result.get("provider") or "").strip()
                            edit_model_id = str(transformed_result.get("model_id") or "").strip()
                            transformed_checksum = str(transformed_result.get("checksum_sha256") or "").strip()
                            transformed_size = int(transformed_result.get("size_bytes") or 0)
                            transformed_asset = create_video_asset(
                                asset_id=f"asset_{uuid.uuid4().hex}",
                                video_run_id=video_run_id,
                                clip_id=clip_id,
                                revision_id=revision_id,
                                asset_type="transformed_frame",
                                storage_path=str(transformed_path),
                                source_url=str(chosen_path),
                                file_name=transformed_path.name,
                                mime_type="image/png",
                                byte_size=transformed_size or int(transformed_path.stat().st_size),
                                checksum_sha256=transformed_checksum
                                or hashlib.sha256(transformed_path.read_bytes()).hexdigest(),
                                metadata={
                                    "assignment_stage": "storyboard",
                                    "prompt": edit_prompt_text,
                                    "source_image_asset_id": str(chosen_asset.get("asset_id") or ""),
                                    "source_pool": chosen_source_pool or "image_bank",
                                    "provider": edit_provider_name,
                                    "prompt_model_provider": str(prompt_model_provider or ""),
                                    "prompt_model_id": str(prompt_model_id or ""),
                                    "prompt_model_label": str(prompt_model_label or ""),
                                    "prompt_generator_provider": str(
                                        (prompt_payload or {}).get("provider")
                                        if isinstance(prompt_payload, dict)
                                        else ""
                                    ),
                                    "prompt_generator_change_summary": str(
                                        (prompt_payload or {}).get("change_summary")
                                        if isinstance(prompt_payload, dict)
                                        else ""
                                    ),
                                },
                            )
                            transformed_asset_id = str(transformed_asset.get("asset_id") or "")
                            transformed_score_payload = await asyncio.to_thread(
                                vision_provider.score_scene_match,
                                image_path=transformed_path,
                                scene_intent=scene_intent,
                                style_profile=style_profile,
                                model_id=str(prompt_model_id or _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID),
                                idempotency_key=f"{video_run_id}:{scene_line_id}:transformed_score",
                            )
                            transformed_score = _phase4_v1_storyboard_score(
                                (transformed_score_payload or {}).get("score_1_to_10")
                                if isinstance(transformed_score_payload, dict)
                                else 0
                            )
                            transformed_reason = str(
                                (transformed_score_payload or {}).get("reason_short")
                                if isinstance(transformed_score_payload, dict)
                                else ""
                            ).strip()
                            accept_transformed = transformed_score >= chosen_score
                            if force_broll_edit:
                                accept_transformed = True
                            elif not accept_transformed and consecutive_reuse_forced:
                                accept_transformed = True
                            elif not accept_transformed and chosen_score <= low_flag_threshold:
                                accept_transformed = transformed_score >= max(1, chosen_score - 1)
                            if accept_transformed:
                                chosen_path = transformed_path
                                if force_broll_edit:
                                    chosen_score = max(chosen_score, transformed_score)
                                    chosen_reason = transformed_reason or chosen_reason
                                else:
                                    chosen_score = transformed_score
                                    chosen_reason = transformed_reason or chosen_reason
                                edited = True
                        except Exception as exc:
                            edit_error = str(exc)
                            logger.warning(
                                "Storyboard transform failed for run=%s scene=%s mode=%s: %s",
                                video_run_id,
                                scene_line_id,
                                mode,
                                exc,
                            )

                    target_filename = deterministic_start_frame_filename(
                        brief_unit_id=str(clip.get("brief_unit_id") or ""),
                        hook_id=str(clip.get("hook_id") or ""),
                        script_line_id=str(clip.get("script_line_id") or ""),
                        mode=mode,
                        ext="png",
                    )
                    target_path = asset_dirs["start_frames"] / target_filename
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    _phase4_v1_storyboard_render_start_frame_9_16(source_path=chosen_path, output_path=target_path)
                    target_bytes = target_path.read_bytes()
                    target_checksum = hashlib.sha256(target_bytes).hexdigest()
                    create_video_asset(
                        asset_id=f"asset_{uuid.uuid4().hex}",
                        video_run_id=video_run_id,
                        clip_id=clip_id,
                        revision_id=revision_id,
                        asset_type="start_frame",
                        storage_path=str(target_path),
                        source_url=str(chosen_path),
                        file_name=target_filename,
                        mime_type=(mimetypes.guess_type(target_filename)[0] or ""),
                        byte_size=len(target_bytes),
                        checksum_sha256=target_checksum,
                        metadata={
                            "assignment_stage": "storyboard",
                            "scene_line_id": scene_line_id,
                            "source_image_asset_id": str(chosen_asset.get("asset_id") or ""),
                            "transformed_frame_asset_id": transformed_asset_id,
                            "assignment_score": chosen_score,
                            "assignment_reason": chosen_reason,
                            "edited": edited,
                            "force_broll_edit": force_broll_edit,
                            "edit_error": edit_error,
                            "edit_prompt": edit_prompt_text,
                            "edit_model_id": edit_model_id
                            or (
                                str(image_edit_model_id or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID)
                                if edit_prompt_text
                                else ""
                            ),
                            "edit_provider": edit_provider_name,
                            "prompt_model_provider": str(prompt_model_provider or ""),
                            "prompt_model_id": str(prompt_model_id or ""),
                            "prompt_model_label": str(prompt_model_label or ""),
                            "consecutive_reuse_forced": consecutive_reuse_forced,
                            "source_pool": chosen_source_pool or "image_bank",
                            "final_frame_width": _PHASE4_STORYBOARD_START_FRAME_WIDTH,
                            "final_frame_height": _PHASE4_STORYBOARD_START_FRAME_HEIGHT,
                            "final_frame_aspect_ratio": "9:16",
                        },
                    )

                    if revision_id:
                        updated_snapshot = dict(snapshot)
                        updated_snapshot["start_frame_filename"] = target_filename
                        updated_snapshot["start_frame_checksum"] = target_checksum
                        if mode == "a_roll":
                            updated_snapshot["avatar_filename"] = target_filename
                            updated_snapshot["avatar_checksum"] = target_checksum
                        update_video_clip_revision(
                            revision_id,
                            input_snapshot=updated_snapshot,
                        )

                    needs_review = chosen_score <= low_flag_threshold
                    _phase4_v1_storyboard_update_scene_status(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        task_key=task_key,
                        scene_line_id=scene_line_id,
                        updates={
                            "clip_id": clip_id,
                            "script_line_id": str(clip.get("script_line_id") or ""),
                            "mode": mode,
                            "assignment_status": "assigned_needs_review" if needs_review else "assigned",
                            "assignment_score": chosen_score,
                            "low_confidence": needs_review,
                            "start_frame_url": _phase4_v1_storage_path_to_outputs_url(str(target_path)),
                            "start_frame_filename": target_filename,
                            "source_image_asset_id": str(chosen_asset.get("asset_id") or ""),
                            "source_image_filename": str(
                                Path(str(chosen_asset.get("file_name") or chosen_path.name)).name
                            ),
                            "edited": edited,
                            "edit_prompt": edit_prompt_text,
                            "edit_model_id": edit_model_id
                            or (
                                str(image_edit_model_id or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID)
                                if edit_prompt_text
                                else ""
                            ),
                            "edit_provider": edit_provider_name,
                            "consecutive_reuse_forced": consecutive_reuse_forced,
                            "assignment_note": (
                                (
                                    f"{chosen_reason} (B-roll scene forced through image edit)."
                                    if chosen_reason
                                    else "B-roll scene forced through image edit."
                                )
                                if force_broll_edit and edited
                                else (
                                    (
                                        f"{chosen_reason}. Edit fallback used original image because edit failed: {edit_error}"
                                        if chosen_reason
                                        else f"Edit fallback used original image because edit failed: {edit_error}"
                                    )
                                    if edit_error
                                    else (
                                        (
                                            f"{chosen_reason} (A-roll fallback used B-roll source)."
                                            if chosen_reason
                                            else "Assigned using B-roll fallback source."
                                        )
                                        if a_roll_fallback_to_broll and mode == "a_roll"
                                        else (chosen_reason or ("Edited candidate image." if edited else "Assigned."))
                                    )
                                )
                            ),
                        },
                    )
                    if not needs_review:
                        autosave_metadata = _phase4_v1_storyboard_ai_library_metadata(
                            mode_hint=chosen_source_mode_hint or mode,
                            source_pool=chosen_source_pool or "image_bank",
                            source_image_asset_id=str(chosen_asset.get("asset_id") or ""),
                            source_image_filename=str(
                                Path(str(chosen_asset.get("file_name") or chosen_path.name)).name
                            ),
                            originating_video_run_id=video_run_id,
                            originating_scene_line_id=scene_line_id,
                            originating_clip_id=clip_id,
                            assignment_score=chosen_score,
                            assignment_status="assigned",
                            prompt_model_provider=str(prompt_model_provider or ""),
                            prompt_model_id=str(prompt_model_id or ""),
                            prompt_model_label=str(prompt_model_label or ""),
                            image_edit_model_id=edit_model_id
                            or str(image_edit_model_id or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID),
                            image_edit_model_label=str(image_edit_model_label or ""),
                            edit_provider=edit_provider_name,
                            edit_prompt=edit_prompt_text,
                        )
                        async with library_lock:
                            upsert_result = _phase4_v1_broll_upsert_from_source(
                                brand_slug=brand_slug,
                                branch_id=branch_id,
                                source_path=target_path,
                                preferred_file_name=target_filename,
                                metadata_updates=autosave_metadata,
                                rows=library_rows,
                                row_index=library_row_index,
                                checksum_index=library_checksum_index,
                                increment_usage_count=True,
                            )
                            library_rows = (
                                upsert_result.get("rows")
                                if isinstance(upsert_result.get("rows"), list)
                                else library_rows
                            )
                            if bool(upsert_result.get("changed")):
                                library_rows = _phase4_v1_save_broll_library(brand_slug, branch_id, library_rows)
                                library_row_index.clear()
                                library_row_index.update(_phase4_v1_broll_build_row_index(library_rows))
                            if bool(upsert_result.get("dedup_hit")):
                                library_dedup_hit_count += 1
                            else:
                                library_saved_count += 1
                    await _update_progress(completed_delta=1, failed_delta=0)
                except Exception as exc:
                    logger.warning(
                        "Storyboard assignment failed for run=%s scene=%s mode=%s: %s",
                        video_run_id,
                        scene_line_id,
                        mode,
                        exc,
                    )
                    _phase4_v1_storyboard_update_scene_status(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        task_key=task_key,
                        scene_line_id=scene_line_id,
                        updates={
                            "assignment_status": "failed",
                            "assignment_score": 0,
                            "assignment_note": f"Failed to render start frame: {exc}",
                        },
                    )
                    await _update_progress(completed_delta=0, failed_delta=1)

        if assignment_plans:
            await asyncio.gather(*[_process_assignment_plan(plan) for plan in assignment_plans])

        status_payload = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
        )
        _phase4_v1_storyboard_update_metrics(
            video_run_id=video_run_id,
            updates={
                "storyboard_ai_library_saved_count": int(library_saved_count or 0),
                "storyboard_ai_library_dedup_hit_count": int(library_dedup_hit_count or 0),
                "storyboard_ai_library_reused_count": int(library_reused_count or 0),
            },
        )
        logger.info(
            "storyboard_ai_library_saved_count=%s storyboard_ai_library_dedup_hit_count=%s storyboard_ai_library_reused_count=%s brand=%s branch=%s run=%s",
            int(library_saved_count or 0),
            int(library_dedup_hit_count or 0),
            int(library_reused_count or 0),
            brand_slug,
            branch_id,
            video_run_id,
        )

        brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)
        if not brief:
            raise RuntimeError("Missing start-frame brief during storyboard assignment finalization.")

        # Ensure avatar master exists for generation fallback.
        avatar_required = next(
            (item for item in brief.required_items if str(item.file_role) == "avatar_master"),
            None,
        )
        if avatar_required:
            avatar_name = str(avatar_required.filename or "").strip()
            avatar_asset = find_video_asset_by_filename(video_run_id, avatar_name) if avatar_name else None
            if not avatar_asset:
                first_a_roll = next(
                    (
                        row for row in list_video_clips(video_run_id)
                        if normalize_phase4_clip_mode(row.get("mode")) == "a_roll"
                    ),
                    None,
                )
                fallback_asset = None
                if first_a_roll:
                    first_revision = _phase4_v1_get_current_revision_row(first_a_roll) or {}
                    first_snapshot = (
                        first_revision.get("input_snapshot")
                        if isinstance(first_revision.get("input_snapshot"), dict)
                        else {}
                    )
                    first_avatar = str(first_snapshot.get("avatar_filename") or first_snapshot.get("start_frame_filename") or "").strip()
                    if first_avatar:
                        fallback_asset = find_video_asset_by_filename(video_run_id, first_avatar)
                if fallback_asset and avatar_name:
                    src_path = Path(str(fallback_asset.get("storage_path") or ""))
                    if src_path.exists():
                        avatar_target = _phase4_v1_assets_root(brand_slug, branch_id, video_run_id) / "start_frames" / avatar_name
                        avatar_target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src_path, avatar_target)
                        avatar_bytes = avatar_target.read_bytes()
                        create_video_asset(
                            asset_id=f"asset_{uuid.uuid4().hex}",
                            video_run_id=video_run_id,
                            asset_type="start_frame",
                            storage_path=str(avatar_target),
                            source_url=str(src_path),
                            file_name=avatar_name,
                            mime_type=(mimetypes.guess_type(avatar_name)[0] or ""),
                            byte_size=len(avatar_bytes),
                            checksum_sha256=hashlib.sha256(avatar_bytes).hexdigest(),
                            metadata={"storyboard_avatar_master": True},
                        )

        # Write a validation report compatible with existing generation gate.
        latest_assets = list_video_assets(video_run_id)
        matched_by_name: dict[str, dict[str, Any]] = {}
        for asset in latest_assets:
            if str(asset.get("asset_type") or "") != "start_frame":
                continue
            name = str(asset.get("file_name") or "").strip()
            if not name:
                continue
            matched_by_name[name] = asset
        validation_report = _phase4_v1_storyboard_generate_validation_report(
            video_run_id=video_run_id,
            folder_url=effective_folder_url,
            brief=brief,
            matched_assets_by_name=matched_by_name,
        )
        save_video_validation_report(
            report_id=validation_report.report_id,
            video_run_id=video_run_id,
            status=validation_report.status,
            folder_url=effective_folder_url,
            summary=validation_report.model_dump(exclude={"items"}),
            items=[row.model_dump() for row in validation_report.items],
        )
        _phase4_v1_write_json(
            _phase4_v1_drive_validation_report_path(brand_slug, branch_id, video_run_id),
            validation_report.model_dump(),
        )

        run_workflow = "assets_validated" if validation_report.status == "passed" else "validation_failed"
        run_error = "" if validation_report.status == "passed" else "Storyboard assignment left required frames unresolved."
        update_video_run(
            video_run_id,
            status="active",
            workflow_state=run_workflow,
            drive_folder_url=effective_folder_url,
            error=run_error,
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)

        status_payload["status"] = "completed"
        status_payload["updated_at"] = now_iso()
        status_payload["error"] = ""
        _phase4_v1_storyboard_write_runtime_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            task_key=task_key,
            payload=status_payload,
        )
    except asyncio.CancelledError:
        aborted_status = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
        )
        by_scene = (
            aborted_status.get("by_scene_line_id")
            if isinstance(aborted_status.get("by_scene_line_id"), dict)
            else {}
        )
        for scene_line_id, row in by_scene.items():
            if not isinstance(row, dict):
                continue
            if str(row.get("assignment_status") or "").strip().lower() == "analyzing":
                row["assignment_status"] = "pending"
                row["assignment_note"] = "Stopped by user."
                row["updated_at"] = now_iso()
                by_scene[scene_line_id] = row
        aborted_status["by_scene_line_id"] = by_scene
        aborted_status["status"] = "aborted"
        aborted_status["updated_at"] = now_iso()
        if not str(aborted_status.get("error") or "").strip():
            aborted_status["error"] = "Storyboard assignment stopped by user."
        _phase4_v1_storyboard_write_runtime_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            task_key=task_key,
            payload=aborted_status,
        )
        update_video_run(
            video_run_id,
            status="active",
            workflow_state="brief_approved",
            error=str(aborted_status.get("error") or ""),
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        raise
    except Exception as exc:
        logger.exception("Storyboard assignment failed for run %s", video_run_id)
        failed_status = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
        )
        failed_status["status"] = "failed"
        failed_status["updated_at"] = now_iso()
        failed_status["error"] = str(exc)
        _phase4_v1_storyboard_write_runtime_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            task_key=task_key,
            payload=failed_status,
        )
        update_video_run(
            video_run_id,
            status="active",
            workflow_state="validation_failed",
            error=f"Storyboard assignment failed: {exc}",
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    finally:
        phase4_v1_storyboard_assign_tasks.pop(task_key, None)
        phase4_v1_workflow_backend.clear_job(task_key)


async def _phase4_v1_execute_generation(brand_slug: str, branch_id: str, video_run_id: str):
    run_key = _phase4_v1_run_key(brand_slug, branch_id, video_run_id)
    try:
        run_row = get_video_run(video_run_id)
        if not run_row:
            return
        voice_preset = _phase4_v1_voice_preset_by_id(str(run_row.get("voice_preset_id") or ""))
        if not voice_preset:
            update_video_run(
                video_run_id,
                status="failed",
                workflow_state="failed",
                error="Voice preset not found.",
            )
            _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
            return

        report = _phase4_v1_validation_report_model(video_run_id)
        if not report or report.status != "passed":
            update_video_run(
                video_run_id,
                status="failed",
                workflow_state="failed",
                error="Drive validation must pass before generation.",
            )
            _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
            return
        validation_assets = validation_asset_lookup(report)
        brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)
        if not brief:
            update_video_run(
                video_run_id,
                status="failed",
                workflow_state="failed",
                error="Missing start frame brief.",
            )
            _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
            return
        required_avatar_item = next(
            (
                row for row in brief.required_items
                if str(row.file_role) == "avatar_master"
            ),
            None,
        )
        model_registry = _phase4_v1_model_registry()
        tts_provider, fal_provider, gemini_provider = build_generation_providers()
        run_dir = _phase4_v1_run_dir(brand_slug, branch_id, video_run_id)
        asset_dirs = ensure_phase4_asset_dirs(run_dir)

        clip_rows = list_video_clips(video_run_id)
        clip_rows.sort(key=lambda c: int(c.get("line_index") or 0))
        talking_head_failed = False
        run_error = ""

        for clip in clip_rows:
            clip_id = str(clip.get("clip_id") or "")
            mode = normalize_phase4_clip_mode(clip.get("mode"))
            clip_status = str(clip.get("status") or "")
            if str(clip.get("status") or "") == "approved":
                continue
            revision = _phase4_v1_get_current_revision_row(clip)
            if not revision:
                continue
            revision_id = str(revision.get("revision_id") or "")
            revision_index = int(revision.get("revision_index") or 1)
            input_snapshot = revision.get("input_snapshot") if isinstance(revision.get("input_snapshot"), dict) else {}
            transform_prompt = str(input_snapshot.get("transform_prompt") or "").strip()
            narration_text = str(clip.get("narration_text") or "").strip()
            planned_duration = float(input_snapshot.get("planned_duration_seconds") or clip.get("planned_duration_seconds") or 2.0)

            try:
                if mode == "a_roll":
                    update_video_clip(clip_id, status="generating_tts")
                    avatar_filename = str(input_snapshot.get("avatar_filename") or "").strip()
                    if not avatar_filename and required_avatar_item:
                        avatar_filename = required_avatar_item.filename
                    avatar_asset = find_video_asset_by_filename(video_run_id, avatar_filename) if avatar_filename else None
                    if not avatar_asset and avatar_filename in validation_assets:
                        avatar_asset = _phase4_v1_copy_start_frame_asset(
                            brand_slug=brand_slug,
                            branch_id=branch_id,
                            video_run_id=video_run_id,
                            matched_asset=validation_assets[avatar_filename].model_dump(),
                        )
                    if not avatar_asset:
                        raise RuntimeError(
                            f"Missing avatar asset `{avatar_filename}` for talking-head generation."
                        )

                    avatar_checksum = str(avatar_asset.get("checksum_sha256") or "")
                    snapshot_model = build_clip_input_snapshot(
                        mode="a_roll",
                        voice_preset_id=voice_preset.voice_preset_id,
                        narration_text=narration_text,
                        planned_duration_seconds=planned_duration,
                        start_frame_filename=avatar_filename,
                        start_frame_checksum=avatar_checksum,
                        avatar_filename=avatar_filename,
                        avatar_checksum=avatar_checksum,
                        transform_prompt=transform_prompt,
                        model_ids=model_registry,
                    )
                    idempotency_key = compute_idempotency_key(
                        run_id=video_run_id,
                        clip_id=clip_id,
                        revision_index=revision_index,
                        mode="a_roll",
                        start_frame_checksum=snapshot_model.start_frame_checksum,
                        transform_hash=snapshot_model.transform_hash,
                        narration_text_hash=snapshot_model.narration_text_hash,
                        voice_preset_id=voice_preset.voice_preset_id,
                        model_ids=model_registry,
                        avatar_checksum=snapshot_model.avatar_checksum,
                    )

                    tts_key = f"{idempotency_key}:tts"
                    tts_call = create_or_get_video_provider_call(
                        provider_call_id=f"pc_{uuid.uuid4().hex}",
                        video_run_id=video_run_id,
                        clip_id=clip_id,
                        revision_id=revision_id,
                        provider_name="tts",
                        operation="synthesize",
                        idempotency_key=tts_key,
                        request_payload={
                            "voice_preset_id": voice_preset.voice_preset_id,
                            "tts_model": voice_preset.tts_model,
                            "text_hash": snapshot_model.narration_text_hash,
                        },
                    )

                    audio_asset_id = ""
                    narration_duration = 0.0
                    response_payload = tts_call.get("response_payload") if isinstance(tts_call.get("response_payload"), dict) else {}
                    reuse_tts_call = str(tts_call.get("status")) == "completed"
                    if reuse_tts_call and clip_status == "failed":
                        reuse_tts_call = not str(response_payload.get("provider") or "").startswith("mock_")
                    if reuse_tts_call:
                        audio_asset_id = str(response_payload.get("audio_asset_id") or "")
                        narration_duration = float(response_payload.get("duration_seconds") or 0.0)
                    else:
                        audio_file = asset_dirs["narration_audio"] / f"{clip_id}__r{revision_index}.wav"
                        tts_result = await asyncio.to_thread(
                            tts_provider.synthesize,
                            text=narration_text,
                            voice_preset_id=voice_preset.voice_preset_id,
                            tts_model=voice_preset.tts_model,
                            output_path=audio_file,
                            speed=float(voice_preset.settings.get("speed", config.PHASE4_V1_TTS_SPEED)),
                            pitch=float(voice_preset.settings.get("pitch", config.PHASE4_V1_TTS_PITCH)),
                            gain_db=float(voice_preset.settings.get("gain_db", config.PHASE4_V1_TTS_GAIN_DB)),
                            idempotency_key=tts_key,
                        )
                        audio_asset = create_video_asset(
                            asset_id=f"asset_{uuid.uuid4().hex}",
                            video_run_id=video_run_id,
                            clip_id=clip_id,
                            revision_id=revision_id,
                            asset_type="narration_audio",
                            storage_path=str(audio_file),
                            file_name=audio_file.name,
                            mime_type="audio/wav",
                            byte_size=int(tts_result.get("size_bytes") or audio_file.stat().st_size),
                            checksum_sha256=str(tts_result.get("checksum_sha256") or ""),
                            metadata=tts_result,
                        )
                        audio_asset_id = str(audio_asset.get("asset_id") or "")
                        narration_duration = float(tts_result.get("duration_seconds") or 0.0)
                        update_video_provider_call(
                            tts_key,
                            status="completed",
                            response_payload={
                                **tts_result,
                                "audio_asset_id": audio_asset_id,
                                "duration_seconds": narration_duration,
                            },
                        )

                    update_video_clip(clip_id, status="generating_a_roll")
                    talking_key = f"{idempotency_key}:talking_head"
                    talking_call = create_or_get_video_provider_call(
                        provider_call_id=f"pc_{uuid.uuid4().hex}",
                        video_run_id=video_run_id,
                        clip_id=clip_id,
                        revision_id=revision_id,
                        provider_name="fal",
                        operation="talking_head",
                        idempotency_key=talking_key,
                        request_payload={
                            "model_id": model_registry["fal_talking_head"],
                            "audio_asset_id": audio_asset_id,
                            "avatar_asset_id": str(avatar_asset.get("asset_id") or ""),
                        },
                    )

                    talking_asset_id = ""
                    talking_duration = 0.0
                    talking_size = 0
                    response_payload = talking_call.get("response_payload") if isinstance(talking_call.get("response_payload"), dict) else {}
                    reuse_talking_call = str(talking_call.get("status")) == "completed"
                    if reuse_talking_call and clip_status == "failed":
                        reuse_talking_call = not str(response_payload.get("provider") or "").startswith("mock_")
                    if reuse_talking_call:
                        talking_asset_id = str(response_payload.get("talking_head_asset_id") or "")
                        talking_duration = float(response_payload.get("duration_seconds") or 0.0)
                        talking_size = int(response_payload.get("size_bytes") or 0)
                    else:
                        audio_asset_row = get_video_asset(audio_asset_id)
                        if not audio_asset_row or not str(audio_asset_row.get("storage_path") or "").strip():
                            raise RuntimeError("Narration audio asset missing before talking-head generation.")
                        audio_path = Path(str(audio_asset_row.get("storage_path")))
                        avatar_path = Path(str(avatar_asset.get("storage_path") or ""))
                        if not avatar_path.exists():
                            raise RuntimeError(f"Avatar frame path is not available: {avatar_path}")
                        talking_file = asset_dirs["talking_heads"] / f"{clip_id}__r{revision_index}.mp4"
                        talking_result = await asyncio.to_thread(
                            fal_provider.generate_talking_head,
                            avatar_image_path=avatar_path,
                            narration_audio_path=audio_path,
                            output_path=talking_file,
                            model_id=model_registry["fal_talking_head"],
                            idempotency_key=talking_key,
                            planned_duration_seconds=planned_duration,
                            prompt=narration_text,
                        )
                        talking_asset = create_video_asset(
                            asset_id=f"asset_{uuid.uuid4().hex}",
                            video_run_id=video_run_id,
                            clip_id=clip_id,
                            revision_id=revision_id,
                            asset_type="talking_head",
                            storage_path=str(talking_file),
                            file_name=talking_file.name,
                            mime_type="video/mp4",
                            byte_size=int(talking_result.get("size_bytes") or talking_file.stat().st_size),
                            checksum_sha256=str(talking_result.get("checksum_sha256") or ""),
                            metadata=talking_result,
                        )
                        talking_asset_id = str(talking_asset.get("asset_id") or "")
                        talking_duration = float(talking_result.get("duration_seconds") or 0.0)
                        talking_size = int(talking_result.get("size_bytes") or 0)
                        update_video_provider_call(
                            talking_key,
                            status="completed",
                            response_payload={
                                **talking_result,
                                "talking_head_asset_id": talking_asset_id,
                                "duration_seconds": talking_duration,
                            },
                        )

                    provenance = ClipProvenanceV1(
                        idempotency_key=idempotency_key,
                        provider_call_ids=[
                            str(tts_call.get("provider_call_id") or ""),
                            str(talking_call.get("provider_call_id") or ""),
                        ],
                        voice_preset_id=voice_preset.voice_preset_id,
                        tts_model=voice_preset.tts_model,
                        audio_asset_id=audio_asset_id,
                        talking_head_asset_id=talking_asset_id,
                        start_frame_asset_id=str(avatar_asset.get("asset_id") or ""),
                        timestamps={"generated_at": now_iso()},
                    )
                    provenance.completeness_pct = compute_provenance_completeness("a_roll", provenance)
                    qc = build_a_roll_qc(
                        planned_duration=planned_duration,
                        narration_duration=narration_duration,
                        audio_size=int(get_video_asset(audio_asset_id).get("byte_size") if get_video_asset(audio_asset_id) else 0),
                        talking_head_duration=talking_duration,
                        talking_head_size=talking_size,
                    )
                    next_status = "pending_review" if (qc.pass_qc and provenance.completeness_pct == 100) else "failed"
                    update_video_clip_revision(
                        revision_id,
                        status=next_status,
                        input_snapshot=snapshot_model.model_dump(),
                        provenance=provenance.model_dump(),
                        qc_report=qc.model_dump(),
                    )
                    update_video_clip(clip_id, status=next_status)
                    if next_status == "failed":
                        talking_head_failed = True
                        run_error = f"Talking-head generation failed for clip {clip_id}."

                else:
                    update_video_clip(clip_id, status="generating_b_roll")
                    default_filename = (
                        f"sf__{clip.get('brief_unit_id')}__{clip.get('hook_id')}__{clip.get('script_line_id')}__b_roll.png"
                    )
                    start_frame_filename = str(input_snapshot.get("start_frame_filename") or default_filename)
                    start_frame_asset = find_video_asset_by_filename(video_run_id, start_frame_filename)
                    if not start_frame_asset and start_frame_filename in validation_assets:
                        start_frame_asset = _phase4_v1_copy_start_frame_asset(
                            brand_slug=brand_slug,
                            branch_id=branch_id,
                            video_run_id=video_run_id,
                            matched_asset=validation_assets[start_frame_filename].model_dump(),
                        )
                    if not start_frame_asset:
                        raise RuntimeError(f"Missing required B-roll start frame `{start_frame_filename}`.")
                    start_frame_path = Path(str(start_frame_asset.get("storage_path") or ""))
                    if not start_frame_path.exists():
                        raise RuntimeError(f"Start frame file is not available: {start_frame_path}")

                    transformed_frame_asset_id = ""
                    transform_hash = sha256_text(transform_prompt) if transform_prompt else ""
                    effective_start_frame_path = start_frame_path
                    if transform_prompt:
                        update_video_clip(clip_id, status="transforming")
                        transform_key = (
                            compute_idempotency_key(
                                run_id=video_run_id,
                                clip_id=clip_id,
                                revision_index=revision_index,
                                mode="b_roll",
                                start_frame_checksum=str(start_frame_asset.get("checksum_sha256") or ""),
                                transform_hash=transform_hash,
                                narration_text_hash=sha256_text(narration_text),
                                voice_preset_id=voice_preset.voice_preset_id,
                                model_ids=model_registry,
                                avatar_checksum="",
                            )
                            + ":transform"
                        )
                        transform_call = create_or_get_video_provider_call(
                            provider_call_id=f"pc_{uuid.uuid4().hex}",
                            video_run_id=video_run_id,
                            clip_id=clip_id,
                            revision_id=revision_id,
                            provider_name="gemini",
                            operation="transform_image",
                            idempotency_key=transform_key,
                            request_payload={
                                "model_id": model_registry["gemini_image_edit"],
                                "prompt_hash": transform_hash,
                                "start_frame_asset_id": str(start_frame_asset.get("asset_id") or ""),
                            },
                        )
                        if str(transform_call.get("status")) == "completed":
                            response_payload = transform_call.get("response_payload") if isinstance(transform_call.get("response_payload"), dict) else {}
                            transformed_frame_asset_id = str(response_payload.get("transformed_frame_asset_id") or "")
                            transformed_asset = get_video_asset(transformed_frame_asset_id)
                            if transformed_asset and transformed_asset.get("storage_path"):
                                effective_start_frame_path = Path(str(transformed_asset.get("storage_path")))
                        else:
                            transformed_path = asset_dirs["transformed_frames"] / f"{clip_id}__r{revision_index}.png"
                            transformed_result = await asyncio.to_thread(
                                gemini_provider.transform_image,
                                input_path=start_frame_path,
                                prompt=transform_prompt,
                                output_path=transformed_path,
                                model_id=model_registry["gemini_image_edit"],
                                idempotency_key=transform_key,
                            )
                            transformed_asset = create_video_asset(
                                asset_id=f"asset_{uuid.uuid4().hex}",
                                video_run_id=video_run_id,
                                clip_id=clip_id,
                                revision_id=revision_id,
                                asset_type="transformed_frame",
                                storage_path=str(transformed_path),
                                file_name=transformed_path.name,
                                mime_type="image/png",
                                byte_size=int(transformed_result.get("size_bytes") or transformed_path.stat().st_size),
                                checksum_sha256=str(transformed_result.get("checksum_sha256") or ""),
                                metadata=transformed_result,
                            )
                            transformed_frame_asset_id = str(transformed_asset.get("asset_id") or "")
                            effective_start_frame_path = transformed_path
                            update_video_provider_call(
                                transform_key,
                                status="completed",
                                response_payload={
                                    **transformed_result,
                                    "transformed_frame_asset_id": transformed_frame_asset_id,
                                },
                            )

                    snapshot_model = build_clip_input_snapshot(
                        mode="b_roll",
                        voice_preset_id=voice_preset.voice_preset_id,
                        narration_text=narration_text,
                        planned_duration_seconds=planned_duration,
                        start_frame_filename=start_frame_filename,
                        start_frame_checksum=str(start_frame_asset.get("checksum_sha256") or ""),
                        avatar_filename="",
                        avatar_checksum="",
                        transform_prompt=transform_prompt,
                        model_ids=model_registry,
                    )
                    broll_key = compute_idempotency_key(
                        run_id=video_run_id,
                        clip_id=clip_id,
                        revision_index=revision_index,
                        mode="b_roll",
                        start_frame_checksum=snapshot_model.start_frame_checksum,
                        transform_hash=snapshot_model.transform_hash,
                        narration_text_hash=snapshot_model.narration_text_hash,
                        voice_preset_id=voice_preset.voice_preset_id,
                        model_ids=model_registry,
                        avatar_checksum="",
                    ) + ":broll"
                    broll_call = create_or_get_video_provider_call(
                        provider_call_id=f"pc_{uuid.uuid4().hex}",
                        video_run_id=video_run_id,
                        clip_id=clip_id,
                        revision_id=revision_id,
                        provider_name="fal",
                        operation="image_to_video",
                        idempotency_key=broll_key,
                        request_payload={
                            "model_id": model_registry["fal_broll"],
                            "start_frame_asset_id": str(start_frame_asset.get("asset_id") or ""),
                        },
                    )
                    broll_asset_id = ""
                    broll_duration = 0.0
                    broll_size = 0
                    response_payload = broll_call.get("response_payload") if isinstance(broll_call.get("response_payload"), dict) else {}
                    reuse_broll_call = str(broll_call.get("status")) == "completed"
                    if reuse_broll_call and clip_status == "failed":
                        reuse_broll_call = not str(response_payload.get("provider") or "").startswith("mock_")
                    if reuse_broll_call:
                        broll_asset_id = str(response_payload.get("broll_asset_id") or "")
                        broll_duration = float(response_payload.get("duration_seconds") or 0.0)
                        broll_size = int(response_payload.get("size_bytes") or 0)
                    else:
                        broll_file = asset_dirs["broll"] / f"{clip_id}__r{revision_index}.mp4"
                        broll_result = await asyncio.to_thread(
                            fal_provider.generate_broll,
                            start_frame_path=effective_start_frame_path,
                            output_path=broll_file,
                            model_id=model_registry["fal_broll"],
                            idempotency_key=broll_key,
                            planned_duration_seconds=planned_duration,
                            prompt=narration_text,
                        )
                        broll_asset = create_video_asset(
                            asset_id=f"asset_{uuid.uuid4().hex}",
                            video_run_id=video_run_id,
                            clip_id=clip_id,
                            revision_id=revision_id,
                            asset_type="broll",
                            storage_path=str(broll_file),
                            file_name=broll_file.name,
                            mime_type="video/mp4",
                            byte_size=int(broll_result.get("size_bytes") or broll_file.stat().st_size),
                            checksum_sha256=str(broll_result.get("checksum_sha256") or ""),
                            metadata=broll_result,
                        )
                        broll_asset_id = str(broll_asset.get("asset_id") or "")
                        broll_duration = float(broll_result.get("duration_seconds") or 0.0)
                        broll_size = int(broll_result.get("size_bytes") or 0)
                        update_video_provider_call(
                            broll_key,
                            status="completed",
                            response_payload={
                                **broll_result,
                                "broll_asset_id": broll_asset_id,
                            },
                        )

                    provenance = ClipProvenanceV1(
                        idempotency_key=broll_key,
                        provider_call_ids=[str(broll_call.get("provider_call_id") or "")],
                        voice_preset_id=voice_preset.voice_preset_id,
                        start_frame_asset_id=str(start_frame_asset.get("asset_id") or ""),
                        transformed_frame_asset_id=transformed_frame_asset_id,
                        broll_asset_id=broll_asset_id,
                        timestamps={"generated_at": now_iso()},
                    )
                    provenance.completeness_pct = compute_provenance_completeness("b_roll", provenance)
                    qc = build_b_roll_qc(
                        duration_seconds=broll_duration,
                        file_size=broll_size,
                        planned_duration=planned_duration,
                    )
                    next_status = "pending_review" if (qc.pass_qc and provenance.completeness_pct == 100) else "failed"
                    update_video_clip_revision(
                        revision_id,
                        status=next_status,
                        input_snapshot=snapshot_model.model_dump(),
                        provenance=provenance.model_dump(),
                        qc_report=qc.model_dump(),
                    )
                    update_video_clip(clip_id, status=next_status)

            except Exception as clip_exc:
                logger.exception("Phase4 generation failed for clip %s", clip_id)
                update_video_clip(clip_id, status="failed")
                if revision_id:
                    update_video_clip_revision(
                        revision_id,
                        status="failed",
                        operator_note=f"Generation error: {clip_exc}",
                    )
                run_error = str(clip_exc)
                if mode == "a_roll":
                    talking_head_failed = True

        if talking_head_failed:
            update_video_run(
                video_run_id,
                status="failed",
                workflow_state="failed",
                error=run_error or "Talking-head generation failure blocked run completion.",
            )
        else:
            if _phase4_v1_all_clips_approved(video_run_id) and _phase4_v1_all_latest_revisions_complete(video_run_id):
                current = get_video_run(video_run_id) or {}
                completed_at = str(current.get("completed_at") or "").strip() or now_iso()
                update_video_run(
                    video_run_id,
                    status="completed",
                    workflow_state="completed",
                    completed_at=completed_at,
                    error="",
                )
            else:
                update_video_run(
                    video_run_id,
                    status="active",
                    workflow_state="review_pending",
                    error=run_error,
                )
        _phase4_v1_refresh_review_queue_artifact(brand_slug, branch_id, video_run_id)
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    except Exception as exc:
        logger.exception("Phase4 generation pipeline crashed for run %s", video_run_id)
        update_video_run(
            video_run_id,
            status="failed",
            workflow_state="failed",
            error=f"Generation pipeline error: {exc}",
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    finally:
        phase4_v1_generation_tasks.pop(run_key, None)
        phase4_v1_workflow_backend.clear_job(run_key)


@app.get("/api/phase4-v1/voice-presets")
async def api_phase4_v1_voice_presets():
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    return {"voice_presets": [row.model_dump() for row in _phase4_v1_voice_presets()]}


@app.post("/api/branches/{branch_id}/phase4-v1/storyboard/bootstrap")
async def api_phase4_v1_storyboard_bootstrap(
    branch_id: str,
    req: StoryboardBootstrapRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    reused_existing_run = False
    reused = _phase4_v1_storyboard_find_reusable_run(
        brand_slug=brand_slug,
        branch_id=branch_id,
        phase3_run_id=req.phase3_run_id,
    )
    if reused:
        video_run_id = str(reused.get("video_run_id") or "").strip()
        reused_existing_run = True
    else:
        voice_preset_id = str(req.voice_preset_id or "").strip() or _phase4_v1_default_voice_preset_id()
        created_req = CreateVideoRunRequestV1(
            brand=brand_slug,
            phase3_run_id=req.phase3_run_id,
            voice_preset_id=voice_preset_id,
            reviewer_role="operator",
        )
        created = await api_phase4_v1_create_run(branch_id, created_req)
        if isinstance(created, JSONResponse):
            should_retry = bool(config.PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN) and int(created.status_code) == 409
            if not should_retry:
                return created
            blocking = _phase4_v1_storyboard_find_any_active_run()
            if not blocking:
                return created
            blocking_video_run_id = str(blocking.get("video_run_id") or "").strip()
            blocking_brand = str(blocking.get("brand_slug") or "").strip()
            blocking_branch = str(blocking.get("branch_id") or "").strip()
            if blocking_video_run_id:
                update_video_run(
                    blocking_video_run_id,
                    status="aborted",
                    workflow_state="aborted",
                    error=(
                        "Auto-aborted active test-mode run during storyboard bootstrap "
                        f"for phase3 run `{req.phase3_run_id}`."
                    ),
                )
                if blocking_brand and blocking_branch:
                    _phase4_v1_update_run_manifest_mirror(blocking_brand, blocking_branch, blocking_video_run_id)
            created = await api_phase4_v1_create_run(branch_id, created_req)
            if isinstance(created, JSONResponse):
                return created
        video_run = created.get("run") if isinstance(created, dict) else {}
        video_run_id = str(video_run.get("video_run_id") or "").strip()
        if not video_run_id:
            return JSONResponse({"error": "Failed to create storyboard run."}, status_code=500)

    # Ensure brief exists and is approved for generation compatibility.
    brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)
    if not brief:
        generated = await api_phase4_v1_generate_start_frame_brief(
            branch_id,
            video_run_id,
            GenerateBriefRequestV1(brand=brand_slug),
        )
        if isinstance(generated, JSONResponse):
            return generated
        try:
            brief = StartFrameBriefV1.model_validate(generated)
        except Exception:
            brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)

    approval = _phase4_v1_read_json(
        _phase4_v1_start_frame_brief_approval_path(brand_slug, branch_id, video_run_id),
        {},
    )
    approved = bool(isinstance(approval, dict) and approval.get("approved"))
    if not approved:
        approved_resp = await api_phase4_v1_approve_start_frame_brief(
            branch_id,
            video_run_id,
            ApproveBriefRequestV1(brand=brand_slug, approved_by="storyboard_bootstrap", notes="Auto-approved for storyboard flow."),
        )
        if isinstance(approved_resp, JSONResponse):
            return approved_resp

    run_row = get_video_run(video_run_id) or {}
    clip_count = len(list_video_clips(video_run_id))
    payload = StoryboardBootstrapResponseV1(
        video_run_id=video_run_id,
        reused_existing_run=reused_existing_run,
        workflow_state=str(run_row.get("workflow_state") or "brief_approved"),
        clip_count=clip_count,
    )
    _phase4_v1_storyboard_update_metrics(
        video_run_id=video_run_id,
        updates={
            "storyboard_assignment_job_id": str((run_row.get("metrics") or {}).get("storyboard_assignment_job_id") if isinstance(run_row.get("metrics"), dict) else ""),
            "clip_count": clip_count,
        },
    )
    backfill_state = _phase4_v1_storyboard_backfill_latest_assigned_outputs(
        brand_slug=brand_slug,
        branch_id=branch_id,
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    response = payload.model_dump()
    response["backfill"] = backfill_state
    return response


@app.post("/api/branches/{branch_id}/phase4-v1/runs")
async def api_phase4_v1_create_run(branch_id: str, req: CreateVideoRunRequestV1):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)

    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    voice_preset = _phase4_v1_voice_preset_by_id(req.voice_preset_id)
    if not voice_preset:
        return JSONResponse({"error": f"Unknown voice preset: {req.voice_preset_id}"}, status_code=400)

    phase3_detail = _phase3_v2_collect_run_detail(brand_slug, branch_id, req.phase3_run_id)
    if not phase3_detail:
        return JSONResponse({"error": "Phase 3 run not found"}, status_code=404)
    production_handoff = phase3_detail.get("production_handoff_packet")
    if not isinstance(production_handoff, dict) or not bool(production_handoff.get("ready")):
        raw_handoff = _phase3_v2_read_json(
            _phase3_v2_production_handoff_path(brand_slug, branch_id, req.phase3_run_id),
            {},
        )
        if isinstance(raw_handoff, dict) and bool(raw_handoff.get("ready")):
            production_handoff = raw_handoff
    if not isinstance(production_handoff, dict) or not bool(production_handoff.get("ready")):
        return JSONResponse(
            {"error": "Production handoff is not ready for this Phase 3 run."},
            status_code=400,
        )

    script_lookup = build_script_text_lookup(phase3_detail)
    mapping_rows = build_scene_line_mapping(
        production_handoff_packet=production_handoff,
        script_text_lookup=script_lookup,
    )
    if not mapping_rows:
        return JSONResponse({"error": "No scene lines found in production handoff."}, status_code=400)

    video_run_id = f"p4v1_{int(time.time() * 1000)}"
    try:
        run_row = create_video_run(
            video_run_id=video_run_id,
            brand_slug=brand_slug,
            branch_id=branch_id,
            phase3_run_id=req.phase3_run_id,
            voice_preset_id=voice_preset.voice_preset_id,
            reviewer_role=str(req.reviewer_role or "operator").strip() or "operator",
            status="active",
            workflow_state="draft",
            parallelism=max(1, int(config.PHASE4_V1_MAX_PARALLEL_CLIPS)),
            metrics={
                "test_mode_single_active_run": bool(config.PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN),
                "model_registry": _phase4_v1_model_registry(),
                "clip_count": len(mapping_rows),
            },
        )
    except sqlite3.IntegrityError:
        if bool(config.PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN):
            return JSONResponse(
                {
                    "error": (
                        "Test mode allows only one active Phase 4 run globally. "
                        "Complete/fail/abort the current active run before creating another."
                    )
                },
                status_code=409,
            )
        raise

    run_dir = _phase4_v1_run_dir(brand_slug, branch_id, video_run_id)
    ensure_phase4_asset_dirs(run_dir)

    mapping_rows = [
        row.model_copy(update={"clip_id": f"{video_run_id}__{row.clip_id}"})
        for row in mapping_rows
    ]

    try:
        for row in mapping_rows:
            clip_row = create_video_clip(
                clip_id=row.clip_id,
                video_run_id=video_run_id,
                scene_unit_id=row.scene_unit_id,
                scene_line_id=row.scene_line_id,
                brief_unit_id=row.brief_unit_id,
                hook_id=row.hook_id,
                arm=row.arm,
                script_line_id=row.script_line_id,
                mode=row.mode,
                line_index=row.line_index,
                narration_text=row.narration_text,
                status="pending",
                current_revision_index=1,
            )
            default_start_frame = (
                deterministic_start_frame_filename(
                    brief_unit_id=row.brief_unit_id,
                    hook_id=row.hook_id,
                    script_line_id=row.script_line_id,
                    mode=normalize_phase4_clip_mode(row.mode),
                    ext="png",
                )
                if is_phase4_b_roll_mode(row.mode)
                else deterministic_start_frame_filename(
                    brief_unit_id="avatar_master",
                    hook_id="global",
                    script_line_id="global",
                    mode="a_roll",
                    ext="png",
                )
            )
            snapshot = build_clip_input_snapshot(
                mode=normalize_phase4_clip_mode(row.mode),
                voice_preset_id=voice_preset.voice_preset_id,
                narration_text=row.narration_text,
                planned_duration_seconds=row.duration_seconds,
                start_frame_filename=default_start_frame,
                start_frame_checksum="",
                avatar_filename=default_start_frame if normalize_phase4_clip_mode(row.mode) == "a_roll" else "",
                avatar_checksum="",
                transform_prompt="",
                model_ids=_phase4_v1_model_registry(),
            )
            create_video_clip_revision(
                revision_id=f"rev_{uuid.uuid4().hex}",
                video_run_id=video_run_id,
                clip_id=clip_row["clip_id"],
                revision_index=1,
                status="pending",
                input_snapshot=snapshot.model_dump(),
                provenance=ClipProvenanceV1().model_dump(),
                qc_report={},
            )
    except sqlite3.IntegrityError as exc:
        update_video_run(
            video_run_id,
            status="failed",
            workflow_state="failed",
            error=f"Phase 4 run initialization failed: {exc}",
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        return JSONResponse(
            {
                "error": (
                    "Phase 4 run initialization failed due to a duplicate record. "
                    "Please create a new run."
                )
            },
            status_code=409,
        )
    except Exception as exc:
        logger.exception("Phase4 create run initialization failed: %s", video_run_id)
        update_video_run(
            video_run_id,
            status="failed",
            workflow_state="failed",
            error=f"Phase 4 run initialization failed: {exc}",
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        return JSONResponse(
            {"error": f"Phase 4 run initialization failed: {exc}"},
            status_code=500,
        )

    _phase4_v1_write_json(
        _phase4_v1_scene_line_mapping_path(brand_slug, branch_id, video_run_id),
        [row.model_dump() for row in mapping_rows],
    )
    _phase4_v1_write_json(_phase4_v1_start_frame_brief_approval_path(brand_slug, branch_id, video_run_id), {})
    _phase4_v1_write_json(_phase4_v1_drive_validation_report_path(brand_slug, branch_id, video_run_id), {})
    _phase4_v1_write_json(_phase4_v1_review_queue_path(brand_slug, branch_id, video_run_id), [])
    _phase4_v1_write_json(_phase4_v1_audit_pack_path(brand_slug, branch_id, video_run_id), {})
    _phase4_v1_write_json(
        _phase4_v1_storyboard_assignment_report_path(brand_slug, branch_id, video_run_id),
        _phase4_v1_storyboard_build_initial_status(video_run_id=video_run_id, clips=list_video_clips(video_run_id)),
    )
    _phase4_v1_write_json(
        _phase4_v1_storyboard_saved_versions_path(brand_slug, branch_id, video_run_id),
        [],
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    detail = _phase4_v1_collect_run_detail(brand_slug, branch_id, video_run_id)
    return detail or {"run": run_row}


@app.get("/api/branches/{branch_id}/phase4-v1/runs")
async def api_phase4_v1_list_runs(branch_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    rows = list_video_runs_for_branch(brand_slug, branch_id)
    return rows


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}")
async def api_phase4_v1_run_detail(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    detail = _phase4_v1_collect_run_detail(brand_slug, branch_id, video_run_id)
    if not detail:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    return detail


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/start-frame-brief/generate")
async def api_phase4_v1_generate_start_frame_brief(
    branch_id: str,
    video_run_id: str,
    req: GenerateBriefRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug")) != brand_slug or str(run_row.get("branch_id")) != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    mapping_rows = _phase4_v1_load_mapping_rows(brand_slug, branch_id, video_run_id)
    if not mapping_rows:
        return JSONResponse({"error": "Scene line mapping is missing."}, status_code=400)
    brief = generate_start_frame_brief(
        video_run_id=video_run_id,
        phase3_run_id=str(run_row.get("phase3_run_id") or ""),
        mapping_rows=mapping_rows,
    )
    _phase4_v1_write_json(
        _phase4_v1_start_frame_brief_path(brand_slug, branch_id, video_run_id),
        brief.model_dump(),
    )
    update_video_run(
        video_run_id,
        status="active",
        workflow_state="brief_generated",
        error="",
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return brief.model_dump()


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/start-frame-brief/approve")
async def api_phase4_v1_approve_start_frame_brief(
    branch_id: str,
    video_run_id: str,
    req: ApproveBriefRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)
    if not brief:
        return JSONResponse({"error": "Start frame brief must be generated first."}, status_code=400)
    approval = StartFrameBriefApprovalV1(
        video_run_id=video_run_id,
        approved=True,
        approved_by=str(req.approved_by or "").strip(),
        approved_at=now_iso(),
        notes=str(req.notes or "").strip(),
    )
    _phase4_v1_write_json(
        _phase4_v1_start_frame_brief_approval_path(brand_slug, branch_id, video_run_id),
        approval.model_dump(),
    )
    update_video_run(
        video_run_id,
        status="active",
        workflow_state="brief_approved",
        error="",
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return approval.model_dump()


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/start-frame-brief")
async def api_phase4_v1_get_start_frame_brief(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    brief = _phase4_v1_read_json(_phase4_v1_start_frame_brief_path(brand_slug, branch_id, video_run_id), {})
    approval = _phase4_v1_read_json(
        _phase4_v1_start_frame_brief_approval_path(brand_slug, branch_id, video_run_id),
        {},
    )
    if not brief:
        return JSONResponse({"error": "Start frame brief not generated yet."}, status_code=404)
    return {"brief": brief, "approval": approval}


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/drive/local-folder-ingest")
async def api_phase4_v1_ingest_local_folder(
    branch_id: str,
    video_run_id: str,
    brand: str = Form(""),
    files: list[UploadFile] = File(...),
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS):
        return JSONResponse(
            {"error": "Local folder ingest is disabled. Enable PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS=true."},
            status_code=400,
        )

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    if not files:
        return JSONResponse({"error": "No files uploaded."}, status_code=400)

    named_files: list[tuple[UploadFile, str, str, bool]] = []
    rename_map: list[dict[str, str]] = []
    seen_names: dict[str, int] = {}
    used_stored_names: set[str] = set()
    for upload in files:
        original_name = Path(str(upload.filename or "").strip()).name
        if not original_name:
            continue
        stem = Path(original_name).stem
        suffix = Path(original_name).suffix
        key = original_name.lower()
        count = int(seen_names.get(key, 0)) + 1
        seen_names[key] = count
        stored_name = original_name if count == 1 else f"{stem}__dup{count}{suffix}"
        dedupe_count = count
        while stored_name.lower() in used_stored_names:
            dedupe_count += 1
            stored_name = f"{stem}__dup{dedupe_count}{suffix}"
        used_stored_names.add(stored_name.lower())
        if stored_name != original_name:
            rename_map.append({"original_name": original_name, "stored_name": stored_name})
        named_files.append(
            (
                upload,
                original_name,
                stored_name,
                _phase4_v1_storyboard_supported_image(stored_name),
            )
        )

    if not named_files:
        return JSONResponse({"error": "No valid files found in folder selection."}, status_code=400)

    upload_dir = _phase4_v1_local_uploads_root(brand_slug, branch_id, video_run_id) / f"upload_{int(time.time() * 1000)}"
    upload_dir.mkdir(parents=True, exist_ok=True)
    saved_names: list[str] = []
    supported_names: list[str] = []
    skipped_unsupported: list[dict[str, str]] = []
    total_bytes = 0

    try:
        for upload, original_name, stored_name, supported in named_files:
            payload = await upload.read()
            target_path = upload_dir / stored_name
            target_path.write_bytes(payload)
            total_bytes += len(payload)
            saved_names.append(stored_name)
            if supported:
                supported_names.append(stored_name)
            else:
                skipped_unsupported.append(
                    {
                        "original_name": original_name,
                        "stored_name": stored_name,
                        "reason": "unsupported_image_type",
                    }
                )
            try:
                await upload.close()
            except Exception:
                pass
    except Exception as exc:
        shutil.rmtree(upload_dir, ignore_errors=True)
        return JSONResponse({"error": f"Failed to stage local folder files: {exc}"}, status_code=500)

    return {
        "ok": True,
        "video_run_id": video_run_id,
        "folder_path": str(upload_dir),
        "file_count": len(saved_names),
        "total_bytes": total_bytes,
        "files": saved_names,
        "supported_image_count": len(supported_names),
        "supported_images": supported_names,
        "renamed_files": rename_map,
        "skipped_files": skipped_unsupported,
    }


@app.get("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library")
async def api_phase4_v1_list_broll_library(branch_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    files = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=_phase4_v1_clean_broll_library(brand_slug, branch_id),
    )
    return BrollCatalogListResponseV1(
        folder_path=str(library_dir),
        folder_label="B-roll Library",
        file_count=len(files),
        files=files,
    ).model_dump()


@app.post("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library/files")
async def api_phase4_v1_add_broll_library_files(
    branch_id: str,
    brand: str = Form(""),
    mode_hint: str = Form("unknown"),
    files: list[UploadFile] = File(...),
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if not bool(config.PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS):
        return JSONResponse(
            {"error": "Local folder ingest is disabled. Enable PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS=true."},
            status_code=400,
        )

    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    if not files:
        return JSONResponse({"error": "No files uploaded."}, status_code=400)

    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    existing = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    used_names = {
        str(row.get("file_name") or "").strip().lower()
        for row in existing
        if str(row.get("file_name") or "").strip()
    }
    added_at = now_iso()
    renamed_files: list[dict[str, str]] = []
    skipped_files: list[dict[str, str]] = []
    index_failed_files: list[dict[str, str]] = []
    new_rows: list[dict[str, Any]] = []
    supported_count = 0
    indexed_count = 0
    normalized_mode_hint = _phase4_v1_normalize_broll_mode_hint(mode_hint)
    ready_analysis_cache = _phase4_v1_storyboard_build_ready_analysis_cache(existing)
    vision_provider = build_vision_scene_provider(_PHASE4_V1_STORYBOARD_FIXED_PROMPT_PROVIDER)

    try:
        for upload in files:
            rel_name = str(upload.filename or "").strip()
            original_name = Path(rel_name).name
            if not original_name:
                try:
                    await upload.close()
                except Exception:
                    pass
                continue

            if not _phase4_v1_storyboard_supported_image(original_name):
                skipped_files.append(
                    {
                        "original_name": original_name,
                        "stored_name": "",
                        "reason": "unsupported_image_type",
                    }
                )
                try:
                    await upload.close()
                except Exception:
                    pass
                continue

            stem = Path(original_name).stem
            suffix = Path(original_name).suffix
            stored_name = original_name
            dedupe_count = 1
            lower_name = stored_name.lower()
            while lower_name in used_names:
                dedupe_count += 1
                stored_name = f"{stem}__dup{dedupe_count}{suffix}"
                lower_name = stored_name.lower()
            used_names.add(lower_name)

            if stored_name != original_name:
                renamed_files.append({"original_name": original_name, "stored_name": stored_name})

            payload = await upload.read()
            target_path = library_dir / stored_name
            target_path.write_bytes(payload)
            payload_checksum = hashlib.sha256(payload).hexdigest().lower()
            supported_count += 1
            metadata = _phase4_v1_normalize_broll_metadata(
                {
                    "library_item_type": "original_upload",
                    "ai_generated": False,
                    "mode_hint": normalized_mode_hint,
                    "tags": [],
                    "usage_count": 0,
                    "source_checksum_sha256": payload_checksum,
                    "indexing_status": "unindexed",
                    "indexing_error": "",
                }
            )
            cached_analysis = ready_analysis_cache.get(payload_checksum, {})
            if cached_analysis:
                metadata = _phase4_v1_storyboard_apply_indexing_metadata(
                    metadata=metadata,
                    analysis=(
                        cached_analysis.get("analysis")
                        if isinstance(cached_analysis.get("analysis"), dict)
                        else {}
                    ),
                    indexing_ok=True,
                    indexing_error="",
                    indexing_provider=str(cached_analysis.get("indexing_provider") or "").strip(),
                    indexing_model_id=str(
                        cached_analysis.get("indexing_model_id") or _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID
                    ).strip(),
                    indexing_input_checksum=str(cached_analysis.get("indexing_input_checksum") or "").strip(),
                )
                indexed_count += 1
            else:
                analysis, index_info = _phase4_v1_storyboard_index_image_analysis(
                    vision_provider=vision_provider,
                    source_path=target_path,
                    model_id=_PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID,
                    idempotency_key=f"index:{brand_slug}:{stored_name}:{payload_checksum}",
                )
                indexing_ok = bool(index_info.get("ok"))
                metadata = _phase4_v1_storyboard_apply_indexing_metadata(
                    metadata=metadata,
                    analysis=analysis if isinstance(analysis, dict) else {},
                    indexing_ok=indexing_ok,
                    indexing_error=str(index_info.get("error") or "").strip(),
                    indexing_provider=str(index_info.get("indexing_provider") or "").strip(),
                    indexing_model_id=str(index_info.get("indexing_model_id") or "").strip(),
                    indexing_input_checksum=str(index_info.get("indexing_input_checksum") or "").strip(),
                )
                if indexing_ok:
                    indexed_count += 1
                    ready_analysis_cache[payload_checksum] = {
                        "analysis": analysis if isinstance(analysis, dict) else {},
                        "indexing_provider": str(index_info.get("indexing_provider") or "").strip(),
                        "indexing_model_id": str(index_info.get("indexing_model_id") or "").strip(),
                        "indexing_input_checksum": str(index_info.get("indexing_input_checksum") or "").strip(),
                    }
                else:
                    index_failed_files.append(
                        {
                            "file_name": stored_name,
                            "error": str(index_info.get("error") or "Image indexing failed.").strip(),
                        }
                    )
            new_rows.append(
                {
                    "file_name": stored_name,
                    "size_bytes": len(payload),
                    "added_at": added_at,
                    "metadata": metadata,
                }
            )

            try:
                await upload.close()
            except Exception:
                pass
    except Exception as exc:
        return JSONResponse(
            {"error": f"Failed to stage B-roll library images: {exc}"},
            status_code=500,
        )

    if not supported_count:
        return JSONResponse(
            {"error": "No supported images found. Upload PNG/JPG/JPEG/WEBP files."},
            status_code=400,
        )

    merged = existing + new_rows
    merged = _phase4_v1_save_broll_library(brand_slug, branch_id, merged)
    enriched = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=merged,
    )

    response_payload = {
        "ok": True,
        "folder_path": str(library_dir),
        "folder_label": "B-roll Library",
        "file_count": len(enriched),
        "files": enriched,
        "added_count": len(new_rows),
        "indexed_count": int(indexed_count or 0),
        "index_failed_count": len(index_failed_files),
        "index_failed_files": index_failed_files,
        "renamed_files": renamed_files,
        "skipped_files": skipped_files,
    }
    return response_payload


@app.delete("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library/files")
async def api_phase4_v1_delete_broll_library_files(branch_id: str, req: BrollCatalogDeleteRequestV1):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    requested_raw = req.file_names if isinstance(req.file_names, list) else []
    requested = []
    requested_set: set[str] = set()
    for name in requested_raw:
        file_name = str(name or "").strip()
        if not file_name:
            continue
        lower_name = file_name.lower()
        if lower_name in requested_set:
            continue
        requested_set.add(lower_name)
        requested.append(file_name)

    if not requested:
        return JSONResponse({"error": "No files provided to remove."}, status_code=400)

    cleaned = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    target_library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    keep_rows: list[dict[str, Any]] = []
    removed_rows: list[dict[str, Any]] = []
    for row in cleaned:
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        if file_name.lower() in requested_set:
            removed_rows.append(row)
            target_path = target_library_dir / file_name
            if target_path.exists():
                try:
                    target_path.unlink(missing_ok=True)
                except TypeError:
                    if target_path.exists():
                        target_path.unlink()
            continue
        keep_rows.append(row)

    removed_lower = {
        str(row.get("file_name") or "").strip().lower()
        for row in removed_rows
    }
    missing = [name for name in requested if name.lower() not in removed_lower]
    remaining = _phase4_v1_save_broll_library(brand_slug, branch_id, keep_rows)
    _phase4_v1_broll_remove_unused_thumbnails(
        brand_slug=brand_slug,
        branch_id=branch_id,
        active_rows=remaining,
    )
    enriched = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=remaining,
    )

    return {
        "ok": True,
        "folder_path": str(target_library_dir),
        "folder_label": "B-roll Library",
        "file_count": len(enriched),
        "files": enriched,
        "removed_file_names": [str(row.get("file_name") or "") for row in removed_rows],
        "missing_file_names": missing,
    }


@app.patch("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library/files/metadata")
async def api_phase4_v1_update_broll_library_file_metadata(
    branch_id: str,
    req: BrollCatalogUpdateMetadataRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    file_name = _phase4_v1_broll_sanitize_file_name(req.file_name)
    if not file_name:
        return JSONResponse({"error": "file_name is required."}, status_code=400)

    rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    row_index = _phase4_v1_broll_build_row_index(rows)
    idx = row_index.get(file_name.lower(), -1)
    if idx < 0:
        return JSONResponse({"error": "B-roll file not found."}, status_code=404)

    row = dict(rows[idx])
    metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
    metadata["tags"] = _phase4_v1_normalize_broll_tags(req.tags)
    row["metadata"] = metadata
    rows[idx] = row
    saved = _phase4_v1_save_broll_library(brand_slug, branch_id, rows)
    enriched = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=saved,
    )
    updated = next(
        (item for item in enriched if str(item.get("file_name") or "").lower() == file_name.lower()),
        None,
    )
    return {
        "ok": True,
        "updated_file": updated,
        "files": enriched,
        "file_count": len(enriched),
    }


@app.post("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library/files/reindex")
async def api_phase4_v1_reindex_broll_library_files(
    branch_id: str,
    req: BrollCatalogDeleteRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    requested = _phase4_v1_storyboard_normalize_selected_files(req.file_names)
    if not requested:
        return JSONResponse({"error": "No files provided to reindex."}, status_code=400)

    rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    row_index = _phase4_v1_broll_build_row_index(rows)
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    ready_analysis_cache = _phase4_v1_storyboard_build_ready_analysis_cache(rows)
    vision_provider = build_vision_scene_provider(_PHASE4_V1_STORYBOARD_FIXED_PROMPT_PROVIDER)
    reindexed: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []
    changed = False

    for file_name in requested:
        lookup = row_index.get(file_name.lower(), -1)
        if lookup < 0:
            failed.append({"file_name": file_name, "error": "File not found in image bank."})
            continue
        row = dict(rows[lookup])
        metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
        target_path = library_dir / str(row.get("file_name") or "")
        if not target_path.exists() or not target_path.is_file():
            failed.append({"file_name": file_name, "error": "File missing on disk."})
            continue
        source_checksum = str(metadata.get("source_checksum_sha256") or "").strip().lower()
        if not _phase4_v1_is_sha256_hex(source_checksum):
            source_checksum = _phase4_v1_broll_checksum_sha256(target_path)
            metadata["source_checksum_sha256"] = source_checksum

        cached_analysis = ready_analysis_cache.get(source_checksum, {})
        if cached_analysis:
            row["metadata"] = _phase4_v1_storyboard_apply_indexing_metadata(
                metadata=metadata,
                analysis=(
                    cached_analysis.get("analysis")
                    if isinstance(cached_analysis.get("analysis"), dict)
                    else {}
                ),
                indexing_ok=True,
                indexing_error="",
                indexing_provider=str(cached_analysis.get("indexing_provider") or "").strip(),
                indexing_model_id=str(
                    cached_analysis.get("indexing_model_id") or _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID
                ).strip(),
                indexing_input_checksum=str(cached_analysis.get("indexing_input_checksum") or "").strip(),
            )
            rows[lookup] = row
            changed = True
            reindexed.append({"file_name": str(row.get("file_name") or ""), "error": ""})
            continue

        analysis, index_info = _phase4_v1_storyboard_index_image_analysis(
            vision_provider=vision_provider,
            source_path=target_path,
            model_id=_PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID,
            idempotency_key=f"reindex:{brand_slug}:{target_path.name}:{source_checksum}",
        )
        if bool(index_info.get("ok")):
            row["metadata"] = _phase4_v1_storyboard_apply_indexing_metadata(
                metadata=metadata,
                analysis=analysis if isinstance(analysis, dict) else {},
                indexing_ok=True,
                indexing_error="",
                indexing_provider=str(index_info.get("indexing_provider") or "").strip(),
                indexing_model_id=str(index_info.get("indexing_model_id") or "").strip(),
                indexing_input_checksum=str(index_info.get("indexing_input_checksum") or "").strip(),
            )
            rows[lookup] = row
            changed = True
            reindexed.append({"file_name": str(row.get("file_name") or ""), "error": ""})
            ready_analysis_cache[source_checksum] = {
                "analysis": analysis if isinstance(analysis, dict) else {},
                "indexing_provider": str(index_info.get("indexing_provider") or "").strip(),
                "indexing_model_id": str(index_info.get("indexing_model_id") or "").strip(),
                "indexing_input_checksum": str(index_info.get("indexing_input_checksum") or "").strip(),
            }
        else:
            # Preserve previous metadata state on failure.
            failed.append(
                {
                    "file_name": str(row.get("file_name") or file_name),
                    "error": str(index_info.get("error") or "Image indexing failed.").strip(),
                }
            )

    if changed:
        rows = _phase4_v1_save_broll_library(brand_slug, branch_id, rows)
    enriched = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=rows,
    )
    return {
        "ok": True,
        "reindexed": reindexed,
        "failed": failed,
        "files": enriched,
        "file_count": len(enriched),
    }


@app.post("/api/branches/{branch_id}/phase4-v1/storyboard/broll-library/files/rename")
async def api_phase4_v1_rename_broll_library_file(
    branch_id: str,
    req: BrollCatalogRenameRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    original_name = _phase4_v1_broll_sanitize_file_name(req.file_name)
    if not original_name:
        return JSONResponse({"error": "file_name is required."}, status_code=400)
    try:
        resolved_target_name = _phase4_v1_broll_resolve_rename_target(
            file_name=original_name,
            new_file_name=req.new_file_name,
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    row_index = _phase4_v1_broll_build_row_index(rows)
    source_idx = row_index.get(original_name.lower(), -1)
    if source_idx < 0:
        return JSONResponse({"error": "B-roll file not found."}, status_code=404)
    if (
        resolved_target_name.lower() != original_name.lower()
        and resolved_target_name.lower() in row_index
    ):
        logger.warning(
            "storyboard_broll_rename_collision brand=%s branch=%s source=%s target=%s",
            brand_slug,
            branch_id,
            original_name,
            resolved_target_name,
        )
        return JSONResponse({"error": "Target file name already exists."}, status_code=409)

    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    source_path = library_dir / original_name
    target_path = library_dir / resolved_target_name
    if not source_path.exists() or not source_path.is_file():
        return JSONResponse({"error": "Source file does not exist on disk."}, status_code=404)
    if resolved_target_name.lower() != original_name.lower():
        try:
            source_path.rename(target_path)
        except Exception as exc:
            return JSONResponse({"error": f"Rename failed: {exc}"}, status_code=500)
    row = dict(rows[source_idx])
    row["file_name"] = resolved_target_name
    rows[source_idx] = row
    saved = _phase4_v1_save_broll_library(brand_slug, branch_id, rows)
    _phase4_v1_broll_remove_unused_thumbnails(
        brand_slug=brand_slug,
        branch_id=branch_id,
        active_rows=saved,
    )
    enriched = _phase4_v1_broll_enrich_rows_for_response(
        brand_slug=brand_slug,
        branch_id=branch_id,
        rows=saved,
    )
    renamed_row = next(
        (
            item
            for item in enriched
            if str(item.get("file_name") or "").lower() == resolved_target_name.lower()
        ),
        None,
    )
    return {
        "ok": True,
        "old_file_name": original_name,
        "new_file_name": resolved_target_name,
        "renamed_file": renamed_row,
        "files": enriched,
        "file_count": len(enriched),
    }


@app.patch("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/source-selection")
async def api_phase4_v1_storyboard_source_selection(
    branch_id: str,
    video_run_id: str,
    req: StoryboardSourceSelectionRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    library_rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    source_selection = _phase4_v1_storyboard_resolve_source_selection(
        run_row=run_row,
        rows=library_rows,
        requested_a_roll=req.selected_a_roll_files,
        requested_b_roll=req.selected_b_roll_files,
    )
    selected_a_roll_files = _phase4_v1_storyboard_normalize_selected_files(
        source_selection.get("selected_a_roll_files")
    )
    selected_b_roll_files = _phase4_v1_storyboard_normalize_selected_files(
        source_selection.get("selected_b_roll_files")
    )
    updates = _phase4_v1_storyboard_write_source_selection_metrics(
        video_run_id=video_run_id,
        selected_a_roll_files=selected_a_roll_files,
        selected_b_roll_files=selected_b_roll_files,
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    payload = StoryboardSourceSelectionResponseV1(
        video_run_id=video_run_id,
        selected_a_roll_files=selected_a_roll_files,
        selected_b_roll_files=selected_b_roll_files,
        selectable_a_roll_count=int(source_selection.get("selectable_a_roll_count") or 0),
        selectable_b_roll_count=int(source_selection.get("selectable_b_roll_count") or 0),
        updated_at=str(updates.get("storyboard_selected_updated_at") or ""),
    )
    return payload.model_dump()


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/assign/start")
async def api_phase4_v1_storyboard_assign_start(
    branch_id: str,
    video_run_id: str,
    req: StoryboardAssignStartRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    folder_url = str(req.folder_url or "").strip()
    if folder_url:
        # Backward compatibility only: if provided, keep as run metadata.
        try:
            folder_url = str(_phase4_v1_storyboard_local_folder_path(folder_url))
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
    else:
        folder_url = str(_phase4_v1_broll_library_dir(brand_slug, branch_id))

    library_rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    if not library_rows:
        return JSONResponse(
            {"error": "No images saved in the image bank. Upload A-roll and B-roll images first."},
            status_code=400,
        )

    requested_a_roll = (
        req.selected_a_roll_files if "selected_a_roll_files" in req.model_fields_set else None
    )
    requested_b_roll = (
        req.selected_b_roll_files if "selected_b_roll_files" in req.model_fields_set else None
    )
    source_selection = _phase4_v1_storyboard_resolve_source_selection(
        run_row=run_row,
        rows=library_rows,
        requested_a_roll=requested_a_roll,
        requested_b_roll=requested_b_roll,
    )
    selected_a_roll_files = _phase4_v1_storyboard_normalize_selected_files(
        source_selection.get("selected_a_roll_files")
    )
    selected_b_roll_files = _phase4_v1_storyboard_normalize_selected_files(
        source_selection.get("selected_b_roll_files")
    )
    if not selected_b_roll_files:
        return JSONResponse(
            {"error": "No ready B-roll images selected. Select at least one indexed B-roll image."},
            status_code=400,
        )
    _phase4_v1_storyboard_write_source_selection_metrics(
        video_run_id=video_run_id,
        selected_a_roll_files=selected_a_roll_files,
        selected_b_roll_files=selected_b_roll_files,
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)

    clips = list_video_clips(video_run_id)
    if not clips:
        return JSONResponse({"error": "No clips found for this run."}, status_code=400)
    image_edit_model_id, image_edit_model_label = _phase4_v1_storyboard_resolve_image_edit_model(
        str(req.image_edit_model or "").strip()
    )
    prompt_model_provider = _PHASE4_V1_STORYBOARD_FIXED_PROMPT_PROVIDER
    prompt_model_id = _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_ID
    prompt_model_label = _PHASE4_V1_STORYBOARD_FIXED_PROMPT_MODEL_LABEL

    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    existing_task = phase4_v1_storyboard_assign_tasks.get(task_key)
    if existing_task and not existing_task.done():
        current = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
        )
        return {
            "ok": True,
            "job_id": str(current.get("job_id") or ""),
            "status": "running",
            "image_edit_model_id": image_edit_model_id,
            "image_edit_model_label": image_edit_model_label,
            "prompt_model_provider": prompt_model_provider,
            "prompt_model_id": prompt_model_id,
            "prompt_model_label": prompt_model_label,
            "selected_a_roll_files": selected_a_roll_files,
            "selected_b_roll_files": selected_b_roll_files,
        }

    job_id = f"storyboard_assign_{uuid.uuid4().hex}"
    started_at = now_iso()
    initial = _phase4_v1_storyboard_build_initial_status(video_run_id=video_run_id, clips=clips)
    initial.update(
        {
            "job_id": job_id,
            "status": "running",
            "started_at": started_at,
            "updated_at": started_at,
            "error": "",
        }
    )
    _phase4_v1_storyboard_write_runtime_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        task_key=task_key,
        payload=initial,
    )

    task = phase4_v1_workflow_backend.start_job(
        task_key,
        lambda: _phase4_v1_execute_storyboard_assignment(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            folder_url=folder_url,
            edit_threshold=int(req.edit_threshold or 5),
            low_flag_threshold=int(req.low_flag_threshold or 6),
            image_edit_model_id=image_edit_model_id,
            image_edit_model_label=image_edit_model_label,
            prompt_model_provider=prompt_model_provider,
            prompt_model_id=prompt_model_id,
            prompt_model_label=prompt_model_label,
            selected_a_roll_files=selected_a_roll_files,
            selected_b_roll_files=selected_b_roll_files,
            job_id=job_id,
        ),
    )
    phase4_v1_storyboard_assign_tasks[task_key] = task
    return {
        "ok": True,
        "job_id": job_id,
        "status": "running",
        "image_edit_model_id": image_edit_model_id,
        "image_edit_model_label": image_edit_model_label,
        "prompt_model_provider": prompt_model_provider,
        "prompt_model_id": prompt_model_id,
        "prompt_model_label": prompt_model_label,
        "selected_a_roll_files": selected_a_roll_files,
        "selected_b_roll_files": selected_b_roll_files,
    }


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/assign/status")
async def api_phase4_v1_storyboard_assign_status(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    status_payload = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )

    if (
        not isinstance(status_payload.get("by_scene_line_id"), dict)
        or not status_payload.get("by_scene_line_id")
    ):
        initialized = _phase4_v1_storyboard_build_initial_status(
            video_run_id=video_run_id,
            clips=list_video_clips(video_run_id),
        )
        status_payload.update(
            {
                "by_scene_line_id": initialized.get("by_scene_line_id", {}),
                "totals": initialized.get("totals", {}),
                "updated_at": now_iso(),
            }
        )
        status_payload = _phase4_v1_storyboard_save_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            payload=status_payload,
        )

    task = phase4_v1_storyboard_assign_tasks.get(task_key)
    if task and not task.done():
        status_payload["status"] = "running"
    elif str(status_payload.get("status") or "") == "running":
        status_payload["status"] = "completed" if not str(status_payload.get("error") or "").strip() else "failed"
    status_payload["updated_at"] = str(status_payload.get("updated_at") or now_iso())

    try:
        model = StoryboardAssignStatusV1.model_validate(status_payload)
    except Exception:
        model = StoryboardAssignStatusV1(video_run_id=video_run_id)
    return model.model_dump()


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/versions")
async def api_phase4_v1_storyboard_versions_list(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    versions = _phase4_v1_storyboard_load_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    return {
        "video_run_id": video_run_id,
        "count": len(versions),
        "versions": versions,
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/versions/save")
async def api_phase4_v1_storyboard_versions_save(
    branch_id: str,
    video_run_id: str,
    req: StoryboardSaveVersionRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    detail = _phase4_v1_collect_run_detail(brand_slug, branch_id, video_run_id)
    if not detail:
        return JSONResponse({"error": "Video run detail not found"}, status_code=404)
    clips = detail.get("clips") if isinstance(detail.get("clips"), list) else []
    if not clips:
        return JSONResponse({"error": "No storyboard clips found to save."}, status_code=400)

    assignment = _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    totals = assignment.get("totals") if isinstance(assignment.get("totals"), dict) else {}
    metrics = run_row.get("metrics") if isinstance(run_row.get("metrics"), dict) else {}
    image_edit_model_id = str(metrics.get("storyboard_image_edit_model_id") or "").strip()
    image_edit_model_label = str(metrics.get("storyboard_image_edit_model_label") or "").strip()
    prompt_model_provider = str(metrics.get("storyboard_prompt_model_provider") or "").strip()
    prompt_model_id = str(metrics.get("storyboard_prompt_model_id") or "").strip()
    prompt_model_label = str(metrics.get("storyboard_prompt_model_label") or "").strip()

    created_at = now_iso()
    label = str(req.label or "").strip()
    if not label:
        model_label = image_edit_model_label or image_edit_model_id or "model"
        label = f"{model_label} · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    ordered_clips = sorted(
        [row for row in clips if isinstance(row, dict)],
        key=lambda row: (
            int(row.get("line_index") or 0),
            str(row.get("script_line_id") or ""),
            str(row.get("clip_id") or ""),
        ),
    )
    snapshot_clips: list[dict[str, Any]] = []
    for clip in ordered_clips:
        payload = StoryboardSavedVersionClipV1(
            clip_id=str(clip.get("clip_id") or ""),
            scene_line_id=str(clip.get("scene_line_id") or ""),
            script_line_id=str(clip.get("script_line_id") or ""),
            mode=normalize_phase4_clip_mode(clip.get("mode")),
            narration_line=str(clip.get("narration_line") or clip.get("narration_text") or ""),
            scene_description=str(clip.get("scene_description") or ""),
            start_frame_url=str(clip.get("start_frame_url") or ""),
            start_frame_filename=str(clip.get("start_frame_filename") or ""),
            assignment_status=str(clip.get("assignment_status") or ""),
            assignment_score=_phase4_v1_storyboard_score(clip.get("assignment_score") or 0),
            assignment_note=str(clip.get("assignment_note") or ""),
            transform_prompt=str(clip.get("edit_prompt") or clip.get("transform_prompt") or ""),
            preview_url=str(clip.get("preview_url") or ""),
        ).model_dump()
        snapshot_clips.append(payload)

    version = StoryboardSavedVersionV1(
        version_id=f"sv_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}",
        created_at=created_at,
        label=label,
        image_edit_model_id=image_edit_model_id,
        image_edit_model_label=image_edit_model_label,
        prompt_model_provider=prompt_model_provider,
        prompt_model_id=prompt_model_id,
        prompt_model_label=prompt_model_label,
        totals=_phase4_v1_storyboard_int_dict(totals),
        clips=snapshot_clips,
    ).model_dump()

    existing = _phase4_v1_storyboard_load_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    saved = _phase4_v1_storyboard_save_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        rows=[version, *existing],
    )
    _phase4_v1_storyboard_update_metrics(
        video_run_id=video_run_id,
        updates={
            "storyboard_last_saved_version_id": str(version.get("version_id") or ""),
            "storyboard_saved_version_count": len(saved),
            "storyboard_last_saved_at": created_at,
        },
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "video_run_id": video_run_id,
        "version": version,
        "count": len(saved),
        "versions": saved,
    }


@app.delete("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/versions")
async def api_phase4_v1_storyboard_versions_delete(
    branch_id: str,
    video_run_id: str,
    req: StoryboardDeleteVersionRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    version_id = str(req.version_id or "").strip()
    if not version_id:
        return JSONResponse({"error": "version_id is required."}, status_code=400)

    existing = _phase4_v1_storyboard_load_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    before_count = len(existing)
    kept = [row for row in existing if str(row.get("version_id") or "").strip() != version_id]
    if len(kept) == before_count:
        return JSONResponse({"error": "Saved version not found."}, status_code=404)

    saved = _phase4_v1_storyboard_save_saved_versions(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        rows=kept,
    )
    _phase4_v1_storyboard_update_metrics(
        video_run_id=video_run_id,
        updates={
            "storyboard_saved_version_count": len(saved),
            "storyboard_last_saved_version_id": str(saved[0].get("version_id") or "") if saved else "",
        },
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "video_run_id": video_run_id,
        "deleted_version_id": version_id,
        "count": len(saved),
        "versions": saved,
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/versions/delete")
async def api_phase4_v1_storyboard_versions_delete_post(
    branch_id: str,
    video_run_id: str,
    req: StoryboardDeleteVersionRequestV1,
):
    return await api_phase4_v1_storyboard_versions_delete(
        branch_id=branch_id,
        video_run_id=video_run_id,
        req=req,
    )


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/versions/rename")
async def api_phase4_v1_storyboard_versions_rename(
    branch_id: str,
    video_run_id: str,
    req: StoryboardRenameVersionRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)
    version_id = str(req.version_id or "").strip()
    new_label = str(req.label or "").strip()
    if not version_id:
        return JSONResponse({"error": "version_id is required"}, status_code=400)
    if not new_label:
        return JSONResponse({"error": "label is required"}, status_code=400)
    existing = _phase4_v1_storyboard_load_saved_versions(
        brand_slug=brand_slug, branch_id=branch_id, video_run_id=video_run_id,
    )
    found = False
    for row in existing:
        if str(row.get("version_id") or "").strip() == version_id:
            row["label"] = new_label
            found = True
            break
    if not found:
        return JSONResponse({"error": "Version not found"}, status_code=404)
    saved = _phase4_v1_storyboard_save_saved_versions(
        brand_slug=brand_slug, branch_id=branch_id, video_run_id=video_run_id, rows=existing,
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "video_run_id": video_run_id,
        "version_id": version_id,
        "label": new_label,
        "count": len(saved),
        "versions": saved,
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/scenes/{scene_line_id}/redo")
async def api_phase4_v1_storyboard_scene_redo(
    branch_id: str,
    video_run_id: str,
    scene_line_id: str,
    req: StoryboardSceneRedoRequestV1,
):
    """Redo a single storyboard scene with user guidance.

    The AI decides the best strategy:
    - reedit_current: re-edit the current start frame with new guidance
    - reedit_original: go back to the original source image and re-edit
    - new_image: pick a new image from the bank and edit it
    """
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    guidance = str(req.guidance or "").strip()
    if not guidance:
        return JSONResponse({"error": "Guidance text is required."}, status_code=400)

    strategy = str(req.strategy or "auto").strip()
    clip_id = str(req.clip_id or "").strip()
    mode = normalize_phase4_clip_mode(req.mode or "b_roll")
    source_image_filename = str(req.source_image_filename or "").strip()
    phase3_run_id = str(run_row.get("phase3_run_id") or "").strip()

    # Load current assignment status to get existing scene data
    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    status_payload = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    by_scene = (
        status_payload.get("by_scene_line_id")
        if isinstance(status_payload.get("by_scene_line_id"), dict)
        else {}
    )
    scene_data = by_scene.get(scene_line_id) if isinstance(by_scene, dict) else {}
    if not isinstance(scene_data, dict):
        scene_data = {}
    if not source_image_filename:
        source_image_filename = str(scene_data.get("source_image_filename") or "").strip()

    # Resolve paths
    library_dir = _phase4_v1_broll_library_dir(brand_slug, branch_id)
    run_dir = _phase4_v1_run_dir(brand_slug, branch_id, video_run_id)
    asset_dirs = ensure_phase4_asset_dirs(run_dir)

    # Get current start frame path
    current_start_frame_filename = str(scene_data.get("start_frame_filename") or "").strip()
    current_start_frame_path: Path | None = None
    if current_start_frame_filename:
        current_start_frame_path = _phase4_v1_storyboard_resolve_start_frame_path(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            start_frame_filename=current_start_frame_filename,
        )

    # Get original source image path from library
    original_source_path: Path | None = None
    if source_image_filename:
        candidate = library_dir / source_image_filename
        if candidate.exists() and candidate.is_file():
            original_source_path = candidate

    # Build scene intent
    scene_lookup = _phase4_v1_storyboard_scene_lookup(
        brand_slug=brand_slug,
        branch_id=branch_id,
        phase3_run_id=phase3_run_id,
    ) if phase3_run_id else {}
    lookup = scene_lookup.get(scene_line_id, {})
    narration_line = str(lookup.get("narration_line") or "").strip()
    scene_description = str(lookup.get("scene_description") or "").strip()
    scene_intent = {
        "mode": mode,
        "script_line_id": str(scene_data.get("script_line_id") or ""),
        "narration_line": narration_line,
        "scene_description": scene_description,
    }

    # Load library for potential new image selection
    library_rows = _phase4_v1_clean_broll_library(brand_slug, branch_id)
    analyzed_candidates: list[dict[str, Any]] = []
    for row in library_rows:
        if not isinstance(row, dict):
            continue
        file_name = str(row.get("file_name") or "").strip()
        if not file_name:
            continue
        metadata = _phase4_v1_normalize_broll_metadata(row.get("metadata"))
        if str(metadata.get("indexing_status") or "").strip().lower() != "ready":
            continue
        analysis = metadata.get("analysis")
        if not isinstance(analysis, dict) or not analysis:
            continue
        image_path = library_dir / file_name
        if not image_path.exists() or not image_path.is_file():
            continue
        analyzed_candidates.append({
            "path": image_path,
            "file_name": file_name,
            "analysis": analysis,
            "metadata": metadata,
        })

    # Determine strategy
    strategy_used = strategy
    if strategy == "auto":
        # Use AI to decide the best approach based on guidance
        strategy_used = await asyncio.to_thread(
            _phase4_v1_storyboard_redo_decide_strategy,
            guidance=guidance,
            has_current_frame=bool(current_start_frame_path and current_start_frame_path.exists()),
            has_original_source=bool(original_source_path and original_source_path.exists()),
            has_bank_images=len(analyzed_candidates) > 1,
        )

    # Pick the source image based on strategy
    chosen_path: Path | None = None
    chosen_source_label = ""

    if strategy_used == "reedit_current" and current_start_frame_path and current_start_frame_path.exists():
        chosen_path = current_start_frame_path
        chosen_source_label = "current start frame"
    elif strategy_used == "reedit_original" and original_source_path and original_source_path.exists():
        chosen_path = original_source_path
        chosen_source_label = f"original source ({source_image_filename})"
    elif strategy_used == "new_image" and analyzed_candidates:
        # Score candidates and pick the best one (different from current source)
        scene_keywords = _phase4_v1_storyboard_scene_keywords(scene_intent)
        best_candidate: dict[str, Any] | None = None
        best_score = -9999
        for cand in analyzed_candidates:
            cand_name = str(cand.get("file_name") or "")
            # Skip the current source to ensure variety
            if cand_name.lower() == source_image_filename.lower():
                continue
            retrieval_score, _, _ = _phase4_v1_storyboard_retrieval_score(
                candidate={
                    "analysis": cand.get("analysis", {}),
                    "library_mode_hint": str(cand.get("metadata", {}).get("mode_hint") or "unknown"),
                    "library_tags": _phase4_v1_normalize_broll_tags(cand.get("metadata", {}).get("tags")),
                    "library_usage_count": int(cand.get("metadata", {}).get("usage_count") or 0),
                },
                mode=mode,
                scene_keywords=scene_keywords,
                recent_fingerprints=set(),
            )
            if retrieval_score > best_score:
                best_score = retrieval_score
                best_candidate = cand
        if best_candidate:
            chosen_path = Path(str(best_candidate.get("path") or ""))
            chosen_source_label = f"new bank image ({best_candidate.get('file_name', '')})"
        else:
            # Fallback: no alternative found, use original
            if original_source_path and original_source_path.exists():
                chosen_path = original_source_path
                chosen_source_label = f"original source ({source_image_filename}) [no alternatives]"
                strategy_used = "reedit_original"
    else:
        # Fallback chain
        if current_start_frame_path and current_start_frame_path.exists():
            chosen_path = current_start_frame_path
            chosen_source_label = "current start frame (fallback)"
            strategy_used = "reedit_current"
        elif original_source_path and original_source_path.exists():
            chosen_path = original_source_path
            chosen_source_label = f"original source ({source_image_filename}) (fallback)"
            strategy_used = "reedit_original"

    if not chosen_path or not chosen_path.exists():
        return JSONResponse({"error": "No source image available for redo."}, status_code=400)

    # Build style profile from analyzed images
    style_profile = _phase4_v1_storyboard_style_profile(
        [c.get("analysis") for c in analyzed_candidates if isinstance(c.get("analysis"), dict)]
    )

    # Build edit prompt incorporating user guidance
    edit_prompt_text = _phase4_v1_storyboard_redo_build_prompt(
        guidance=guidance,
        strategy=strategy_used,
        scene_intent=scene_intent,
        style_profile=style_profile,
    )

    # Transform the image
    try:
        _, _, gemini_provider = build_generation_providers()
        timestamp = int(time.time() * 1000)
        redo_clip_id = clip_id or f"redo_{scene_line_id}"
        transformed_path = (
            asset_dirs["transformed_frames"] / f"{redo_clip_id}__redo__{timestamp}.png"
        )
        image_edit_model_id = str(
            (run_row.get("metrics") or {}).get("storyboard_image_edit_model_id")
            or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID
        )
        transformed_result = await asyncio.to_thread(
            gemini_provider.transform_image,
            input_path=chosen_path,
            prompt=edit_prompt_text,
            output_path=transformed_path,
            model_id=image_edit_model_id,
            idempotency_key=f"{video_run_id}:{scene_line_id}:redo:{timestamp}",
        )
        edit_provider_name = str(transformed_result.get("provider") or "").strip()
        edit_model_id = str(transformed_result.get("model_id") or "").strip()

        # Get clip info for deterministic filename
        clips = list_video_clips(video_run_id)
        matching_clip: dict[str, Any] = {}
        for c in clips:
            if str(c.get("scene_line_id") or "") == scene_line_id:
                matching_clip = c
                break
        redo_brief_unit_id = str(matching_clip.get("brief_unit_id") or "").strip()
        redo_hook_id = str(matching_clip.get("hook_id") or "").strip()
        redo_script_line_id = str(matching_clip.get("script_line_id") or scene_data.get("script_line_id") or "").strip()

        # Render the final 9:16 start frame
        target_filename = deterministic_start_frame_filename(
            brief_unit_id=redo_brief_unit_id,
            hook_id=redo_hook_id,
            script_line_id=redo_script_line_id,
            mode=mode,
            ext="png",
        )
        target_path = asset_dirs["start_frames"] / target_filename
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _phase4_v1_storyboard_render_start_frame_9_16(source_path=transformed_path, output_path=target_path)
        target_bytes = target_path.read_bytes()
        target_checksum = hashlib.sha256(target_bytes).hexdigest()

        # Create asset records
        create_video_asset(
            asset_id=f"asset_{uuid.uuid4().hex}",
            video_run_id=video_run_id,
            clip_id=clip_id,
            asset_type="transformed_frame",
            storage_path=str(transformed_path),
            source_url=str(chosen_path),
            file_name=transformed_path.name,
            mime_type="image/png",
            byte_size=int(transformed_result.get("size_bytes") or transformed_path.stat().st_size),
            checksum_sha256=str(transformed_result.get("checksum_sha256") or ""),
            metadata={
                "assignment_stage": "storyboard_redo",
                "prompt": edit_prompt_text,
                "user_guidance": guidance,
                "strategy": strategy_used,
                "source_label": chosen_source_label,
                "provider": edit_provider_name,
            },
        )
        create_video_asset(
            asset_id=f"asset_{uuid.uuid4().hex}",
            video_run_id=video_run_id,
            clip_id=clip_id,
            asset_type="start_frame",
            storage_path=str(target_path),
            source_url=str(chosen_path),
            file_name=target_filename,
            mime_type="image/png",
            byte_size=len(target_bytes),
            checksum_sha256=target_checksum,
            metadata={
                "assignment_stage": "storyboard_redo",
                "scene_line_id": scene_line_id,
                "user_guidance": guidance,
                "strategy": strategy_used,
                "source_label": chosen_source_label,
                "edit_prompt": edit_prompt_text,
                "edit_model_id": edit_model_id,
                "edit_provider": edit_provider_name,
            },
        )

        # Update assignment status for this scene
        _phase4_v1_storyboard_update_scene_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
            task_key=task_key,
            scene_line_id=scene_line_id,
            updates={
                "assignment_status": "assigned",
                "start_frame_url": _phase4_v1_storage_path_to_outputs_url(str(target_path)),
                "start_frame_filename": target_filename,
                "edited": True,
                "edit_prompt": edit_prompt_text,
                "edit_model_id": edit_model_id,
                "edit_provider": edit_provider_name,
                "assignment_note": f"Redo ({strategy_used}): {guidance[:120]}",
            },
        )

        # Update clip revision snapshot if available
        if matching_clip:
            revision = _phase4_v1_get_current_revision_row(matching_clip)
            if isinstance(revision, dict) and revision.get("revision_id"):
                snapshot = (
                    revision.get("input_snapshot")
                    if isinstance(revision.get("input_snapshot"), dict)
                    else {}
                )
                updated_snapshot = dict(snapshot)
                updated_snapshot["start_frame_filename"] = target_filename
                updated_snapshot["start_frame_checksum"] = target_checksum
                if mode == "a_roll":
                    updated_snapshot["avatar_filename"] = target_filename
                    updated_snapshot["avatar_checksum"] = target_checksum
                update_video_clip_revision(
                    str(revision["revision_id"]),
                    input_snapshot=updated_snapshot,
                )

        return {
            "ok": True,
            "video_run_id": video_run_id,
            "scene_line_id": scene_line_id,
            "strategy_used": strategy_used,
            "source_label": chosen_source_label,
            "start_frame_url": _phase4_v1_storage_path_to_outputs_url(str(target_path)),
        }

    except Exception as exc:
        logger.error("Storyboard redo failed for run=%s scene=%s: %s", video_run_id, scene_line_id, exc)
        return JSONResponse(
            {"error": f"Redo failed: {exc}"},
            status_code=500,
        )


def _phase4_v1_storyboard_redo_decide_strategy(
    *,
    guidance: str,
    has_current_frame: bool,
    has_original_source: bool,
    has_bank_images: bool,
) -> str:
    """Decide the best redo strategy based on user guidance text."""
    g = guidance.lower()

    # Keywords suggesting the user wants a completely new/different image
    new_image_signals = [
        "new image", "different image", "different photo", "different picture",
        "pick a new", "choose another", "find another", "something else",
        "different shot", "swap", "replace with", "use another",
        "more energy", "completely different", "try something",
    ]
    for signal in new_image_signals:
        if signal in g:
            return "new_image" if has_bank_images else ("reedit_original" if has_original_source else "reedit_current")

    # Keywords suggesting going back to the original source
    original_signals = [
        "original", "go back", "start over", "from scratch",
        "undo", "reset", "base image", "source image",
        "the raw", "unedited",
    ]
    for signal in original_signals:
        if signal in g:
            return "reedit_original" if has_original_source else "reedit_current"

    # Default: re-edit the current image (most common case for tweaks)
    return "reedit_current" if has_current_frame else ("reedit_original" if has_original_source else "new_image")


def _phase4_v1_storyboard_redo_build_prompt(
    *,
    guidance: str,
    strategy: str,
    scene_intent: dict[str, Any],
    style_profile: dict[str, Any],
) -> str:
    """Build the image edit prompt for a redo, incorporating user guidance."""
    mode = str(scene_intent.get("mode") or "b_roll").strip()
    narration = str(scene_intent.get("narration_line") or "").strip()
    description = str(scene_intent.get("scene_description") or "").strip()

    style_chunks = []
    for key in ("shot_type", "camera_angle", "lighting", "mood", "setting"):
        value = str(style_profile.get(key) or "").strip()
        if value:
            style_chunks.append(f"{key}: {value}")
    tags = style_profile.get("style_tags") if isinstance(style_profile.get("style_tags"), list) else []
    if tags:
        style_chunks.append(f"style tags: {', '.join([str(v) for v in tags[:8]])}")
    style_text = "; ".join(style_chunks)

    strategy_instruction = ""
    if strategy == "reedit_current":
        strategy_instruction = (
            "You are refining an already-edited image. Apply the user's requested changes "
            "while preserving what already works well."
        )
    elif strategy == "reedit_original":
        strategy_instruction = (
            "You are re-editing the original source photograph. Apply the user's requested changes "
            "to create a fresh version from the raw source material."
        )
    elif strategy == "new_image":
        strategy_instruction = (
            "You are editing a brand-new source image chosen from the image bank. "
            "Transform it according to the user's vision and the scene requirements."
        )

    return (
        f"USER DIRECTION: {guidance}\n\n"
        f"{strategy_instruction}\n"
        f"Mode: {mode}\n"
        f"Narration line: {narration}\n"
        f"Scene description: {description}\n"
        f"Style profile to match: {style_text or 'keep current visual style consistent'}.\n"
        "Create a visually distinct variation for this specific scene beat.\n"
        "Preserve identity/product consistency and 9:16 composition.\n"
        "STRICT: Never render any text, words, letters, numbers, captions, subtitles, "
        "watermarks, or logos anywhere in the image — including on screens, signs, "
        "clothing, or surfaces."
    )


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/assign/stop")
async def api_phase4_v1_storyboard_assign_stop(
    branch_id: str,
    video_run_id: str,
    req: StoryboardAssignControlRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    running_task = phase4_v1_storyboard_assign_tasks.get(task_key)
    was_running = bool(running_task and not running_task.done())
    if not was_running:
        current = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
            brand_slug=brand_slug,
            branch_id=branch_id,
            video_run_id=video_run_id,
        )
        current_status = str(current.get("status") or "").strip().lower()
        if current_status == "running":
            by_scene = (
                current.get("by_scene_line_id")
                if isinstance(current.get("by_scene_line_id"), dict)
                else {}
            )
            for scene_line_id, row in by_scene.items():
                if not isinstance(row, dict):
                    continue
                if str(row.get("assignment_status") or "").strip().lower() == "analyzing":
                    row["assignment_status"] = "pending"
                    row["assignment_note"] = "Stopped by user."
                    row["updated_at"] = now_iso()
                    by_scene[scene_line_id] = row
            current["by_scene_line_id"] = by_scene
            current["status"] = "aborted"
            current["updated_at"] = now_iso()
            current["error"] = "Storyboard assignment stopped by user."
            current = _phase4_v1_storyboard_write_runtime_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
                task_key=task_key,
                payload=current,
            )
            update_video_run(
                video_run_id,
                status="active",
                workflow_state="brief_approved",
                error="Storyboard assignment stopped by user.",
            )
            _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        return {
            "ok": True,
            "video_run_id": video_run_id,
            "status": str(current.get("status") or "idle"),
            "was_running": False,
            "job_id": str(current.get("job_id") or ""),
        }

    if running_task and not running_task.done():
        running_task.cancel()
    phase4_v1_storyboard_assign_tasks.pop(task_key, None)
    phase4_v1_workflow_backend.clear_job(task_key)

    status_payload = phase4_v1_storyboard_assign_state.get(task_key) or _phase4_v1_storyboard_load_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
    )
    by_scene = (
        status_payload.get("by_scene_line_id")
        if isinstance(status_payload.get("by_scene_line_id"), dict)
        else {}
    )
    for scene_line_id, row in by_scene.items():
        if not isinstance(row, dict):
            continue
        if str(row.get("assignment_status") or "").strip().lower() == "analyzing":
            row["assignment_status"] = "pending"
            row["assignment_note"] = "Stopped by user."
            row["updated_at"] = now_iso()
            by_scene[scene_line_id] = row
    status_payload["by_scene_line_id"] = by_scene
    status_payload["status"] = "aborted"
    status_payload["updated_at"] = now_iso()
    status_payload["error"] = "Storyboard assignment stopped by user."
    status_payload = _phase4_v1_storyboard_write_runtime_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        task_key=task_key,
        payload=status_payload,
    )

    update_video_run(
        video_run_id,
        status="active",
        workflow_state="brief_approved",
        error="Storyboard assignment stopped by user.",
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "video_run_id": video_run_id,
        "status": "aborted",
        "was_running": was_running,
        "job_id": str(status_payload.get("job_id") or ""),
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/storyboard/assign/reset")
async def api_phase4_v1_storyboard_assign_reset(
    branch_id: str,
    video_run_id: str,
    req: StoryboardAssignControlRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("brand_slug") or "") != brand_slug or str(run_row.get("branch_id") or "") != branch_id:
        return JSONResponse({"error": "Video run does not belong to this branch"}, status_code=404)

    task_key = _phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id)
    running_task = phase4_v1_storyboard_assign_tasks.get(task_key)
    if running_task and not running_task.done():
        return JSONResponse(
            {"error": "Storyboard assignment is running. Stop it first, then reset."},
            status_code=409,
        )

    phase4_v1_storyboard_assign_tasks.pop(task_key, None)
    phase4_v1_workflow_backend.clear_job(task_key)

    clips = list_video_clips(video_run_id)
    reset_payload = _phase4_v1_storyboard_build_initial_status(video_run_id=video_run_id, clips=clips)
    reset_payload.update(
        {
            "job_id": "",
            "status": "idle",
            "started_at": "",
            "updated_at": now_iso(),
            "error": "",
        }
    )
    _phase4_v1_storyboard_write_runtime_status(
        brand_slug=brand_slug,
        branch_id=branch_id,
        video_run_id=video_run_id,
        task_key=task_key,
        payload=reset_payload,
    )

    cleared_revisions = _phase4_v1_storyboard_clear_clip_start_frames(video_run_id)
    _phase4_v1_storyboard_update_metrics(
        video_run_id=video_run_id,
        updates={
            "storyboard_assignment_job_id": "",
            "image_bank_count": 0,
            "assignment_completed_count": 0,
            "assignment_failed_count": 0,
        },
    )
    update_video_run(
        video_run_id,
        status="active",
        workflow_state="brief_approved",
        drive_folder_url="",
        error="",
    )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "video_run_id": video_run_id,
        "status": "reset",
        "cleared_revisions": int(cleared_revisions),
        "clip_count": len(clips),
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/drive/validate")
async def api_phase4_v1_validate_drive(
    branch_id: str,
    video_run_id: str,
    req: DriveValidateRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    if str(run_row.get("workflow_state") or "") not in {
        "brief_approved",
        "validation_failed",
        "assets_validated",
        "review_pending",
    }:
        return JSONResponse(
            {
                "error": (
                    "Start frame brief must be approved before validating drive assets."
                )
            },
            status_code=409,
        )
    brief = _phase4_v1_load_brief(brand_slug, branch_id, video_run_id)
    if not brief:
        return JSONResponse({"error": "Start frame brief missing."}, status_code=400)

    update_video_run(video_run_id, status="active", workflow_state="validating_assets", error="")
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    try:
        drive_client = build_drive_client_for_folder(req.folder_url)
        drive_assets = drive_client.list_assets(req.folder_url)
    except Exception as exc:
        update_video_run(video_run_id, status="active", workflow_state="validation_failed", error=str(exc))
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        return JSONResponse({"error": f"Drive validation failed: {exc}"}, status_code=400)

    report = validate_drive_assets(
        video_run_id=video_run_id,
        folder_url=req.folder_url,
        brief=brief,
        drive_assets=drive_assets,
    )
    save_video_validation_report(
        report_id=report.report_id,
        video_run_id=video_run_id,
        status=report.status,
        folder_url=req.folder_url,
        summary=report.model_dump(exclude={"items"}),
        items=[row.model_dump() for row in report.items],
    )
    _phase4_v1_write_json(
        _phase4_v1_drive_validation_report_path(brand_slug, branch_id, video_run_id),
        report.model_dump(),
    )

    if report.status == "passed":
        for item in report.items:
            if item.status != "ok" or item.matched_asset is None:
                continue
            try:
                _phase4_v1_copy_start_frame_asset(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    video_run_id=video_run_id,
                    matched_asset=item.matched_asset.model_dump(),
                    drive_client=drive_client,
                )
            except Exception:
                logger.exception("Failed to ingest validated start frame: %s", item.filename)
        update_video_run(
            video_run_id,
            status="active",
            workflow_state="assets_validated",
            drive_folder_url=req.folder_url,
            error="",
        )
    else:
        update_video_run(
            video_run_id,
            status="active",
            workflow_state="validation_failed",
            drive_folder_url=req.folder_url,
            error="Validation report failed.",
        )
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return report.model_dump()


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/drive/validation")
async def api_phase4_v1_get_drive_validation(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    report = _phase4_v1_validation_report_model(video_run_id)
    if not report:
        return JSONResponse({"error": "Validation report not found"}, status_code=404)
    return report.model_dump()


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/generation/start")
async def api_phase4_v1_start_generation(
    branch_id: str,
    video_run_id: str,
    req: StartGenerationRequestV1,
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)

    workflow_state = str(run_row.get("workflow_state") or "")
    if workflow_state not in {"assets_validated", "review_pending", "failed"}:
        return JSONResponse(
            {
                "error": (
                    "Generation can only start after assets are validated."
                )
            },
            status_code=409,
        )

    if _phase4_v1_all_clips_approved(video_run_id) and _phase4_v1_all_latest_revisions_complete(video_run_id):
        completed_at = str(run_row.get("completed_at") or "").strip() or now_iso()
        update_video_run(
            video_run_id,
            status="completed",
            workflow_state="completed",
            completed_at=completed_at,
            error="",
        )
        _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
        return {
            "ok": True,
            "status": "already_completed",
            "video_run_id": video_run_id,
            "message": "All clips are already approved.",
        }

    run_key = _phase4_v1_run_key(brand_slug, branch_id, video_run_id)
    existing = phase4_v1_generation_tasks.get(run_key)
    if existing and not existing.done():
        return JSONResponse({"error": "Generation is already running for this video run."}, status_code=409)

    update_video_run(video_run_id, status="active", workflow_state="generating", error="")
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    task = phase4_v1_workflow_backend.start_job(
        run_key,
        lambda: _phase4_v1_execute_generation(brand_slug, branch_id, video_run_id),
    )
    phase4_v1_generation_tasks[run_key] = task
    return {"ok": True, "status": "started", "video_run_id": video_run_id}


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/clips")
async def api_phase4_v1_list_clips(branch_id: str, video_run_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    detail = _phase4_v1_collect_run_detail(brand_slug, branch_id, video_run_id)
    if not detail:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    return {"clips": detail.get("clips", [])}


@app.get("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/clips/{clip_id}")
async def api_phase4_v1_clip_detail(branch_id: str, video_run_id: str, clip_id: str, brand: str = ""):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    clip_row = get_video_clip(clip_id)
    if not clip_row or str(clip_row.get("video_run_id")) != video_run_id:
        return JSONResponse({"error": "Clip not found"}, status_code=404)
    revisions = list_video_clip_revisions(clip_id)
    assets = list_video_assets(video_run_id, clip_id=clip_id)
    calls = list_video_provider_calls(video_run_id, clip_id=clip_id)
    payload = ClipHistoryResponseV1(
        clip=clip_row,  # type: ignore[arg-type]
        revisions=revisions,  # type: ignore[arg-type]
        assets=assets,
        provider_calls=calls,  # type: ignore[arg-type]
    )
    return payload.model_dump()


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/clips/{clip_id}/review")
async def api_phase4_v1_clip_review(
    branch_id: str,
    video_run_id: str,
    clip_id: str,
    req: ReviewDecisionRequestV1,
    brand: str = "",
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    clip_row = get_video_clip(clip_id)
    if not clip_row or str(clip_row.get("video_run_id")) != video_run_id:
        return JSONResponse({"error": "Clip not found"}, status_code=404)
    revision = _phase4_v1_get_current_revision_row(clip_row)
    if not revision:
        return JSONResponse({"error": "Clip revision not found"}, status_code=404)
    revision_id = str(revision.get("revision_id") or "")
    provenance = revision.get("provenance") if isinstance(revision.get("provenance"), dict) else {}
    completeness = int(provenance.get("completeness_pct") or 0)

    if req.decision == "approve":
        if completeness < 100:
            return JSONResponse(
                {"error": "Cannot approve clip because provenance completeness is below 100%."},
                status_code=409,
            )
        update_video_clip_revision(
            revision_id,
            status="approved",
            operator_note=str(req.note or ""),
        )
        update_video_clip(clip_id, status="approved")
    else:
        update_video_clip_revision(
            revision_id,
            status="needs_revision",
            operator_note=str(req.note or ""),
        )
        update_video_clip(clip_id, status="needs_revision")

    create_video_operator_action(
        action_id=f"act_{uuid.uuid4().hex}",
        video_run_id=video_run_id,
        clip_id=clip_id,
        revision_id=revision_id,
        action_type=f"review_{req.decision}",
        actor=str(req.reviewer_id or ""),
        payload={"decision": req.decision, "note": req.note or ""},
    )

    queue = _phase4_v1_refresh_review_queue_artifact(brand_slug, branch_id, video_run_id)
    if _phase4_v1_all_clips_approved(video_run_id) and _phase4_v1_all_latest_revisions_complete(video_run_id):
        completed_at = now_iso()
        update_video_run(
            video_run_id,
            status="completed",
            workflow_state="completed",
            completed_at=completed_at,
            error="",
        )
        audit_pack = {
            "video_run_id": video_run_id,
            "completed_at": completed_at,
            "run": get_video_run(video_run_id),
            "clips": list_video_clips(video_run_id),
            "actions": list_video_operator_actions(video_run_id),
            "validation_report": _phase4_v1_validation_report_model(video_run_id).model_dump()
            if _phase4_v1_validation_report_model(video_run_id)
            else {},
        }
        _phase4_v1_write_json(_phase4_v1_audit_pack_path(brand_slug, branch_id, video_run_id), audit_pack)
    else:
        if str(get_video_run(video_run_id).get("status") or "") != "failed":
            update_video_run(
                video_run_id,
                status="active",
                workflow_state="review_pending",
            )

    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "clip_id": clip_id,
        "decision": req.decision,
        "review_queue_count": len(queue),
        "run": get_video_run(video_run_id),
    }


@app.post("/api/branches/{branch_id}/phase4-v1/runs/{video_run_id}/clips/{clip_id}/revise")
async def api_phase4_v1_clip_revise(
    branch_id: str,
    video_run_id: str,
    clip_id: str,
    req: ReviseClipRequestV1,
    brand: str = "",
):
    err = _phase4_v1_disabled_error()
    if err:
        return JSONResponse({"error": err}, status_code=400)
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    run_row = get_video_run(video_run_id)
    if not run_row:
        return JSONResponse({"error": "Video run not found"}, status_code=404)
    clip_row = get_video_clip(clip_id)
    if not clip_row or str(clip_row.get("video_run_id")) != video_run_id:
        return JSONResponse({"error": "Clip not found"}, status_code=404)
    current_revision = _phase4_v1_get_current_revision_row(clip_row)
    if not current_revision:
        return JSONResponse({"error": "Clip revision not found"}, status_code=404)

    previous_revision_id = str(current_revision.get("revision_id") or "")
    previous_index = int(current_revision.get("revision_index") or 1)
    snapshot = current_revision.get("input_snapshot") if isinstance(current_revision.get("input_snapshot"), dict) else {}
    if str(req.transform_prompt or "").strip():
        snapshot["transform_prompt"] = str(req.transform_prompt or "").strip()
    if str(req.a_roll_avatar_override_filename or "").strip():
        snapshot["avatar_filename"] = str(req.a_roll_avatar_override_filename or "").strip()

    new_revision_index = previous_index + 1
    new_revision = create_video_clip_revision(
        revision_id=f"rev_{uuid.uuid4().hex}",
        video_run_id=video_run_id,
        clip_id=clip_id,
        revision_index=new_revision_index,
        status="pending",
        created_by=str(req.reviewer_id or ""),
        operator_note=str(req.note or ""),
        input_snapshot=snapshot,
        provenance={},
        qc_report={},
    )
    update_video_clip(
        clip_id,
        status="pending",
        current_revision_index=new_revision_index,
    )
    if previous_revision_id:
        update_video_clip_revision(
            previous_revision_id,
            status="needs_revision",
        )
    update_video_run(
        video_run_id,
        status="active",
        workflow_state="assets_validated",
        error="",
    )
    create_video_operator_action(
        action_id=f"act_{uuid.uuid4().hex}",
        video_run_id=video_run_id,
        clip_id=clip_id,
        revision_id=str(new_revision.get("revision_id") or ""),
        action_type="revise",
        actor=str(req.reviewer_id or ""),
        payload={
            "note": req.note or "",
            "transform_prompt": req.transform_prompt or "",
            "a_roll_avatar_override_filename": req.a_roll_avatar_override_filename or "",
        },
    )
    queue = _phase4_v1_refresh_review_queue_artifact(brand_slug, branch_id, video_run_id)
    _phase4_v1_update_run_manifest_mirror(brand_slug, branch_id, video_run_id)
    return {
        "ok": True,
        "clip_id": clip_id,
        "new_revision": new_revision,
        "review_queue_count": len(queue),
    }


async def run_branch_pipeline(
    branch_id: str,
    phases: list[int],
    inputs: dict,
    model_overrides: dict | None = None,
    brand_slug: str | None = None,
):
    """Execute Phase 2+ for a specific branch, saving outputs to the branch directory."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["abort_requested"] = False
    pipeline_state["model_overrides"] = _normalize_model_overrides(model_overrides)
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []
    pipeline_state["active_branch"] = branch_id
    pipeline_state["active_brand_slug"] = brand_slug
    pipeline_state["copywriter_failed_jobs"] = []
    pipeline_state["copywriter_parallel_context"] = None
    pipeline_state["copywriter_rewrite_in_progress"] = False
    pipeline_state["selected_concepts"] = []

    # Keep live terminal scoped to this branch run only.
    _reset_server_log_stream()

    reset_usage()
    clear_usage_context()

    output_dir = _branch_output_dir(brand_slug, branch_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a DB run record
    run_id = create_run(phases, inputs, brand_slug=brand_slug)
    pipeline_state["run_id"] = run_id

    _update_branch(branch_id, {"status": "running", "completed_agents": [], "failed_agents": []}, brand_slug)

    start_cost = _persist_run_cost_snapshot(run_id)
    await broadcast({
        "type": "pipeline_start",
        "phases": phases,
        "run_id": run_id,
        "branch_id": branch_id,
        "brand_slug": brand_slug,
        "cost": start_cost,
    })

    try:
        # Phase 2 — Matrix planning (Matrix Planner)
        if 2 in phases:
            phase2_disabled = _phase2_disabled_error()
            if phase2_disabled:
                _add_log(f"Phase 2 blocked — {phase2_disabled}", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["creative_engine"]}, brand_slug)
                await broadcast({"type": "pipeline_error", "message": phase2_disabled, "branch_id": branch_id})
                return

            pipeline_state["current_phase"] = 2
            _add_log(f"═══ PHASE 2 — MATRIX PLANNING (Branch: {_get_branch(branch_id, brand_slug)['label']}) ═══")
            await broadcast({"type": "phase_start", "phase": 2, "branch_id": branch_id})

            # Always load/validate Phase 1 from the shared output directory
            foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
            if foundation_err:
                _add_log(f"Phase 2 blocked — {foundation_err}", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["creative_engine"]}, brand_slug)
                await broadcast({"type": "pipeline_error", "message": foundation_err, "branch_id": branch_id})
                return

            # Use branch-specific temperature for Creative Engine (if set)
            branch_data = _get_branch(branch_id, brand_slug)
            branch_temp = branch_data.get("temperature") if branch_data else None

            r02 = await _run_single_agent_async("creative_engine", inputs, loop, run_id, output_dir=output_dir, temperature=branch_temp)
            if not r02:
                _add_log("Phase 2 failed — Creative Engine is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["creative_engine"]}, brand_slug)
                return
            inputs["idea_brief"] = r02

            branch_completed = ["creative_engine"]
            _update_branch(branch_id, {"completed_agents": branch_completed}, brand_slug)

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Creative Engine → before Copywriter (with concept selection) ---
        if 2 in phases and 3 in phases:
            await _wait_for_agent_gate("creative_engine", "copywriter", "Copywriter", show_concept_selection=True, phase=2)

        # Phase 3 — Scripting (one agent at a time with gates)
        if 3 in phases:
            phase3_disabled = _phase3_disabled_error()
            if phase3_disabled:
                _add_log(f"Phase 3 blocked — {phase3_disabled}", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["copywriter", "hook_specialist"]}, brand_slug)
                await broadcast({"type": "pipeline_error", "message": phase3_disabled, "branch_id": branch_id})
                return

            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3, "branch_id": branch_id})

            # Load upstream: Phase 1 from shared, Phase 2 from branch
            _auto_load_upstream(inputs, ["foundation_brief"], sync_foundation_identity=True, brand_slug=brand_slug)
            if "idea_brief" not in inputs or inputs["idea_brief"] is None:
                data = _load_branch_output(brand_slug, branch_id, "creative_engine")
                if data:
                    inputs["idea_brief"] = data

            selected = pipeline_state.get("selected_concepts", [])
            if selected and inputs.get("idea_brief"):
                inputs["selected_concepts"] = selected
                _add_log(f"User selected {len(selected)} video concepts")

            # --- Copywriter ---
            r04 = await _run_copywriter_parallel_async(inputs, loop, run_id, output_dir=output_dir)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": pipeline_state["failed_agents"]}, brand_slug)
                return
            inputs["copywriter_brief"] = r04

            # --- GATE: After Copywriter → before Hook Specialist ---
            await _wait_for_agent_gate("copywriter", "hook_specialist", "Hook Specialist", phase=3)

            # --- Hook Specialist ---
            r05 = await _run_single_agent_async("hook_specialist", inputs, loop, run_id, output_dir=output_dir)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": pipeline_state["failed_agents"]}, brand_slug)
                return
            inputs["hook_brief"] = r05

        total = time.time() - pipeline_state["start_time"]
        complete_run(run_id, total)
        final_cost = _persist_run_cost_snapshot(run_id)
        cost_str = f"${final_cost['total_cost']:.2f}" if final_cost['total_cost'] >= 0.01 else f"${final_cost['total_cost']:.4f}"
        _add_log(f"Branch pipeline complete in {total:.1f}s — total cost: {cost_str}", "success")

        _update_branch(branch_id, {
            "status": "completed",
            "completed_agents": pipeline_state["completed_agents"],
        }, brand_slug)

        await broadcast({
            "type": "pipeline_complete",
            "elapsed": round(total, 1),
            "run_id": run_id,
            "cost": final_cost,
            "branch_id": branch_id,
            "brand_slug": brand_slug,
        })

    except (PipelineAborted, asyncio.CancelledError):
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        _update_branch(branch_id, {"status": "failed"}, brand_slug)
        abort_cost = _persist_run_cost_snapshot(run_id)
        _add_log(f"Branch pipeline aborted — cost so far: ${abort_cost['total_cost']:.4f}", "warning")
        try:
            await asyncio.shield(broadcast({
                "type": "pipeline_error",
                "message": "Pipeline aborted by user",
                "aborted": True,
                "cost": abort_cost,
                "branch_id": branch_id,
            }))
        except asyncio.CancelledError:
            pass
    except Exception as e:
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        _update_branch(branch_id, {"status": "failed"}, brand_slug)
        err_cost = _persist_run_cost_snapshot(run_id)
        _add_log(f"Branch pipeline error: {e}", "error")
        logger.exception("Branch pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost, "branch_id": branch_id})
    finally:
        clear_usage_context()
        pipeline_state["running"] = False
        pipeline_state["abort_requested"] = False
        pipeline_state["pipeline_task"] = None
        pipeline_state["current_phase"] = None
        pipeline_state["current_agent"] = None
        pipeline_state["run_id"] = None
        pipeline_state["active_branch"] = None
        pipeline_state["copywriter_rewrite_in_progress"] = False
        pipeline_state["gate_info"] = None
        pipeline_state["waiting_for_approval"] = False
        pipeline_state["phase_gate"] = None


class ContinueRequest(BaseModel):
    model_override: dict = {}  # {"provider": "openai", "model": "gpt-5.2"}


@app.post("/api/continue")
async def api_continue(req: ContinueRequest = None):
    """Approve the current phase gate and continue to the next agent."""
    if req is None:
        req = ContinueRequest()
    if pipeline_state.get("copywriter_rewrite_in_progress"):
        return JSONResponse(
            {"error": "Rewrite is in progress. Wait for it to finish before continuing."},
            status_code=409,
        )
    if not pipeline_state["waiting_for_approval"]:
        return JSONResponse(
            {"error": "Pipeline is not waiting for approval"}, status_code=409
        )

    # Store the model override for the next agent
    if req.model_override:
        pipeline_state["next_agent_override"] = req.model_override
        logger.info("Continue with model override: %s", req.model_override)

    gate = pipeline_state.get("phase_gate")
    if gate:
        gate.set()
    return {"status": "continued"}


class RewriteFailedCopywriterRequest(BaseModel):
    model_override: dict = {}  # optional: {"provider": "...", "model": "..."}


@app.post("/api/rewrite-failed-copywriter")
async def api_rewrite_failed_copywriter(req: RewriteFailedCopywriterRequest = None):
    """Retry only failed parallel Copywriter jobs while paused at the phase gate."""
    if req is None:
        req = RewriteFailedCopywriterRequest()

    phase3_disabled = _phase3_disabled_error()
    if phase3_disabled:
        return JSONResponse({"error": phase3_disabled}, status_code=400)

    if not pipeline_state.get("waiting_for_approval"):
        return JSONResponse(
            {"error": "Pipeline is not paused at a phase gate."},
            status_code=409,
        )

    if pipeline_state.get("copywriter_rewrite_in_progress"):
        return JSONResponse(
            {"error": "Rewrite already in progress."},
            status_code=409,
        )

    failed_jobs = list(pipeline_state.get("copywriter_failed_jobs") or [])
    if not failed_jobs:
        return {"status": "nothing_to_rewrite", "rewritten": 0, "remaining_failed": 0}

    ctx = pipeline_state.get("copywriter_parallel_context") or {}
    base_inputs = dict(ctx.get("base_inputs") or {})
    brand_slug = pipeline_state.get("active_brand_slug") or ""
    if not base_inputs.get("foundation_brief"):
        _auto_load_upstream(base_inputs, ["foundation_brief"], sync_foundation_identity=True, brand_slug=brand_slug)

    base_output_dir = Path(ctx.get("output_dir") or (_brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR))
    run_id = int(ctx.get("run_id") or pipeline_state.get("run_id") or 0)
    jobs_dir_raw = ctx.get("jobs_dir")
    if jobs_dir_raw:
        jobs_dir = Path(str(jobs_dir_raw))
    else:
        new_jobs_dir = base_output_dir / "copywriter_jobs" / f"run_{run_id}"
        old_jobs_dir = base_output_dir / "agent_04_jobs" / f"run_{run_id}"
        jobs_dir = old_jobs_dir if old_jobs_dir.exists() and not new_jobs_dir.exists() else new_jobs_dir

    provider = str(ctx.get("provider") or config.get_agent_llm_config("copywriter")["provider"])
    model = str(ctx.get("model") or config.get_agent_llm_config("copywriter")["model"])

    if req.model_override:
        provider = req.model_override.get("provider", provider)
        model = req.model_override.get("model", model)

    loop = asyncio.get_event_loop()
    started = time.time()
    pipeline_state["copywriter_rewrite_in_progress"] = True
    pipeline_state["current_agent"] = "copywriter"
    set_usage_context(agent_name="copywriter", phase=str(pipeline_state.get("current_phase", "")))

    model_label = _friendly_model_label(model)
    _add_log(
        f"Retrying {len(failed_jobs)} failed Copywriter scripts [{model_label}]...",
        "info",
    )
    await broadcast({
        "type": "agent_start",
        "slug": "copywriter",
        "name": AGENT_META["copywriter"]["name"],
        "model": model_label,
        "provider": provider,
        "cost": _running_cost_summary(),
    })

    try:
        successes, remaining_failures = await _run_copywriter_jobs_parallel(
            jobs=failed_jobs,
            base_inputs=base_inputs,
            loop=loop,
            provider=provider,
            model=model,
            jobs_dir=jobs_dir,
            max_parallel=4,
        )

        new_scripts = [s["script"] for s in successes]
        out_path = _output_write_path(base_output_dir, "copywriter")
        existing_output = _load_output_from_base(base_output_dir, "copywriter") or {}
        existing_scripts: list[dict[str, Any]] = []
        data_scripts = existing_output.get("scripts", [])
        if isinstance(data_scripts, list):
            existing_scripts = [s for s in data_scripts if isinstance(s, dict)]

        merged_scripts = existing_scripts + new_scripts
        merged_inputs = {
            "brand_name": existing_output.get("brand_name") or base_inputs.get("brand_name"),
            "product_name": existing_output.get("product_name") or base_inputs.get("product_name"),
            "batch_id": existing_output.get("batch_id") or base_inputs.get("batch_id"),
        }
        merged_output = _build_copywriter_output(merged_inputs, merged_scripts)
        out_path.write_text(json.dumps(merged_output, indent=2), encoding="utf-8")
        logger.info("Output saved: %s", out_path)

        elapsed = time.time() - started
        pipeline_state["copywriter_failed_jobs"] = remaining_failures
        pipeline_state["copywriter_parallel_context"] = {
            **ctx,
            "provider": provider,
            "model": model,
            "output_dir": str(base_output_dir),
            "jobs_dir": str(jobs_dir),
            "run_id": run_id,
            "base_inputs": base_inputs,
        }

        if run_id:
            save_agent_output(
                run_id=run_id,
                agent_slug="copywriter",
                agent_name=AGENT_META["copywriter"]["name"],
                output=merged_output,
                elapsed=elapsed,
            )

        cost = _running_cost_summary()
        if run_id:
            update_run_cost(run_id, float(cost.get("total_cost", 0.0) or 0.0))
        rewritten = len(new_scripts)
        remaining = len(remaining_failures)
        _add_log(
            f"Copywriter rewrite finished — {rewritten} recovered, {remaining} still failed.",
            "success" if remaining == 0 else "warning",
        )
        await broadcast({
            "type": "agent_complete",
            "slug": "copywriter",
            "name": AGENT_META["copywriter"]["name"],
            "elapsed": round(elapsed, 1),
            "cost": cost,
            "parallel_jobs": len(failed_jobs),
            "failed_jobs": remaining,
        })
        return {
            "status": "completed",
            "rewritten": rewritten,
            "remaining_failed": remaining,
            "cost": cost,
        }
    except Exception as e:
        logger.exception("Failed rewriting copywriter jobs")
        await broadcast({
            "type": "agent_error",
            "slug": "copywriter",
            "name": AGENT_META["copywriter"]["name"],
            "error": str(e),
            "elapsed": round(time.time() - started, 1),
        })
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        pipeline_state["copywriter_rewrite_in_progress"] = False
        pipeline_state["current_agent"] = None


@app.get("/api/status")
async def api_status(brand: str = ""):
    """Get current pipeline status."""
    brand_slug = str(brand or pipeline_state.get("active_brand_slug") or "").strip()
    cost_summary = _current_cost_summary_for_status(brand_slug=brand_slug)
    return {
        "running": pipeline_state["running"],
        "current_phase": pipeline_state["current_phase"],
        "current_agent": pipeline_state["current_agent"],
        "completed_agents": pipeline_state["completed_agents"],
        "failed_agents": pipeline_state["failed_agents"],
        "elapsed": (
            round(time.time() - pipeline_state["start_time"], 1)
            if pipeline_state["start_time"] and pipeline_state["running"]
            else None
        ),
        "log": pipeline_state["log"][-50:],
        "server_log_tail": list(_recent_server_logs)[-150:],
        "active_brand_slug": pipeline_state.get("active_brand_slug"),
        "waiting_for_approval": pipeline_state.get("waiting_for_approval", False),
        "gate_info": pipeline_state.get("gate_info"),
        "cost": cost_summary,
    }


@app.get("/api/costs")
async def api_costs(brand: str = ""):
    """Return detailed cost breakdown for the current/latest run."""
    log = get_usage_log()

    total_cost = 0.0
    by_provider: dict[str, dict] = {}
    by_model: dict[str, dict] = {}
    by_agent: dict[str, dict] = {}
    by_phase: dict[str, dict] = {}

    for e in log:
        c = float(e.get("cost", 0) or 0)
        total_cost += c
        prov = e.get("provider", "unknown")
        model = e.get("model", "unknown")
        agent = e.get("agent_name", "unattributed")
        phase = e.get("phase", "unknown") or "unknown"
        in_t = int(e.get("input_tokens", 0) or 0)
        out_t = int(e.get("output_tokens", 0) or 0)

        if prov not in by_provider:
            by_provider[prov] = {"cost": 0.0, "calls": 0, "input_tokens": 0, "output_tokens": 0}
        by_provider[prov]["cost"] += c
        by_provider[prov]["calls"] += 1
        by_provider[prov]["input_tokens"] += in_t
        by_provider[prov]["output_tokens"] += out_t

        if model not in by_model:
            by_model[model] = {"provider": prov, "cost": 0.0, "calls": 0, "input_tokens": 0, "output_tokens": 0}
        by_model[model]["cost"] += c
        by_model[model]["calls"] += 1
        by_model[model]["input_tokens"] += in_t
        by_model[model]["output_tokens"] += out_t

        if agent not in by_agent:
            by_agent[agent] = {"cost": 0.0, "calls": 0}
        by_agent[agent]["cost"] += c
        by_agent[agent]["calls"] += 1

        if phase not in by_phase:
            by_phase[phase] = {"cost": 0.0, "calls": 0}
        by_phase[phase]["cost"] += c
        by_phase[phase]["calls"] += 1

    # Round all costs
    for bucket in (by_provider, by_model, by_agent, by_phase):
        for v in bucket.values():
            v["cost"] = round(v["cost"], 6)

    return {
        "total_cost": round(total_cost, 6),
        "total_calls": len(log),
        "by_provider": by_provider,
        "by_model": by_model,
        "by_agent": by_agent,
        "by_phase": by_phase,
        "entries": log,
    }


@app.get("/api/costs/history")
async def api_costs_history(brand: str = "", limit: int = 50):
    """Return cost history for past pipeline runs."""
    runs = list_runs(limit=limit)
    if brand:
        runs = [r for r in runs if r.get("brand_slug") == brand]
    return {
        "runs": [
            {
                "run_id": r["id"],
                "created_at": r.get("created_at", ""),
                "brand_slug": r.get("brand_slug", ""),
                "brand_name": r.get("brand_name", ""),
                "status": r.get("status", ""),
                "total_cost_usd": r.get("total_cost_usd", 0),
                "elapsed_seconds": r.get("elapsed_seconds"),
            }
            for r in runs
        ],
    }


@app.post("/api/server-log/clear")
async def api_clear_server_log():
    """Clear buffered live server log tail/queue for the current UI session."""
    _reset_server_log_stream()
    return {"ok": True}


@app.get("/api/outputs")
async def api_list_outputs(brand: str = ""):
    """List all available agent outputs (from disk — brand-scoped)."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    outputs = []
    for slug in AGENT_META:
        path = _output_write_path(base, slug)
        legacy_path = None
        if not path.exists():
            legacy = CANONICAL_TO_LEGACY_SLUG.get(slug)
            if legacy:
                legacy_path = base / f"{legacy}_output.json"
        meta = AGENT_META[slug]
        exists = path.exists() or bool(legacy_path and legacy_path.exists())
        stat_path = path if path.exists() else legacy_path
        entry = {
            "slug": slug,
            "name": meta["name"],
            "phase": meta["phase"],
            "icon": meta["icon"],
            "available": exists,
        }
        if exists and stat_path:
            stat = stat_path.stat()
            entry["size_kb"] = round(stat.st_size / 1024, 1)
            entry["modified"] = datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        outputs.append(entry)
    return outputs


@app.get("/api/outputs/{slug}")
async def api_get_output(slug: str, brand: str = ""):
    """Get a specific agent's output (from disk — brand-scoped)."""
    slug = _canonical_slug(slug)
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    data = _load_output(slug, brand_slug=brand_slug)
    if not data:
        return JSONResponse({"error": f"No output for {slug}"}, status_code=404)
    meta = AGENT_META.get(slug, {"name": slug, "phase": 0, "icon": ""})
    return {
        "slug": slug,
        "name": meta["name"],
        "phase": meta["phase"],
        "data": data,
    }


@app.get("/api/outputs/foundation_research/collectors")
async def api_get_foundation_collectors_snapshot(brand: str = ""):
    """Get Phase 1 Step 1 collectors snapshot (separate from final Step 2 output)."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    path = base / "foundation_research_collectors_snapshot.json"
    if not path.exists():
        return JSONResponse({"error": "No collectors snapshot found"}, status_code=404)
    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception:
        return JSONResponse({"error": "Collectors snapshot unreadable"}, status_code=500)
    return {
        "slug": "foundation_research_collectors",
        "name": "Foundation Research Collectors Snapshot",
        "phase": 1,
        "data": data,
    }


@app.post("/api/foundation/pillar6/rebuild")
async def api_rebuild_foundation_pillar6(brand: str = ""):
    """Recompute Pillar 6 from Step 1 collectors (Gemini + Claude) using current Pillar 2 as truth-check."""
    if pipeline_state.get("running"):
        return JSONResponse(
            {"error": "Pipeline is running. Stop it before rebuilding Pillar 6."},
            status_code=409,
        )

    brand_slug = str(brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "Brand is required."}, status_code=400)

    base = _brand_output_dir(brand_slug)
    output_path = _output_write_path(base, "foundation_research")
    if not output_path.exists():
        return JSONResponse(
            {"error": "No Foundation Research output found for this brand."},
            status_code=404,
        )

    try:
        foundation = json.loads(output_path.read_text("utf-8"))
    except Exception:
        return JSONResponse(
            {"error": "Foundation Research output is unreadable."},
            status_code=500,
        )
    if not isinstance(foundation, dict):
        return JSONResponse(
            {"error": "Foundation Research output has invalid structure."},
            status_code=500,
        )

    try:
        pillar_2 = Pillar2VocLanguageBank.model_validate(
            foundation.get("pillar_2_voc_language_bank") or {}
        )
    except Exception as exc:
        return JSONResponse(
            {"error": f"Pillar 2 data is invalid: {exc}"},
            status_code=400,
        )

    previous_rows = (
        foundation.get("pillar_6_emotional_driver_inventory", {}).get("dominant_emotions", [])
        if isinstance(foundation.get("pillar_6_emotional_driver_inventory"), dict)
        else []
    )
    previous_lf8_rows = (
        foundation.get("pillar_6_emotional_driver_inventory", {}).get("lf8_rows_by_segment", {})
        if isinstance(foundation.get("pillar_6_emotional_driver_inventory"), dict)
        else {}
    )
    previous_labels = [
        str(row.get("emotion") or row.get("emotion_label") or "").strip()
        for row in (previous_rows if isinstance(previous_rows, list) else [])
        if isinstance(row, dict) and str(row.get("emotion") or row.get("emotion_label") or "").strip()
    ]
    previous_lf8_counts: dict[str, int] = {}
    if isinstance(previous_lf8_rows, dict):
        for key, rows in previous_lf8_rows.items():
            if not isinstance(rows, list):
                continue
            segment_name = str(key or "").strip()
            if not segment_name:
                continue
            previous_lf8_counts[segment_name] = len([row for row in rows if isinstance(row, dict)])

    collector_reports = _load_step1_collector_reports(base)
    if not collector_reports:
        return JSONResponse(
            {
                "error": (
                    "No Step 1 collector reports found for this brand. "
                    "Run Foundation Research again so Gemini + Claude reports are available for Pillar 6 rebuild."
                )
            },
            status_code=400,
        )

    evidence_rows = foundation.get("evidence_ledger", [])
    evidence: list[EvidenceItem] = []
    if isinstance(evidence_rows, list):
        for row in evidence_rows:
            if not isinstance(row, dict):
                continue
            try:
                evidence.append(EvidenceItem.model_validate(row))
            except Exception:
                continue

    allowed_segments = [
        str(row.get("segment_name") or "").strip()
        for row in _extract_pillar1_audience_options(foundation)
        if isinstance(row, dict) and str(row.get("segment_name") or "").strip()
    ]

    pillar_6 = derive_emotional_inventory_from_collectors(
        collector_reports,
        pillar_2,
        evidence=evidence,
        mutate_pillar2_labels=True,
        allowed_segments=allowed_segments,
    )
    foundation["pillar_2_voc_language_bank"] = pillar_2.model_dump()
    foundation["pillar_6_emotional_driver_inventory"] = pillar_6.model_dump()

    # Keep cross-pillar traceability aligned after the rebuild.
    quote_ids = {str(q.quote_id or "").strip() for q in pillar_2.quotes if str(q.quote_id or "").strip()}
    emotions_traced = bool(pillar_6.dominant_emotions) and all(
        any(qid in quote_ids for qid in emo.sample_quote_ids)
        for emo in pillar_6.dominant_emotions
    )
    cross_payload = foundation.get("cross_pillar_consistency_report")
    if isinstance(cross_payload, dict):
        cross_payload["dominant_emotions_traced_to_voc"] = emotions_traced
        issues = cross_payload.get("issues")
        issue_text = "Dominant emotions are not fully traceable to VOC quote IDs."
        if not isinstance(issues, list):
            issues = []
        issues = [str(item) for item in issues if str(item or "").strip()]
        has_issue = any(item == issue_text for item in issues)
        if emotions_traced and has_issue:
            issues = [item for item in issues if item != issue_text]
        elif (not emotions_traced) and (not has_issue):
            issues.append(issue_text)
        cross_payload["issues"] = issues
        foundation["cross_pillar_consistency_report"] = cross_payload

    quality_raw = foundation.get("quality_gate_report")
    quality_warning = (
        str(quality_raw.get("warning") or "").strip()
        if isinstance(quality_raw, dict)
        else ""
    )
    recomputed_quality: dict[str, Any] = {}
    quality_recomputed = False
    try:
        pillar_1 = Pillar1ProspectProfile.model_validate(
            foundation.get("pillar_1_prospect_profile") or {}
        )
        pillar_3 = Pillar3CompetitiveIntelligence.model_validate(
            foundation.get("pillar_3_competitive_intelligence") or {}
        )
        pillar_4 = Pillar4ProductMechanismAnalysis.model_validate(
            foundation.get("pillar_4_product_mechanism_analysis") or {}
        )
        pillar_5 = Pillar5AwarenessClassification.model_validate(
            foundation.get("pillar_5_awareness_classification") or {}
        )
        pillar_7 = Pillar7ProofCredibilityInventory.model_validate(
            foundation.get("pillar_7_proof_credibility_inventory") or {}
        )
        cross = CrossPillarConsistencyReport.model_validate(
            foundation.get("cross_pillar_consistency_report") or {}
        )
        retry_rounds_used = (
            int(quality_raw.get("retry_rounds_used", 0) or 0)
            if isinstance(quality_raw, dict)
            else 0
        )
        quality = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=pillar_1,
            pillar_2=pillar_2,
            pillar_3=pillar_3,
            pillar_4=pillar_4,
            pillar_5=pillar_5,
            pillar_6=pillar_6,
            pillar_7=pillar_7,
            cross_report=cross,
            retry_rounds_used=retry_rounds_used,
        )
        recomputed_quality = quality.model_dump()
        if quality_warning and not bool(recomputed_quality.get("overall_pass")):
            recomputed_quality["warning"] = quality_warning
        foundation["quality_gate_report"] = recomputed_quality
        quality_recomputed = True
    except Exception as exc:
        logger.warning("Pillar 6 rebuild: quality recomputation skipped (%s)", exc)
        if isinstance(quality_raw, dict):
            recomputed_quality = dict(quality_raw)

    foundation["pillar_6_refreshed_at"] = datetime.now().isoformat()
    output_path.write_text(
        json.dumps(foundation, indent=2, default=str),
        encoding="utf-8",
    )

    if recomputed_quality:
        (base / "foundation_research_quality_report.json").write_text(
            json.dumps(recomputed_quality, indent=2, default=str),
            encoding="utf-8",
        )

    current_labels = [str(emo.emotion or "").strip() for emo in pillar_6.dominant_emotions if str(emo.emotion or "").strip()]
    changed = previous_labels != current_labels
    current_lf8_counts = {
        str(segment).strip(): len(rows)
        for segment, rows in (pillar_6.lf8_rows_by_segment or {}).items()
        if str(segment or "").strip()
    }
    lf8_rows_total = int(sum(current_lf8_counts.values()))
    lf8_changed = previous_lf8_counts != current_lf8_counts
    _add_log(
        (
            "Pillar 6 rebuilt from Step 1 collectors + Pillar 2 truth-check "
            f"({len(current_labels)} emotions; LF8 rows={lf8_rows_total}; changed={str(changed).lower()})"
        ),
        "success",
    )
    if recomputed_quality:
        _emit_phase1_quality_logs(recomputed_quality)

    return {
        "status": "ok",
        "brand_slug": brand_slug,
        "pillar_6_refreshed_at": foundation["pillar_6_refreshed_at"],
        "changed": changed,
        "emotion_count_before": len(previous_labels),
        "emotion_count_after": len(current_labels),
        "emotions_after": current_labels,
        "lf8_segments_count": len(current_lf8_counts),
        "lf8_rows_total": lf8_rows_total,
        "lf8_rows_by_segment_counts": current_lf8_counts,
        "lf8_changed": lf8_changed,
        "collector_reports_used": len(collector_reports),
        "quality_recomputed": quality_recomputed,
        "quality_gate_report": recomputed_quality if recomputed_quality else foundation.get("quality_gate_report", {}),
    }


@app.get("/api/matrix-axes")
async def api_matrix_axes(brand: str = "", selected_audience_segment: str = ""):
    """Return Phase 2 matrix axes derived from validated Foundation Research."""
    brand_slug = str(brand or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "Brand is required for matrix axes."}, status_code=400)
    inputs: dict[str, Any] = {}
    foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
    if foundation_err:
        return JSONResponse({"error": foundation_err}, status_code=400)

    foundation = inputs.get("foundation_brief", {})
    foundation_data = foundation if isinstance(foundation, dict) else {}
    selected_raw = str(selected_audience_segment or "").strip()
    try:
        awareness_levels, emotion_rows, emotion_source_mode, requires_audience_selection, message = _extract_matrix_axes(
            foundation_data,
            selected_audience_segment=selected_raw,
            require_audience_selection=True,
            allow_global_legacy=False,
        )
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    audience_options = _extract_pillar1_audience_options(
        foundation_data
    )

    return {
        "awareness_levels": awareness_levels,
        "emotion_rows": emotion_rows,
        "audience_options": audience_options,
        "requires_audience_selection": bool(requires_audience_selection),
        "emotion_source_mode": emotion_source_mode,
        "message": str(message or ""),
        "max_briefs_per_cell": MATRIX_CELL_MAX_BRIEFS,
    }


@app.get("/api/agent-models")
async def api_agent_models():
    """Return the default model config for each active agent."""
    result = {}
    for slug in AGENT_META:
        conf = config.get_agent_llm_config(slug)
        model_name = conf["model"]
        # Foundation Research runs two stages by default:
        # Step 1 collectors (Deep Research + Claude scout), then Step 2 synthesis
        # using the configured foundation model (currently Claude Opus 4.6).
        if slug == "foundation_research":
            label = "Deep Research + Claude Opus 4.6"
        elif slug == "creative_engine" and config.PHASE2_MATRIX_ONLY_MODE:
            label = "Matrix Planner"
        else:
            label = _friendly_model_label(model_name)
        result[slug] = {
            "provider": conf["provider"],
            "model": model_name,
            "label": label,
        }
    return result


@app.get("/api/health")
async def api_health():
    """Check system health — API keys, config, etc."""
    providers = {
        "openai": bool(config.OPENAI_API_KEY),
        "anthropic": bool(config.ANTHROPIC_API_KEY),
        "google": bool(config.GOOGLE_API_KEY),
    }
    default_ok = providers.get(config.DEFAULT_PROVIDER, False)
    warnings = _check_api_keys()

    return {
        "ok": default_ok,
        "default_provider": config.DEFAULT_PROVIDER,
        "default_model": config.DEFAULT_MODEL,
        "providers": providers,
        "any_provider_configured": any(providers.values()),
        "phase2_matrix_only_mode": bool(config.PHASE2_MATRIX_ONLY_MODE),
        "phase3_disabled": bool(config.PHASE3_TEMPORARILY_DISABLED),
        "phase3_v2_enabled": bool(config.PHASE3_V2_ENABLED),
        "phase3_v2_default_path": str(config.PHASE3_V2_DEFAULT_PATH),
        "phase3_v2_default_pilot_size": int(config.PHASE3_V2_DEFAULT_PILOT_SIZE),
        "phase3_v2_core_drafter_max_parallel": int(config.PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL),
        "phase3_v2_hooks_enabled": bool(config.PHASE3_V2_HOOKS_ENABLED),
        "phase3_v2_hook_max_parallel": int(config.PHASE3_V2_HOOK_MAX_PARALLEL),
        "phase3_v2_hook_candidates_per_unit": int(config.PHASE3_V2_HOOK_CANDIDATES_PER_UNIT),
        "phase3_v2_hook_final_variants_per_unit": int(config.PHASE3_V2_HOOK_FINAL_VARIANTS_PER_UNIT),
        "phase3_v2_hook_min_new_variants": int(config.PHASE3_V2_HOOK_MIN_NEW_VARIANTS),
        "phase3_v2_hook_max_repair_rounds": int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS),
        "phase3_v2_scenes_enabled": bool(config.PHASE3_V2_SCENES_ENABLED),
        "phase3_v2_scene_max_parallel": int(config.PHASE3_V2_SCENE_MAX_PARALLEL),
        "phase3_v2_scene_max_repair_rounds": int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
        "phase3_v2_scene_max_consecutive_mode": int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
        "phase3_v2_scene_min_a_roll_lines": int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
        "phase3_v2_scene_enable_beat_split": bool(config.PHASE3_V2_SCENE_ENABLE_BEAT_SPLIT),
        "phase3_v2_scene_beat_target_words_min": int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MIN),
        "phase3_v2_scene_beat_target_words_max": int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MAX),
        "phase3_v2_scene_beat_hard_max_words": int(config.PHASE3_V2_SCENE_BEAT_HARD_MAX_WORDS),
        "phase3_v2_scene_max_beats_per_line": int(config.PHASE3_V2_SCENE_MAX_BEATS_PER_LINE),
        "phase3_v2_scene_model_draft": str(config.PHASE3_V2_SCENE_MODEL_DRAFT),
        "phase3_v2_scene_model_repair": str(config.PHASE3_V2_SCENE_MODEL_REPAIR),
        "phase3_v2_scene_model_polish": str(config.PHASE3_V2_SCENE_MODEL_POLISH),
        "phase3_v2_scene_model_gate": str(config.PHASE3_V2_SCENE_MODEL_GATE),
        "phase3_v2_reviewer_role_default": str(config.PHASE3_V2_REVIEWER_ROLE_DEFAULT),
        "phase3_v2_sdk_toggles_default": dict(config.PHASE3_V2_SDK_TOGGLES_DEFAULT),
        "phase4_v1_enabled": bool(config.PHASE4_V1_ENABLED),
        "phase4_v1_test_mode_single_active_run": bool(config.PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN),
        "phase4_v1_max_parallel_clips": int(config.PHASE4_V1_MAX_PARALLEL_CLIPS),
        "phase4_v1_storyboard_assign_max_parallel": int(_PHASE4_STORYBOARD_ASSIGN_MAX_PARALLEL),
        "phase4_v1_tts_model": str(config.PHASE4_V1_TTS_MODEL),
        "phase4_v1_fal_broll_model_id": str(config.PHASE4_V1_FAL_BROLL_MODEL_ID),
        "phase4_v1_fal_talking_head_model_id": str(config.PHASE4_V1_FAL_TALKING_HEAD_MODEL_ID),
        "phase4_v1_gemini_image_edit_model_id": str(config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID),
        "phase4_v1_openai_image_edit_model_id": str(config.PHASE4_V1_OPENAI_IMAGE_EDIT_MODEL_ID),
        "phase4_v1_vision_scene_model_id": str(config.PHASE4_V1_VISION_SCENE_MODEL_ID),
        "phase4_v1_voice_presets": list(config.PHASE4_V1_VOICE_PRESETS),
        "warnings": warnings,
    }


@app.get("/api/sample-input")
async def api_sample_input(name: str = "animus"):
    """Return a sample input JSON. Use ?name=animus or ?name=nord."""
    filename_map = {
        "animus": "sample_input.json",
        "nord": "sample_input_nord.json",
    }
    filename = filename_map.get(name, "sample_input.json")
    path = Path(filename)
    if path.exists():
        return json.loads(path.read_text())
    return {}


@app.delete("/api/outputs")
async def api_clear_outputs(brand: str = ""):
    """Clear all agent outputs from disk (brand-scoped)."""
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    count = 0
    for slug in AGENT_META:
        for candidate in _slug_variants(slug):
            path = base / f"{candidate}_output.json"
            if path.exists():
                path.unlink()
                count += 1
    return {"cleared": count}


# ---------------------------------------------------------------------------
# Brand API
# ---------------------------------------------------------------------------

@app.get("/api/brands")
async def api_list_brands_endpoint(limit: int = 50):
    """List all brands, most recently opened first."""
    brands = list_brands(limit=limit)
    # Enrich with agent availability
    for b in brands:
        bdir = config.OUTPUT_DIR / b["slug"]
        available_agents = [
            slug for slug in AGENT_META
            if _output_exists(bdir, slug)
        ]
        b["available_agents"] = available_agents
    return brands


@app.get("/api/brands/{brand_slug_param}")
async def api_get_brand_endpoint(brand_slug_param: str):
    """Get a brand with its brief and available agents."""
    brand = get_brand(brand_slug_param)
    if not brand:
        return JSONResponse({"error": "Brand not found"}, status_code=404)
    # Enrich with agent availability
    bdir = config.OUTPUT_DIR / brand_slug_param
    brand["available_agents"] = [
        slug for slug in AGENT_META
        if _output_exists(bdir, slug)
    ]
    # Load branches for this brand
    brand["branches"] = _load_branches(brand_slug_param)
    return brand


@app.post("/api/brands/{brand_slug_param}/open")
async def api_open_brand(brand_slug_param: str):
    """Touch the brand's last_opened_at and set it as the active brand."""
    try:
        brand = get_brand(brand_slug_param)
    except Exception as e:
        logger.exception("Failed loading brand %s", brand_slug_param)
        return JSONResponse({"error": f"Failed to load brand: {e}"}, status_code=500)

    if not brand:
        return JSONResponse({"error": "Brand not found"}, status_code=404)

    try:
        touch_brand(brand_slug_param)
    except Exception as e:
        # Non-fatal: opening a brand should still work even if the timestamp write fails.
        logger.warning("Failed touching brand %s: %s", brand_slug_param, e)

    pipeline_state["active_brand_slug"] = brand_slug_param

    try:
        bdir = config.OUTPUT_DIR / brand_slug_param
        brand["available_agents"] = [
            slug for slug in AGENT_META
            if _output_exists(bdir, slug)
        ]
    except Exception as e:
        logger.warning("Failed computing available agents for brand %s: %s", brand_slug_param, e)
        brand["available_agents"] = []

    try:
        brand["branches"] = _load_branches(brand_slug_param)
    except Exception as e:
        logger.warning("Failed loading branches for brand %s: %s", brand_slug_param, e)
        brand["branches"] = []

    return brand


@app.delete("/api/brands/{brand_slug_param}")
async def api_delete_brand_endpoint(brand_slug_param: str):
    """Delete a brand, its outputs directory, and all runs."""
    found = storage_delete_brand(brand_slug_param)
    if not found:
        return JSONResponse({"error": "Brand not found"}, status_code=404)
    brand_dir = config.OUTPUT_DIR / brand_slug_param
    if brand_dir.exists():
        shutil.rmtree(brand_dir, ignore_errors=True)
    if pipeline_state.get("active_brand_slug") == brand_slug_param:
        pipeline_state["active_brand_slug"] = None
    return {"ok": True, "deleted": brand_slug_param}


# ---------------------------------------------------------------------------
# Run History API (SQLite-backed)
# ---------------------------------------------------------------------------

@app.get("/api/runs")
async def api_list_runs(limit: int = 50):
    """List past pipeline runs, newest first."""
    return list_runs(limit=limit)


@app.get("/api/runs/{run_id}")
async def api_get_run(run_id: int):
    """Get a specific run with all its agent outputs."""
    run = get_run(run_id)
    if not run:
        return JSONResponse({"error": f"Run #{run_id} not found"}, status_code=404)
    return run


class LabelUpdate(BaseModel):
    label: str


@app.patch("/api/runs/{run_id}")
async def api_update_run(run_id: int, body: LabelUpdate):
    """Update a run's label."""
    update_run_label(run_id, body.label)
    return {"ok": True, "run_id": run_id, "label": body.label.strip()}


@app.delete("/api/runs/{run_id}")
async def api_delete_run(run_id: int):
    """Delete a run and all its agent outputs."""
    found = delete_run(run_id)
    if not found:
        return JSONResponse({"error": f"Run #{run_id} not found"}, status_code=404)
    return {"ok": True, "deleted": run_id}


# ---------------------------------------------------------------------------
# WebSocket for real-time updates
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    try:
        # Build gate info if pipeline is paused at a phase gate
        gate_info = pipeline_state.get("gate_info")
        if pipeline_state.get("waiting_for_approval") and gate_info is None:
            current_phase = pipeline_state.get("current_phase", 1)
            last_completed = pipeline_state.get("completed_agents", [])
            completed_slug = last_completed[-1] if last_completed else "foundation_research"
            _next_agent_map = {
                "foundation_research": ("creative_engine", "Creative Engine"),
                "creative_engine": ("copywriter", "Copywriter"),
                "copywriter": ("hook_specialist", "Hook Specialist"),
            }
            next_slug, next_name = _next_agent_map.get(completed_slug, ("unknown", "Next Agent"))
            copywriter_failed_count = 0
            if completed_slug == "copywriter":
                copywriter_failed_count = len(pipeline_state.get("copywriter_failed_jobs", []))
            gate_info = {
                "type": "phase_gate",
                "completed_agent": completed_slug,
                "next_agent": next_slug,
                "next_agent_name": next_name,
                "phase": current_phase,
                "show_concept_selection": completed_slug == "creative_engine",
                "copywriter_failed_count": copywriter_failed_count,
                "gate_mode": "standard",
                "cost": _running_cost_summary(),
            }

        await ws.send_json({
            "type": "state_sync",
            "running": pipeline_state["running"],
            "current_phase": pipeline_state["current_phase"],
            "current_agent": pipeline_state["current_agent"],
            "completed_agents": pipeline_state["completed_agents"],
            "log": pipeline_state["log"][-50:],
            "server_log_tail": list(_recent_server_logs)[-150:],
            "waiting_for_approval": pipeline_state.get("waiting_for_approval", False),
            "gate_info": gate_info,
            "active_branch": pipeline_state.get("active_branch"),
            "active_brand_slug": pipeline_state.get("active_brand_slug"),
            "cost": _current_cost_summary_for_status(
                brand_slug=str(pipeline_state.get("active_brand_slug") or "").strip()
            ),
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)


# ---------------------------------------------------------------------------
# Static files & SPA fallback
# ---------------------------------------------------------------------------

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.mount("/outputs", StaticFiles(directory=str(config.OUTPUT_DIR)), name="outputs")


@app.get("/")
async def index():
    index_path = static_dir / "index.html"
    style_path = static_dir / "style.css"
    app_js_path = static_dir / "app.js"
    html = index_path.read_text("utf-8")
    style_version = int(style_path.stat().st_mtime)
    app_version = int(app_js_path.stat().st_mtime)
    html = re.sub(
        r"/static/style\.css(?:\?[^\"']*)?",
        f"/static/style.css?v={style_version}",
        html,
    )
    html = re.sub(
        r"/static/app\.js(?:\?[^\"']*)?",
        f"/static/app.js?v={app_version}",
        html,
    )
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    print("\n  Creative Maker Pipeline Dashboard")
    print("  http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
