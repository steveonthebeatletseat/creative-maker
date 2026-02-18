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
from collections import deque
import hashlib
import json
import logging
import queue as queue_mod
import re
import shutil
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import config
from agents.agent_01a_foundation_research import Agent01AFoundationResearch
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from pipeline.phase1_engine import run_phase1_collectors_only
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
from schemas.foundation_research import AwarenessLevel
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
        max_difficulty=int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY),
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
        out[arm] = rows if isinstance(rows, list) else []
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


from pipeline.llm import reset_usage, get_usage_summary
from pipeline.scraper import scrape_website
from pipeline.storage import (
    init_db,
    create_run,
    complete_run,
    fail_run,
    save_agent_output,
    list_runs,
    get_run,
    update_run_label,
    delete_run,
    # Brand system
    get_or_create_brand,
    get_brand,
    list_brands,
    touch_brand,
    delete_brand as storage_delete_brand,
    _slugify,
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


def _extract_matrix_axes(foundation: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    awareness_levels = list(MATRIX_AWARENESS_LEVELS)
    dominant = (
        foundation.get("pillar_6_emotional_driver_inventory", {}).get("dominant_emotions", [])
        if isinstance(foundation, dict)
        else []
    )

    emotion_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if isinstance(dominant, list):
        for idx, item in enumerate(dominant):
            if not isinstance(item, dict):
                continue
            label = str(item.get("emotion") or "").strip()
            if not label:
                continue
            emotion_key = _normalize_emotion_key(label) or f"emotion_{idx + 1}"
            if emotion_key in seen:
                continue
            seen.add(emotion_key)

            sample_quote_ids = item.get("sample_quote_ids", [])
            if not isinstance(sample_quote_ids, list):
                sample_quote_ids = []

            emotion_rows.append(
                {
                    "emotion_key": emotion_key,
                    "emotion_label": label,
                    "tagged_quote_count": int(item.get("tagged_quote_count", 0) or 0),
                    "share_of_voc": float(item.get("share_of_voc", 0.0) or 0.0),
                    "sample_quote_ids": [str(v) for v in sample_quote_ids if str(v or "").strip()],
                }
            )

    return awareness_levels, emotion_rows


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

    awareness_levels, emotion_rows = _extract_matrix_axes(foundation)
    if not emotion_rows:
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
    cost_summary = get_usage_summary()
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
    _add_log(f"Starting {meta['icon']} {meta['name']} [{model_label}]...")
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
        "model": model_label,
        "provider": final_provider,
    })

    # Set up streaming progress callback to broadcast to frontend
    from pipeline.llm import set_stream_progress_callback

    def _on_stream_progress(msg):
        """Called from LLM thread during streaming — fire-and-forget broadcast."""
        try:
            asyncio.run_coroutine_threadsafe(
                broadcast({"type": "stream_progress", "slug": slug, "message": msg}),
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
        cost_summary = get_usage_summary()
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
    _add_log(f"Starting {meta['icon']} {meta['name']} Step 1/2 [{model_label}]...")
    await broadcast(
        {
            "type": "agent_start",
            "slug": slug,
            "name": f"{meta['name']} (Step 1/2)",
            "model": model_label,
            "provider": final_provider,
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

        cost_summary = get_usage_summary()
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

    await broadcast({"type": "pipeline_start", "phases": phases, "run_id": run_id, "brand_slug": brand_slug or ""})

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
        final_cost = get_usage_summary()
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
        abort_cost = get_usage_summary()
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
        err_cost = get_usage_summary()
        _add_log(f"Pipeline error: {e}", "error")
        logger.exception("Pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost})
    finally:
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
        from pipeline.llm import get_usage_summary
        cost = get_usage_summary()

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
    visual_pattern_interrupt: str
    on_screen_text: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    source: Literal["manual", "chat_apply"] = "manual"


class Phase3V2HookProposedPayload(BaseModel):
    verbal_open: str
    visual_pattern_interrupt: str
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
    mode: Literal["a_roll", "b_roll"]
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


def _phase3_v2_scene_line_id(brief_unit_id: str, hook_id: str, script_line_id: str) -> str:
    return f"sl_{brief_unit_id}_{hook_id}_{_phase3_v2_safe_scene_token(script_line_id)}"


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
        mode = "a_roll" if str(row.mode or "").strip().lower() == "a_roll" else "b_roll"
        evidence_ids = _phase3_v2_normalize_hook_evidence_ids(list(row.evidence_ids or []))
        a_roll = None
        b_roll = None
        if mode == "a_roll":
            try:
                a_roll = ARollDirectionV1.model_validate(row.a_roll or {})
            except Exception:
                a_roll = ARollDirectionV1()
        else:
            try:
                b_roll = BRollDirectionV1.model_validate(row.b_roll or {})
            except Exception:
                b_roll = BRollDirectionV1()
        normalized.append(
            SceneLinePlanV1(
                scene_line_id=_phase3_v2_scene_line_id(brief_unit_id, hook_id, script_line_id),
                script_line_id=script_line_id,
                mode=mode,  # validated by literal values above
                a_roll=a_roll,
                b_roll=b_roll,
                on_screen_text=str(row.on_screen_text or "").strip(),
                duration_seconds=max(0.1, min(30.0, float(row.duration_seconds or 2.0))),
                evidence_ids=evidence_ids,
                difficulty_1_10=max(1, min(10, int(row.difficulty_1_10 or 5))),
            )
        )
    return normalized


def _phase3_v2_scene_sequence_metrics(lines: list[SceneLinePlanV1]) -> tuple[float, int, int, int]:
    total_duration = round(sum(float(row.duration_seconds or 0.0) for row in lines), 3)
    a_roll_count = sum(1 for row in lines if row.mode == "a_roll")
    b_roll_count = sum(1 for row in lines if row.mode == "b_roll")
    max_consecutive = 0
    streak = 0
    last_mode = ""
    for row in lines:
        mode = str(row.mode)
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
    total_duration, a_roll_count, b_roll_count, max_consecutive = _phase3_v2_scene_sequence_metrics(lines)
    updated = dict(existing)
    updated["scene_plan_id"] = str(existing.get("scene_plan_id") or f"sp_{brief_unit_id}_{hook_id}_{arm}")
    updated["run_id"] = str(existing.get("run_id") or run_id)
    updated["brief_unit_id"] = brief_unit_id
    updated["arm"] = arm
    updated["hook_id"] = hook_id
    updated["lines"] = [line.model_dump() for line in lines]
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
    a_roll_count = int(plan.get("a_roll_line_count") or 0)
    max_consecutive = int(plan.get("max_consecutive_mode") or 0)
    feasibility_pass = all(
        int((row.get("difficulty_1_10") if isinstance(row, dict) else 0) or 0) <= int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY)
        for row in lines
    )
    mode_pass = all(
        isinstance(row, dict)
        and str(row.get("mode") or "").strip() in {"a_roll", "b_roll"}
        and str(row.get("script_line_id") or "").strip()
        for row in lines
    )
    ugc_pass = a_roll_count >= int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES)
    pacing_pass = max_consecutive <= int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE)
    line_coverage_pass = bool(lines)
    overall_pass = bool(line_coverage_pass and mode_pass and ugc_pass and feasibility_pass and pacing_pass)
    failure_reasons: list[str] = []
    if not line_coverage_pass:
        failure_reasons.append("line_coverage_failed")
    if not mode_pass:
        failure_reasons.append("mode_missing_or_direction_missing")
    if not ugc_pass:
        failure_reasons.append("ugc_min_a_roll_failed")
    if not feasibility_pass:
        failure_reasons.append("feasibility_failed")
    if not pacing_pass:
        failure_reasons.append("pacing_failed")
    return {
        "scene_plan_id": str(plan.get("scene_plan_id") or ""),
        "scene_unit_id": f"su_{str(plan.get('brief_unit_id') or '')}_{str(plan.get('hook_id') or '')}",
        "run_id": str(plan.get("run_id") or ""),
        "brief_unit_id": str(plan.get("brief_unit_id") or ""),
        "arm": str(plan.get("arm") or "claude_sdk"),
        "hook_id": str(plan.get("hook_id") or ""),
        "line_coverage_pass": line_coverage_pass,
        "mode_pass": mode_pass,
        "ugc_pass": ugc_pass,
        "evidence_pass": True,
        "claim_safety_pass": True,
        "feasibility_pass": feasibility_pass,
        "pacing_pass": pacing_pass,
        "post_polish_pass": True,
        "overall_pass": overall_pass,
        "failure_reasons": failure_reasons,
        "failing_line_ids": [],
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
            max_difficulty=int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY),
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
            max_difficulty=int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY),
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
    if not visual_pattern_interrupt:
        return JSONResponse({"error": "visual_pattern_interrupt is required."}, status_code=400)

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
        "- If proposed_hook is returned, include complete fields:"
        " verbal_open, visual_pattern_interrupt, on_screen_text, evidence_ids."
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
    if not visual_pattern_interrupt:
        return JSONResponse({"error": "proposed_hook.visual_pattern_interrupt is required."}, status_code=400)

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
        max_difficulty=int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY),
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
        "Improve scene clarity and production feasibility while preserving script intent.\\n"
        "Keep mode decisions explicit (a_roll vs b_roll).\\n"
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
            mode=str(line.mode or "a_roll"),
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

    output_dir = _branch_output_dir(brand_slug, branch_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a DB run record
    run_id = create_run(phases, inputs, brand_slug=brand_slug)
    pipeline_state["run_id"] = run_id

    _update_branch(branch_id, {"status": "running", "completed_agents": [], "failed_agents": []}, brand_slug)

    await broadcast({
        "type": "pipeline_start",
        "phases": phases,
        "run_id": run_id,
        "branch_id": branch_id,
        "brand_slug": brand_slug,
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
        final_cost = get_usage_summary()
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
        abort_cost = get_usage_summary()
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
        err_cost = get_usage_summary()
        _add_log(f"Branch pipeline error: {e}", "error")
        logger.exception("Branch pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost, "branch_id": branch_id})
    finally:
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

        cost = get_usage_summary()
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
async def api_status():
    """Get current pipeline status."""
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
    }


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


@app.get("/api/matrix-axes")
async def api_matrix_axes(brand: str = ""):
    """Return Phase 2 matrix axes derived from validated Foundation Research."""
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    inputs: dict[str, Any] = {}
    foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
    if foundation_err:
        return JSONResponse({"error": foundation_err}, status_code=400)

    foundation = inputs.get("foundation_brief", {})
    awareness_levels, emotion_rows = _extract_matrix_axes(foundation if isinstance(foundation, dict) else {})
    if not emotion_rows:
        return JSONResponse(
            {
                "error": (
                    "No emotional drivers found in Phase 1 output "
                    "(pillar_6_emotional_driver_inventory.dominant_emotions)."
                )
            },
            status_code=400,
        )

    return {
        "awareness_levels": awareness_levels,
        "emotion_rows": emotion_rows,
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
        "phase3_v2_scene_max_difficulty": int(config.PHASE3_V2_SCENE_MAX_DIFFICULTY),
        "phase3_v2_scene_max_consecutive_mode": int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
        "phase3_v2_scene_min_a_roll_lines": int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
        "phase3_v2_scene_model_draft": str(config.PHASE3_V2_SCENE_MODEL_DRAFT),
        "phase3_v2_scene_model_repair": str(config.PHASE3_V2_SCENE_MODEL_REPAIR),
        "phase3_v2_scene_model_polish": str(config.PHASE3_V2_SCENE_MODEL_POLISH),
        "phase3_v2_scene_model_gate": str(config.PHASE3_V2_SCENE_MODEL_GATE),
        "phase3_v2_reviewer_role_default": str(config.PHASE3_V2_REVIEWER_ROLE_DEFAULT),
        "phase3_v2_sdk_toggles_default": dict(config.PHASE3_V2_SDK_TOGGLES_DEFAULT),
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

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(static_dir / "index.html"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    print("\n  Creative Maker Pipeline Dashboard")
    print("  http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
