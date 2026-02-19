"""Phase 3C v2 Scene Writer engine.

Constraint-first architecture:
- deterministic IDs + validators own pass/fail authority
- Claude SDK handles creative drafting/repair/polish
- GPT structured evaluation contributes advisory scoring only
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import math
import re
import time
from typing import Any

import config
from pipeline.claude_agent_runtime import call_claude_agent_structured
from pipeline.llm import call_llm_structured, get_usage_log
from pydantic import BaseModel, Field
from schemas.phase3_v2 import (
    ARollDirectionV1,
    ArmName,
    BRollDirectionV1,
    BriefUnitV1,
    CoreScriptDraftV1,
    EvidencePackV1,
    HookVariantV1,
    ProductionHandoffPacketV1,
    ProductionHandoffUnitV1,
    SceneGateReportV1,
    SceneLinePlanV1,
    ScenePlanV1,
    SceneStageManifestV1,
)

logger = logging.getLogger(__name__)

_UNSUPPORTED_CLAIM_RE = re.compile(
    r"(\b100%\b|\bguarantee(?:d)?\b|\bclinically\s+proven\b|\bcure(?:s|d)?\b|\bfix(?:es|ed)?\s+everything\b)",
    re.IGNORECASE,
)
_BEAT_LINE_ID_RE = re.compile(r"^(?P<source>[A-Za-z0-9_]+)\.(?P<index>\d+)$")


class _SceneDraftLineModel(BaseModel):
    script_line_id: str
    source_script_line_id: str = ""
    beat_index: int = Field(default=1, ge=1)
    beat_text: str = ""
    mode: str
    a_roll: ARollDirectionV1 | None = None
    b_roll: BRollDirectionV1 | None = None
    on_screen_text: str = ""
    duration_seconds: float = Field(default=2.0, ge=0.1, le=30.0)
    evidence_ids: list[str] = Field(default_factory=list)
    difficulty_1_10: int = Field(default=5, ge=1, le=10)


class _SceneDraftBatchModel(BaseModel):
    lines: list[_SceneDraftLineModel] = Field(default_factory=list)


class _SceneRepairBatchModel(BaseModel):
    lines: list[_SceneDraftLineModel] = Field(default_factory=list)


class _SceneQualityEvalModel(BaseModel):
    persuasion_score: int = Field(default=0, ge=0, le=100)
    coherence_score: int = Field(default=0, ge=0, le=100)
    rationale: str = ""


@dataclass
class _SceneUnitResult:
    arm: str
    brief_unit_id: str
    hook_id: str
    scene_unit_id: str
    scene_plan: ScenePlanV1
    gate_report: SceneGateReportV1
    elapsed_seconds: float
    error: str = ""


def _usage_delta(start_index: int) -> float:
    try:
        log = get_usage_log()
    except Exception:
        return 0.0
    if start_index < 0 or start_index >= len(log):
        return 0.0
    return round(float(sum(float(entry.get("cost", 0.0) or 0.0) for entry in log[start_index:])), 6)


def _model_override_name(model_overrides: dict[str, Any], stage_key: str, default: str) -> str:
    if not isinstance(model_overrides, dict):
        return default
    payload = model_overrides.get(stage_key)
    if isinstance(payload, str) and payload.strip():
        return payload.strip()
    if isinstance(payload, dict):
        model = str(payload.get("model") or "").strip()
        if model:
            return model
    return default


def _safe_line_id(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_")
    return text or "L00"


def _scene_unit_id(brief_unit_id: str, hook_id: str) -> str:
    return f"su_{brief_unit_id}_{hook_id}"


def _scene_plan_id(brief_unit_id: str, hook_id: str, arm: str) -> str:
    return f"sp_{brief_unit_id}_{hook_id}_{arm}"


def _scene_line_id(brief_unit_id: str, hook_id: str, script_line_id: str) -> str:
    return f"sl_{brief_unit_id}_{hook_id}_{_safe_line_id(script_line_id)}"


def _line_text(line: dict[str, Any]) -> str:
    return str(line.get("text") or "").strip()


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", str(text or "")))


def _chunk_words(text: str, *, target_words: int) -> list[str]:
    words = [w for w in str(text or "").split() if w]
    if not words:
        return []
    target = max(1, int(target_words))
    return [" ".join(words[idx : idx + target]) for idx in range(0, len(words), target)]


def _split_sentence_chunks(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?;:])\s+|(?:\s*[—]+\s*)", cleaned)
    return [str(part or "").strip(" ,;:-") for part in parts if str(part or "").strip(" ,;:-")]


def _split_clause_chunks(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return []
    parts = re.split(r"\s*(?:,|;|\b(?:and|but|so|because|while|which|that)\b)\s*", cleaned, flags=re.IGNORECASE)
    return [str(part or "").strip(" ,;:-") for part in parts if str(part or "").strip(" ,;:-")]


def _merge_short_chunks(chunks: list[str], *, min_words: int) -> list[str]:
    out: list[str] = []
    for chunk in chunks:
        piece = str(chunk or "").strip()
        if not piece:
            continue
        if out and _word_count(piece) < min_words:
            out[-1] = f"{out[-1]} {piece}".strip()
        else:
            out.append(piece)
    if len(out) > 1 and _word_count(out[0]) < min_words:
        out[1] = f"{out[0]} {out[1]}".strip()
        out = out[1:]
    return out


def _enforce_max_beats(chunks: list[str], *, max_beats: int) -> list[str]:
    out = [str(chunk or "").strip() for chunk in chunks if str(chunk or "").strip()]
    cap = max(1, int(max_beats))
    while len(out) > cap:
        out[-2] = f"{out[-2]} {out[-1]}".strip()
        out.pop()
    return out


def _split_line_into_beats(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return []

    target_min = int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MIN)
    target_max = int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MAX)
    hard_max = int(config.PHASE3_V2_SCENE_BEAT_HARD_MAX_WORDS)
    max_beats = int(config.PHASE3_V2_SCENE_MAX_BEATS_PER_LINE)
    min_words = int(config.PHASE3_V2_SCENE_BEAT_MIN_WORDS)

    pieces = _split_sentence_chunks(cleaned)
    if not pieces:
        pieces = [cleaned]

    if len(pieces) == 1 and _word_count(pieces[0]) > target_max:
        clause_parts = _split_clause_chunks(pieces[0])
        if clause_parts:
            pieces = clause_parts

    expanded: list[str] = []
    for piece in pieces:
        wc = _word_count(piece)
        if wc > hard_max:
            clause_parts = _split_clause_chunks(piece)
            if clause_parts and len(clause_parts) > 1:
                for clause in clause_parts:
                    if _word_count(clause) > hard_max:
                        expanded.extend(_chunk_words(clause, target_words=target_max))
                    else:
                        expanded.append(clause)
            else:
                expanded.extend(_chunk_words(piece, target_words=target_max))
        else:
            expanded.append(piece)

    merged = _merge_short_chunks(expanded, min_words=min_words)
    merged = _enforce_max_beats(merged, max_beats=max_beats)

    if len(merged) == 1 and _word_count(merged[0]) <= target_max:
        return merged
    if len(merged) == 1 and _word_count(merged[0]) > hard_max and max_beats > 1:
        merged = _chunk_words(merged[0], target_words=max(target_min, target_max))
        merged = _merge_short_chunks(merged, min_words=min_words)
        merged = _enforce_max_beats(merged, max_beats=max_beats)
    return merged or [cleaned]


def _lineage_from_line_id(script_line_id: str) -> tuple[str, int]:
    raw = str(script_line_id or "").strip()
    if not raw:
        return "", 1
    match = _BEAT_LINE_ID_RE.match(raw)
    if not match:
        return raw, 1
    source = str(match.group("source") or "").strip() or raw
    try:
        beat_index = max(1, int(match.group("index") or 1))
    except Exception:
        beat_index = 1
    return source, beat_index


def preprocess_script_lines_for_beats(
    script_lines: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str], list[str]]:
    beat_lines: list[dict[str, Any]] = []
    beat_to_source: dict[str, str] = {}
    source_line_ids: list[str] = []

    for idx, row in enumerate(script_lines):
        if not isinstance(row, dict):
            continue
        source_line_id = str(row.get("line_id") or f"L{idx + 1:02d}").strip()
        if not source_line_id:
            continue
        source_line_ids.append(source_line_id)
        text = _line_text(row)
        evidence_ids = [str(v).strip() for v in (row.get("evidence_ids") or []) if str(v or "").strip()]
        split_enabled = bool(config.PHASE3_V2_SCENE_ENABLE_BEAT_SPLIT)
        chunks = _split_line_into_beats(text) if split_enabled else [text]
        use_split_ids = len(chunks) > 1
        for beat_idx, chunk in enumerate(chunks, start=1):
            beat_text = str(chunk or "").strip()
            beat_id = f"{source_line_id}.{beat_idx}" if use_split_ids else source_line_id
            beat_row = {
                "line_id": beat_id,
                "text": beat_text,
                "evidence_ids": list(evidence_ids),
                "source_line_id": source_line_id,
                "beat_index": beat_idx,
                "beat_text": beat_text,
            }
            beat_lines.append(beat_row)
            beat_to_source[beat_id] = source_line_id

    return beat_lines, beat_to_source, list(dict.fromkeys(source_line_ids))


def build_scene_items_from_handoff(
    *,
    run_id: str,
    scene_handoff_packet: dict[str, Any],
    drafts_by_arm: dict[str, Any],
    evidence_packs: list[dict[str, Any]],
    brief_units: list[dict[str, Any]],
    selected_brief_unit_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected = {str(v).strip() for v in (selected_brief_unit_ids or []) if str(v or "").strip()}
    evidence_by_unit = {
        str(row.get("brief_unit_id") or "").strip(): row
        for row in (evidence_packs if isinstance(evidence_packs, list) else [])
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }
    unit_by_id = {
        str(row.get("brief_unit_id") or "").strip(): row
        for row in (brief_units if isinstance(brief_units, list) else [])
        if isinstance(row, dict) and str(row.get("brief_unit_id") or "").strip()
    }

    handoff_items = scene_handoff_packet.get("items", []) if isinstance(scene_handoff_packet, dict) else []
    eligible: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for row in (handoff_items if isinstance(handoff_items, list) else []):
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().lower()
        brief_unit_id = str(row.get("brief_unit_id") or "").strip()
        arm = str(row.get("arm") or "").strip() or "claude_sdk"
        if not brief_unit_id:
            continue
        if selected and brief_unit_id not in selected:
            continue
        if status != "ready":
            skipped.append(
                {
                    "brief_unit_id": brief_unit_id,
                    "arm": arm,
                    "hook_id": "",
                    "reason": status or "not_ready",
                }
            )
            continue

        draft_rows = drafts_by_arm.get(arm, []) if isinstance(drafts_by_arm, dict) else []
        draft = next(
            (
                d
                for d in (draft_rows if isinstance(draft_rows, list) else [])
                if isinstance(d, dict) and str(d.get("brief_unit_id") or "").strip() == brief_unit_id
            ),
            None,
        )
        evidence_pack = evidence_by_unit.get(brief_unit_id)
        brief_unit = unit_by_id.get(brief_unit_id)
        if not isinstance(draft, dict) or not isinstance(evidence_pack, dict) or not isinstance(brief_unit, dict):
            skipped.append(
                {
                    "brief_unit_id": brief_unit_id,
                    "arm": arm,
                    "hook_id": "",
                    "reason": "missing_script_or_evidence_or_brief_unit",
                }
            )
            continue

        selected_hooks = row.get("selected_hooks", []) if isinstance(row.get("selected_hooks"), list) else []
        normalized_selected_hooks: list[dict[str, Any]] = []
        selected_hook_ids: list[str] = []
        for hook in selected_hooks:
            if not isinstance(hook, dict):
                continue
            hook_id = str(hook.get("hook_id") or "").strip()
            if not hook_id:
                continue
            normalized_selected_hooks.append(hook)
            selected_hook_ids.append(hook_id)

        if not normalized_selected_hooks:
            skipped.append(
                {
                    "brief_unit_id": brief_unit_id,
                    "arm": arm,
                    "hook_id": "",
                    "reason": "missing_selected_hook",
                }
            )
            continue

        primary_hook = normalized_selected_hooks[0]
        primary_hook_id = str(primary_hook.get("hook_id") or "").strip()
        if not primary_hook_id:
            skipped.append(
                {
                    "brief_unit_id": brief_unit_id,
                    "arm": arm,
                    "hook_id": "",
                    "reason": "missing_primary_hook_id",
                }
            )
            continue

        eligible.append(
            {
                "run_id": run_id,
                "brief_unit_id": brief_unit_id,
                "arm": arm,
                "hook_id": primary_hook_id,
                "hook": primary_hook,
                "selected_hook_ids": selected_hook_ids,
                "selected_hooks": normalized_selected_hooks,
                "draft": draft,
                "evidence_pack": evidence_pack,
                "brief_unit": brief_unit,
            }
        )

    return (
        eligible,
        {
            "eligible_count": len(eligible),
            "skipped_count": len(skipped),
            "eligible": [
                {
                    "brief_unit_id": row.get("brief_unit_id"),
                    "arm": row.get("arm"),
                    "hook_id": row.get("hook_id"),
                }
                for row in eligible
            ],
            "skipped": skipped,
        },
    )


def compile_scene_constraints_ir(scene_item: dict[str, Any]) -> dict[str, Any]:
    brief_unit = BriefUnitV1.model_validate(scene_item["brief_unit"])
    draft = CoreScriptDraftV1.model_validate(scene_item["draft"])
    evidence_pack = EvidencePackV1.model_validate(scene_item["evidence_pack"])
    hook = HookVariantV1.model_validate(scene_item["hook"])
    selected_hook_ids = [
        str(v).strip()
        for v in (scene_item.get("selected_hook_ids", []) if isinstance(scene_item.get("selected_hook_ids"), list) else [])
        if str(v or "").strip()
    ]
    selected_hooks = [
        row
        for row in (scene_item.get("selected_hooks", []) if isinstance(scene_item.get("selected_hooks"), list) else [])
        if isinstance(row, dict)
    ]
    arm = str(scene_item.get("arm") or "claude_sdk")
    run_id = str(scene_item.get("run_id") or "")

    script_lines = [
        {
            "line_id": str(line.line_id or "").strip(),
            "text": str(line.text or "").strip(),
            "evidence_ids": [str(v).strip() for v in (line.evidence_ids or []) if str(v or "").strip()],
        }
        for line in (draft.lines or [])
        if str(line.text or "").strip()
    ]
    script_lines.sort(key=lambda row: int(re.sub(r"[^0-9]", "", row["line_id"]) or 0))
    if not script_lines:
        sections = draft.sections
        fallback_texts = [
            str(getattr(sections, "hook", "") or "").strip(),
            str(getattr(sections, "problem", "") or "").strip(),
            str(getattr(sections, "mechanism", "") or "").strip(),
            str(getattr(sections, "proof", "") or "").strip(),
            str(getattr(sections, "cta", "") or "").strip(),
        ]
        script_lines = [
            {
                "line_id": f"L{idx + 1:02d}",
                "text": text,
                "evidence_ids": [str(v).strip() for v in (hook.evidence_ids or []) if str(v or "").strip()][:2],
            }
            for idx, text in enumerate(fallback_texts)
            if text
        ]

    evidence_catalog: dict[str, str] = {}
    for ref in evidence_pack.voc_quote_refs:
        evidence_catalog[str(ref.quote_id)] = str(ref.quote_excerpt or "").strip()
    for ref in evidence_pack.proof_refs:
        evidence_catalog[str(ref.asset_id)] = " ".join([str(ref.title or "").strip(), str(ref.detail or "").strip()]).strip()
    for ref in evidence_pack.mechanism_refs:
        evidence_catalog[str(ref.mechanism_id)] = " ".join([str(ref.title or "").strip(), str(ref.detail or "").strip()]).strip()

    beat_script_lines, beat_source_map, source_line_ids = preprocess_script_lines_for_beats(script_lines)

    return {
        "run_id": run_id,
        "arm": arm,
        "brief_unit_id": brief_unit.brief_unit_id,
        "hook_id": hook.hook_id,
        "scene_unit_id": _scene_unit_id(brief_unit.brief_unit_id, hook.hook_id),
        "scene_plan_id": _scene_plan_id(brief_unit.brief_unit_id, hook.hook_id, arm),
        "awareness_level": brief_unit.awareness_level,
        "emotion_key": brief_unit.emotion_key,
        "emotion_label": brief_unit.emotion_label,
        "lf8_code": str(brief_unit.lf8_code or "").strip(),
        "lf8_label": str(brief_unit.lf8_label or "").strip(),
        "emotion_angle": str(brief_unit.emotion_angle or "").strip(),
        "blocking_objection": str(brief_unit.blocking_objection or "").strip(),
        "required_proof": str(brief_unit.required_proof or "").strip(),
        "confidence": float(brief_unit.confidence or 0.0),
        "sample_quote_ids": [str(v).strip() for v in (brief_unit.sample_quote_ids or []) if str(v or "").strip()],
        "lf8_context": {
            "lf8_code": str(brief_unit.lf8_code or "").strip(),
            "lf8_label": str(brief_unit.lf8_label or "").strip(),
            "emotion_angle": str(brief_unit.emotion_angle or "").strip(),
            "blocking_objection": str(brief_unit.blocking_objection or "").strip(),
            "required_proof": str(brief_unit.required_proof or "").strip(),
            "confidence": float(brief_unit.confidence or 0.0),
            "sample_quote_ids": [str(v).strip() for v in (brief_unit.sample_quote_ids or []) if str(v or "").strip()],
        },
        "audience_segment_name": str(brief_unit.audience_segment_name or "").strip(),
        "audience_goals": [str(v).strip() for v in (brief_unit.audience_goals or []) if str(v or "").strip()],
        "audience_pains": [str(v).strip() for v in (brief_unit.audience_pains or []) if str(v or "").strip()],
        "audience_triggers": [str(v).strip() for v in (brief_unit.audience_triggers or []) if str(v or "").strip()],
        "audience_objections": [str(v).strip() for v in (brief_unit.audience_objections or []) if str(v or "").strip()],
        "audience_information_sources": [
            str(v).strip() for v in (brief_unit.audience_information_sources or []) if str(v or "").strip()
        ],
        "audience": {
            "segment_name": str(brief_unit.audience_segment_name or "").strip(),
            "goals": [str(v).strip() for v in (brief_unit.audience_goals or []) if str(v or "").strip()],
            "pains": [str(v).strip() for v in (brief_unit.audience_pains or []) if str(v or "").strip()],
            "triggers": [str(v).strip() for v in (brief_unit.audience_triggers or []) if str(v or "").strip()],
            "objections": [str(v).strip() for v in (brief_unit.audience_objections or []) if str(v or "").strip()],
            "information_sources": [
                str(v).strip() for v in (brief_unit.audience_information_sources or []) if str(v or "").strip()
            ],
        },
        "hook": hook.model_dump(),
        "selected_hook_ids": selected_hook_ids or [hook.hook_id],
        "selected_hooks": selected_hooks or [hook.model_dump()],
        "source_script_lines": script_lines,
        "script_lines": beat_script_lines,
        "beat_source_map": beat_source_map,
        "source_line_ids": source_line_ids,
        "evidence_catalog": evidence_catalog,
        "constraints": {
            "max_consecutive_mode": int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
            "min_a_roll_lines": int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
            "require_mode_per_line": True,
            "require_evidence_subset": True,
            "enable_beat_split": bool(config.PHASE3_V2_SCENE_ENABLE_BEAT_SPLIT),
            "beat_target_words_min": int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MIN),
            "beat_target_words_max": int(config.PHASE3_V2_SCENE_BEAT_TARGET_WORDS_MAX),
            "beat_hard_max_words": int(config.PHASE3_V2_SCENE_BEAT_HARD_MAX_WORDS),
            "max_beats_per_line": int(config.PHASE3_V2_SCENE_MAX_BEATS_PER_LINE),
        },
    }


def _default_a_roll_direction(line_text: str, hook: dict[str, Any]) -> ARollDirectionV1:
    return ARollDirectionV1(
        framing="Talking-head medium close-up",
        creator_action="Speak this line naturally while maintaining eye contact",
        performance_direction="Conversational, credible, no hype",
        product_interaction="Hold or point to the product while speaking",
        location="Desk or home setup with clean background",
    )


def _default_b_roll_direction(line_text: str) -> BRollDirectionV1:
    return BRollDirectionV1(
        shot_description=f"Visualize: {line_text[:140]}",
        subject_action="Show the action implied by the script line",
        camera_motion="Slow push-in or handheld micro-movement",
        props_assets="Use real product and real environment",
        transition_intent="Bridge to next spoken line",
    )


def _sequence_metrics(lines: list[SceneLinePlanV1]) -> tuple[float, int, int, int]:
    total_duration = round(sum(float(row.duration_seconds or 0.0) for row in lines), 3)
    a_roll = sum(1 for row in lines if row.mode == "a_roll")
    b_roll = sum(1 for row in lines if row.mode == "b_roll")
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
    return total_duration, a_roll, b_roll, max_consecutive


def _resolve_lineage(
    *,
    candidate_id: str,
    row_source_script_line_id: str,
    row_beat_index: int | None,
    script_line: dict[str, Any],
) -> tuple[str, int, str]:
    inferred_source, inferred_index = _lineage_from_line_id(candidate_id)
    source_script_line_id = (
        str(row_source_script_line_id or "").strip()
        or str(script_line.get("source_line_id") or "").strip()
    )
    beat_index_raw = row_beat_index if row_beat_index is not None else script_line.get("beat_index")
    beat_text = str(script_line.get("beat_text") or script_line.get("text") or "").strip()

    if "." in str(candidate_id or ""):
        source_script_line_id = inferred_source
        beat_index_raw = inferred_index
    elif not source_script_line_id:
        source_script_line_id = inferred_source
        if beat_index_raw in {None, "", 0}:
            beat_index_raw = inferred_index

    try:
        beat_index = max(1, int(beat_index_raw or 1))
    except Exception:
        beat_index = 1
    return source_script_line_id or candidate_id, beat_index, beat_text


def _normalize_scene_lines(raw_lines: list[_SceneDraftLineModel], ir: dict[str, Any]) -> list[SceneLinePlanV1]:
    script_lines = ir.get("script_lines", []) if isinstance(ir.get("script_lines"), list) else []
    script_line_ids = [str(row.get("line_id") or "").strip() for row in script_lines if str(row.get("line_id") or "").strip()]
    script_line_ids = list(dict.fromkeys(script_line_ids))
    script_line_map = {
        str(row.get("line_id") or "").strip(): row
        for row in script_lines
        if isinstance(row, dict) and str(row.get("line_id") or "").strip()
    }

    normalized_by_script: dict[str, SceneLinePlanV1] = {}
    for idx, row in enumerate(raw_lines):
        candidate_id = str(row.script_line_id or "").strip()
        if candidate_id not in script_line_map:
            if idx < len(script_line_ids):
                candidate_id = script_line_ids[idx]
            else:
                continue
        mode = "a_roll" if str(row.mode or "").strip().lower() == "a_roll" else "b_roll"
        a_roll = row.a_roll if isinstance(row.a_roll, ARollDirectionV1) else None
        b_roll = row.b_roll if isinstance(row.b_roll, BRollDirectionV1) else None
        script_line = script_line_map.get(candidate_id, {})
        text = _line_text(script_line)
        if mode == "a_roll" and not a_roll:
            a_roll = _default_a_roll_direction(text, ir.get("hook", {}))
            b_roll = None
        if mode == "b_roll" and not b_roll:
            b_roll = _default_b_roll_direction(text)
            a_roll = None
        evidence_ids = [str(v).strip() for v in (row.evidence_ids or []) if str(v or "").strip()]
        if not evidence_ids:
            evidence_ids = [str(v).strip() for v in (script_line.get("evidence_ids") or []) if str(v or "").strip()]
        source_script_line_id, beat_index, beat_text = _resolve_lineage(
            candidate_id=candidate_id,
            row_source_script_line_id=str(row.source_script_line_id or "").strip(),
            row_beat_index=int(row.beat_index or 1) if row.beat_index is not None else None,
            script_line=script_line,
        )
        normalized_by_script[candidate_id] = SceneLinePlanV1(
            scene_line_id=_scene_line_id(ir["brief_unit_id"], ir["hook_id"], candidate_id),
            script_line_id=candidate_id,
            source_script_line_id=source_script_line_id,
            beat_index=beat_index,
            beat_text=str(row.beat_text or "").strip() or beat_text,
            mode=mode,
            a_roll=a_roll,
            b_roll=b_roll,
            on_screen_text=str(row.on_screen_text or "").strip(),
            duration_seconds=max(0.1, min(30.0, float(row.duration_seconds or 2.0))),
            evidence_ids=evidence_ids,
            difficulty_1_10=max(1, min(10, int(row.difficulty_1_10 or 5))),
        )

    out: list[SceneLinePlanV1] = []
    for line_id in script_line_ids:
        existing = normalized_by_script.get(line_id)
        if existing:
            out.append(existing)
            continue
        script_line = script_line_map.get(line_id, {})
        source_script_line_id, beat_index, beat_text = _resolve_lineage(
            candidate_id=line_id,
            row_source_script_line_id="",
            row_beat_index=None,
            script_line=script_line,
        )
        out.append(
            SceneLinePlanV1(
                scene_line_id=_scene_line_id(ir["brief_unit_id"], ir["hook_id"], line_id),
                script_line_id=line_id,
                source_script_line_id=source_script_line_id,
                beat_index=beat_index,
                beat_text=beat_text,
                mode="a_roll",
                a_roll=_default_a_roll_direction(_line_text(script_line), ir.get("hook", {})),
                b_roll=None,
                on_screen_text="",
                duration_seconds=2.0,
                evidence_ids=[str(v).strip() for v in (script_line.get("evidence_ids") or []) if str(v or "").strip()],
                difficulty_1_10=5,
            )
        )
    return out


def _build_scene_plan_from_lines(*, ir: dict[str, Any], lines: list[SceneLinePlanV1], status: str = "ok", error: str = "") -> ScenePlanV1:
    total_duration, a_roll_count, b_roll_count, max_consecutive = _sequence_metrics(lines)
    return ScenePlanV1(
        scene_plan_id=str(ir["scene_plan_id"]),
        run_id=str(ir["run_id"]),
        brief_unit_id=str(ir["brief_unit_id"]),
        arm=str(ir["arm"]),
        hook_id=str(ir["hook_id"]),
        lines=lines,
        total_duration_seconds=total_duration,
        a_roll_line_count=a_roll_count,
        b_roll_line_count=b_roll_count,
        max_consecutive_mode=max_consecutive,
        status=status,
        stale=False,
        stale_reason="",
        error=error,
        generated_at=datetime.now().isoformat(),
    )


def _deterministic_scene_plan(ir: dict[str, Any]) -> ScenePlanV1:
    script_lines = ir.get("script_lines", []) if isinstance(ir.get("script_lines"), list) else []
    drafted: list[_SceneDraftLineModel] = []
    for idx, row in enumerate(script_lines):
        line_id = str(row.get("line_id") or f"L{idx + 1:02d}")
        mode = "a_roll" if idx % 2 == 0 else "b_roll"
        text = _line_text(row)
        if mode == "a_roll":
            drafted.append(
                _SceneDraftLineModel(
                    script_line_id=line_id,
                    mode=mode,
                    a_roll=_default_a_roll_direction(text, ir.get("hook", {})),
                    on_screen_text="",
                    duration_seconds=2.2,
                    evidence_ids=[str(v).strip() for v in (row.get("evidence_ids") or []) if str(v or "").strip()],
                    difficulty_1_10=5,
                )
            )
        else:
            drafted.append(
                _SceneDraftLineModel(
                    script_line_id=line_id,
                    mode=mode,
                    b_roll=_default_b_roll_direction(text),
                    on_screen_text="",
                    duration_seconds=1.8,
                    evidence_ids=[str(v).strip() for v in (row.get("evidence_ids") or []) if str(v or "").strip()],
                    difficulty_1_10=5,
                )
            )

    normalized = _normalize_scene_lines(drafted, ir)
    return _build_scene_plan_from_lines(ir=ir, lines=normalized)


def draft_scene_plan(*, ir: dict[str, Any], model_overrides: dict[str, Any] | None = None) -> ScenePlanV1:
    model_name = _model_override_name(
        dict(model_overrides or {}),
        "scene_writer_draft",
        str(config.PHASE3_V2_SCENE_MODEL_DRAFT),
    )
    system_prompt = (
        "You are a UGC scene director creating beat-by-beat scene plans.\n"
        "Return one scene line for every script beat line exactly once.\n"
        "Each scene line must choose mode='a_roll' or mode='b_roll'.\n"
        "A-roll means talking-head performance direction. B-roll means visual cutaway direction.\n"
        "Creative visuals are allowed, including 3D animation, motion graphics, VFX, and metaphor visuals.\n"
        "Preserve script intent and do not alter script_line_id values.\n"
        "Never invent claims outside evidence context."
    )
    user_payload = {
        "scene_constraints": ir,
        "requirements": {
            "cover_all_script_lines_exactly_once": True,
            "explicit_mode_per_line": True,
            "min_a_roll_lines": int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
            "preserve_lineage_fields": True,
        },
    }
    user_prompt = f"Draft the full scene plan as JSON only.\n{json.dumps(user_payload, ensure_ascii=True)}"

    usage_before = len(get_usage_log())
    try:
        generated, _usage = call_claude_agent_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_SceneDraftBatchModel,
            model=model_name,
            allowed_tools=[],
            max_turns=6,
            max_thinking_tokens=8_000,
            timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
            return_usage=True,
        )
        _ = _usage
        lines = _normalize_scene_lines(list(generated.lines or []), ir)
        plan = _build_scene_plan_from_lines(ir=ir, lines=lines)
        logger.info(
            "Scene draft complete: brief_unit_id=%s hook_id=%s lines=%d cost=$%.4f",
            ir.get("brief_unit_id"),
            ir.get("hook_id"),
            len(lines),
            _usage_delta(usage_before),
        )
        return plan
    except Exception:
        logger.exception("Scene draft failed; using deterministic fallback for %s/%s", ir.get("brief_unit_id"), ir.get("hook_id"))
        return _deterministic_scene_plan(ir)


def _line_mode_has_direction(line: SceneLinePlanV1) -> bool:
    if line.mode == "a_roll":
        return bool(line.a_roll)
    if line.mode == "b_roll":
        return bool(line.b_roll)
    return False


def _line_direction_text(line: SceneLinePlanV1) -> str:
    parts: list[str] = [str(line.on_screen_text or "").strip()]
    if line.a_roll:
        parts.extend(
            [
                str(line.a_roll.framing or ""),
                str(line.a_roll.creator_action or ""),
                str(line.a_roll.performance_direction or ""),
                str(line.a_roll.product_interaction or ""),
                str(line.a_roll.location or ""),
            ]
        )
    if line.b_roll:
        parts.extend(
            [
                str(line.b_roll.shot_description or ""),
                str(line.b_roll.subject_action or ""),
                str(line.b_roll.camera_motion or ""),
                str(line.b_roll.props_assets or ""),
                str(line.b_roll.transition_intent or ""),
            ]
        )
    return " ".join(v for v in parts if v)


def evaluate_scene_gates(*, scene_plan: ScenePlanV1, ir: dict[str, Any], repaired_rounds: int = 0, post_polish: bool = False) -> SceneGateReportV1:
    script_lines = ir.get("script_lines", []) if isinstance(ir.get("script_lines"), list) else []
    expected_ids = [str(row.get("line_id") or "").strip() for row in script_lines if str(row.get("line_id") or "").strip()]
    expected_ids = list(dict.fromkeys(expected_ids))
    evidence_allowed = {
        str(row.get("line_id") or "").strip(): {str(v).strip() for v in (row.get("evidence_ids") or []) if str(v or "").strip()}
        for row in script_lines
        if isinstance(row, dict) and str(row.get("line_id") or "").strip()
    }

    seen_counts: dict[str, int] = {}
    for line in scene_plan.lines:
        sid = str(line.script_line_id or "").strip()
        if sid:
            seen_counts[sid] = seen_counts.get(sid, 0) + 1

    line_coverage_pass = (
        set(seen_counts.keys()) == set(expected_ids)
        and all(seen_counts.get(line_id, 0) == 1 for line_id in expected_ids)
    )

    mode_pass = all(str(line.mode) in {"a_roll", "b_roll"} and _line_mode_has_direction(line) for line in scene_plan.lines)
    ugc_pass = sum(1 for line in scene_plan.lines if line.mode == "a_roll") >= int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES)

    evidence_pass = True
    failing_line_ids: list[str] = []
    for line in scene_plan.lines:
        sid = str(line.script_line_id or "").strip()
        allowed = evidence_allowed.get(sid, set())
        line_evidence = {str(v).strip() for v in (line.evidence_ids or []) if str(v or "").strip()}
        if not line_evidence or not line_evidence.issubset(allowed):
            evidence_pass = False
            failing_line_ids.append(sid)

    claim_safety_pass = True
    for line in scene_plan.lines:
        if _UNSUPPORTED_CLAIM_RE.search(_line_direction_text(line)):
            claim_safety_pass = False
            failing_line_ids.append(str(line.script_line_id or "").strip())
    source_script_lines = (
        ir.get("source_script_lines", [])
        if isinstance(ir.get("source_script_lines"), list)
        else []
    )
    source_ids = [
        str(row.get("line_id") or "").strip()
        for row in source_script_lines
        if isinstance(row, dict) and str(row.get("line_id") or "").strip()
    ]
    source_count = len(list(dict.fromkeys(source_ids))) or len(expected_ids)
    scene_count_cap = max(1, int(math.ceil(float(source_count) * 2.0)))
    scene_count_pass = len(scene_plan.lines) <= scene_count_cap

    _, _, _, max_consecutive = _sequence_metrics(scene_plan.lines)
    pacing_pass = max_consecutive <= int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE)

    quality_metadata: dict[str, Any] = {
        "provider": "openai",
        "model": _model_override_name({}, "scene_writer_gate", str(config.PHASE3_V2_SCENE_MODEL_GATE)),
        "scene_count_cap": scene_count_cap,
        "scene_count_actual": len(scene_plan.lines),
    }
    try:
        eval_model = _model_override_name({}, "scene_writer_gate", str(config.PHASE3_V2_SCENE_MODEL_GATE))
        evaluator = call_llm_structured(
            system_prompt=(
                "Score persuasion and coherence for a scene plan. "
                "Return numeric scores only; do not enforce pass/fail."
            ),
            user_prompt=json.dumps(
                {
                    "awareness_level": ir.get("awareness_level"),
                    "emotion_key": ir.get("emotion_key"),
                    "hook": ir.get("hook", {}),
                    "scene_plan": scene_plan.model_dump(),
                },
                ensure_ascii=True,
            ),
            response_model=_SceneQualityEvalModel,
            provider="openai",
            model=eval_model,
            temperature=0.2,
            max_tokens=2_000,
        )
        quality_metadata.update(
            {
                "persuasion_score": int(evaluator.persuasion_score),
                "coherence_score": int(evaluator.coherence_score),
                "quality_rationale": str(evaluator.rationale or "").strip(),
            }
        )
    except Exception:
        pass

    failure_reasons: list[str] = []
    if not line_coverage_pass:
        failure_reasons.append("line_coverage_failed")
    if not mode_pass:
        failure_reasons.append("mode_missing_or_direction_missing")
    if not ugc_pass:
        failure_reasons.append("ugc_min_a_roll_failed")
    if not evidence_pass:
        failure_reasons.append("evidence_subset_failed")
    if not claim_safety_pass:
        failure_reasons.append("claim_safety_failed")
    if not scene_count_pass:
        failure_reasons.append("scene_count_excessive")
    if not pacing_pass:
        failure_reasons.append("pacing_failed")

    overall_pass = not failure_reasons
    post_polish_pass = overall_pass if post_polish else not failure_reasons

    return SceneGateReportV1(
        scene_plan_id=scene_plan.scene_plan_id,
        scene_unit_id=_scene_unit_id(scene_plan.brief_unit_id, scene_plan.hook_id),
        run_id=scene_plan.run_id,
        brief_unit_id=scene_plan.brief_unit_id,
        arm=scene_plan.arm,
        hook_id=scene_plan.hook_id,
        line_coverage_pass=line_coverage_pass,
        mode_pass=mode_pass,
        ugc_pass=ugc_pass,
        evidence_pass=evidence_pass,
        claim_safety_pass=claim_safety_pass,
        pacing_pass=pacing_pass,
        post_polish_pass=post_polish_pass,
        overall_pass=overall_pass,
        failure_reasons=list(dict.fromkeys([reason for reason in failure_reasons if reason])),
        failing_line_ids=list(dict.fromkeys([sid for sid in failing_line_ids if sid])),
        repair_rounds_used=max(0, int(repaired_rounds or 0)),
        evaluated_at=datetime.now().isoformat(),
        evaluator_metadata=quality_metadata,
    )


def _deterministic_repair_line(line: SceneLinePlanV1, ir: dict[str, Any]) -> SceneLinePlanV1:
    script_line_map = {
        str(row.get("line_id") or "").strip(): row
        for row in (ir.get("script_lines", []) if isinstance(ir.get("script_lines"), list) else [])
        if isinstance(row, dict)
    }
    script_row = script_line_map.get(str(line.script_line_id or "").strip(), {})
    mode = line.mode if str(line.mode or "") in {"a_roll", "b_roll"} else "a_roll"
    if mode == "a_roll":
        return line.model_copy(
            update={
                "a_roll": line.a_roll or _default_a_roll_direction(_line_text(script_row), ir.get("hook", {})),
                "b_roll": None,
                "difficulty_1_10": max(1, min(10, int(line.difficulty_1_10 or 5))),
                "evidence_ids": [
                    str(v).strip() for v in (script_row.get("evidence_ids") or []) if str(v or "").strip()
                ][:2],
            }
        )
    return line.model_copy(
        update={
            "a_roll": None,
            "b_roll": line.b_roll or _default_b_roll_direction(_line_text(script_row)),
            "difficulty_1_10": max(1, min(10, int(line.difficulty_1_10 or 5))),
            "evidence_ids": [
                str(v).strip() for v in (script_row.get("evidence_ids") or []) if str(v or "").strip()
            ][:2],
        }
    )


def repair_scene_plan(
    *,
    scene_plan: ScenePlanV1,
    gate_report: SceneGateReportV1,
    ir: dict[str, Any],
    model_overrides: dict[str, Any] | None = None,
) -> ScenePlanV1:
    failing_ids = set(gate_report.failing_line_ids or [])
    if not failing_ids:
        return scene_plan

    model_name = _model_override_name(
        dict(model_overrides or {}),
        "scene_writer_repair",
        str(config.PHASE3_V2_SCENE_MODEL_REPAIR),
    )
    failing_lines = [line.model_dump() for line in scene_plan.lines if str(line.script_line_id or "").strip() in failing_ids]
    payload = {
        "constraints": ir,
        "failing_lines": failing_lines,
        "failure_reasons": gate_report.failure_reasons,
        "instructions": (
            "Rewrite only failing lines. Prioritize beat coverage + evidence validity first, "
            "then mode quality. Keep script_line_id and lineage fields stable."
        ),
    }

    repaired_lines_map: dict[str, SceneLinePlanV1] = {}
    usage_before = len(get_usage_log())
    try:
        repaired, _usage = call_claude_agent_structured(
            system_prompt=(
                "You repair failing scene lines only. "
                "Return JSON lines array for those failing script_line_id entries."
            ),
            user_prompt=json.dumps(payload, ensure_ascii=True),
            response_model=_SceneRepairBatchModel,
            model=model_name,
            allowed_tools=[],
            max_turns=5,
            max_thinking_tokens=6_000,
            timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
            return_usage=True,
        )
        _ = _usage
        normalized = _normalize_scene_lines(list(repaired.lines or []), ir)
        repaired_lines_map = {str(row.script_line_id): row for row in normalized}
    except Exception:
        logger.exception(
            "Scene repair failed for brief_unit_id=%s hook_id=%s; using deterministic repair",
            scene_plan.brief_unit_id,
            scene_plan.hook_id,
        )
    finally:
        logger.info(
            "Scene repair complete: brief_unit_id=%s hook_id=%s failing=%d cost=$%.4f",
            scene_plan.brief_unit_id,
            scene_plan.hook_id,
            len(failing_ids),
            _usage_delta(usage_before),
        )

    updated_lines: list[SceneLinePlanV1] = []
    for line in scene_plan.lines:
        sid = str(line.script_line_id or "").strip()
        if sid in repaired_lines_map:
            updated_lines.append(repaired_lines_map[sid])
        elif sid in failing_ids:
            updated_lines.append(_deterministic_repair_line(line, ir))
        else:
            updated_lines.append(line)

    return _build_scene_plan_from_lines(ir=ir, lines=updated_lines, status="needs_repair")


def polish_scene_plan(
    *,
    scene_plan: ScenePlanV1,
    ir: dict[str, Any],
    model_overrides: dict[str, Any] | None = None,
) -> ScenePlanV1:
    model_name = _model_override_name(
        dict(model_overrides or {}),
        "scene_writer_polish",
        str(config.PHASE3_V2_SCENE_MODEL_POLISH),
    )
    payload = {
        "constraints": ir,
        "scene_plan": scene_plan.model_dump(),
        "instructions": "Polish wording and pacing only; keep line coverage, IDs, lineage, and evidence IDs stable.",
    }

    usage_before = len(get_usage_log())
    try:
        polished, _usage = call_claude_agent_structured(
            system_prompt="Polish a scene plan without changing structure coverage.",
            user_prompt=json.dumps(payload, ensure_ascii=True),
            response_model=_SceneDraftBatchModel,
            model=model_name,
            allowed_tools=[],
            max_turns=4,
            max_thinking_tokens=4_000,
            timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
            return_usage=True,
        )
        _ = _usage
        lines = _normalize_scene_lines(list(polished.lines or []), ir)
        logger.info(
            "Scene polish complete: brief_unit_id=%s hook_id=%s cost=$%.4f",
            scene_plan.brief_unit_id,
            scene_plan.hook_id,
            _usage_delta(usage_before),
        )
        return _build_scene_plan_from_lines(ir=ir, lines=lines, status=scene_plan.status)
    except Exception:
        logger.exception(
            "Scene polish skipped due to failure for brief_unit_id=%s hook_id=%s",
            scene_plan.brief_unit_id,
            scene_plan.hook_id,
        )
        return scene_plan


def run_scene_unit(
    *,
    run_id: str,
    scene_run_id: str,
    scene_item: dict[str, Any],
    model_overrides: dict[str, Any] | None = None,
) -> _SceneUnitResult:
    _ = scene_run_id
    started = time.time()
    brief_unit_id = str(scene_item.get("brief_unit_id") or "")
    hook_id = str(scene_item.get("hook_id") or "")
    arm = str(scene_item.get("arm") or "claude_sdk")
    logger.info("Phase3 v2 scene unit start: arm=%s brief_unit_id=%s hook_id=%s", arm, brief_unit_id, hook_id)

    try:
        ir = compile_scene_constraints_ir(scene_item)
    except Exception as exc:
        fallback_ir = {
            "scene_plan_id": _scene_plan_id(brief_unit_id, hook_id, arm),
            "run_id": run_id,
            "brief_unit_id": brief_unit_id,
            "arm": arm,
            "hook_id": hook_id,
            "scene_unit_id": _scene_unit_id(brief_unit_id, hook_id),
        }
        plan = ScenePlanV1(
            scene_plan_id=fallback_ir["scene_plan_id"],
            run_id=run_id,
            brief_unit_id=brief_unit_id,
            arm=arm,
            hook_id=hook_id,
            status="error",
            error=str(exc),
            generated_at=datetime.now().isoformat(),
        )
        gate = SceneGateReportV1(
            scene_plan_id=plan.scene_plan_id,
            scene_unit_id=fallback_ir["scene_unit_id"],
            run_id=run_id,
            brief_unit_id=brief_unit_id,
            arm=arm,
            hook_id=hook_id,
            overall_pass=False,
            line_coverage_pass=False,
            mode_pass=False,
            ugc_pass=False,
            evidence_pass=False,
            claim_safety_pass=False,
            pacing_pass=False,
            post_polish_pass=False,
            failure_reasons=["constraint_compile_failed"],
            failing_line_ids=[],
            repair_rounds_used=0,
            evaluated_at=datetime.now().isoformat(),
        )
        return _SceneUnitResult(
            arm=arm,
            brief_unit_id=brief_unit_id,
            hook_id=hook_id,
            scene_unit_id=fallback_ir["scene_unit_id"],
            scene_plan=plan,
            gate_report=gate,
            elapsed_seconds=round(time.time() - started, 3),
            error=str(exc),
        )

    plan = draft_scene_plan(ir=ir, model_overrides=model_overrides)
    gate = evaluate_scene_gates(scene_plan=plan, ir=ir, repaired_rounds=0, post_polish=False)

    repair_rounds = 0
    max_repairs = int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS)
    while not gate.overall_pass and repair_rounds < max_repairs:
        repair_rounds += 1
        plan = repair_scene_plan(scene_plan=plan, gate_report=gate, ir=ir, model_overrides=model_overrides)
        gate = evaluate_scene_gates(scene_plan=plan, ir=ir, repaired_rounds=repair_rounds, post_polish=False)

    plan = polish_scene_plan(scene_plan=plan, ir=ir, model_overrides=model_overrides)
    gate = evaluate_scene_gates(scene_plan=plan, ir=ir, repaired_rounds=repair_rounds, post_polish=True)

    final_status = "ok" if gate.overall_pass else "error"
    plan = plan.model_copy(update={"status": final_status, "error": "" if gate.overall_pass else "scene_gate_failed"})

    elapsed = round(time.time() - started, 3)
    logger.info(
        "Phase3 v2 scene unit done: arm=%s brief_unit_id=%s hook_id=%s status=%s latency=%.3fs",
        arm,
        brief_unit_id,
        hook_id,
        final_status,
        elapsed,
    )
    return _SceneUnitResult(
        arm=arm,
        brief_unit_id=brief_unit_id,
        hook_id=hook_id,
        scene_unit_id=str(ir["scene_unit_id"]),
        scene_plan=plan,
        gate_report=gate,
        elapsed_seconds=elapsed,
        error="" if gate.overall_pass else plan.error,
    )


def _build_production_handoff_packet(
    *,
    run_id: str,
    scene_run_id: str,
    scene_items: list[dict[str, Any]],
    results: list[_SceneUnitResult],
) -> ProductionHandoffPacketV1:
    result_map_by_pair = {
        (row.brief_unit_id, row.arm): row
        for row in results
    }
    result_map_by_hook = {
        (row.brief_unit_id, row.arm, row.hook_id): row
        for row in results
    }

    units: list[ProductionHandoffUnitV1] = []
    ready_count = 0
    for item in scene_items:
        brief_unit_id = str(item.get("brief_unit_id") or "").strip()
        arm = str(item.get("arm") or "claude_sdk").strip()
        hook_id = str(item.get("hook_id") or "").strip()
        selected_hook_ids = [
            str(v).strip()
            for v in (item.get("selected_hook_ids", []) if isinstance(item.get("selected_hook_ids"), list) else [])
            if str(v or "").strip()
        ]
        if not selected_hook_ids and hook_id:
            selected_hook_ids = [hook_id]
        result = result_map_by_pair.get((brief_unit_id, arm)) or result_map_by_hook.get((brief_unit_id, arm, hook_id))
        resolved_hook_id = str(result.scene_plan.hook_id if result else hook_id).strip()
        if resolved_hook_id and resolved_hook_id not in selected_hook_ids:
            selected_hook_ids = [resolved_hook_id, *selected_hook_ids]
        scene_unit_id = _scene_unit_id(brief_unit_id, resolved_hook_id or hook_id)
        if not result:
            units.append(
                ProductionHandoffUnitV1(
                    scene_unit_id=scene_unit_id,
                    run_id=run_id,
                    brief_unit_id=brief_unit_id,
                    arm=arm,
                    hook_id=resolved_hook_id,
                    selected_hook_ids=selected_hook_ids,
                    selected_hook_id=selected_hook_ids[0] if selected_hook_ids else "",
                    status="missing",
                )
            )
            continue

        status = "ready" if result.gate_report.overall_pass and not result.scene_plan.stale else "failed"
        if result.scene_plan.stale:
            status = "stale"
        if status == "ready":
            ready_count += 1

        units.append(
            ProductionHandoffUnitV1(
                scene_unit_id=result.scene_unit_id,
                scene_plan_id=result.scene_plan.scene_plan_id,
                run_id=run_id,
                brief_unit_id=brief_unit_id,
                arm=arm,
                hook_id=resolved_hook_id,
                selected_hook_ids=selected_hook_ids,
                selected_hook_id=selected_hook_ids[0] if selected_hook_ids else "",
                status=status,
                stale=bool(result.scene_plan.stale),
                stale_reason=str(result.scene_plan.stale_reason or ""),
                lines=list(result.scene_plan.lines or []),
                gate_report=result.gate_report,
            )
        )

    total_required = len(scene_items)
    return ProductionHandoffPacketV1(
        run_id=run_id,
        scene_run_id=scene_run_id,
        ready=(total_required > 0 and ready_count == total_required),
        ready_count=ready_count,
        total_required=total_required,
        generated_at=datetime.now().isoformat(),
        items=units,
        metrics={
            "failed": sum(1 for row in units if row.status == "failed"),
            "stale": sum(1 for row in units if row.status == "stale"),
            "missing": sum(1 for row in units if row.status == "missing"),
        },
    )


def run_phase3_v2_scenes(
    *,
    run_id: str,
    scene_run_id: str,
    scene_items: list[dict[str, Any]],
    model_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_parallel = max(1, int(config.PHASE3_V2_SCENE_MAX_PARALLEL))
    started = time.time()

    manifest_seed = {
        "run_id": run_id,
        "scene_run_id": scene_run_id,
        "status": "running",
        "created_at": datetime.now().isoformat(),
        "started_at": datetime.now().isoformat(),
        "eligible_count": len(scene_items),
        "max_parallel": max_parallel,
        "max_repair_rounds": int(config.PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS),
        "max_consecutive_mode": int(config.PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE),
        "min_a_roll_lines": int(config.PHASE3_V2_SCENE_MIN_A_ROLL_LINES),
        "model_registry": {
            "scene_writer_draft": _model_override_name(
                dict(model_overrides or {}),
                "scene_writer_draft",
                str(config.PHASE3_V2_SCENE_MODEL_DRAFT),
            ),
            "scene_writer_repair": _model_override_name(
                dict(model_overrides or {}),
                "scene_writer_repair",
                str(config.PHASE3_V2_SCENE_MODEL_REPAIR),
            ),
            "scene_writer_polish": _model_override_name(
                dict(model_overrides or {}),
                "scene_writer_polish",
                str(config.PHASE3_V2_SCENE_MODEL_POLISH),
            ),
            "scene_writer_gate": _model_override_name(
                dict(model_overrides or {}),
                "scene_writer_gate",
                str(config.PHASE3_V2_SCENE_MODEL_GATE),
            ),
        },
    }

    if not scene_items:
        manifest = SceneStageManifestV1.model_validate(
            {
                **manifest_seed,
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "processed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "stale_count": 0,
                "metrics": {"elapsed_seconds": round(time.time() - started, 3)},
            }
        )
        packet = ProductionHandoffPacketV1(
            run_id=run_id,
            scene_run_id=scene_run_id,
            ready=False,
            ready_count=0,
            total_required=0,
            generated_at=datetime.now().isoformat(),
            items=[],
            metrics={},
        )
        return {
            "scene_stage_manifest": manifest.model_dump(),
            "scene_plans_by_arm": {},
            "scene_gate_reports_by_arm": {},
            "production_handoff_packet": packet.model_dump(),
        }

    workers = min(max_parallel, len(scene_items))
    logger.info("Phase3 v2 scene stage parallel mode: workers=%d eligible=%d", workers, len(scene_items))

    ordered: list[_SceneUnitResult | None] = [None] * len(scene_items)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="p3v2-scene") as pool:
        future_map: dict[Any, int] = {}
        for idx, item in enumerate(scene_items):
            logger.info(
                "Phase3 v2 scene unit queued: arm=%s brief_unit_id=%s hook_id=%s queue_index=%d/%d",
                item.get("arm"),
                item.get("brief_unit_id"),
                item.get("hook_id"),
                idx + 1,
                len(scene_items),
            )
            future = pool.submit(
                run_scene_unit,
                run_id=run_id,
                scene_run_id=scene_run_id,
                scene_item=item,
                model_overrides=model_overrides,
            )
            future_map[future] = idx

        for future in as_completed(future_map):
            idx = future_map[future]
            item = scene_items[idx]
            try:
                result = future.result()
            except Exception as exc:
                logger.exception("Phase3 v2 scene unit failed unexpectedly")
                arm = str(item.get("arm") or "claude_sdk")
                brief_unit_id = str(item.get("brief_unit_id") or "")
                hook_id = str(item.get("hook_id") or "")
                scene_unit_id = _scene_unit_id(brief_unit_id, hook_id)
                plan = ScenePlanV1(
                    scene_plan_id=_scene_plan_id(brief_unit_id, hook_id, arm),
                    run_id=run_id,
                    brief_unit_id=brief_unit_id,
                    arm=arm,
                    hook_id=hook_id,
                    status="error",
                    error=str(exc),
                    generated_at=datetime.now().isoformat(),
                )
                gate = SceneGateReportV1(
                    scene_plan_id=plan.scene_plan_id,
                    scene_unit_id=scene_unit_id,
                    run_id=run_id,
                    brief_unit_id=brief_unit_id,
                    arm=arm,
                    hook_id=hook_id,
                    line_coverage_pass=False,
                    mode_pass=False,
                    ugc_pass=False,
                    evidence_pass=False,
                    claim_safety_pass=False,
                    pacing_pass=False,
                    post_polish_pass=False,
                    overall_pass=False,
                    failure_reasons=["unit_runtime_failure"],
                    failing_line_ids=[],
                    repair_rounds_used=0,
                    evaluated_at=datetime.now().isoformat(),
                )
                result = _SceneUnitResult(
                    arm=arm,
                    brief_unit_id=brief_unit_id,
                    hook_id=hook_id,
                    scene_unit_id=scene_unit_id,
                    scene_plan=plan,
                    gate_report=gate,
                    elapsed_seconds=0.0,
                    error=str(exc),
                )
            ordered[idx] = result
            logger.info(
                "Phase3 v2 scene unit collected: arm=%s brief_unit_id=%s hook_id=%s completed=%d/%d",
                result.arm,
                result.brief_unit_id,
                result.hook_id,
                sum(1 for row in ordered if row is not None),
                len(ordered),
            )

    results = [row for row in ordered if row is not None]

    plans_by_arm: dict[str, list[dict[str, Any]]] = {}
    gates_by_arm: dict[str, list[dict[str, Any]]] = {}
    failed = 0
    stale = 0
    for row in results:
        plans_by_arm.setdefault(row.arm, []).append(row.scene_plan.model_dump())
        gates_by_arm.setdefault(row.arm, []).append(row.gate_report.model_dump())
        if row.scene_plan.status == "error":
            failed += 1
        if row.scene_plan.stale:
            stale += 1

    handoff_packet = _build_production_handoff_packet(
        run_id=run_id,
        scene_run_id=scene_run_id,
        scene_items=scene_items,
        results=results,
    )

    manifest = SceneStageManifestV1.model_validate(
        {
            **manifest_seed,
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "processed_count": len(results),
            "failed_count": failed,
            "skipped_count": 0,
            "stale_count": stale,
            "metrics": {
                "elapsed_seconds": round(time.time() - started, 3),
                "scene_plan_count_total": sum(len(v) for v in plans_by_arm.values()),
                "handoff_ready": bool(handoff_packet.ready),
            },
        }
    )

    return {
        "scene_stage_manifest": manifest.model_dump(),
        "scene_plans_by_arm": plans_by_arm,
        "scene_gate_reports_by_arm": gates_by_arm,
        "production_handoff_packet": handoff_packet.model_dump(),
    }
