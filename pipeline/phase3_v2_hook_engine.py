"""Phase 3 v2 Hook Generator engine (Milestone 2).

Implements the council-selected pattern:
Diverge -> Gate/Score -> Repair -> Rank
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
    ArmName,
    BriefUnitV1,
    CoreScriptDraftV1,
    EvidencePackV1,
    HookBundleV1,
    HookCandidateV1,
    HookContextV1,
    HookGateResultV1,
    HookScoreV1,
    HookStageManifestV1,
    HookVariantV1,
)

logger = logging.getLogger(__name__)


_LANE_LIBRARY: list[dict[str, str]] = [
    {"lane_id": "pattern_interrupt", "lane_label": "Pattern Interrupt", "focus": "unexpected opening frame"},
    {"lane_id": "identity_callout", "lane_label": "Identity Callout", "focus": "call out who this is for"},
    {"lane_id": "pain_spike", "lane_label": "Pain Spike", "focus": "name the pain with specificity"},
    {"lane_id": "myth_bust", "lane_label": "Myth Bust", "focus": "counter a common assumption"},
    {"lane_id": "mechanism_reveal", "lane_label": "Mechanism Reveal", "focus": "show why this works"},
    {"lane_id": "social_proof", "lane_label": "Social Proof", "focus": "credibility and trust"},
    {"lane_id": "urgency_window", "lane_label": "Urgency Window", "focus": "time pressure without hype"},
    {"lane_id": "contrast", "lane_label": "Before/After Contrast", "focus": "compare current vs improved state"},
]

_WORD_RE = re.compile(r"[a-z0-9']+")
_META_COPY_TERM_RE = re.compile(
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
_META_SUMMARY_LEADIN_RE = re.compile(
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
_META_SUMMARY_PHRASE_RE = re.compile(
    r"(\bimmediately\s+signaling\b|\bsignaling\s+this\s+is\b|\bthis\s+is\s+a\s+different\s+kind\s+of\s+fix\b)",
    re.IGNORECASE,
)


def _safe_lane(value: str) -> str:
    lane = str(value or "").strip().lower()
    lane = re.sub(r"[^a-z0-9]+", "_", lane)
    lane = re.sub(r"_+", "_", lane).strip("_")
    return lane or "lane"


def _usage_delta(start_index: int) -> float:
    try:
        log = get_usage_log()
    except Exception:
        return 0.0
    if start_index < 0 or start_index >= len(log):
        return 0.0
    return round(float(sum(float(entry.get("cost", 0.0) or 0.0) for entry in log[start_index:])), 6)


def _token_set(text: str) -> set[str]:
    return set(_WORD_RE.findall(str(text or "").lower()))


def _similarity(a: str, b: str) -> float:
    ta = _token_set(a)
    tb = _token_set(b)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    overlap = len(ta.intersection(tb))
    union = len(ta.union(tb))
    return overlap / max(1, union)


def _candidate_text(candidate: HookCandidateV1) -> str:
    return " ".join(
        [
            str(candidate.verbal_open or "").strip(),
            str(candidate.visual_pattern_interrupt or "").strip(),
            str(candidate.on_screen_text or "").strip(),
        ]
    ).strip()


def _contains_meta_copy_terms(text: str) -> bool:
    value = str(text or "")
    return bool(
        _META_COPY_TERM_RE.search(value)
        or _META_SUMMARY_LEADIN_RE.search(value)
        or _META_SUMMARY_PHRASE_RE.search(value)
    )


def _evidence_catalog(evidence_pack: EvidencePackV1) -> tuple[list[str], dict[str, str]]:
    allowed_ids: list[str] = []
    catalog: dict[str, str] = {}

    for ref in evidence_pack.voc_quote_refs:
        eid = str(ref.quote_id or "").strip()
        if not eid:
            continue
        allowed_ids.append(eid)
        catalog[eid] = str(ref.quote_excerpt or "").strip()
    for ref in evidence_pack.proof_refs:
        eid = str(ref.asset_id or "").strip()
        if not eid:
            continue
        allowed_ids.append(eid)
        catalog[eid] = " ".join(
            [
                str(ref.title or "").strip(),
                str(ref.detail or "").strip(),
            ]
        ).strip()
    for ref in evidence_pack.mechanism_refs:
        eid = str(ref.mechanism_id or "").strip()
        if eid:
            allowed_ids.append(eid)
            catalog[eid] = " ".join([str(ref.title or "").strip(), str(ref.detail or "").strip()]).strip()
        for support_id in ref.support_evidence_ids or []:
            sid = str(support_id or "").strip()
            if not sid:
                continue
            allowed_ids.append(sid)
            catalog.setdefault(sid, "")

    deduped = sorted({v for v in allowed_ids if v})
    return deduped, catalog


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


def build_hook_context(
    *,
    run_id: str,
    brief_unit: BriefUnitV1,
    arm: ArmName,
    draft: CoreScriptDraftV1,
    evidence_pack: EvidencePackV1,
) -> HookContextV1:
    allowed_ids, catalog = _evidence_catalog(evidence_pack)
    return HookContextV1(
        run_id=run_id,
        brief_unit_id=brief_unit.brief_unit_id,
        arm=arm,
        awareness_level=brief_unit.awareness_level,
        emotion_key=brief_unit.emotion_key,
        emotion_label=brief_unit.emotion_label,
        audience_segment_name=brief_unit.audience_segment_name,
        audience_goals=list(brief_unit.audience_goals or []),
        audience_pains=list(brief_unit.audience_pains or []),
        audience_triggers=list(brief_unit.audience_triggers or []),
        audience_objections=list(brief_unit.audience_objections or []),
        audience_information_sources=list(brief_unit.audience_information_sources or []),
        lf8_code=str(brief_unit.lf8_code or "").strip(),
        lf8_label=str(brief_unit.lf8_label or "").strip(),
        emotion_angle=str(brief_unit.emotion_angle or "").strip(),
        blocking_objection=str(brief_unit.blocking_objection or "").strip(),
        required_proof=str(brief_unit.required_proof or "").strip(),
        confidence=float(brief_unit.confidence or 0.0),
        sample_quote_ids=[str(v).strip() for v in (brief_unit.sample_quote_ids or []) if str(v or "").strip()],
        script_id=str(draft.script_id or ""),
        script_sections=draft.sections,
        script_lines=list(draft.lines or []),
        evidence_ids_allowed=allowed_ids,
        evidence_catalog=catalog,
    )


def _lane_plan(candidate_target_per_unit: int) -> list[dict[str, Any]]:
    target = max(1, int(candidate_target_per_unit or 1))
    lanes = list(_LANE_LIBRARY)
    per_lane = max(1, math.ceil(target / max(1, len(lanes))))
    return [
        {
            **lane,
            "variant_target": per_lane,
        }
        for lane in lanes
    ]


def _hook_variant_targets(final_variants_per_unit: int, include_default_anchor: bool) -> tuple[int, int]:
    min_new = max(1, int(getattr(config, "PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 4) or 1))
    target_new = max(min_new, int(final_variants_per_unit or 0), 1)
    target_total = target_new + (1 if include_default_anchor else 0)
    return target_new, target_total


def _build_candidate_id(brief_unit_id: str, lane_id: str, lane_counts: dict[str, int]) -> str:
    safe_lane = _safe_lane(lane_id)
    next_n = lane_counts.get(safe_lane, 0) + 1
    lane_counts[safe_lane] = next_n
    return f"hc_{brief_unit_id}_{safe_lane}_{next_n:03d}"


class _GeneratedHookCandidateModel(BaseModel):
    lane_id: str
    lane_label: str = ""
    verbal_open: str
    visual_pattern_interrupt: str = ""
    on_screen_text: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    rationale: str = ""


class _GeneratedHookBatchModel(BaseModel):
    candidates: list[_GeneratedHookCandidateModel] = Field(default_factory=list)


class _GateScoreItemModel(BaseModel):
    candidate_id: str
    alignment_pass: bool = False
    claim_boundary_pass: bool = False
    scroll_stop_score: int = Field(default=0, ge=0, le=100)
    specificity_score: int = Field(default=0, ge=0, le=100)
    rationale: str = ""


class _GateScoreBatchModel(BaseModel):
    evaluations: list[_GateScoreItemModel] = Field(default_factory=list)


class _RepairItemModel(BaseModel):
    candidate_id: str
    verbal_open: str
    visual_pattern_interrupt: str = ""
    on_screen_text: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    rationale: str = ""


class _RepairBatchModel(BaseModel):
    repaired: list[_RepairItemModel] = Field(default_factory=list)


def _default_script_hook_text(context: HookContextV1) -> str:
    # Prefer actual script lines over section summaries so default hooks stay natural.
    for row in context.script_lines or []:
        text = str(row.text or "").strip()
        if text and not _contains_meta_copy_terms(text):
            return text
    hook = str(context.script_sections.hook if context.script_sections else "").strip()
    if hook and not _contains_meta_copy_terms(hook):
        return hook
    if hook:
        return hook
    for row in context.script_lines or []:
        text = str(row.text or "").strip()
        if text:
            return text
    return ""


def _script_line_pool(context: HookContextV1) -> list[str]:
    pool: list[str] = []
    seen: set[str] = set()
    for row in context.script_lines or []:
        text = str(row.text or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        if _contains_meta_copy_terms(text):
            continue
        seen.add(key)
        pool.append(text)
    return pool


def _safe_verbal_fallback(context: HookContextV1, lane_id: str, fallback_index: int) -> str:
    pool = _script_line_pool(context)
    if pool:
        lane_hash = sum(ord(ch) for ch in str(lane_id or "lane"))
        idx = (lane_hash + max(0, int(fallback_index or 0))) % len(pool)
        return pool[idx]
    seed = _default_script_hook_text(context)
    if seed and not _contains_meta_copy_terms(seed):
        return seed
    section_hook = str(context.script_sections.hook if context.script_sections else "").strip()
    if section_hook and not _contains_meta_copy_terms(section_hook):
        return section_hook
    return "You should not have to end your session early because of strap pain."


def generate_candidates_divergent(
    *,
    context: HookContextV1,
    candidate_target_per_unit: int,
    model_overrides: dict[str, Any] | None = None,
) -> list[HookCandidateV1]:
    default_hook_verbal = _default_script_hook_text(context)
    lane_plan = _lane_plan(candidate_target_per_unit)
    model_name = _model_override_name(
        dict(model_overrides or {}),
        "hook_generator_generation",
        str(config.PHASE3_V2_HOOK_MODEL_GENERATION),
    )
    system_prompt = (
        "You are a world class Scroll Stopper.\n"
        "You understand the psychology to steal someone's attention and get them to stop scrolling on platforms like Meta, Instagram, and TikTok.\n"
        "You generate direct-response hook candidates for paid social ads.\n"
        "Create diverse hooks across the provided lanes.\n"
        "Each candidate must include verbal_open and at least one evidence_id.\n"
        "Output verbal copy only: do not include visual_pattern_interrupt or on_screen_text content (set both empty if fields exist).\n"
        "Always include one default hook anchor candidate using lane_id='script_default'.\n"
        "For that default anchor candidate: keep verbal_open exactly equal to the provided fixed text.\n"
        "Never include framework/meta labels in verbal_open (e.g. pattern interrupt/interupt, scroll stopper, myth bust, identity callout, CTA).\n"
        "Avoid cliches, generic claims, and hype."
    )
    payload = {
        "hook_context": context.model_dump(),
        "lane_plan": lane_plan,
        "candidate_target": int(candidate_target_per_unit),
        "requirements": {
            "must_match_awareness_emotion": True,
            "must_use_evidence_ids_from_context": True,
            "style": "scroll-stopping but credible",
            "output_mode": "verbal_only",
        },
        "default_hook_anchor": {
            "required": bool(default_hook_verbal),
            "lane_id": "script_default",
            "verbal_open_fixed": default_hook_verbal,
            "keep_verbal_exact": True,
        },
    }
    user_prompt = (
        "Generate hook candidates as JSON.\n"
        f"{json.dumps(payload, ensure_ascii=True, indent=2)}"
    )

    logger.info(
        "Hook generation start: brief_unit_id=%s arm=%s candidate_target=%d model=%s",
        context.brief_unit_id,
        context.arm,
        int(candidate_target_per_unit),
        model_name,
    )
    start = time.time()
    usage_before = len(get_usage_log())
    try:
        generated, usage_meta = call_claude_agent_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_GeneratedHookBatchModel,
            model=model_name,
            allowed_tools=[],
            max_turns=6,
            max_thinking_tokens=8_000,
            timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
            return_usage=True,
        )
        _ = usage_meta
    except Exception:
        logger.exception("Hook generation failed for %s", context.brief_unit_id)
        generated = _GeneratedHookBatchModel(candidates=[])
    finally:
        elapsed = round(time.time() - start, 3)
        logger.info(
            "Hook generation complete: brief_unit_id=%s candidates=%d latency=%.3fs cost=$%.4f",
            context.brief_unit_id,
            len(generated.candidates),
            elapsed,
            _usage_delta(usage_before),
        )

    lane_counts: dict[str, int] = {}
    anchor_evidence = context.evidence_ids_allowed[:1]
    anchor_index = -1
    if default_hook_verbal:
        for idx, raw in enumerate(generated.candidates):
            lane_id = _safe_lane(raw.lane_id or "")
            verbal = str(raw.verbal_open or "").strip()
            if lane_id == "script_default" or verbal == default_hook_verbal:
                anchor_index = idx
                break

    out: list[HookCandidateV1] = []

    if default_hook_verbal:
        raw_anchor = generated.candidates[anchor_index] if anchor_index >= 0 else None
        anchor_visual = ""
        anchor_on_screen = ""
        anchor_evidence_ids = [str(v).strip() for v in ((raw_anchor.evidence_ids if raw_anchor else []) or []) if str(v).strip()]
        if not anchor_evidence_ids:
            anchor_evidence_ids = anchor_evidence
        out.append(
            HookCandidateV1(
                candidate_id=_build_candidate_id(context.brief_unit_id, "script_default", lane_counts),
                brief_unit_id=context.brief_unit_id,
                arm=context.arm,
                lane_id="script_default",
                lane_label="Script Default",
                verbal_open=default_hook_verbal,
                visual_pattern_interrupt=anchor_visual,
                on_screen_text=anchor_on_screen,
                awareness_level=context.awareness_level,
                emotion_key=context.emotion_key,
                evidence_ids=anchor_evidence_ids,
                rationale=str(raw_anchor.rationale if raw_anchor else "").strip() or "Default script verbal anchor.",
                model_metadata={"provider": "anthropic", "model": model_name, "sdk_used": True, "default_anchor": True},
            )
        )

    for idx, raw in enumerate(generated.candidates):
        if idx == anchor_index:
            continue
        lane_id = _safe_lane(raw.lane_id or "lane")
        verbal = str(raw.verbal_open or "").strip()
        if not verbal:
            continue
        out.append(
            HookCandidateV1(
                candidate_id=_build_candidate_id(context.brief_unit_id, lane_id, lane_counts),
                brief_unit_id=context.brief_unit_id,
                arm=context.arm,
                lane_id=lane_id,
                lane_label=str(raw.lane_label or lane_id),
                verbal_open=verbal,
                visual_pattern_interrupt=str(raw.visual_pattern_interrupt or "").strip(),
                on_screen_text=str(raw.on_screen_text or "").strip(),
                awareness_level=context.awareness_level,
                emotion_key=context.emotion_key,
                evidence_ids=[
                    str(v).strip() for v in raw.evidence_ids if str(v).strip()
                ] or anchor_evidence,
                rationale=str(raw.rationale or "").strip(),
                model_metadata={"provider": "anthropic", "model": model_name, "sdk_used": True},
            )
        )

    target_total = max(1, int(candidate_target_per_unit or 1))
    if default_hook_verbal:
        _, guaranteed_total = _hook_variant_targets(
            int(candidate_target_per_unit or 1),
            include_default_anchor=True,
        )
        target_total = max(target_total, guaranteed_total)
    else:
        min_new = max(1, int(getattr(config, "PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 4) or 1))
        target_total = max(target_total, min_new)

    if not out:
        line_text = ""
        if context.script_lines:
            line_text = str(context.script_lines[0].text or "").strip()
        fallback_text = line_text or "Stop scrolling for one second."
        lane_counts = {}
        for lane in _LANE_LIBRARY[: min(max(4, target_total), len(_LANE_LIBRARY))]:
            lane_id = _safe_lane(lane["lane_id"])
            out.append(
                HookCandidateV1(
                    candidate_id=_build_candidate_id(context.brief_unit_id, lane_id, lane_counts),
                    brief_unit_id=context.brief_unit_id,
                    arm=context.arm,
                    lane_id=lane_id,
                    lane_label=lane["lane_label"],
                    verbal_open=fallback_text,
                    visual_pattern_interrupt="",
                    on_screen_text="",
                    awareness_level=context.awareness_level,
                    emotion_key=context.emotion_key,
                    evidence_ids=context.evidence_ids_allowed[:1],
                    rationale="Fallback candidate due to generation error.",
                    model_metadata={"provider": "fallback", "model": "deterministic", "sdk_used": False},
                )
            )
    if len(out) < target_total:
        seed_text = default_hook_verbal if not _contains_meta_copy_terms(default_hook_verbal) else ""
        if not seed_text and context.script_lines:
            seed_text = _safe_verbal_fallback(context, "top_up_seed", 0)
        seed_text = seed_text or f"{context.emotion_label or context.emotion_key} for {context.awareness_level}"
        suffixes = [
            "without gimmicks.",
            "before another session gets cut short.",
            "with specifics you can verify.",
            "from users who already felt this pain.",
            "so the first 3 seconds actually stop the scroll.",
            "using only claims tied to evidence.",
        ]
        existing_texts = {_candidate_text(row).lower() for row in out}
        idx = 0
        while len(out) < target_total and idx < 48:
            lane = _LANE_LIBRARY[idx % len(_LANE_LIBRARY)]
            lane_id = _safe_lane(lane["lane_id"])
            verbal = f"{seed_text} {suffixes[idx % len(suffixes)]}".strip()
            visual = ""
            on_screen = ""
            candidate_text = verbal.lower()
            idx += 1
            if candidate_text in existing_texts:
                continue
            existing_texts.add(candidate_text)
            out.append(
                HookCandidateV1(
                    candidate_id=_build_candidate_id(context.brief_unit_id, lane_id, lane_counts),
                    brief_unit_id=context.brief_unit_id,
                    arm=context.arm,
                    lane_id=lane_id,
                    lane_label=lane["lane_label"],
                    verbal_open=verbal,
                    visual_pattern_interrupt=visual,
                    on_screen_text=on_screen,
                    awareness_level=context.awareness_level,
                    emotion_key=context.emotion_key,
                    evidence_ids=context.evidence_ids_allowed[:1],
                    rationale="Deterministic top-up to satisfy minimum hook count.",
                    model_metadata={"provider": "fallback", "model": "deterministic", "sdk_used": False, "top_up": True},
                )
            )
    sanitized: list[HookCandidateV1] = []
    for idx, row in enumerate(out[:target_total]):
        verbal = str(row.verbal_open or "").strip()
        if _contains_meta_copy_terms(verbal):
            if str(row.lane_id or "") == "script_default" and default_hook_verbal:
                replacement = default_hook_verbal
            else:
                replacement = _safe_verbal_fallback(context, str(row.lane_id or ""), idx)
            verbal = str(replacement or "").strip()
            if verbal:
                row = row.model_copy(
                    update={
                        "verbal_open": verbal,
                        "rationale": (
                            f"{str(row.rationale or '').strip()} "
                            "[auto_sanitized_meta_verbal]"
                        ).strip(),
                    }
                )
        sanitized.append(row)
    return sanitized


def _heuristic_score(candidate: HookCandidateV1) -> tuple[int, int]:
    verbal = str(candidate.verbal_open or "")
    visual = str(candidate.visual_pattern_interrupt or "")
    words = len(_WORD_RE.findall(f"{verbal} {visual}"))
    specificity = 62 + min(28, words // 2)
    if any(ch.isdigit() for ch in verbal):
        specificity += 4
    if any(term in verbal.lower() for term in ("exactly", "because", "instead", "when", "after")):
        specificity += 3
    specificity = max(0, min(100, specificity))

    scroll = 58 + min(30, words // 2)
    if len(verbal) < 24:
        scroll += 6
    if "?" in verbal:
        scroll += 4
    scroll = max(0, min(100, scroll))
    return int(scroll), int(specificity)


def run_alignment_evidence_gate(
    *,
    context: HookContextV1,
    candidates: list[HookCandidateV1],
    model_overrides: dict[str, Any] | None = None,
) -> tuple[list[HookGateResultV1], list[HookScoreV1]]:
    if not candidates:
        return [], []

    allowed = set(context.evidence_ids_allowed)
    model_name = _model_override_name(
        dict(model_overrides or {}),
        "hook_generator_gate",
        str(config.PHASE3_V2_HOOK_MODEL_GATE),
    )

    system_prompt = (
        "You are a strict hook evaluator.\n"
        "For each candidate: judge awareness/emotion alignment, claim-boundary safety, "
        "scroll-stop potential (0-100), and specificity (0-100).\n"
        "Mark framework/meta wording in verbal_open as a failure.\n"
        "Do not rewrite. Only evaluate."
    )
    payload = {
        "brief_context": {
            "awareness_level": context.awareness_level,
            "emotion_key": context.emotion_key,
            "emotion_label": context.emotion_label,
            "evidence_catalog": context.evidence_catalog,
        },
        "candidates": [
            {
                "candidate_id": c.candidate_id,
                "lane_id": c.lane_id,
                "verbal_open": c.verbal_open,
                "evidence_ids": c.evidence_ids,
            }
            for c in candidates
        ],
    }
    user_prompt = (
        "Evaluate these hook candidates and return JSON only.\n"
        f"{json.dumps(payload, ensure_ascii=True, indent=2)}"
    )

    logger.info(
        "Hook gate/scoring start: brief_unit_id=%s arm=%s candidates=%d model=%s",
        context.brief_unit_id,
        context.arm,
        len(candidates),
        model_name,
    )
    usage_before = len(get_usage_log())
    llm_eval: dict[str, _GateScoreItemModel] = {}
    try:
        parsed = call_llm_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_GateScoreBatchModel,
            provider="openai",
            model=model_name,
            temperature=0.2,
            max_tokens=10_000,
        )
        llm_eval = {row.candidate_id: row for row in parsed.evaluations}
    except Exception:
        logger.exception("Hook gate/scoring evaluator failed for %s", context.brief_unit_id)
    finally:
        logger.info(
            "Hook gate/scoring complete: brief_unit_id=%s candidates=%d cost=$%.4f",
            context.brief_unit_id,
            len(candidates),
            _usage_delta(usage_before),
        )

    gate_rows: list[HookGateResultV1] = []
    score_rows: list[HookScoreV1] = []
    for candidate in candidates:
        evaluation = llm_eval.get(candidate.candidate_id)
        heuristic_scroll, heuristic_specificity = _heuristic_score(candidate)
        scroll = int(evaluation.scroll_stop_score) if evaluation else heuristic_scroll
        specificity = int(evaluation.specificity_score) if evaluation else heuristic_specificity
        alignment = bool(evaluation.alignment_pass) if evaluation else True
        claim_ok = bool(evaluation.claim_boundary_pass) if evaluation else True

        evidence_ids = [eid for eid in candidate.evidence_ids if eid in allowed]
        evidence_pass = bool(evidence_ids)

        reasons: list[str] = []
        if not alignment:
            reasons.append("alignment_mismatch")
        if not evidence_pass:
            reasons.append("missing_or_invalid_evidence")
        if not claim_ok:
            reasons.append("claim_boundary_risk")
        if _contains_meta_copy_terms(str(candidate.verbal_open or "")):
            reasons.append("meta_copy_term_in_verbal")
        if scroll < int(config.PHASE3_V2_HOOK_MIN_SCROLL_STOP_SCORE):
            reasons.append("scroll_stop_below_threshold")
        if specificity < int(config.PHASE3_V2_HOOK_MIN_SPECIFICITY_SCORE):
            reasons.append("specificity_below_threshold")

        gate_pass = not reasons
        gate_rows.append(
            HookGateResultV1(
                candidate_id=candidate.candidate_id,
                brief_unit_id=candidate.brief_unit_id,
                arm=candidate.arm,
                alignment_pass=alignment,
                evidence_pass=evidence_pass,
                claim_boundary_pass=claim_ok,
                scroll_stop_score=scroll,
                specificity_score=specificity,
                gate_pass=gate_pass,
                failure_reasons=reasons,
                evaluator_metadata={
                    "provider": "openai",
                    "model": model_name,
                    "llm_rationale": str(evaluation.rationale if evaluation else "").strip(),
                },
            )
        )
        score_rows.append(
            HookScoreV1(
                candidate_id=candidate.candidate_id,
                brief_unit_id=candidate.brief_unit_id,
                arm=candidate.arm,
                scroll_stop_score=scroll,
                specificity_score=specificity,
                diversity_penalty=0.0,
                composite_score=round((scroll * 0.6) + (specificity * 0.4), 3),
            )
        )
    return gate_rows, score_rows


def _pick_repair_targets(
    candidates: list[HookCandidateV1],
    gate_rows: list[HookGateResultV1],
    max_targets: int,
) -> list[HookCandidateV1]:
    if not candidates or not gate_rows:
        return []
    gate_by_id = {row.candidate_id: row for row in gate_rows}
    failing = []
    for candidate in candidates:
        gate = gate_by_id.get(candidate.candidate_id)
        if not gate or gate.gate_pass:
            continue
        priority = int(gate.scroll_stop_score) + int(gate.specificity_score)
        failing.append((priority, candidate))
    failing.sort(key=lambda item: item[0], reverse=True)
    return [row[1] for row in failing[: max(1, max_targets)]]


def repair_candidates(
    *,
    context: HookContextV1,
    candidates: list[HookCandidateV1],
    gate_rows: list[HookGateResultV1],
    model_overrides: dict[str, Any] | None = None,
) -> list[HookCandidateV1]:
    if not candidates:
        logger.info("Hook repair skipped: no candidates (brief_unit_id=%s arm=%s)", context.brief_unit_id, context.arm)
        return candidates

    max_rounds = max(0, int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS))
    if max_rounds <= 0:
        logger.info(
            "Hook repair skipped: max rounds disabled (brief_unit_id=%s arm=%s)",
            context.brief_unit_id,
            context.arm,
        )
        return candidates
    targets = _pick_repair_targets(candidates, gate_rows, max_targets=6)
    if not targets:
        logger.info(
            "Hook repair skipped: no failing targets (brief_unit_id=%s arm=%s)",
            context.brief_unit_id,
            context.arm,
        )
        return candidates

    gate_by_id = {row.candidate_id: row for row in gate_rows}
    model_name = _model_override_name(
        dict(model_overrides or {}),
        "hook_generator_repair",
        str(config.PHASE3_V2_HOOK_MODEL_REPAIR),
    )
    system_prompt = (
        "You are a hook repair specialist.\n"
        "Rewrite only the provided failed candidates.\n"
        "Keep lane intent while improving alignment, specificity, and evidence linkage.\n"
        "Return verbal_open only; keep visual_pattern_interrupt and on_screen_text empty.\n"
        "Remove framework/meta wording from verbal_open (pattern interrupt/interupt, scroll stopper, myth bust, identity callout, CTA).\n"
        "Do not invent evidence IDs."
    )
    payload = {
        "hook_context": context.model_dump(),
        "failed_candidates": [
            {
                "candidate_id": c.candidate_id,
                "lane_id": c.lane_id,
                "verbal_open": c.verbal_open,
                "evidence_ids": c.evidence_ids,
                "failure_reasons": gate_by_id.get(c.candidate_id).failure_reasons if gate_by_id.get(c.candidate_id) else [],
            }
            for c in targets
        ],
    }
    user_prompt = (
        "Repair these hook candidates and return JSON only.\n"
        f"{json.dumps(payload, ensure_ascii=True, indent=2)}"
    )

    logger.info(
        "Hook repair start: brief_unit_id=%s arm=%s targets=%d model=%s",
        context.brief_unit_id,
        context.arm,
        len(targets),
        model_name,
    )
    usage_before = len(get_usage_log())
    try:
        repaired, usage_meta = call_claude_agent_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_RepairBatchModel,
            model=model_name,
            allowed_tools=[],
            max_turns=5,
            max_thinking_tokens=6_000,
            timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
            return_usage=True,
        )
        _ = usage_meta
    except Exception:
        logger.exception("Hook repair failed for %s", context.brief_unit_id)
        repaired = _RepairBatchModel(repaired=[])
    finally:
        logger.info(
            "Hook repair complete: brief_unit_id=%s repaired=%d cost=$%.4f",
            context.brief_unit_id,
            len(repaired.repaired),
            _usage_delta(usage_before),
        )

    if not repaired.repaired:
        return candidates
    replace_map = {row.candidate_id: row for row in repaired.repaired}
    updated: list[HookCandidateV1] = []
    for candidate in candidates:
        patch = replace_map.get(candidate.candidate_id)
        if not patch:
            updated.append(candidate)
            continue
        updated.append(
            HookCandidateV1(
                candidate_id=candidate.candidate_id,
                brief_unit_id=candidate.brief_unit_id,
                arm=candidate.arm,
                lane_id=candidate.lane_id,
                lane_label=candidate.lane_label,
                verbal_open=str(patch.verbal_open or candidate.verbal_open).strip(),
                visual_pattern_interrupt=str(patch.visual_pattern_interrupt or "").strip(),
                on_screen_text=str(patch.on_screen_text or "").strip(),
                awareness_level=candidate.awareness_level,
                emotion_key=candidate.emotion_key,
                evidence_ids=[str(v).strip() for v in (patch.evidence_ids or candidate.evidence_ids) if str(v).strip()],
                rationale=str(patch.rationale or candidate.rationale).strip(),
                model_metadata={
                    **candidate.model_metadata,
                    "repaired": True,
                    "repair_model": model_name,
                },
            )
        )
    return updated


def score_and_rank_candidates(
    *,
    candidates: list[HookCandidateV1],
    gate_rows: list[HookGateResultV1],
    score_rows: list[HookScoreV1],
    final_variants_per_unit: int,
    forced_first_candidate_id: str = "",
) -> tuple[list[str], list[str]]:
    if not candidates:
        return [], ["no_candidates"]

    candidate_by_id = {row.candidate_id: row for row in candidates}
    gate_by_id = {row.candidate_id: row for row in gate_rows}
    score_by_id = {row.candidate_id: row for row in score_rows}
    sorted_candidates = sorted(
        candidates,
        key=lambda c: float(score_by_id.get(c.candidate_id).composite_score if score_by_id.get(c.candidate_id) else 0.0),
        reverse=True,
    )

    forced_id = str(forced_first_candidate_id or "").strip()
    forced_candidate_exists = bool(forced_id and forced_id in candidate_by_id)
    target_new, target_total = _hook_variant_targets(
        int(final_variants_per_unit or 0),
        include_default_anchor=forced_candidate_exists,
    )

    non_forced_sorted = [c for c in sorted_candidates if c.candidate_id != forced_id]
    selected: list[HookCandidateV1] = []
    selected_texts: list[str] = []
    selected_new_ids: list[str] = []
    deficiency_flags: list[str] = []
    used_gate_fallback = False

    for candidate in non_forced_sorted:
        gate = gate_by_id.get(candidate.candidate_id)
        if not gate or not gate.gate_pass:
            continue
        if _contains_meta_copy_terms(str(candidate.verbal_open or "")):
            continue
        current_text = _candidate_text(candidate)
        too_similar = False
        for existing_text in selected_texts:
            if _similarity(current_text, existing_text) >= float(config.PHASE3_V2_HOOK_DIVERSITY_THRESHOLD):
                too_similar = True
                break
        if too_similar:
            continue
        selected.append(candidate)
        selected_texts.append(current_text)
        selected_new_ids.append(candidate.candidate_id)
        if len(selected) >= target_new:
            break

    if len(selected) < target_new:
        for candidate in non_forced_sorted:
            if candidate.candidate_id in selected_new_ids:
                continue
            gate = gate_by_id.get(candidate.candidate_id)
            if not gate or not gate.gate_pass:
                continue
            if _contains_meta_copy_terms(str(candidate.verbal_open or "")):
                continue
            selected.append(candidate)
            selected_new_ids.append(candidate.candidate_id)
            if len(selected) >= target_new:
                break

    if len(selected_new_ids) < target_new:
        for candidate in non_forced_sorted:
            if candidate.candidate_id in selected_new_ids:
                continue
            if _contains_meta_copy_terms(str(candidate.verbal_open or "")):
                continue
            selected.append(candidate)
            selected_new_ids.append(candidate.candidate_id)
            gate = gate_by_id.get(candidate.candidate_id)
            if not gate or not gate.gate_pass:
                used_gate_fallback = True
            if len(selected_new_ids) >= target_new:
                break

    selected_ids = list(selected_new_ids[:target_new])
    if forced_candidate_exists:
        selected_ids = [cid for cid in selected_ids if cid != forced_id]
        selected_ids.insert(0, forced_id)
        if len(selected_ids) > target_total:
            selected_ids = selected_ids[:target_total]
        forced_gate = gate_by_id.get(forced_id)
        if not forced_gate:
            deficiency_flags.append("default_hook_missing_gate")
        elif not forced_gate.gate_pass:
            deficiency_flags.append("default_hook_below_gate")
    if used_gate_fallback:
        deficiency_flags.append("quality_gate_fallback_used")

    selected = [candidate_by_id[cid] for cid in selected_ids if cid in candidate_by_id]

    if len(selected) < target_total:
        deficiency_flags.append("final_variant_shortfall")

    distinct_lanes = {c.lane_id for c in selected}
    if len(distinct_lanes) < int(config.PHASE3_V2_HOOK_MIN_LANE_COVERAGE):
        deficiency_flags.append("lane_coverage_shortfall")

    if selected:
        sims = []
        for i in range(len(selected)):
            for j in range(i + 1, len(selected)):
                sims.append(_similarity(_candidate_text(selected[i]), _candidate_text(selected[j])))
        if sims:
            avg_sim = sum(sims) / len(sims)
            if avg_sim >= float(config.PHASE3_V2_HOOK_DIVERSITY_THRESHOLD):
                deficiency_flags.append("diversity_similarity_high")

    return selected_ids, deficiency_flags


def build_final_bundle(
    *,
    hook_run_id: str,
    context: HookContextV1,
    candidates: list[HookCandidateV1],
    gate_rows: list[HookGateResultV1],
    score_rows: list[HookScoreV1],
    selected_candidate_ids: list[str],
    deficiency_flags: list[str],
    repair_rounds_used: int,
    final_variants_per_unit: int,
) -> HookBundleV1:
    gate_by_id = {row.candidate_id: row for row in gate_rows}
    score_by_id = {row.candidate_id: row for row in score_rows}
    candidate_by_id = {row.candidate_id: row for row in candidates}

    variants: list[HookVariantV1] = []
    for idx, candidate_id in enumerate(selected_candidate_ids, start=1):
        candidate = candidate_by_id.get(candidate_id)
        gate = gate_by_id.get(candidate_id)
        score = score_by_id.get(candidate_id)
        if not candidate:
            continue
        variants.append(
            HookVariantV1(
                hook_id=f"hk_{context.brief_unit_id}_{idx:03d}",
                brief_unit_id=context.brief_unit_id,
                arm=context.arm,
                verbal_open=candidate.verbal_open,
                visual_pattern_interrupt=candidate.visual_pattern_interrupt,
                on_screen_text=candidate.on_screen_text,
                awareness_level=context.awareness_level,
                emotion_key=context.emotion_key,
                evidence_ids=list(candidate.evidence_ids or []),
                scroll_stop_score=int(score.scroll_stop_score) if score else int(gate.scroll_stop_score) if gate else 0,
                specificity_score=int(score.specificity_score) if score else int(gate.specificity_score) if gate else 0,
                lane_id=candidate.lane_id,
                selection_status="candidate",
                gate_pass=bool(gate.gate_pass) if gate else False,
                rank=idx,
            )
        )

    passed_gate_count = sum(1 for row in gate_rows if row.gate_pass)
    status = "ok"
    if deficiency_flags:
        status = "shortfall"
    if not variants:
        status = "error"

    if len(variants) < max(1, int(final_variants_per_unit or 1)):
        deficiency_flags = sorted(set([*deficiency_flags, "final_variant_shortfall"]))
        if status == "ok":
            status = "shortfall"

    return HookBundleV1(
        hook_run_id=hook_run_id,
        brief_unit_id=context.brief_unit_id,
        arm=context.arm,
        variants=variants,
        candidate_count=len(candidates),
        passed_gate_count=passed_gate_count,
        repair_rounds_used=repair_rounds_used,
        deficiency_flags=sorted(set(deficiency_flags)),
        status=status,
        error="" if variants else "no_final_hooks",
        generated_at=datetime.now().isoformat(),
    )


@dataclass
class _HookUnitResult:
    arm: str
    brief_unit_id: str
    candidates: list[HookCandidateV1]
    gate_rows: list[HookGateResultV1]
    score_rows: list[HookScoreV1]
    bundle: HookBundleV1
    elapsed_seconds: float
    error: str = ""


def _run_hook_unit(
    *,
    run_id: str,
    hook_run_id: str,
    arm: ArmName,
    brief_unit: BriefUnitV1,
    draft: CoreScriptDraftV1,
    evidence_pack: EvidencePackV1,
    candidate_target_per_unit: int,
    final_variants_per_unit: int,
    model_overrides: dict[str, Any] | None = None,
) -> _HookUnitResult:
    started = time.time()
    logger.info(
        "Phase3 v2 hook unit start: arm=%s brief_unit_id=%s",
        arm,
        brief_unit.brief_unit_id,
    )
    if str(draft.status or "").strip().lower() != "ok":
        bundle = HookBundleV1(
            hook_run_id=hook_run_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=arm,
            variants=[],
            candidate_count=0,
            passed_gate_count=0,
            repair_rounds_used=0,
            deficiency_flags=["script_not_ok"],
            status="skipped",
            error=f"script_status_{draft.status}",
            generated_at=datetime.now().isoformat(),
        )
        return _HookUnitResult(
            arm=arm,
            brief_unit_id=brief_unit.brief_unit_id,
            candidates=[],
            gate_rows=[],
            score_rows=[],
            bundle=bundle,
            elapsed_seconds=round(time.time() - started, 3),
            error=f"script_status_{draft.status}",
        )

    context = build_hook_context(
        run_id=run_id,
        brief_unit=brief_unit,
        arm=arm,
        draft=draft,
        evidence_pack=evidence_pack,
    )
    candidates = generate_candidates_divergent(
        context=context,
        candidate_target_per_unit=candidate_target_per_unit,
        model_overrides=model_overrides,
    )
    gate_rows, score_rows = run_alignment_evidence_gate(
        context=context,
        candidates=candidates,
        model_overrides=model_overrides,
    )
    forced_id = next(
        (row.candidate_id for row in candidates if str(row.lane_id or "") == "script_default"),
        "",
    )
    _, target_total = _hook_variant_targets(
        int(final_variants_per_unit or 0),
        include_default_anchor=bool(forced_id),
    )

    selected_ids, deficiency_flags = score_and_rank_candidates(
        candidates=candidates,
        gate_rows=gate_rows,
        score_rows=score_rows,
        final_variants_per_unit=final_variants_per_unit,
        forced_first_candidate_id=forced_id,
    )

    repair_rounds_used = 0
    if deficiency_flags and int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS) > 0:
        repair_rounds_used = 1
        repaired_candidates = repair_candidates(
            context=context,
            candidates=candidates,
            gate_rows=gate_rows,
            model_overrides=model_overrides,
        )
        if repaired_candidates != candidates:
            candidates = repaired_candidates
            gate_rows, score_rows = run_alignment_evidence_gate(
                context=context,
                candidates=candidates,
                model_overrides=model_overrides,
            )
            selected_ids, deficiency_flags = score_and_rank_candidates(
                candidates=candidates,
                gate_rows=gate_rows,
                score_rows=score_rows,
                final_variants_per_unit=final_variants_per_unit,
                forced_first_candidate_id=forced_id,
            )

    bundle = build_final_bundle(
        hook_run_id=hook_run_id,
        context=context,
        candidates=candidates,
        gate_rows=gate_rows,
        score_rows=score_rows,
        selected_candidate_ids=selected_ids,
        deficiency_flags=deficiency_flags,
        repair_rounds_used=repair_rounds_used,
        final_variants_per_unit=target_total,
    )

    elapsed = round(time.time() - started, 3)
    logger.info(
        "Phase3 v2 hook unit done: arm=%s brief_unit_id=%s status=%s candidates=%d finals=%d latency=%.3fs",
        arm,
        brief_unit.brief_unit_id,
        bundle.status,
        len(candidates),
        len(bundle.variants),
        elapsed,
    )
    return _HookUnitResult(
        arm=arm,
        brief_unit_id=brief_unit.brief_unit_id,
        candidates=candidates,
        gate_rows=gate_rows,
        score_rows=score_rows,
        bundle=bundle,
        elapsed_seconds=elapsed,
        error=str(bundle.error or ""),
    )


def run_phase3_v2_hooks(
    *,
    run_id: str,
    hook_run_id: str,
    hook_items: list[dict[str, Any]],
    candidate_target_per_unit: int = 20,
    final_variants_per_unit: int = 5,
    model_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_target_per_unit = max(1, int(candidate_target_per_unit or 1))
    min_new = max(1, int(getattr(config, "PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 4) or 1))
    final_variants_per_unit = max(min_new, int(final_variants_per_unit or 1))
    max_parallel = max(1, int(config.PHASE3_V2_HOOK_MAX_PARALLEL))
    max_repair_rounds = max(0, int(config.PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS))

    started = time.time()
    manifests: dict[str, Any] = {
        "run_id": run_id,
        "hook_run_id": hook_run_id,
        "status": "running",
        "created_at": datetime.now().isoformat(),
        "started_at": datetime.now().isoformat(),
        "eligible_count": len(hook_items),
        "candidate_target_per_unit": candidate_target_per_unit,
        "final_variants_per_unit": final_variants_per_unit,
        "max_parallel": max_parallel,
        "max_repair_rounds": max_repair_rounds,
        "model_registry": {
            "hook_generator_generation": _model_override_name(
                dict(model_overrides or {}),
                "hook_generator_generation",
                str(config.PHASE3_V2_HOOK_MODEL_GENERATION),
            ),
            "hook_generator_gate": _model_override_name(
                dict(model_overrides or {}),
                "hook_generator_gate",
                str(config.PHASE3_V2_HOOK_MODEL_GATE),
            ),
            "hook_generator_repair": _model_override_name(
                dict(model_overrides or {}),
                "hook_generator_repair",
                str(config.PHASE3_V2_HOOK_MODEL_REPAIR),
            ),
            "hook_generator_rank": _model_override_name(
                dict(model_overrides or {}),
                "hook_generator_rank",
                str(config.PHASE3_V2_HOOK_MODEL_RANK),
            ),
        },
    }

    candidates_by_arm: dict[str, list[dict[str, Any]]] = {}
    gate_reports_by_arm: dict[str, list[dict[str, Any]]] = {}
    bundles_by_arm: dict[str, list[dict[str, Any]]] = {}
    score_rows_by_arm: dict[str, list[dict[str, Any]]] = {}

    if not hook_items:
        manifest = HookStageManifestV1.model_validate(
            {
                **manifests,
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "processed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "metrics": {"elapsed_seconds": round(time.time() - started, 3)},
            }
        )
        return {
            "hook_stage_manifest": manifest.model_dump(),
            "hook_candidates_by_arm": {},
            "hook_gate_reports_by_arm": {},
            "hook_bundles_by_arm": {},
            "hook_scores_by_arm": {},
        }

    workers = min(max_parallel, len(hook_items))
    logger.info(
        "Phase3 v2 hook stage parallel mode: workers=%d eligible=%d",
        workers,
        len(hook_items),
    )
    results: list[_HookUnitResult] = []

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="p3v2-hook") as pool:
        future_map: dict[Any, int] = {}
        for idx, item in enumerate(hook_items):
            logger.info(
                "Phase3 v2 hook unit queued: arm=%s brief_unit_id=%s queue_index=%d/%d",
                item.get("arm"),
                item.get("brief_unit_id"),
                idx + 1,
                len(hook_items),
            )
            brief_unit = BriefUnitV1.model_validate(item["brief_unit"])
            draft = CoreScriptDraftV1.model_validate(item["draft"])
            evidence_pack = EvidencePackV1.model_validate(item["evidence_pack"])
            arm = str(item.get("arm") or "claude_sdk")
            future = pool.submit(
                _run_hook_unit,
                run_id=run_id,
                hook_run_id=hook_run_id,
                arm=arm,  # type: ignore[arg-type]
                brief_unit=brief_unit,
                draft=draft,
                evidence_pack=evidence_pack,
                candidate_target_per_unit=candidate_target_per_unit,
                final_variants_per_unit=final_variants_per_unit,
                model_overrides=model_overrides,
            )
            future_map[future] = idx

        ordered: list[_HookUnitResult | None] = [None] * len(hook_items)
        for future in as_completed(future_map):
            idx = future_map[future]
            item = hook_items[idx]
            try:
                result = future.result()
            except Exception as exc:
                logger.exception("Phase3 v2 hook unit failed unexpectedly")
                arm = str(item.get("arm") or "claude_sdk")
                unit_id = str(item.get("brief_unit_id") or "")
                fallback_bundle = HookBundleV1(
                    hook_run_id=hook_run_id,
                    brief_unit_id=unit_id,
                    arm=arm,  # type: ignore[arg-type]
                    variants=[],
                    candidate_count=0,
                    passed_gate_count=0,
                    repair_rounds_used=0,
                    deficiency_flags=["unit_runtime_failure"],
                    status="error",
                    error=str(exc),
                    generated_at=datetime.now().isoformat(),
                )
                result = _HookUnitResult(
                    arm=arm,
                    brief_unit_id=unit_id,
                    candidates=[],
                    gate_rows=[],
                    score_rows=[],
                    bundle=fallback_bundle,
                    elapsed_seconds=0.0,
                    error=str(exc),
                )
            ordered[idx] = result
            logger.info(
                "Phase3 v2 hook unit collected: arm=%s brief_unit_id=%s completed=%d/%d",
                result.arm,
                result.brief_unit_id,
                sum(1 for row in ordered if row is not None),
                len(ordered),
            )
        results = [row for row in ordered if row is not None]

    failed_count = 0
    skipped_count = 0
    for result in results:
        if result.bundle.status == "error":
            failed_count += 1
        if result.bundle.status == "skipped":
            skipped_count += 1
        candidates_by_arm.setdefault(result.arm, []).extend([row.model_dump() for row in result.candidates])
        gate_reports_by_arm.setdefault(result.arm, []).extend([row.model_dump() for row in result.gate_rows])
        bundles_by_arm.setdefault(result.arm, []).append(result.bundle.model_dump())
        score_rows_by_arm.setdefault(result.arm, []).extend([row.model_dump() for row in result.score_rows])

    manifest = HookStageManifestV1.model_validate(
        {
            **manifests,
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "processed_count": len(results),
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "metrics": {
                "elapsed_seconds": round(time.time() - started, 3),
                "candidate_count_total": sum(len(v) for v in candidates_by_arm.values()),
                "bundle_count_total": sum(len(v) for v in bundles_by_arm.values()),
                "repair_hit_rate": round(
                    (
                        sum(
                            1
                            for bundles in bundles_by_arm.values()
                            for bundle in bundles
                            if int(bundle.get("repair_rounds_used", 0) or 0) > 0
                        )
                        / max(1, len(results))
                    ),
                    4,
                ),
            },
        }
    )

    return {
        "hook_stage_manifest": manifest.model_dump(),
        "hook_candidates_by_arm": candidates_by_arm,
        "hook_gate_reports_by_arm": gate_reports_by_arm,
        "hook_bundles_by_arm": bundles_by_arm,
        "hook_scores_by_arm": score_rows_by_arm,
    }
