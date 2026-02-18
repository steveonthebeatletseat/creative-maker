"""Phase 3 v2 engine (Milestone 1).

Scope:
- Brief Unit expansion from matrix plan
- Evidence pack construction from Foundation Research
- Deterministic script spec compilation
- Core script drafting (Claude SDK arm)
- M1 quality gate evaluation
- Summary computation from drafts + human reviews
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import logging
import math
import re
import time
from statistics import median
from typing import Any

import config
from pipeline.claude_agent_runtime import call_claude_agent_structured
from pipeline.llm import call_llm_structured, get_usage_log
from schemas.phase3_v2 import (
    ArmName,
    ArmSummaryV1,
    BriefUnitV1,
    CoreScriptDraftV1,
    CoreScriptGeneratedV1,
    EvidenceCoverageReportV1,
    EvidencePackV1,
    HumanQualityReviewV1,
    MechanismRefV1,
    Phase3V2ABSummaryV1,
    ProofRefV1,
    ScriptSpecV1,
    VocQuoteRefV1,
    compute_score_stats,
)

logger = logging.getLogger(__name__)

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


def _contains_meta_copy_terms(text: str) -> bool:
    value = str(text or "")
    return bool(
        _META_COPY_TERM_RE.search(value)
        or _META_SUMMARY_LEADIN_RE.search(value)
        or _META_SUMMARY_PHRASE_RE.search(value)
    )


def _looks_like_claude_model(model_name: str) -> bool:
    value = str(model_name or "").strip().lower()
    if not value:
        return False
    return "claude" in value or value.startswith("anthropic/")


def _resolve_drafter_target(
    run_mode: ArmName,
    *,
    provider: str | None = None,
    model: str | None = None,
) -> tuple[str, str]:
    sdk_used = run_mode == "claude_sdk"
    if sdk_used:
        requested_model = str(model or "").strip()
        if requested_model and _looks_like_claude_model(requested_model):
            return "anthropic", requested_model
        if requested_model:
            logger.warning(
                "Phase3 v2 SDK arm received non-Claude model '%s'; falling back to %s",
                requested_model,
                config.ANTHROPIC_FRONTIER,
            )
        return "anthropic", config.ANTHROPIC_FRONTIER

    default_conf = config.get_agent_llm_config("copywriter")
    final_provider = str(provider or default_conf.get("provider") or "openai").strip().lower()
    final_model = str(model or default_conf.get("model") or config.DEFAULT_MODEL).strip() or config.DEFAULT_MODEL
    return final_provider, final_model


def _normalize_emotion_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def _hash_json(data: Any) -> str:
    blob = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _matrix_cell_sort_key(
    cell: dict[str, Any],
    awareness_order: dict[str, int],
    emotion_order: dict[str, int],
) -> tuple[int, int, str, str]:
    awareness = str(cell.get("awareness_level") or "").strip().lower()
    emotion = _normalize_emotion_key(cell.get("emotion_key"))
    return (
        awareness_order.get(awareness, 10_000),
        emotion_order.get(emotion, 10_000),
        awareness,
        emotion,
    )


def expand_brief_units(
    matrix_plan: dict[str, Any],
    *,
    branch_id: str,
    brand_slug: str,
    pilot_size: int = 20,
    selection_strategy: str = "round_robin",
) -> list[BriefUnitV1]:
    """Expand matrix cells into individual Brief Units.

    Deterministic order:
    - cell order by awareness axis, then emotion axis
    - within each cell by ordinal 1..N
    Round-robin sampling is applied across ordered cells for diversity.
    """
    if not isinstance(matrix_plan, dict):
        return []

    awareness_levels = (
        matrix_plan.get("awareness_axis", {}).get("levels", [])
        if isinstance(matrix_plan.get("awareness_axis"), dict)
        else []
    )
    awareness_levels = [str(v).strip().lower() for v in awareness_levels if str(v or "").strip()]
    awareness_order = {key: idx for idx, key in enumerate(awareness_levels)}

    emotion_rows = (
        matrix_plan.get("emotion_axis", {}).get("rows", [])
        if isinstance(matrix_plan.get("emotion_axis"), dict)
        else []
    )
    emotion_order: dict[str, int] = {}
    emotion_label_by_key: dict[str, str] = {}
    if isinstance(emotion_rows, list):
        for idx, row in enumerate(emotion_rows):
            if not isinstance(row, dict):
                continue
            key = _normalize_emotion_key(row.get("emotion_key") or row.get("emotion_label") or row.get("emotion"))
            label = str(row.get("emotion_label") or row.get("emotion") or key).strip()
            if not key:
                continue
            if key not in emotion_order:
                emotion_order[key] = idx
            if key not in emotion_label_by_key:
                emotion_label_by_key[key] = label or key

    raw_cells = matrix_plan.get("cells", [])
    if not isinstance(raw_cells, list):
        raw_cells = []

    expanded_cells: list[dict[str, Any]] = []
    for cell in raw_cells:
        if not isinstance(cell, dict):
            continue
        awareness = str(cell.get("awareness_level") or "").strip().lower()
        emotion_key = _normalize_emotion_key(cell.get("emotion_key"))
        if not awareness or not emotion_key:
            continue
        try:
            count = int(cell.get("brief_count", 0) or 0)
        except (TypeError, ValueError):
            count = 0
        if count <= 0:
            continue
        expanded_cells.append(
            {
                "awareness_level": awareness,
                "emotion_key": emotion_key,
                "emotion_label": emotion_label_by_key.get(emotion_key, emotion_key),
                "brief_count": count,
            }
        )

    expanded_cells.sort(key=lambda c: _matrix_cell_sort_key(c, awareness_order, emotion_order))

    target = max(1, int(pilot_size or 0))
    source_hash = _hash_json(matrix_plan)
    units: list[BriefUnitV1] = []

    if selection_strategy != "round_robin":
        # Fallback deterministic flat expansion.
        for cell in expanded_cells:
            for ordinal in range(1, int(cell["brief_count"]) + 1):
                if len(units) >= target:
                    return units
                awareness = str(cell["awareness_level"])
                emotion = str(cell["emotion_key"])
                units.append(
                    BriefUnitV1(
                        brief_unit_id=f"bu_{awareness}_{emotion}_{ordinal:03d}",
                        matrix_cell_id=f"cell_{awareness}_{emotion}",
                        branch_id=branch_id,
                        brand_slug=brand_slug,
                        awareness_level=awareness,
                        emotion_key=emotion,
                        emotion_label=str(cell.get("emotion_label") or emotion),
                        ordinal_in_cell=ordinal,
                        source_matrix_plan_hash=source_hash,
                    )
                )
        return units

    # Round-robin across cells in deterministic cell order.
    ordinals = [1 for _ in expanded_cells]
    done = False
    while not done and len(units) < target:
        done = True
        for idx, cell in enumerate(expanded_cells):
            ordinal = ordinals[idx]
            count = int(cell["brief_count"])
            if ordinal > count:
                continue
            done = False
            awareness = str(cell["awareness_level"])
            emotion = str(cell["emotion_key"])
            units.append(
                BriefUnitV1(
                    brief_unit_id=f"bu_{awareness}_{emotion}_{ordinal:03d}",
                    matrix_cell_id=f"cell_{awareness}_{emotion}",
                    branch_id=branch_id,
                    brand_slug=brand_slug,
                    awareness_level=awareness,
                    emotion_key=emotion,
                    emotion_label=str(cell.get("emotion_label") or emotion),
                    ordinal_in_cell=ordinal,
                    source_matrix_plan_hash=source_hash,
                )
            )
            ordinals[idx] = ordinal + 1
            if len(units) >= target:
                break
    return units


def build_evidence_pack(brief_unit: BriefUnitV1, foundation_brief: dict[str, Any]) -> EvidencePackV1:
    """Build one evidence pack from Foundation Research for a Brief Unit."""
    p2 = foundation_brief.get("pillar_2_voc_language_bank", {}) if isinstance(foundation_brief, dict) else {}
    p4 = foundation_brief.get("pillar_4_product_mechanism_analysis", {}) if isinstance(foundation_brief, dict) else {}
    p7 = foundation_brief.get("pillar_7_proof_credibility_inventory", {}) if isinstance(foundation_brief, dict) else {}

    quotes = p2.get("quotes", []) if isinstance(p2, dict) else []
    assets = p7.get("assets", []) if isinstance(p7, dict) else []

    voc_refs: list[VocQuoteRefV1] = []
    emotion_match = _normalize_emotion_key(brief_unit.emotion_key)
    if isinstance(quotes, list):
        aligned = []
        fallback = []
        for item in quotes:
            if not isinstance(item, dict):
                continue
            quote_id = str(item.get("quote_id") or "").strip()
            quote_text = str(item.get("quote") or "").strip()
            source_url = str(item.get("source_url") or "").strip()
            source_type = str(item.get("source_type") or "").strip()
            if not quote_id or not quote_text:
                continue
            ref = VocQuoteRefV1(
                quote_id=quote_id,
                quote_excerpt=quote_text[:260],
                source_url=source_url,
                source_type=source_type,
            )
            item_emotion = _normalize_emotion_key(item.get("dominant_emotion"))
            if item_emotion and item_emotion == emotion_match:
                aligned.append(ref)
            else:
                fallback.append(ref)
        voc_refs = (aligned[:12] + fallback[:12])[:12]

    proof_refs: list[ProofRefV1] = []
    if isinstance(assets, list):
        for item in assets:
            if not isinstance(item, dict):
                continue
            asset_id = str(item.get("asset_id") or "").strip()
            title = str(item.get("title") or "").strip()
            detail = str(item.get("detail") or "").strip()
            if not asset_id or not title:
                continue
            proof_refs.append(
                ProofRefV1(
                    asset_id=asset_id,
                    proof_type=str(item.get("proof_type") or "").strip(),
                    title=title,
                    detail=detail[:360],
                    source_url=str(item.get("source_url") or "").strip(),
                )
            )
            if len(proof_refs) >= 6:
                break

    mechanism_refs: list[MechanismRefV1] = []
    if isinstance(p4, dict):
        support_ids = p4.get("mechanism_supporting_evidence_ids", [])
        if not isinstance(support_ids, list):
            support_ids = []
        support_ids = [str(v) for v in support_ids if str(v or "").strip()]

        primary_name = str(
            p4.get("primary_mechanism_name")
            or p4.get("mechanism_name")
            or "Primary Mechanism"
        ).strip()
        why_exists = str(p4.get("why_problem_exists") or "").strip()
        why_works = str(p4.get("why_solution_uniquely_works") or "").strip()

        if why_exists:
            mechanism_refs.append(
                MechanismRefV1(
                    mechanism_id=f"mref_{brief_unit.brief_unit_id}_problem",
                    title="Why the Problem Persists",
                    detail=why_exists[:420],
                    support_evidence_ids=support_ids[:8],
                )
            )
        if why_works:
            mechanism_refs.append(
                MechanismRefV1(
                    mechanism_id=f"mref_{brief_unit.brief_unit_id}_solution",
                    title=primary_name or "Why This Works",
                    detail=why_works[:420],
                    support_evidence_ids=support_ids[:12],
                )
            )

    coverage = EvidenceCoverageReportV1(
        has_voc=len(voc_refs) > 0,
        has_proof=len(proof_refs) > 0,
        has_mechanism=len(mechanism_refs) > 0,
        voc_count=len(voc_refs),
        proof_count=len(proof_refs),
        mechanism_count=len(mechanism_refs),
    )
    coverage.blocked_evidence_insufficient = not (
        coverage.has_voc and coverage.has_proof and coverage.has_mechanism
    )

    return EvidencePackV1(
        pack_id=f"pack_{brief_unit.brief_unit_id}",
        brief_unit_id=brief_unit.brief_unit_id,
        voc_quote_refs=voc_refs,
        proof_refs=proof_refs,
        mechanism_refs=mechanism_refs,
        coverage_report=coverage,
    )


def compile_script_spec_v1(brief_unit: BriefUnitV1, evidence_pack: EvidencePackV1) -> ScriptSpecV1:
    """Compile deterministic script constraints for M1."""
    awareness_tone = {
        "unaware": "Open with immediate emotional contrast and empathy before mentioning product.",
        "problem_aware": "Name the pain clearly, then pivot to mechanism-based relief.",
        "solution_aware": "Differentiate mechanism vs alternatives with concrete proof.",
        "product_aware": "Reinforce trust and proof density; remove remaining objections.",
        "most_aware": "Use direct offer framing with compact proof and urgency.",
    }
    tone = awareness_tone.get(
        brief_unit.awareness_level,
        "Use direct-response clarity with concrete language and believable proof.",
    )
    tone += f" Mirror '{brief_unit.emotion_label}' language from customer voice."

    return ScriptSpecV1(
        brief_unit_id=brief_unit.brief_unit_id,
        required_sections=["hook", "problem", "mechanism", "proof", "cta"],
        tone_instruction=tone,
        word_count_min=95,
        word_count_max=240,
        cta_rule="One primary CTA only. No conflicting CTAs.",
        citation_rule="Every line must include at least one evidence_id from evidence pack.",
    )


def _build_generation_prompts(
    brief_unit: BriefUnitV1,
    spec: ScriptSpecV1,
    evidence_pack: EvidencePackV1,
) -> tuple[str, str]:
    system_prompt = (
        "You are the Core Script Drafter for a direct-response ad workflow.\n"
        "Return a structured script draft with exactly 5 sections and evidence-linked lines.\n"
        "Do not invent evidence IDs. Use only IDs provided in the evidence pack.\n"
        "Each section field must be a concise real sentence summary, not placeholders or line ranges.\n"
        "Never include framework/meta terms in customer-facing copy: pattern interrupt/interupt, scroll stopper, "
        "myth bust, identity callout, CTA, call to action."
    )
    payload = {
        "brief_unit": brief_unit.model_dump(),
        "script_spec": spec.model_dump(),
        "evidence_pack": evidence_pack.model_dump(),
        "requirements": {
            "sections": ["hook", "problem", "mechanism", "proof", "cta"],
            "line_id_format": "L01, L02, ...",
            "citation_rule": "Every line must include one or more evidence_ids from evidence pack.",
            "style": "human, direct-response, non-generic, concrete language",
        },
    }
    user_prompt = (
        "Generate a CoreScriptGeneratedV1 JSON object for this brief unit.\n\n"
        f"{json.dumps(payload, indent=2, ensure_ascii=True)}"
    )
    return system_prompt, user_prompt


def _usage_delta(start_index: int) -> float:
    try:
        log = get_usage_log()
    except Exception:
        return 0.0
    if start_index < 0 or start_index >= len(log):
        return 0.0
    return round(float(sum(float(entry.get("cost", 0.0) or 0.0) for entry in log[start_index:])), 6)


_SECTION_LINE_REF_RE = re.compile(r"^l\d{2}(?:\s*-\s*l\d{2})?$", re.IGNORECASE)


def _line_number(line_id: str) -> int:
    match = re.search(r"(\d+)", str(line_id or ""))
    if not match:
        return 0
    try:
        return int(match.group(1))
    except ValueError:
        return 0


def _section_is_placeholder(section_name: str, text: str) -> bool:
    value = str(text or "").strip().lower()
    if not value:
        return True
    if value in {"hook", "problem", "mechanism", "proof", "cta"}:
        return True
    if value == section_name.lower():
        return True
    if _SECTION_LINE_REF_RE.match(value):
        return True
    return False


def _join_lines(lines: list[Any], start: int, end: int) -> str:
    selected: list[str] = []
    for line in lines:
        number = _line_number(getattr(line, "line_id", ""))
        if start <= number <= end:
            text = str(getattr(line, "text", "") or "").strip()
            if text:
                selected.append(text)
    if not selected:
        return ""
    return " ".join(selected)[:320].strip()


def _normalize_generated_sections(generated: CoreScriptGeneratedV1) -> CoreScriptGeneratedV1:
    if not generated.lines:
        return generated
    ordered_lines = sorted(generated.lines, key=lambda line: (_line_number(line.line_id), str(line.line_id)))
    sections = generated.sections

    hook = str(sections.hook or "").strip()
    problem = str(sections.problem or "").strip()
    mechanism = str(sections.mechanism or "").strip()
    proof = str(sections.proof or "").strip()
    cta = str(sections.cta or "").strip()

    if _section_is_placeholder("hook", hook):
        hook = _join_lines(ordered_lines, 1, 2) or _join_lines(ordered_lines, 1, 3)
    elif _contains_meta_copy_terms(hook):
        # Replace meta/framework-style section labels with a real line from the draft.
        hook = _join_lines(ordered_lines, 1, 2) or _join_lines(ordered_lines, 1, 3) or hook
    if _section_is_placeholder("problem", problem):
        problem = _join_lines(ordered_lines, 3, 4) or _join_lines(ordered_lines, 2, 4)
    if _section_is_placeholder("mechanism", mechanism):
        mechanism = _join_lines(ordered_lines, 5, 6) or _join_lines(ordered_lines, 4, 6)
    if _section_is_placeholder("proof", proof):
        proof = _join_lines(ordered_lines, 7, 8) or _join_lines(ordered_lines, 6, 8)
    if _section_is_placeholder("cta", cta):
        cta = _join_lines(ordered_lines, 9, 12) or _join_lines(ordered_lines, 8, 12)

    if hook:
        generated.sections.hook = hook
    if problem:
        generated.sections.problem = problem
    if mechanism:
        generated.sections.mechanism = mechanism
    if proof:
        generated.sections.proof = proof
    if cta:
        generated.sections.cta = cta
    return generated


def draft_core_script_v1(
    brief_unit: BriefUnitV1,
    spec: ScriptSpecV1,
    evidence_pack: EvidencePackV1,
    *,
    run_mode: ArmName,
    provider: str | None = None,
    model: str | None = None,
) -> CoreScriptDraftV1:
    """Draft core script with either standard structured LLM or Claude SDK."""
    script_id = f"script_{brief_unit.brief_unit_id}_{run_mode}"
    sdk_used = run_mode == "claude_sdk"
    final_provider, final_model = _resolve_drafter_target(
        run_mode,
        provider=provider,
        model=model,
    )
    if evidence_pack.coverage_report.blocked_evidence_insufficient:
        return CoreScriptDraftV1(
            script_id=script_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=run_mode,
            status="blocked",
            error="blocked_evidence_insufficient",
            model_metadata={
                "provider": final_provider,
                "model": final_model,
                "sdk_used": sdk_used,
            },
        )

    system_prompt, user_prompt = _build_generation_prompts(brief_unit, spec, evidence_pack)
    started = time.time()
    sdk_usage: dict[str, Any] | None = None
    used_provider = final_provider
    try:
        usage_before = len(get_usage_log())
    except Exception:
        usage_before = 0
    try:
        if sdk_used:
            sdk_result = call_claude_agent_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=CoreScriptGeneratedV1,
                model=final_model,
                # Milestone 1 default: no tools.
                allowed_tools=[],
                max_turns=6,
                max_thinking_tokens=8_000,
                timeout_seconds=float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
                return_usage=True,
            )
            if isinstance(sdk_result, tuple) and len(sdk_result) == 2:
                generated, usage_meta = sdk_result
                sdk_usage = usage_meta if isinstance(usage_meta, dict) else None
            else:
                generated = sdk_result
            used_provider = "anthropic"
        else:
            generated = call_llm_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=CoreScriptGeneratedV1,
                provider=final_provider,
                model=final_model,
                temperature=0.55,
                max_tokens=8_000,
            )
            used_provider = final_provider

        generated = _normalize_generated_sections(generated)
        elapsed = round(time.time() - started, 3)
        draft = CoreScriptDraftV1(
            script_id=script_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=run_mode,
            sections=generated.sections,
            lines=generated.lines,
            status="ok",
            latency_seconds=elapsed,
            cost_usd=(
                round(float(sdk_usage.get("cost_usd", 0.0) or 0.0), 6)
                if sdk_used and isinstance(sdk_usage, dict)
                else _usage_delta(usage_before)
            ),
            model_metadata={
                "provider": used_provider,
                "model": final_model,
                "sdk_used": sdk_used,
            },
        )
        return draft
    except Exception as exc:
        elapsed = round(time.time() - started, 3)
        logger.exception("Phase3 v2 draft failed for %s (%s)", brief_unit.brief_unit_id, run_mode)
        return CoreScriptDraftV1(
            script_id=script_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=run_mode,
            status="error",
            error=str(exc),
            latency_seconds=elapsed,
            cost_usd=_usage_delta(usage_before),
            model_metadata={
                "provider": used_provider,
                "model": final_model,
                "sdk_used": sdk_used,
            },
        )


def evaluate_m1_gates(
    draft: CoreScriptDraftV1,
    *,
    spec: ScriptSpecV1,
    evidence_pack: EvidencePackV1,
) -> dict[str, Any]:
    """Evaluate Milestone 1 hard gates."""
    checks: list[dict[str, Any]] = []

    schema_valid = True
    try:
        CoreScriptDraftV1.model_validate(draft.model_dump())
    except Exception:
        schema_valid = False
    checks.append(
        {
            "gate_id": "schema_valid",
            "passed": schema_valid,
            "required": "CoreScriptDraftV1 validates",
            "actual": "valid" if schema_valid else "invalid",
        }
    )

    sections_present = False
    if draft.sections is not None:
        section_values = [
            str(draft.sections.hook or "").strip(),
            str(draft.sections.problem or "").strip(),
            str(draft.sections.mechanism or "").strip(),
            str(draft.sections.proof or "").strip(),
            str(draft.sections.cta or "").strip(),
        ]
        sections_present = all(bool(v) for v in section_values)
    checks.append(
        {
            "gate_id": "required_sections_present",
            "passed": sections_present,
            "required": ",".join(spec.required_sections),
            "actual": "present" if sections_present else "missing",
        }
    )

    section_quality = False
    if draft.sections is not None:
        section_quality = not any(
            _section_is_placeholder(name, value)
            for name, value in (
                ("hook", str(draft.sections.hook or "")),
                ("problem", str(draft.sections.problem or "")),
                ("mechanism", str(draft.sections.mechanism or "")),
                ("proof", str(draft.sections.proof or "")),
                ("cta", str(draft.sections.cta or "")),
            )
        )
    checks.append(
        {
            "gate_id": "section_quality",
            "passed": section_quality,
            "required": "section summaries must be real phrases, not placeholders/line ranges",
            "actual": "valid" if section_quality else "placeholder_detected",
        }
    )

    meta_term_violations: list[str] = []
    if draft.sections is not None:
        for name, value in (
            ("hook", str(draft.sections.hook or "")),
            ("problem", str(draft.sections.problem or "")),
            ("mechanism", str(draft.sections.mechanism or "")),
            ("proof", str(draft.sections.proof or "")),
            ("cta", str(draft.sections.cta or "")),
        ):
            if _contains_meta_copy_terms(value):
                meta_term_violations.append(f"section:{name}")
    for line in (draft.lines or []):
        if _contains_meta_copy_terms(str(line.text or "")):
            meta_term_violations.append(f"line:{line.line_id or '?'}")
    checks.append(
        {
            "gate_id": "no_meta_copy_terms",
            "passed": len(meta_term_violations) == 0,
            "required": "no framework/meta labels in customer-facing copy",
            "actual": "clean" if not meta_term_violations else ", ".join(meta_term_violations[:20]),
        }
    )

    allowed_ids: set[str] = set()
    for ref in evidence_pack.voc_quote_refs:
        allowed_ids.add(ref.quote_id)
    for ref in evidence_pack.proof_refs:
        allowed_ids.add(ref.asset_id)
    for ref in evidence_pack.mechanism_refs:
        allowed_ids.add(ref.mechanism_id)
        for eid in ref.support_evidence_ids:
            allowed_ids.add(str(eid))

    citations_valid = True
    if not draft.lines:
        citations_valid = False
    else:
        for line in draft.lines:
            line_ids = [str(v) for v in (line.evidence_ids or []) if str(v or "").strip()]
            if not line_ids:
                citations_valid = False
                break
            if any(eid not in allowed_ids for eid in line_ids):
                citations_valid = False
                break
    checks.append(
        {
            "gate_id": "line_citations_valid",
            "passed": citations_valid,
            "required": "every line has only valid evidence_ids",
            "actual": "valid" if citations_valid else "invalid_or_missing",
        }
    )

    total_words = 0
    if draft.lines:
        total_words = sum(len(str(line.text or "").split()) for line in draft.lines)
    within_bounds = spec.word_count_min <= total_words <= spec.word_count_max
    checks.append(
        {
            "gate_id": "word_count_bounds",
            "passed": within_bounds,
            "required": f"{spec.word_count_min}-{spec.word_count_max}",
            "actual": str(total_words),
        }
    )

    status_ok = draft.status == "ok"
    checks.append(
        {
            "gate_id": "draft_status_ok",
            "passed": status_ok,
            "required": "status=ok",
            "actual": draft.status,
        }
    )

    return {
        "overall_pass": all(bool(check.get("passed")) for check in checks),
        "checks": checks,
    }


def _internal_error_draft(
    *,
    brief_unit: BriefUnitV1,
    run_mode: ArmName,
    provider: str | None = None,
    model: str | None = None,
    error: str,
) -> CoreScriptDraftV1:
    fallback_provider, fallback_model = _resolve_drafter_target(
        run_mode,
        provider=provider,
        model=model,
    )
    return CoreScriptDraftV1(
        script_id=f"script_{brief_unit.brief_unit_id}_{run_mode}",
        brief_unit_id=brief_unit.brief_unit_id,
        arm=run_mode,
        status="error",
        error=error,
        model_metadata={
            "provider": fallback_provider,
            "model": fallback_model,
            "sdk_used": run_mode == "claude_sdk",
        },
    )


def _draft_and_gate_unit(
    *,
    unit: BriefUnitV1,
    spec: ScriptSpecV1,
    evidence_pack: EvidencePackV1,
    run_mode: ArmName,
    provider: str | None = None,
    model: str | None = None,
) -> CoreScriptDraftV1:
    started_at = time.time()
    logger.info(
        "Phase3 v2 unit start: arm=%s brief_unit_id=%s",
        run_mode,
        unit.brief_unit_id,
    )
    try:
        draft = draft_core_script_v1(
            unit,
            spec,
            evidence_pack,
            run_mode=run_mode,
            provider=provider,
            model=model,
        )
    except Exception as exc:
        logger.exception("Phase3 v2 unexpected draft crash for %s (%s)", unit.brief_unit_id, run_mode)
        draft = _internal_error_draft(
            brief_unit=unit,
            run_mode=run_mode,
            provider=provider,
            model=model,
            error=f"internal_draft_failure: {exc}",
        )

    try:
        draft.gate_report = evaluate_m1_gates(draft, spec=spec, evidence_pack=evidence_pack)
    except Exception as exc:
        logger.exception("Phase3 v2 gate evaluation failed for %s (%s)", unit.brief_unit_id, run_mode)
        draft.gate_report = {
            "overall_pass": False,
            "checks": [
                {
                    "gate_id": "gate_evaluator_runtime",
                    "passed": False,
                    "required": "gate evaluation should complete",
                    "actual": str(exc),
                }
            ],
        }
    elapsed = round(time.time() - started_at, 3)
    gate_pass = bool((draft.gate_report or {}).get("overall_pass"))
    logger.info(
        "Phase3 v2 unit done: arm=%s brief_unit_id=%s status=%s gate_pass=%s latency=%.3fs",
        run_mode,
        unit.brief_unit_id,
        draft.status,
        gate_pass,
        elapsed,
    )
    return draft


def run_phase3_v2_m1(
    *,
    matrix_plan: dict[str, Any],
    foundation_brief: dict[str, Any],
    branch_id: str,
    brand_slug: str,
    pilot_size: int = 20,
    selected_brief_unit_ids: list[str] | None = None,
    ab_mode: bool = True,
    sdk_toggles: dict[str, Any] | None = None,
    reviewer_role: str = "client_founder",
    model_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute Milestone 1 phase3_v2 run and return all artifacts."""
    sdk_toggles = dict(sdk_toggles or {})
    model_overrides = dict(model_overrides or {})
    selected_brief_unit_ids = list(selected_brief_unit_ids or [])

    units = expand_brief_units(
        matrix_plan,
        branch_id=branch_id,
        brand_slug=brand_slug,
        pilot_size=max(1, int(pilot_size or 1)),
        selection_strategy="round_robin",
    )
    if selected_brief_unit_ids:
        selected = set(selected_brief_unit_ids)
        units = [u for u in units if u.brief_unit_id in selected]

    evidence_packs = [build_evidence_pack(unit, foundation_brief) for unit in units]
    specs = [compile_script_spec_v1(unit, pack) for unit, pack in zip(units, evidence_packs)]

    _ = ab_mode, sdk_toggles
    arms: list[ArmName] = ["claude_sdk"]

    drafts_by_arm: dict[str, list[CoreScriptDraftV1]] = {}
    max_parallel = max(1, int(getattr(config, "PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL", 1)))
    unit_triplets = list(zip(units, evidence_packs, specs))
    for arm in arms:
        arm_provider = None
        arm_model = None
        if isinstance(model_overrides.get(arm), dict):
            arm_provider = model_overrides[arm].get("provider")
            arm_model = model_overrides[arm].get("model")

        run_mode: ArmName = arm  # type: ignore[assignment]
        run_in_parallel = arm == "claude_sdk" and max_parallel > 1 and len(unit_triplets) > 1
        if run_in_parallel:
            workers = min(max_parallel, len(unit_triplets))
            logger.info(
                "Phase3 v2 core drafter parallel mode enabled: arm=%s workers=%d units=%d",
                arm,
                workers,
                len(unit_triplets),
            )
            ordered_drafts: list[CoreScriptDraftV1 | None] = [None] * len(unit_triplets)
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="p3v2-core") as pool:
                future_to_index: dict[Any, int] = {}
                for idx, (unit, pack, spec) in enumerate(unit_triplets):
                    logger.info(
                        "Phase3 v2 unit queued: arm=%s brief_unit_id=%s queue_index=%d/%d",
                        arm,
                        unit.brief_unit_id,
                        idx + 1,
                        len(unit_triplets),
                    )
                    future = pool.submit(
                        _draft_and_gate_unit,
                        unit=unit,
                        spec=spec,
                        evidence_pack=pack,
                        run_mode=run_mode,
                        provider=arm_provider,
                        model=arm_model,
                    )
                    future_to_index[future] = idx
                for future in as_completed(future_to_index):
                    idx = future_to_index[future]
                    unit, pack, spec = unit_triplets[idx]
                    try:
                        ordered_drafts[idx] = future.result()
                        logger.info(
                            "Phase3 v2 unit collected: arm=%s brief_unit_id=%s completed=%d/%d",
                            arm,
                            unit.brief_unit_id,
                            sum(1 for row in ordered_drafts if row is not None),
                            len(ordered_drafts),
                        )
                    except Exception as exc:
                        logger.exception(
                            "Phase3 v2 parallel worker failed for %s (%s)",
                            unit.brief_unit_id,
                            arm,
                        )
                        fallback = _internal_error_draft(
                            brief_unit=unit,
                            run_mode=run_mode,
                            provider=arm_provider,
                            model=arm_model,
                            error=f"parallel_worker_failure: {exc}",
                        )
                        try:
                            fallback.gate_report = evaluate_m1_gates(fallback, spec=spec, evidence_pack=pack)
                        except Exception as gate_exc:
                            fallback.gate_report = {
                                "overall_pass": False,
                                "checks": [
                                    {
                                        "gate_id": "gate_evaluator_runtime",
                                        "passed": False,
                                        "required": "gate evaluation should complete",
                                        "actual": str(gate_exc),
                                    }
                                ],
                            }
                        ordered_drafts[idx] = fallback
            arm_drafts = [draft for draft in ordered_drafts if isinstance(draft, CoreScriptDraftV1)]
        else:
            arm_drafts = [
                _draft_and_gate_unit(
                    unit=unit,
                    spec=spec,
                    evidence_pack=pack,
                    run_mode=run_mode,
                    provider=arm_provider,
                    model=arm_model,
                )
                for unit, pack, spec in unit_triplets
            ]
        drafts_by_arm[arm] = arm_drafts

    return {
        "brief_units": [u.model_dump() for u in units],
        "evidence_packs": [p.model_dump() for p in evidence_packs],
        "script_specs": [s.model_dump() for s in specs],
        "drafts_by_arm": {arm: [d.model_dump() for d in drafts] for arm, drafts in drafts_by_arm.items()},
        "reviewer_role": reviewer_role,
        "arms": arms,
    }


def compute_ab_summary(
    *,
    run_id: str,
    drafts_by_arm: dict[str, list[dict[str, Any]]],
    reviews: list[HumanQualityReviewV1],
) -> Phase3V2ABSummaryV1:
    """Compute A/B summary and winner with defined tie-breakers."""
    summaries: list[ArmSummaryV1] = []
    by_arm_reviews: dict[str, list[HumanQualityReviewV1]] = {}
    for review in reviews:
        by_arm_reviews.setdefault(review.arm, []).append(review)

    for arm, drafts in drafts_by_arm.items():
        if arm not in {"control", "claude_sdk"}:
            continue
        total_units = len(drafts)
        generated = [d for d in drafts if str(d.get("status")) == "ok"]
        blocked = [d for d in drafts if str(d.get("status")) == "blocked"]
        failed = [d for d in drafts if str(d.get("status")) == "error"]
        gate_pass = [
            d for d in generated
            if bool((d.get("gate_report") or {}).get("overall_pass"))
        ]

        latencies = [float(d.get("latency_seconds", 0.0) or 0.0) for d in generated if d.get("latency_seconds") is not None]
        costs = [float(d.get("cost_usd", 0.0) or 0.0) for d in generated if d.get("cost_usd") is not None]
        arm_reviews = by_arm_reviews.get(arm, [])
        quality_values = [float(r.quality_score_1_10) for r in arm_reviews]
        mean_quality, median_quality = compute_score_stats(quality_values)

        rejection_rate = None
        if arm_reviews:
            reject_count = sum(1 for r in arm_reviews if r.decision == "reject")
            rejection_rate = round(reject_count / len(arm_reviews), 4)

        summaries.append(
            ArmSummaryV1(
                arm=arm,  # type: ignore[arg-type]
                total_units=total_units,
                generated_units=len(generated),
                blocked_units=len(blocked),
                failed_units=len(failed),
                gate_pass_rate=round((len(gate_pass) / len(generated)), 4) if generated else 0.0,
                mean_quality_score=mean_quality,
                median_quality_score=median_quality,
                rejection_rate=rejection_rate,
                median_latency_seconds=round(float(median(latencies)), 4) if latencies else None,
                median_cost_usd=round(float(median(costs)), 6) if costs else None,
            )
        )

    winner = "insufficient_reviews"
    winner_reason = "No human review scores submitted yet."

    scored = [s for s in summaries if s.mean_quality_score is not None]
    if scored:
        scored_sorted = sorted(scored, key=lambda s: float(s.mean_quality_score or 0.0), reverse=True)
        top = scored_sorted[0]
        tied = [s for s in scored_sorted if math.isclose(float(s.mean_quality_score or 0.0), float(top.mean_quality_score or 0.0), rel_tol=1e-9, abs_tol=1e-9)]

        if len(tied) == 1:
            winner = top.arm
            winner_reason = "Highest mean human quality score."
        else:
            # Tie-breakers: gate pass rate, lower rejection, lower median latency, lower median cost.
            tied_sorted = sorted(
                tied,
                key=lambda s: (
                    float(s.gate_pass_rate),
                    -float(s.rejection_rate if s.rejection_rate is not None else 1.0),
                    -float(s.median_latency_seconds if s.median_latency_seconds is not None else 1e9),
                    -float(s.median_cost_usd if s.median_cost_usd is not None else 1e9),
                ),
                reverse=True,
            )
            if len(tied_sorted) >= 2:
                first = tied_sorted[0]
                second = tied_sorted[1]
                first_key = (
                    float(first.gate_pass_rate),
                    -float(first.rejection_rate if first.rejection_rate is not None else 1.0),
                    -float(first.median_latency_seconds if first.median_latency_seconds is not None else 1e9),
                    -float(first.median_cost_usd if first.median_cost_usd is not None else 1e9),
                )
                second_key = (
                    float(second.gate_pass_rate),
                    -float(second.rejection_rate if second.rejection_rate is not None else 1.0),
                    -float(second.median_latency_seconds if second.median_latency_seconds is not None else 1e9),
                    -float(second.median_cost_usd if second.median_cost_usd is not None else 1e9),
                )
                if first_key == second_key:
                    winner = "tie"
                    winner_reason = "Mean quality tie and tie-breakers equal."
                else:
                    winner = first.arm
                    winner_reason = "Mean quality tie; tie-breakers applied."
            else:
                winner = top.arm
                winner_reason = "Single arm with scores."

    return Phase3V2ABSummaryV1(
        run_id=run_id,
        arms=summaries,
        winner=winner,  # type: ignore[arg-type]
        winner_reason=winner_reason,
    )
