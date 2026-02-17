"""Phase 3 v2 engine (Milestone 1).

Scope:
- Brief Unit expansion from matrix plan
- Evidence pack construction from Foundation Research
- Deterministic script spec compilation
- Core script drafting (control vs Claude SDK arm)
- M1 quality gate evaluation
- A/B summary computation from drafts + human reviews
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
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
        "unaware": "Lead with pattern interrupt and empathy before mentioning product.",
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
        word_count_max=170,
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
        "Do not invent evidence IDs. Use only IDs provided in the evidence pack."
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
    if evidence_pack.coverage_report.blocked_evidence_insufficient:
        return CoreScriptDraftV1(
            script_id=script_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=run_mode,
            status="blocked",
            error="blocked_evidence_insufficient",
            model_metadata={
                "provider": provider or "",
                "model": model or "",
                "sdk_used": bool(run_mode == "claude_sdk"),
            },
        )

    default_conf = config.get_agent_llm_config("copywriter")
    final_provider = str(provider or default_conf.get("provider") or "openai")
    final_model = str(model or default_conf.get("model") or config.DEFAULT_MODEL)
    sdk_used = run_mode == "claude_sdk"

    system_prompt, user_prompt = _build_generation_prompts(brief_unit, spec, evidence_pack)
    started = time.time()
    try:
        usage_before = len(get_usage_log())
    except Exception:
        usage_before = 0
    try:
        if sdk_used:
            generated = call_claude_agent_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=CoreScriptGeneratedV1,
                model=final_model,
                # Milestone 1 default: no tools.
                allowed_tools=[],
                max_turns=6,
                max_thinking_tokens=8_000,
            )
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

        elapsed = round(time.time() - started, 3)
        draft = CoreScriptDraftV1(
            script_id=script_id,
            brief_unit_id=brief_unit.brief_unit_id,
            arm=run_mode,
            sections=generated.sections,
            lines=generated.lines,
            status="ok",
            latency_seconds=elapsed,
            cost_usd=_usage_delta(usage_before),
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
                "provider": "anthropic" if sdk_used else final_provider,
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

    core_sdk_enabled = bool(sdk_toggles.get("core_script_drafter", False))
    arms: list[ArmName] = ["control"]
    if core_sdk_enabled:
        if bool(ab_mode):
            arms.append("claude_sdk")
        else:
            # Single-arm mode follows the step toggle directly.
            arms = ["claude_sdk"]

    drafts_by_arm: dict[str, list[CoreScriptDraftV1]] = {}
    for arm in arms:
        arm_provider = None
        arm_model = None
        if isinstance(model_overrides.get(arm), dict):
            arm_provider = model_overrides[arm].get("provider")
            arm_model = model_overrides[arm].get("model")

        arm_drafts: list[CoreScriptDraftV1] = []
        for unit, pack, spec in zip(units, evidence_packs, specs):
            draft = draft_core_script_v1(
                unit,
                spec,
                pack,
                run_mode=arm,  # type: ignore[arg-type]
                provider=arm_provider,
                model=arm_model,
            )
            draft.gate_report = evaluate_m1_gates(draft, spec=spec, evidence_pack=pack)
            arm_drafts.append(draft)
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
