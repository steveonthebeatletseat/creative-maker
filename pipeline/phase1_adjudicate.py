"""Adjudication and cross-pillar consistency for Phase 1 v2."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

import config
from pipeline.llm import call_llm_structured
from schemas.foundation_research import (
    CrossPillarConsistencyReport,
    EvidenceItem,
    Pillar1ProspectProfile,
    Pillar2VocLanguageBank,
    Pillar3CompetitiveIntelligence,
    Pillar4ProductMechanismAnalysis,
    Pillar5AwarenessClassification,
    Pillar6EmotionalDriverInventory,
    Pillar7ProofCredibilityInventory,
)


class AdjudicationOutput(BaseModel):
    pillar_1_prospect_profile: Pillar1ProspectProfile
    pillar_2_voc_language_bank: Pillar2VocLanguageBank
    pillar_3_competitive_intelligence: Pillar3CompetitiveIntelligence
    pillar_4_product_mechanism_analysis: Pillar4ProductMechanismAnalysis
    pillar_5_awareness_classification: Pillar5AwarenessClassification
    pillar_6_emotional_driver_inventory: Pillar6EmotionalDriverInventory
    pillar_7_proof_credibility_inventory: Pillar7ProofCredibilityInventory
    cross_pillar_consistency_report: CrossPillarConsistencyReport


def _prompt_template() -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / "judge.md"
    return path.read_text("utf-8").strip()


def _fallback_consistency(
    p1: Pillar1ProspectProfile,
    p2: Pillar2VocLanguageBank,
    p3: Pillar3CompetitiveIntelligence,
    p4: Pillar4ProductMechanismAnalysis,
    p6: Pillar6EmotionalDriverInventory,
) -> CrossPillarConsistencyReport:
    issues: list[str] = []

    p1_obj_tokens = {
        token
        for seg in p1.segment_profiles
        for objection in seg.objections
        for token in objection.lower().split()
        if len(token) > 3
    }
    p2_theme_text = " ".join(q.theme.lower() for q in p2.quotes)
    objections_ok = any(tok in p2_theme_text for tok in p1_obj_tokens) if p1_obj_tokens else False
    if not objections_ok:
        issues.append("Top objections from Pillar 1 are weakly represented in Pillar 2 themes.")

    mechanism_ok = True
    p4_key = p4.primary_mechanism_name.strip().lower()
    if p4_key:
        map_values = [m.mechanism.lower() for m in p3.mechanism_saturation_map]
        mechanism_ok = any(p4_key in m or m in p4_key for m in map_values)
    if not mechanism_ok:
        issues.append("Pillar 4 mechanism is not clearly aligned with Pillar 3 saturation map.")

    quote_ids = {q.quote_id for q in p2.quotes}
    emotions_ok = True
    for emo in p6.dominant_emotions:
        if not any(qid in quote_ids for qid in emo.sample_quote_ids):
            emotions_ok = False
            break
    if not emotions_ok:
        issues.append("Some dominant emotions in Pillar 6 are not traceable to Pillar 2 quote IDs.")

    return CrossPillarConsistencyReport(
        objections_represented_in_voc=objections_ok,
        mechanism_alignment_with_competition=mechanism_ok,
        dominant_emotions_traced_to_voc=emotions_ok,
        issues=issues,
    )


def adjudicate_pillars(
    *,
    context: dict[str, Any],
    evidence: list[EvidenceItem],
    pillars: dict[str, Any],
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
) -> AdjudicationOutput:
    user_prompt = (
        "Research context:\n"
        f"{json.dumps(context, indent=2, default=str)}\n\n"
        "Evidence ledger:\n"
        f"{json.dumps([e.model_dump() for e in evidence[:1200]], indent=2, default=str)}\n\n"
        "Pillar drafts:\n"
        f"{json.dumps({k: v.model_dump() for k, v in pillars.items()}, indent=2, default=str)}"
    )

    try:
        return call_llm_structured(
            system_prompt=_prompt_template(),
            user_prompt=user_prompt,
            response_model=AdjudicationOutput,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max(4000, min(max_tokens, 20000)),
        )
    except Exception:
        # Deterministic fallback if adjudication LLM fails.
        return AdjudicationOutput(
            pillar_1_prospect_profile=pillars["pillar_1"],
            pillar_2_voc_language_bank=pillars["pillar_2"],
            pillar_3_competitive_intelligence=pillars["pillar_3"],
            pillar_4_product_mechanism_analysis=pillars["pillar_4"],
            pillar_5_awareness_classification=pillars["pillar_5"],
            pillar_6_emotional_driver_inventory=pillars["pillar_6"],
            pillar_7_proof_credibility_inventory=pillars["pillar_7"],
            cross_pillar_consistency_report=_fallback_consistency(
                p1=pillars["pillar_1"],
                p2=pillars["pillar_2"],
                p3=pillars["pillar_3"],
                p4=pillars["pillar_4"],
                p6=pillars["pillar_6"],
            ),
        )
