"""Contradiction detection for Phase 1 evidence ledgers."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

import config
from pipeline.llm import call_llm_structured
from schemas.foundation_research import ContradictionReport, EvidenceItem


_POSITIVE_TOKENS = {
    "best",
    "great",
    "excellent",
    "works",
    "working",
    "improved",
    "reliable",
    "legit",
    "comfortable",
    "worth",
    "recommend",
}

_NEGATIVE_TOKENS = {
    "scam",
    "fake",
    "broken",
    "broke",
    "fails",
    "failed",
    "worst",
    "terrible",
    "awful",
    "overpriced",
    "uncomfortable",
    "refund",
    "doesn't work",
    "doesnt work",
}


def _prompt_template() -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / "contradiction_detector.md"
    return path.read_text("utf-8").strip()


def _tokenize(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-zA-Z]{3,}", (text or "").lower()) if t]


def _topic_key(item: EvidenceItem) -> str:
    pillars = sorted(item.pillar_tags or ["pillar_1"])
    pillar_signature = ",".join(pillars[:2])
    return pillar_signature


def _polarity_score(text: str) -> int:
    low = (text or "").lower()
    score = 0
    for token in _POSITIVE_TOKENS:
        if token in low:
            score += 1
    for token in _NEGATIVE_TOKENS:
        if token in low:
            score -= 1
    return score


def _likely_contradiction(a: EvidenceItem, b: EvidenceItem) -> bool:
    if a.provider == b.provider:
        return False
    if abs(a.confidence - b.confidence) > 0.45 and min(a.confidence, b.confidence) < 0.45:
        return False
    a_tokens = {tok for tok in _tokenize(f"{a.claim} {a.verbatim}") if tok not in {"this", "that", "with", "from"}}
    b_tokens = {tok for tok in _tokenize(f"{b.claim} {b.verbatim}") if tok not in {"this", "that", "with", "from"}}
    if len(a_tokens.intersection(b_tokens)) < 2:
        return False
    pa = _polarity_score(f"{a.claim} {a.verbatim}")
    pb = _polarity_score(f"{b.claim} {b.verbatim}")
    return pa * pb < 0


def _severity(a: EvidenceItem, b: EvidenceItem) -> str:
    floor = min(a.confidence, b.confidence)
    if floor >= 0.75:
        return "high"
    if floor >= 0.55:
        return "medium"
    return "low"


def _heuristic_scan(evidence: Iterable[EvidenceItem]) -> list[ContradictionReport]:
    by_topic: dict[str, list[EvidenceItem]] = defaultdict(list)
    for item in evidence:
        by_topic[_topic_key(item)].append(item)

    reports: list[ContradictionReport] = []
    seen_pairs: set[str] = set()
    for items in by_topic.values():
        if len(items) < 2:
            continue
        by_provider: dict[str, list[EvidenceItem]] = defaultdict(list)
        for item in items:
            by_provider[item.provider].append(item)
        providers = sorted(by_provider.keys())
        if len(providers) < 2:
            continue

        for idx in range(len(providers)):
            for jdx in range(idx + 1, len(providers)):
                left = by_provider[providers[idx]]
                right = by_provider[providers[jdx]]
                for a in left:
                    for b in right:
                        if not _likely_contradiction(a, b):
                            continue
                        key = "|".join(sorted([a.evidence_id, b.evidence_id]))
                        if key in seen_pairs:
                            continue
                        seen_pairs.add(key)
                        sev = _severity(a, b)
                        reports.append(
                            ContradictionReport(
                                claim_a_id=a.evidence_id,
                                claim_b_id=b.evidence_id,
                                provider_a=a.provider,
                                provider_b=b.provider,
                                conflict_description=(
                                    "Potential polarity conflict between claims from different collectors."
                                ),
                                severity=sev,
                                resolution=(
                                    "Prefer the claim with stronger confidence and corroboration unless manually overridden."
                                ),
                                resolved=(sev != "high"),
                            )
                        )
    return reports


def _llm_refine_conflicts(
    candidates: list[ContradictionReport],
    evidence_lookup: dict[str, EvidenceItem],
    *,
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
) -> list[ContradictionReport]:
    if not candidates:
        return []

    payload = []
    for item in candidates[:120]:
        a = evidence_lookup.get(item.claim_a_id)
        b = evidence_lookup.get(item.claim_b_id)
        if not a or not b:
            continue
        payload.append(
            {
                "candidate": item.model_dump(),
                "claim_a": a.model_dump(),
                "claim_b": b.model_dump(),
            }
        )
    if not payload:
        return candidates

    from pydantic import BaseModel, Field

    class _Response(BaseModel):
        contradictions: list[ContradictionReport] = Field(default_factory=list)

    user_prompt = (
        "Candidate contradiction pairs:\n"
        f"{json.dumps(payload, indent=2, default=str)}\n\n"
        "Return only contradictions that are true conflicts. Adjust severity/resolution/resolved as needed."
    )
    output = call_llm_structured(
        system_prompt=_prompt_template(),
        user_prompt=user_prompt,
        response_model=_Response,
        provider=provider,
        model=model,
        temperature=min(0.2, max(0.0, temperature)),
        max_tokens=max(3000, min(max_tokens, 12000)),
    )
    return output.contradictions


def detect_contradictions(
    evidence: list[EvidenceItem],
    *,
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
) -> list[ContradictionReport]:
    """Detect cross-provider conflicts and return contradiction reports."""
    heuristic = _heuristic_scan(evidence)
    if not heuristic:
        return []
    if not config.PHASE1_CONTRADICTION_USE_LLM:
        return heuristic
    lookup = {item.evidence_id: item for item in evidence}
    try:
        return _llm_refine_conflicts(
            heuristic,
            lookup,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except Exception:
        return heuristic


def apply_contradiction_flags(
    evidence: list[EvidenceItem],
    contradictions: list[ContradictionReport],
) -> list[EvidenceItem]:
    """Apply highest-severity conflict flags onto evidence rows."""
    severity_rank = {"": 0, "low": 1, "medium": 2, "high_unresolved": 3}
    lookup = {item.evidence_id: item for item in evidence}
    for entry in contradictions:
        target = "high_unresolved" if (entry.severity == "high" and not entry.resolved) else entry.severity
        for evidence_id in (entry.claim_a_id, entry.claim_b_id):
            item = lookup.get(evidence_id)
            if item is None:
                continue
            if severity_rank.get(target, 0) > severity_rank.get(item.conflict_flag, 0):
                item.conflict_flag = target
    return evidence
