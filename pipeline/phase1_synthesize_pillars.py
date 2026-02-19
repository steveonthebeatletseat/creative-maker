"""Pillar synthesis DAG for Phase 1 v2."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import config
from pipeline.llm import call_llm_structured
from schemas.foundation_research import (
    DominantEmotion,
    EvidenceItem,
    Pillar1ProspectProfile,
    Pillar2VocLanguageBank,
    Pillar3CompetitiveIntelligence,
    Pillar4ProductMechanismAnalysis,
    Pillar5AwarenessClassification,
    Pillar6EmotionalDriverInventory,
    Pillar7ProofCredibilityInventory,
)


_PILLAR_MODEL_MAP: dict[str, type] = {
    "pillar_1": Pillar1ProspectProfile,
    "pillar_2": Pillar2VocLanguageBank,
    "pillar_3": Pillar3CompetitiveIntelligence,
    "pillar_4": Pillar4ProductMechanismAnalysis,
    "pillar_5": Pillar5AwarenessClassification,
    "pillar_6": Pillar6EmotionalDriverInventory,
    "pillar_7": Pillar7ProofCredibilityInventory,
}

_FIRST_WAVE = {"pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_7"}


def _prompt_template() -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / "synthesizer.md"
    return path.read_text("utf-8").strip()


def _pillar_instruction(pillar_id: str) -> str:
    if pillar_id == "pillar_1":
        return (
            "Generate Pillar 1 Prospect Profile. For each segment, answer Carlton-style "
            "prospect interrogation prompts in the provided schema fields: what keeps them up "
            "at night, what they fear, what frustrates them daily, what they secretly desire, "
            "what trends shape behavior, and the core objections. Segment profiles must include "
            "goals, pains, triggers, information sources, and objections with concrete language."
        )
    if pillar_id == "pillar_2":
        return (
            "Generate Pillar 2 VOC Language Bank. Include many verbatim quotes, categories, "
            "themes, quote IDs, dominant_emotion tags, and valid source_url/source_type for every quote."
        )
    if pillar_id == "pillar_3":
        return (
            "Generate Pillar 3 Competitive Intelligence. Include direct competitors, substitutes, "
            "and mechanism saturation map."
        )
    if pillar_id == "pillar_4":
        return (
            "Generate Pillar 4 Product Mechanism Analysis using Georgi's two-part mechanism: "
            "(1) why the problem structurally persists and (2) why this mechanism uniquely neutralizes it. "
            "Do not output a feature list. Provide causal explanation and supporting evidence IDs."
        )
    if pillar_id == "pillar_5":
        return (
            "Generate Pillar 5 Awareness Classification per segment with explicit Schwartz 5-level mapping: "
            "unaware, problem_aware, solution_aware, product_aware, most_aware. "
            "Return primary awareness, calibrated distribution, and supporting evidence IDs."
        )
    if pillar_id == "pillar_6":
        return (
            "Generate Pillar 6 Emotional Driver Inventory from Pillar 2 quote tags only (no invented quotes) "
            "with data-driven dominant emotions, counts, shares, and linked quote IDs."
        )
    if pillar_id == "pillar_7":
        return (
            "Generate Pillar 7 Proof and Credibility Inventory with Makepeace-aligned proof taxonomy "
            "(demonstration/testimonial/authority/story). Map each entry into schema proof types and ensure "
            "each proof type has at least one skeptic-surviving asset."
        )
    raise ValueError(f"Unknown pillar id: {pillar_id}")


def _summarize_reports(reports: list[str], max_chars: int | None = None) -> str:
    usable = [r for r in reports if r]
    if not usable:
        return ""
    cap = int(max_chars if max_chars is not None else config.PHASE1_SYNTH_REPORT_MAX_CHARS)
    cap = max(1, cap)
    if len(usable) == 1:
        return usable[0][:cap]

    lengths = [len(report) for report in usable]
    total_length = max(sum(lengths), 1)
    floor = max(750, cap // (len(usable) * 5))
    if floor * len(usable) > cap:
        floor = max(1, cap // len(usable))
    remaining = max(0, cap - floor * len(usable))

    allocations: list[int] = []
    for length in lengths:
        proportional = int(remaining * (length / total_length))
        allocations.append(floor + proportional)

    # Give leftover chars to longest reports deterministically.
    leftover = cap - sum(allocations)
    if leftover > 0:
        order = sorted(range(len(usable)), key=lambda idx: lengths[idx], reverse=True)
        ptr = 0
        while leftover > 0 and order:
            allocations[order[ptr % len(order)]] += 1
            leftover -= 1
            ptr += 1

    return "\n\n---\n\n".join(report[:alloc] for report, alloc in zip(usable, allocations))


def _build_user_prompt(
    *,
    pillar_id: str,
    context: dict[str, Any],
    evidence: list[EvidenceItem],
    collector_reports: list[str],
    dependencies: dict[str, Any] | None = None,
) -> str:
    deps_json = json.dumps(dependencies or {}, indent=2, default=str)
    evidence_json = json.dumps([e.model_dump() for e in evidence[:1200]], indent=2, default=str)
    ctx_json = json.dumps(context, indent=2, default=str)

    return (
        f"Target pillar: {pillar_id}\n"
        f"Instruction: {_pillar_instruction(pillar_id)}\n\n"
        "Research context:\n"
        f"{ctx_json}\n\n"
        "Normalized evidence ledger (partial):\n"
        f"{evidence_json}\n\n"
        "Collector reports (partial):\n"
        f"{_summarize_reports(collector_reports)}\n\n"
        "Dependency pillars:\n"
        f"{deps_json}\n"
    )


def _run_pillar(
    *,
    pillar_id: str,
    context: dict[str, Any],
    evidence: list[EvidenceItem],
    collector_reports: list[str],
    dependencies: dict[str, Any],
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
):
    schema = _PILLAR_MODEL_MAP[pillar_id]
    user_prompt = _build_user_prompt(
        pillar_id=pillar_id,
        context=context,
        evidence=evidence,
        collector_reports=collector_reports,
        dependencies=dependencies,
    )

    return call_llm_structured(
        system_prompt=f"{_prompt_template()}\n\n{_pillar_instruction(pillar_id)}",
        user_prompt=user_prompt,
        response_model=schema,
        provider=provider,
        model=model,
        temperature=temperature,
        max_tokens=max(4000, min(max_tokens, 24000)),
    )


def _normalise_emotion_label(value: str) -> str:
    token = (value or "").strip().lower()
    if not token:
        return ""
    aliases = {
        "frustration": "Frustration / Pain",
        "pain": "Frustration / Pain",
        "anger": "Frustration / Pain",
        "distrust": "Skepticism / Distrust",
        "skepticism": "Skepticism / Distrust",
        "scepticism": "Skepticism / Distrust",
        "fear": "Anxiety / Fear",
        "anxiety": "Anxiety / Fear",
        "relief": "Relief / Satisfaction",
        "satisfaction": "Relief / Satisfaction",
        "joy": "Relief / Satisfaction",
        "desire": "Desire for Freedom / Immersion",
        "freedom": "Desire for Freedom / Immersion",
        "immersion": "Desire for Freedom / Immersion",
        "status": "Pride / Status",
        "pride": "Pride / Status",
        "confidence": "Pride / Status",
        "urgency": "Urgency / FOMO",
        "fomo": "Urgency / FOMO",
    }
    for key, mapped in aliases.items():
        if key in token:
            return mapped
    return value.strip().title()


def _infer_emotion_from_quote(category: str, theme: str, quote: str) -> str:
    joined = f"{category} {theme} {quote}".lower()
    if any(t in joined for t in ("scam", "fake", "dropship", "trust", "legit", "skeptic")):
        return "Skepticism / Distrust"
    if any(t in joined for t in ("headache", "pain", "hurt", "pressure", "frustrat", "annoy")):
        return "Frustration / Pain"
    if any(t in joined for t in ("fear", "worry", "risk", "unsafe", "battery dying", "anx")):
        return "Anxiety / Fear"
    if any(t in joined for t in ("immers", "flow", "freedom", "uninterrupted", "desire")):
        return "Desire for Freedom / Immersion"
    if any(t in joined for t in ("proud", "premium", "best setup", "status")):
        return "Pride / Status"
    if category == "proof":
        return "Relief / Satisfaction"
    if category == "trigger":
        return "Urgency / FOMO"
    if category == "objection":
        return "Skepticism / Distrust"
    if category == "desire":
        return "Desire for Freedom / Immersion"
    if category == "pain":
        return "Frustration / Pain"
    return "Relief / Satisfaction"


def derive_emotional_inventory_from_voc(pillar_2: Pillar2VocLanguageBank) -> Pillar6EmotionalDriverInventory:
    quote_count = len(pillar_2.quotes)
    if quote_count == 0:
        return Pillar6EmotionalDriverInventory(dominant_emotions=[])

    buckets: dict[str, list[str]] = defaultdict(list)
    for quote in pillar_2.quotes:
        emotion = _normalise_emotion_label(quote.dominant_emotion)
        if not emotion:
            emotion = _infer_emotion_from_quote(quote.category, quote.theme, quote.quote)
        buckets[emotion].append(quote.quote_id)

    counts = Counter({k: len(v) for k, v in buckets.items()})
    ranked_all = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

    # Data-driven selection:
    # 1) Always keep high-confidence emotions (count>=8 and share>=0.05).
    # 2) Add additional meaningful tail emotions (count>=3 or share>=0.02).
    # 3) Ensure minimum coverage of 5 rows by backfilling from ranked list.
    ranked: list[tuple[str, int]] = []
    for emotion, count in ranked_all:
        share = count / quote_count
        if count >= 8 and share >= 0.05:
            ranked.append((emotion, count))

    for emotion, count in ranked_all:
        if any(existing[0] == emotion for existing in ranked):
            continue
        share = count / quote_count
        if count >= 3 or share >= 0.02:
            ranked.append((emotion, count))

    if len(ranked) < 5:
        for emotion, count in ranked_all:
            if any(existing[0] == emotion for existing in ranked):
                continue
            ranked.append((emotion, count))
            if len(ranked) >= 5:
                break

    dominant: list[DominantEmotion] = []
    for emotion, count in ranked:
        dominant.append(
            DominantEmotion(
                emotion=emotion,
                tagged_quote_count=count,
                share_of_voc=round(count / quote_count, 4),
                sample_quote_ids=buckets[emotion][:5],
            )
        )

    return Pillar6EmotionalDriverInventory(dominant_emotions=dominant)


def synthesize_pillars_dag(
    *,
    context: dict[str, Any],
    evidence: list[EvidenceItem],
    collector_reports: list[str],
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
    target_pillars: set[str] | None = None,
    existing_pillars: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run synthesis DAG with optional targeted pillar recomputation."""
    pillars: dict[str, Any] = dict(existing_pillars or {})
    requested = set(target_pillars or _PILLAR_MODEL_MAP.keys())

    first_wave_targets = requested.intersection(_FIRST_WAVE)
    if first_wave_targets:
        with ThreadPoolExecutor(max_workers=max(1, len(first_wave_targets))) as pool:
            futures = {
                pool.submit(
                    _run_pillar,
                    pillar_id=pillar_id,
                    context=context,
                    evidence=evidence,
                    collector_reports=collector_reports,
                    dependencies={},
                    provider=provider,
                    model=model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                ): pillar_id
                for pillar_id in sorted(first_wave_targets)
            }
            for future in as_completed(futures):
                pillar_id = futures[future]
                pillars[pillar_id] = future.result()

    if "pillar_6" in requested:
        p2 = pillars.get("pillar_2") or (existing_pillars or {}).get("pillar_2")
        if p2 is not None:
            pillars["pillar_6"] = derive_emotional_inventory_from_voc(p2)
        else:
            deps = {"pillar_2": None}
            pillars["pillar_6"] = _run_pillar(
                pillar_id="pillar_6",
                context=context,
                evidence=evidence,
                collector_reports=collector_reports,
                dependencies=deps,
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
            )

    if "pillar_5" in requested:
        deps = {
            "pillar_1": (pillars.get("pillar_1") or (existing_pillars or {}).get("pillar_1")),
            "pillar_2": (pillars.get("pillar_2") or (existing_pillars or {}).get("pillar_2")),
            "pillar_3": (pillars.get("pillar_3") or (existing_pillars or {}).get("pillar_3")),
            "pillar_4": (pillars.get("pillar_4") or (existing_pillars or {}).get("pillar_4")),
        }
        pillars["pillar_5"] = _run_pillar(
            pillar_id="pillar_5",
            context=context,
            evidence=evidence,
            collector_reports=collector_reports,
            dependencies=deps,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    return pillars
