"""Pillar synthesis DAG for Phase 1 v2."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import config
from pipeline.llm import call_llm_structured
from schemas.foundation_research import (
    DominantEmotion,
    EvidenceItem,
    LF8Code,
    LF8EmotionRow,
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
            "Generate Pillar 6 Emotional Driver Inventory using Step 1 collector Pillar 6 findings as the "
            "primary taxonomy, then verify against Pillar 2 VOC for counts/shares and quote-ID traceability. "
            "Do not collapse into generic legacy labels."
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
    raw = str(value or "").strip()
    if not raw:
        return ""

    cleaned = re.sub(r"\s+", " ", raw).strip(" \t\r\n-_/|")
    cleaned = re.sub(r"\s*/\s*", " / ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    low = cleaned.lower()
    if low in {"na", "n/a", "none", "unknown", "other", "other emotion", "misc", "miscellaneous"}:
        return ""
    if cleaned.islower() or cleaned.isupper():
        out_tokens: list[str] = []
        for token in cleaned.lower().split(" "):
            if token in {"and", "or", "of", "for", "to", "the", "a", "an", "/"}:
                out_tokens.append(token)
            else:
                out_tokens.append(token.capitalize())
        cleaned = " ".join(out_tokens)
        cleaned = re.sub(r"\s*/\s*", " / ", cleaned).strip()
    return cleaned


def _infer_emotion_from_quote(category: str, theme: str, quote: str) -> str:
    joined = f"{category} {theme} {quote}".lower()
    if any(t in joined for t in ("scam", "fake", "dropship", "trust", "legit", "skeptic")):
        return "Skepticism / Distrust"
    if any(t in joined for t in ("headache", "pain", "hurt", "pressure", "frustrat", "annoy")):
        return "Frustration / Pain"
    if any(t in joined for t in ("fear", "worry", "risk", "unsafe", "battery dying", "anx")):
        return "Anxiety / Fear"
    if any(t in joined for t in ("proud", "premium", "best setup", "status")):
        return "Pride / Status"
    if any(t in joined for t in ("relief", "works", "fixed", "solved", "finally")):
        return "Relief / Satisfaction"
    if any(t in joined for t in ("immers", "flow", "freedom", "uninterrupted", "desire", "want", "goal", "aspire")):
        return "Desire / Aspiration"
    if category == "proof":
        return "Relief / Satisfaction"
    if category == "trigger":
        return "Urgency / Pressure"
    if category == "objection":
        return "Skepticism / Distrust"
    if category == "desire":
        return "Desire / Aspiration"
    if category == "pain":
        return "Frustration / Pain"
    return "Neutral / Mixed"


_P6_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "into",
    "just",
    "they",
    "them",
    "their",
    "about",
    "across",
    "driver",
    "drivers",
    "objection",
    "objections",
    "pattern",
    "patterns",
    "dominant",
    "emotion",
    "emotions",
    "emotional",
    "trigger",
    "top",
    "frequency",
    "source",
    "sources",
    "distinct",
    "appears",
    "appear",
    "discussion",
    "discussions",
    "category",
    "level",
    "levels",
}

_P6_TOKEN_ALIASES = {
    "frustrated": "frustration",
    "frustrating": "frustration",
    "skeptical": "skepticism",
    "skeptic": "skepticism",
    "distrust": "skepticism",
    "distrusting": "skepticism",
    "anxious": "anxiety",
    "worried": "anxiety",
    "worry": "anxiety",
    "fearful": "fear",
    "stuck": "stagnation",
    "stagnant": "stagnation",
    "placebo": "placebo",
    "dosage": "dose",
    "dosing": "dose",
    "dosed": "dose",
    "efficacy": "work",
    "effective": "work",
    "ineffective": "work",
    "works": "work",
    "working": "work",
    "rituals": "ritual",
    "identity-level": "identity",
    "identitybased": "identity",
    "expensive": "price",
    "overpriced": "price",
    "costly": "price",
    "wasting": "waste",
    "wasted": "waste",
    "tastes": "taste",
    "aftertaste": "taste",
    "bitter": "taste",
}

_P6_SECTION_RE = re.compile(
    r"(?ims)^##\s*Pillar\s*6\b[^\n]*\n(?P<body>.*?)(?=^##\s*Pillar\s*7\b|^##\s*Contradictions\b|^##\s*Coverage\b|\Z)"
)
_P6_KEY_FINDINGS_RE = re.compile(
    r"(?ims)^###\s*Key Findings\s*(?P<body>.*?)(?=^###\s*Evidence Lines\b|^##\s+|\Z)"
)
_P6_BULLET_RE = re.compile(
    r"(?ms)^\s*-\s+(?P<item>.*?)(?=^\s*-\s+|^\s*###\s+|^\s*##\s+|\Z)"
)

_LF8_LABELS: dict[LF8Code, str] = {
    LF8Code.LF8_1: "Survival & Vitality",
    LF8Code.LF8_2: "Sensory Enjoyment",
    LF8Code.LF8_3: "Freedom from Fear / Pain",
    LF8Code.LF8_4: "Attraction & Intimacy",
    LF8Code.LF8_5: "Comfort & Convenience",
    LF8Code.LF8_6: "Status & Winning",
    LF8Code.LF8_7: "Care for Loved Ones",
    LF8Code.LF8_8: "Belonging & Social Approval",
}

_LF8_TOKEN_HINTS: dict[LF8Code, set[str]] = {
    LF8Code.LF8_1: {
        "survival",
        "energy",
        "vitality",
        "alive",
        "health",
        "brain",
        "focus",
        "stamina",
        "performance",
        "fatigue",
    },
    LF8Code.LF8_2: {
        "taste",
        "flavor",
        "aftertaste",
        "bitter",
        "sweet",
        "enjoy",
        "mouthfeel",
        "sensorial",
    },
    LF8Code.LF8_3: {
        "fear",
        "anxiety",
        "risk",
        "pain",
        "unsafe",
        "scam",
        "skepticism",
        "doubt",
        "waste",
        "price",
        "money",
        "placebo",
        "work",
        "dose",
    },
    LF8Code.LF8_4: {
        "sex",
        "sexual",
        "attract",
        "attractive",
        "dating",
        "desirable",
        "intimacy",
        "romance",
        "libido",
    },
    LF8Code.LF8_5: {
        "comfort",
        "convenience",
        "easy",
        "ease",
        "simple",
        "routine",
        "friction",
        "effort",
        "hassle",
        "calm",
    },
    LF8Code.LF8_6: {
        "status",
        "prestige",
        "elite",
        "winner",
        "winning",
        "superior",
        "dominate",
        "discipline",
        "identity",
        "ritual",
        "competitive",
    },
    LF8Code.LF8_7: {
        "family",
        "child",
        "children",
        "kid",
        "kids",
        "partner",
        "spouse",
        "protect",
        "care",
        "loved",
        "parent",
    },
    LF8Code.LF8_8: {
        "social",
        "belong",
        "approval",
        "accepted",
        "respect",
        "peer",
        "community",
        "reputation",
        "embarrass",
        "judge",
    },
}

_LF8_PURCHASE_TOKENS = {
    "buy",
    "price",
    "money",
    "subscription",
    "expensive",
    "cost",
    "refund",
    "worth",
    "waste",
    "trial",
}

_LF8_PROOF_URGENCY_TOKENS = {
    "proof",
    "evidence",
    "study",
    "clinical",
    "dose",
    "verified",
    "data",
    "results",
    "works",
    "work",
}

_LF8_IDENTITY_TOKENS = {
    "identity",
    "status",
    "elite",
    "winner",
    "discipline",
    "reputation",
    "belong",
}

_LF8_STRICT_MIN_QUOTES = 2
_LF8_STRICT_MIN_DOMAINS = 1


def _extract_explicit_lf8_code(text: str) -> LF8Code | None:
    value = str(text or "")
    if not value:
        return None
    match = re.search(r"(?i)\blf8[\s_\-:]*([1-8])\b", value)
    if match:
        try:
            return LF8Code(f"lf8_{int(match.group(1))}")
        except Exception:
            return None
    match = re.search(r"(?i)\bcandidate\s*lf8\s*code\s*[:=]\s*(lf8_[1-8])\b", value)
    if match:
        try:
            return LF8Code(str(match.group(1)).lower())
        except Exception:
            return None
    return None


def _score_lf8_tokens(tokens: set[str]) -> dict[LF8Code, float]:
    scores: dict[LF8Code, float] = {code: 0.0 for code in _LF8_LABELS}
    if not tokens:
        return scores
    for code, hints in _LF8_TOKEN_HINTS.items():
        overlap = tokens.intersection(hints)
        if overlap:
            scores[code] += float(len(overlap))
    if {"scam", "skepticism", "doubt", "placebo", "risk"} & tokens:
        scores[LF8Code.LF8_3] += 1.25
    if {"comfort", "easy", "convenience", "friction"} & tokens:
        scores[LF8Code.LF8_5] += 0.9
    if {"status", "elite", "winner", "identity"} & tokens:
        scores[LF8Code.LF8_6] += 1.0
    if {"belong", "approval", "social"} & tokens:
        scores[LF8Code.LF8_8] += 0.8
    return scores


def _best_lf8_code(scores: dict[LF8Code, float]) -> tuple[LF8Code | None, float]:
    if not scores:
        return None, 0.0
    ordered = sorted(scores.items(), key=lambda item: (-float(item[1]), item[0].value))
    top_code, top_score = ordered[0]
    if top_score <= 0:
        return None, 0.0
    if len(ordered) >= 2 and (float(top_score) - float(ordered[1][1])) < 0.25:
        return None, float(top_score)
    return top_code, float(top_score)


def _normalize_url_for_match(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw.strip().lower()
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or ""
    if not netloc:
        return raw.strip().lower()
    while path.endswith("/") and len(path) > 1:
        path = path[:-1]
    return f"{scheme}://{netloc}{path}"


def _extract_domain(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        host = (urlparse(raw).netloc or "").lower().strip()
    except Exception:
        host = ""
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _normalize_emotion_key_basic(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _tokenize_match_terms(value: str) -> set[str]:
    tokens: set[str] = set()
    for raw in re.findall(r"[a-z0-9]{3,}", str(value or "").lower()):
        token = _P6_TOKEN_ALIASES.get(raw, raw)
        if token in _P6_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def _strip_markdown_inline(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = re.sub(r"\*\*|__|`", "", text)
    text = re.sub(r"\[(.*?)\]\((https?://[^)]+)\)", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_provider_name(report: str, index: int) -> str:
    header = re.search(r"(?im)^#\s*Collector[^\n]*\(([^)]+)\)", report or "")
    if header:
        label = str(header.group(1) or "").strip().lower()
        if label:
            return label
    return f"collector_{index + 1}"


def _extract_pillar6_bullets(report: str) -> list[str]:
    text = str(report or "")
    if not text:
        return []
    section_match = _P6_SECTION_RE.search(text)
    if not section_match:
        return []
    section_body = section_match.group("body") or ""
    key_findings_match = _P6_KEY_FINDINGS_RE.search(section_body)
    findings_body = key_findings_match.group("body") if key_findings_match else section_body
    bullets: list[str] = []
    for match in _P6_BULLET_RE.finditer(findings_body):
        item = _strip_markdown_inline(match.group("item"))
        if item:
            bullets.append(item)
    return bullets


def _extract_labels_from_p6_bullet(text: str) -> list[tuple[str, str]]:
    line = _strip_markdown_inline(text)
    if not line:
        return []

    labels: list[tuple[str, str]] = []
    for pattern, kind in [
        (r"(?i)\bdriver\s*[:\u2014\-]\s*\"?([^\":]+?)\"?\s*(?::|$)", "driver"),
        (r"(?i)\bobjection(?:\s*pattern\s*#?\d+)?\s*[:\u2014\-]\s*\"?([^\":]+?)\"?\s*(?::|$)", "objection"),
        (r"(?i)\bemotional trigger\s*[:\u2014\-]\s*\"?([^\":]+?)\"?\s*(?::|$)", "driver"),
    ]:
        match = re.search(pattern, line)
        if not match:
            continue
        label = str(match.group(1) or "").strip(" \"'-.")
        if label:
            labels.append((label, kind))

    dom = re.search(r"(?i)\bdominant emotions?\s*:\s*(.+)", line)
    if dom:
        chunk = re.split(r"(?i)\bfrequency\s*:", str(dom.group(1) or ""), maxsplit=1)[0]
        for part in re.split(r",|;", chunk):
            label = re.sub(r"\([^)]*\)", "", part).strip(" \"'-.")
            if label:
                labels.append((label, "emotion"))

    if not labels and ":" in line:
        left = line.split(":", 1)[0].strip(" -")
        if 0 < len(left.split()) <= 6 and left.lower() not in {"frequency", "key findings", "evidence lines"}:
            labels.append((left, "emotion"))

    return labels


def _cluster_p6_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for cand in candidates:
        label_key = str(cand.get("label_key") or "")
        cand_tokens = set(cand.get("tokens") or set())
        cand_support = set(cand.get("support_tokens") or set())
        explicit_code = cand.get("explicit_lf8_code")
        merged = False
        for cluster in clusters:
            cluster_tokens = set(cluster.get("tokens") or set())
            denom = max(len(cand_tokens | cand_support | cluster_tokens), 1)
            overlap = len((cand_tokens | cand_support) & cluster_tokens)
            same_key = bool(label_key and label_key == cluster.get("label_key"))
            if same_key or overlap >= 2 or (overlap / denom) >= 0.45:
                cluster["providers"].add(cand["provider"])
                cluster["mentions"].append(cand)
                cluster["tokens"] = cluster_tokens | cand_tokens | cand_support
                if isinstance(explicit_code, LF8Code):
                    cluster.setdefault("explicit_lf8_votes", Counter())[explicit_code.value] += 1
                # Keep the first observed label; this preserves brand-specific wording from Step 1.
                merged = True
                break
        if merged:
            continue
        explicit_votes: Counter[str] = Counter()
        if isinstance(explicit_code, LF8Code):
            explicit_votes[explicit_code.value] += 1
        clusters.append(
            {
                "label": cand["label"],
                "label_key": label_key,
                "kind": cand["kind"],
                "providers": {cand["provider"]},
                "tokens": set(cand_tokens | cand_support),
                "mentions": [cand],
                "first_order": int(cand.get("order", 0)),
                "explicit_lf8_votes": explicit_votes,
            }
        )
    return clusters


def _collect_p6_candidates(collector_reports: list[str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    order = 0
    for idx, report in enumerate(collector_reports or []):
        report_text = str(report or "").strip()
        if not report_text:
            continue
        provider = _extract_provider_name(report_text, idx)
        for bullet in _extract_pillar6_bullets(report_text):
            labels = _extract_labels_from_p6_bullet(bullet)
            if not labels:
                continue
            explicit_lf8_code = _extract_explicit_lf8_code(bullet)
            support_tokens = _tokenize_match_terms(bullet)
            for label, kind in labels:
                cleaned_label = _strip_markdown_inline(label).strip(" \"'-.")
                if not cleaned_label:
                    continue
                label_tokens = _tokenize_match_terms(cleaned_label)
                if not label_tokens and not support_tokens:
                    continue
                candidates.append(
                    {
                        "label": cleaned_label,
                        "label_key": _normalize_emotion_key_basic(cleaned_label),
                        "kind": kind,
                        "provider": provider,
                        "tokens": label_tokens,
                        "support_tokens": support_tokens,
                        "explicit_lf8_code": explicit_lf8_code,
                        "order": order,
                    }
                )
                order += 1
    return candidates


def _infer_lf8_for_quote(
    quote: Any,
    *,
    allowed_codes: set[LF8Code] | None = None,
    cluster_hint: LF8Code | None = None,
) -> tuple[LF8Code | None, dict[LF8Code, float], set[str]]:
    tokens = _tokenize_match_terms(
        " ".join(
            [
                str(getattr(quote, "quote", "") or ""),
                str(getattr(quote, "theme", "") or ""),
                str(getattr(quote, "category", "") or ""),
                str(getattr(quote, "dominant_emotion", "") or ""),
            ]
        )
    )
    scores = _score_lf8_tokens(tokens)
    category = str(getattr(quote, "category", "") or "").strip().lower()
    if category in {"pain", "objection", "trigger"}:
        scores[LF8Code.LF8_3] += 0.9
    if category == "desire":
        scores[LF8Code.LF8_1] += 0.4
        scores[LF8Code.LF8_5] += 0.35
        scores[LF8Code.LF8_6] += 0.25
    if category == "proof":
        scores[LF8Code.LF8_3] += 0.25
        scores[LF8Code.LF8_5] += 0.3
        scores[LF8Code.LF8_6] += 0.2
    if cluster_hint is not None:
        scores[cluster_hint] += 0.65

    if allowed_codes:
        for code in list(scores.keys()):
            if code not in allowed_codes:
                scores[code] = 0.0

    best_code, _ = _best_lf8_code(scores)
    return best_code, scores, tokens


def _shorten_text(value: str, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)].rstrip()}…"


def _infer_required_proof(code: LF8Code, tokens: set[str]) -> str:
    if code == LF8Code.LF8_3:
        if {"placebo", "dose", "work"} & tokens:
            return "Dose + efficacy proof with third-party support."
        if {"price", "money", "waste"} & tokens:
            return "Price-to-value proof with transparent comparison."
        return "Risk-reduction proof that the claim works in real use."
    if code == LF8Code.LF8_5:
        if {"taste", "aftertaste", "bitter"} & tokens:
            return "Sensory proof from verified users on taste/usability."
        return "Ease-of-use proof with before/after friction reduction."
    if code == LF8Code.LF8_6:
        return "Identity/status proof from credible peers or performance outcomes."
    if code == LF8Code.LF8_8:
        return "Social proof that trusted peers adopt and recommend it."
    if code == LF8Code.LF8_7:
        return "Trust/safety proof suitable for protecting loved ones."
    if code == LF8Code.LF8_1:
        return "Performance/vitality proof tied to measurable outcomes."
    if code == LF8Code.LF8_2:
        return "Taste/sensory proof from first-hand user feedback."
    if code == LF8Code.LF8_4:
        return "Attraction/confidence proof framed with compliant evidence."
    return "Credible proof aligned to the emotional objection."


def _infer_emotion_angle(code: LF8Code, top_theme: str) -> str:
    base = {
        LF8Code.LF8_1: "Regain reliable performance and momentum.",
        LF8Code.LF8_2: "Make the experience enjoyable, not a chore.",
        LF8Code.LF8_3: "Remove risk and fear before asking for trust.",
        LF8Code.LF8_4: "Connect the result to confidence and attraction.",
        LF8Code.LF8_5: "Lower daily friction and make consistency easier.",
        LF8Code.LF8_6: "Signal elite standards and high-performance identity.",
        LF8Code.LF8_7: "Protect people they care about from downside.",
        LF8Code.LF8_8: "Reinforce belonging, respect, and peer approval.",
    }.get(code, "Anchor the decision in a high-buying-power emotional driver.")
    theme = str(top_theme or "").strip()
    if theme:
        return f"{base} Theme anchor: {theme}."
    return base


def _extract_segment_catalog(
    pillar_2: Pillar2VocLanguageBank,
    allowed_segments: list[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    ordered: list[str] = []
    by_key: dict[str, str] = {}

    if allowed_segments:
        for raw in allowed_segments:
            segment = str(raw or "").strip()
            if not segment:
                continue
            key = " ".join(segment.lower().split())
            if not key or key in by_key:
                continue
            by_key[key] = segment
            ordered.append(segment)
    else:
        for quote in pillar_2.quotes:
            segment = str(getattr(quote, "segment_name", "") or "").strip()
            if not segment:
                continue
            key = " ".join(segment.lower().split())
            if not key or key in by_key:
                continue
            by_key[key] = segment
            ordered.append(segment)

    return ordered, by_key


def _build_lf8_rows_by_segment(
    *,
    selected_rows: list[tuple[dict[str, Any], list[str], int]],
    pillar_2: Pillar2VocLanguageBank,
    evidence: list[EvidenceItem],
    allowed_segments: list[str] | None = None,
) -> dict[str, list[LF8EmotionRow]]:
    segment_order, segment_by_key = _extract_segment_catalog(pillar_2, allowed_segments=allowed_segments)
    if not segment_order:
        return {}

    quote_by_id = {
        str(quote.quote_id or "").strip(): quote
        for quote in pillar_2.quotes
        if str(quote.quote_id or "").strip()
    }
    evidence_by_id = {
        str(item.evidence_id or "").strip(): item
        for item in (evidence or [])
        if str(item.evidence_id or "").strip()
    }

    evidence_by_url: dict[str, list[EvidenceItem]] = defaultdict(list)
    evidence_token_rows: list[tuple[EvidenceItem, set[str]]] = []
    for item in evidence_by_id.values():
        url_key = _normalize_url_for_match(item.source_url)
        if url_key:
            evidence_by_url[url_key].append(item)
        tokens = _tokenize_match_terms(
            " ".join([str(item.claim or ""), str(item.verbatim or "")])
        )
        if tokens:
            evidence_token_rows.append((item, tokens))

    # Build LF8 prior from collector-emotion clusters.
    cluster_codes: list[LF8Code] = []
    quote_code_votes: dict[str, Counter[str]] = defaultdict(Counter)
    quote_token_cache: dict[str, set[str]] = {}

    for cluster, quote_ids, _provider_count in selected_rows:
        cluster_tokens = set(cluster.get("tokens") or set())
        scores = _score_lf8_tokens(cluster_tokens)

        explicit_votes = cluster.get("explicit_lf8_votes")
        if isinstance(explicit_votes, Counter):
            for code_value, count in explicit_votes.items():
                try:
                    code = LF8Code(str(code_value))
                except Exception:
                    continue
                scores[code] += 2.5 * int(count)

        kind = str(cluster.get("kind") or "").strip().lower()
        if kind == "objection":
            scores[LF8Code.LF8_3] += 1.0
        elif kind == "driver":
            scores[LF8Code.LF8_1] += 0.35
            scores[LF8Code.LF8_5] += 0.35

        for quote_id in quote_ids:
            quote = quote_by_id.get(str(quote_id or "").strip())
            if quote is None:
                continue
            inferred_code, q_scores, q_tokens = _infer_lf8_for_quote(quote)
            quote_token_cache[str(quote_id)] = q_tokens
            for code, value in q_scores.items():
                scores[code] += 0.35 * float(value)
            if inferred_code is not None:
                scores[inferred_code] += 0.75

        best_code, best_score = _best_lf8_code(scores)
        if best_code is None or best_score < 1.0:
            continue
        cluster_codes.append(best_code)
        for quote_id in quote_ids:
            qid = str(quote_id or "").strip()
            if qid:
                quote_code_votes[qid][best_code.value] += 1

    allowed_codes = set(cluster_codes)

    segment_totals: dict[str, int] = {segment: 0 for segment in segment_order}
    segment_rows: dict[str, dict[LF8Code, dict[str, Any]]] = {
        segment: {} for segment in segment_order
    }

    for quote in pillar_2.quotes:
        quote_id = str(quote.quote_id or "").strip()
        segment_raw = str(quote.segment_name or "").strip()
        if not quote_id or not segment_raw:
            continue
        segment_key = " ".join(segment_raw.lower().split())
        canonical_segment = segment_by_key.get(segment_key)
        if not canonical_segment:
            continue
        segment_totals[canonical_segment] = int(segment_totals.get(canonical_segment, 0)) + 1

        chosen_code: LF8Code | None = None
        vote_counter = quote_code_votes.get(quote_id)
        if vote_counter:
            top_vote = vote_counter.most_common(1)
            if top_vote:
                try:
                    chosen_code = LF8Code(top_vote[0][0])
                except Exception:
                    chosen_code = None

        if chosen_code is None:
            inferred_code, _scores, tokens = _infer_lf8_for_quote(
                quote,
                allowed_codes=allowed_codes if allowed_codes else None,
            )
            quote_token_cache.setdefault(quote_id, tokens)
            chosen_code = inferred_code

        if chosen_code is None:
            continue
        if allowed_codes and chosen_code not in allowed_codes:
            continue

        bucket = segment_rows[canonical_segment].setdefault(
            chosen_code,
            {
                "quote_ids": [],
                "quote_ids_seen": set(),
                "domains": set(),
                "support_ids": set(),
                "theme_counts": Counter(),
                "token_union": set(),
                "objection_samples": [],
                "purchase_hits": 0,
                "proof_hits": 0,
                "identity_hits": 0,
                "objection_hits": 0,
            },
        )

        if quote_id not in bucket["quote_ids_seen"]:
            bucket["quote_ids_seen"].add(quote_id)
            bucket["quote_ids"].append(quote_id)
        domain = _extract_domain(getattr(quote, "source_url", ""))
        if domain:
            bucket["domains"].add(domain)

        quote_tokens = quote_token_cache.get(quote_id)
        if quote_tokens is None:
            quote_tokens = _tokenize_match_terms(
                " ".join(
                    [
                        str(getattr(quote, "quote", "") or ""),
                        str(getattr(quote, "theme", "") or ""),
                        str(getattr(quote, "category", "") or ""),
                        str(getattr(quote, "dominant_emotion", "") or ""),
                    ]
                )
            )
            quote_token_cache[quote_id] = quote_tokens
        bucket["token_union"].update(quote_tokens)

        category = str(getattr(quote, "category", "") or "").strip().lower()
        if category in {"pain", "objection", "trigger"}:
            bucket["objection_hits"] += 1
            quote_text = str(getattr(quote, "quote", "") or "").strip()
            if quote_text:
                bucket["objection_samples"].append(quote_text)
        if _LF8_PURCHASE_TOKENS & quote_tokens:
            bucket["purchase_hits"] += 1
        if _LF8_PROOF_URGENCY_TOKENS & quote_tokens:
            bucket["proof_hits"] += 1
        if _LF8_IDENTITY_TOKENS & quote_tokens:
            bucket["identity_hits"] += 1

        theme = str(getattr(quote, "theme", "") or "").strip()
        if theme:
            bucket["theme_counts"][theme] += 1

        quote_url_key = _normalize_url_for_match(getattr(quote, "source_url", ""))
        if quote_url_key:
            for item in evidence_by_url.get(quote_url_key, []):
                bucket["support_ids"].add(str(item.evidence_id))
        for item, item_tokens in evidence_token_rows:
            if len(quote_tokens.intersection(item_tokens)) >= 3:
                bucket["support_ids"].add(str(item.evidence_id))

    out: dict[str, list[LF8EmotionRow]] = {segment: [] for segment in segment_order}
    for segment in segment_order:
        segment_total = int(segment_totals.get(segment, 0))
        if segment_total <= 0:
            continue
        rows: list[LF8EmotionRow] = []
        for code, bucket in segment_rows.get(segment, {}).items():
            quote_ids = list(bucket.get("quote_ids") or [])
            quote_count = len(quote_ids)
            unique_domains = len(set(bucket.get("domains") or set()))
            support_ids = [sid for sid in sorted(set(bucket.get("support_ids") or set())) if sid]

            risk = "low"
            for sid in support_ids:
                item = evidence_by_id.get(sid)
                if item is None:
                    continue
                conflict = str(item.conflict_flag or "").strip().lower()
                if conflict == "high_unresolved":
                    risk = "high"
                    break
                if conflict == "medium" and risk == "low":
                    risk = "medium"

            # Balanced LF8 gate: quote support + minimal domain diversity + low contradiction risk.
            if quote_count < _LF8_STRICT_MIN_QUOTES:
                continue
            if unique_domains < _LF8_STRICT_MIN_DOMAINS:
                continue
            if risk != "low":
                continue

            theme_counts = bucket.get("theme_counts") or Counter()
            top_theme = ""
            if isinstance(theme_counts, Counter) and theme_counts:
                top_theme = str(theme_counts.most_common(1)[0][0] or "").strip()
            token_union = set(bucket.get("token_union") or set())
            objection_samples = [str(v).strip() for v in (bucket.get("objection_samples") or []) if str(v or "").strip()]
            blocking_objection = _shorten_text(objection_samples[0]) if objection_samples else ""
            required_proof = _infer_required_proof(code, token_union)

            frequency = _safe_ratio(quote_count, segment_total)
            objection_ratio = _safe_ratio(int(bucket.get("objection_hits", 0)), quote_count)
            purchase_ratio = _safe_ratio(int(bucket.get("purchase_hits", 0)), quote_count)
            proof_ratio = _safe_ratio(int(bucket.get("proof_hits", 0)), quote_count)
            identity_ratio = _safe_ratio(int(bucket.get("identity_hits", 0)), quote_count)
            buying_power_score = round(
                100.0
                * (
                    (0.35 * frequency)
                    + (0.25 * objection_ratio)
                    + (0.15 * purchase_ratio)
                    + (0.15 * proof_ratio)
                    + (0.10 * identity_ratio)
                ),
                4,
            )

            confidence = round(
                min(
                    0.99,
                    0.24
                    + (0.12 * min(quote_count, 6))
                    + (0.09 * min(unique_domains, 5))
                    + (0.04 * min(len(support_ids), 8)),
                ),
                4,
            )

            rows.append(
                LF8EmotionRow(
                    lf8_code=code,
                    lf8_label=_LF8_LABELS.get(code, code.value.upper()),
                    emotion_angle=_infer_emotion_angle(code, top_theme),
                    segment_name=segment,
                    tagged_quote_count=quote_count,
                    share_of_segment_voc=round(_safe_ratio(quote_count, segment_total), 4),
                    unique_domains=unique_domains,
                    sample_quote_ids=quote_ids[:10],
                    support_evidence_ids=support_ids[:20],
                    blocking_objection=blocking_objection,
                    required_proof=required_proof,
                    contradiction_risk=risk,
                    confidence=confidence,
                    buying_power_score=buying_power_score,
                )
            )

        rows.sort(
            key=lambda row: (
                -float(row.buying_power_score),
                -int(row.tagged_quote_count),
                str(row.lf8_code.value),
            )
        )
        out[segment] = rows

    return out


def derive_emotional_inventory_from_collectors(
    collector_reports: list[str],
    pillar_2: Pillar2VocLanguageBank,
    *,
    evidence: list[EvidenceItem] | None = None,
    mutate_pillar2_labels: bool = False,
    allowed_segments: list[str] | None = None,
) -> Pillar6EmotionalDriverInventory:
    """Build Pillar 6 from Step 1 collector Pillar 6 findings, truth-checked against VOC/evidence.

    Agreement logic:
    - Keep themes mentioned by multiple collectors when they map to VOC or evidence.
    - For one-sided themes, require stronger support in VOC/evidence before inclusion.
    - Preserve collector phrasing; no legacy bucket remap/cap is applied.
    """
    quote_total = max(1, len(pillar_2.quotes))
    candidates = _collect_p6_candidates(collector_reports or [])
    if not candidates:
        return derive_emotional_inventory_from_voc(pillar_2)

    clusters = _cluster_p6_candidates(candidates)
    if not clusters:
        return derive_emotional_inventory_from_voc(pillar_2)

    quote_index: list[tuple[str, Any, set[str], str]] = []
    for quote in pillar_2.quotes:
        quote_id = str(quote.quote_id or "").strip()
        if not quote_id:
            continue
        quote_text = " ".join(
            [
                str(quote.quote or ""),
                str(quote.theme or ""),
                str(quote.category or ""),
                str(quote.dominant_emotion or ""),
            ]
        )
        quote_index.append(
            (
                quote_id,
                quote,
                _tokenize_match_terms(quote_text),
                _normalize_emotion_key_basic(quote.dominant_emotion),
            )
        )

    cluster_quote_ids: dict[int, list[str]] = {idx: [] for idx in range(len(clusters))}
    for quote_id, _, quote_tokens, quote_label_key in quote_index:
        best_idx = -1
        best_score = 0
        for idx, cluster in enumerate(clusters):
            score = 0
            cluster_tokens = set(cluster.get("tokens") or set())
            if quote_label_key and quote_label_key == str(cluster.get("label_key") or ""):
                score += 6
            overlap = len(cluster_tokens & quote_tokens)
            score += overlap
            kind = str(cluster.get("kind") or "")
            if kind == "objection":
                if any(tok in quote_tokens for tok in {"skepticism", "scam", "placebo", "price", "waste"}):
                    score += 1
            elif kind == "driver":
                if any(tok in quote_tokens for tok in {"desire", "focus", "stagnation", "identity", "ritual"}):
                    score += 1
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx >= 0 and best_score >= 2:
            cluster_quote_ids[best_idx].append(quote_id)

    evidence_tokens: list[tuple[set[str], bool]] = []
    for item in evidence or []:
        text = " ".join([str(item.claim or ""), str(item.verbatim or "")])
        tokens = _tokenize_match_terms(text)
        if not tokens:
            continue
        high_unresolved = str(item.conflict_flag or "").strip().lower() == "high_unresolved"
        evidence_tokens.append((tokens, high_unresolved))

    selected_rows: list[tuple[dict[str, Any], list[str], int]] = []
    for idx, cluster in enumerate(clusters):
        quote_ids = list(dict.fromkeys(cluster_quote_ids.get(idx, [])))
        quote_support = len(quote_ids)
        support_tokens = set(cluster.get("tokens") or set())
        evidence_support = 0
        for item_tokens, high_unresolved in evidence_tokens:
            if len(support_tokens & item_tokens) >= 2 and not high_unresolved:
                evidence_support += 1

        provider_count = len(cluster.get("providers") or set())
        if provider_count >= 2:
            include = quote_support >= 1 or evidence_support >= 2
        else:
            include = quote_support >= 2 or evidence_support >= 3
        if not include:
            continue
        selected_rows.append((cluster, quote_ids, provider_count))

    if not selected_rows:
        return derive_emotional_inventory_from_voc(pillar_2)

    selected_rows.sort(
        key=lambda item: (
            -item[2],  # collector agreement first
            -len(item[1]),  # then stronger VOC support
            int(item[0].get("first_order") or 0),  # then deterministic original order
        )
    )

    if mutate_pillar2_labels:
        by_id = {str(q.quote_id or "").strip(): q for q in pillar_2.quotes if str(q.quote_id or "").strip()}
        for cluster, quote_ids, _provider_count in selected_rows:
            label = str(cluster.get("label") or "").strip()
            if not label:
                continue
            for quote_id in quote_ids:
                quote = by_id.get(quote_id)
                if quote is not None:
                    quote.dominant_emotion = label

    dominant: list[DominantEmotion] = []
    for cluster, quote_ids, _provider_count in selected_rows:
        label = str(cluster.get("label") or "").strip()
        if not label:
            continue
        count = len(quote_ids)
        if count <= 0:
            continue
        dominant.append(
            DominantEmotion(
                emotion=label,
                tagged_quote_count=count,
                share_of_voc=round(count / quote_total, 4),
                sample_quote_ids=quote_ids[:12],
            )
        )

    if not dominant:
        return derive_emotional_inventory_from_voc(pillar_2)

    lf8_rows_by_segment = _build_lf8_rows_by_segment(
        selected_rows=selected_rows,
        pillar_2=pillar_2,
        evidence=list(evidence or []),
        allowed_segments=allowed_segments,
    )

    return Pillar6EmotionalDriverInventory(
        dominant_emotions=dominant,
        lf8_rows_by_segment=lf8_rows_by_segment,
        lf8_mode="strict_lf8",
    )


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
    # 3) If everything is sparse, keep at least the top observed emotion.
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

    if not ranked and ranked_all:
        ranked.append(ranked_all[0])

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
        p1 = pillars.get("pillar_1") or (existing_pillars or {}).get("pillar_1")
        allowed_segments: list[str] = []
        profiles: list[Any] = []
        if isinstance(p1, Pillar1ProspectProfile):
            profiles = list(p1.segment_profiles or [])
        elif isinstance(p1, dict):
            raw_profiles = p1.get("segment_profiles", [])
            if isinstance(raw_profiles, list):
                profiles = list(raw_profiles)
        for profile in profiles:
            segment_name = ""
            if isinstance(profile, dict):
                segment_name = str(profile.get("segment_name") or "").strip()
            else:
                segment_name = str(getattr(profile, "segment_name", "") or "").strip()
            if segment_name and segment_name not in allowed_segments:
                allowed_segments.append(segment_name)
        if p2 is not None:
            pillars["pillar_6"] = derive_emotional_inventory_from_collectors(
                collector_reports,
                p2,
                allowed_segments=allowed_segments,
            )
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
