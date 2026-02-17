"""Evidence normalization utilities for Phase 1 v2."""

from __future__ import annotations

import hashlib
import re
from datetime import date
from collections import deque
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import config
from schemas.foundation_research import EvidenceItem

_URL_RE = re.compile(r"https?://[^\s\]\)\">]+", re.IGNORECASE)
_CITE_RE = re.compile(r"\[cite:\s*[^\]]+\]", re.IGNORECASE)
_MARKDOWN_LINK_ONLY_RE = re.compile(r"^\s*\d*\.?\s*\[[^\]]+\]\(https?://[^)]+\)\s*$")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+")
_TABLE_RULE_RE = re.compile(r"^\s*\|?[-:\s|]{3,}\|?\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])|;\s+")
_SOURCE_WEIGHTS: dict[str, float] = {
    "survey": 0.74,
    "review": 0.72,
    "support": 0.70,
    "forum": 0.66,
    "reddit": 0.62,
    "social": 0.57,
    "ad_library": 0.58,
    "landing_page": 0.55,
    "other": 0.35,
}

_DROP_QUERY_PREFIXES = ("utm_",)
_DROP_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "ref_url",
    "source",
}


def _clean_text(text: str) -> str:
    return " ".join((text or "").strip().split())


def _norm_key(*parts: str) -> str:
    base = "|".join(_clean_text(p).lower() for p in parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def is_valid_http_url(url: str) -> bool:
    value = (url or "").strip()
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def canonicalize_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if not config.PHASE1_URL_CANONICALIZE:
        return value
    if not is_valid_http_url(value):
        return value

    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if ":" in netloc:
        host, _, port = netloc.partition(":")
        if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
            netloc = host
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    query_pairs = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=False):
        low = key.lower()
        if low in _DROP_QUERY_KEYS:
            continue
        if any(low.startswith(prefix) for prefix in _DROP_QUERY_PREFIXES):
            continue
        query_pairs.append((key, val))
    query_pairs.sort(key=lambda pair: (pair[0], pair[1]))
    query = urlencode(query_pairs, doseq=True)

    return urlunparse((scheme, netloc, path, "", query, ""))


def _strip_noise(text: str) -> str:
    line = (text or "").strip()
    if not line:
        return ""
    line = _CITE_RE.sub("", line)
    line = line.replace("**", " ").replace("__", " ")
    line = re.sub(r"^\s*[-*•]\s+", "", line)
    line = re.sub(r"^\s*\d+[.)]\s+", "", line)
    line = line.strip(",")
    if line.startswith(("'", '"')) and line.endswith(("'", '"')):
        line = line[1:-1]
    line = re.sub(r"\s+", " ", line).strip()
    return line


def _extract_verbatim(line: str) -> str:
    # Preserve short quoted fragments when present; otherwise fall back to cleaned line.
    match = re.search(r'[\"“](.{8,420}?)[\"”]', line)
    if match:
        return _clean_text(match.group(1))[:300]
    return _strip_noise(line)[:300]


def _is_scaffold_line(line: str) -> bool:
    stripped = (line or "").strip()
    if not stripped:
        return True
    lower = stripped.lower()
    if _HEADING_RE.match(stripped):
        return True
    if _TABLE_RULE_RE.match(stripped):
        return True
    if _MARKDOWN_LINK_ONLY_RE.match(stripped):
        return True
    if stripped.startswith("|") and stripped.count("|") >= 2:
        return True
    if lower.startswith(("pillar ", "## pillar", "### pillar", "objective:", "requirements:")):
        return True
    return False


def _looks_like_quote(line: str) -> bool:
    cleaned = _strip_noise(line)
    if not cleaned:
        return False
    if '"' in line or "“" in line or "”" in line:
        return True
    # Parent/child-like direct speech often lacks punctuation.
    return bool(re.search(r"\b(i|my|we|our|me)\b", cleaned.lower()))


def _extract_published_date(line: str) -> str:
    # Accept YYYY-MM-DD or YYYY/MM/DD embedded in collector lines.
    match = re.search(r"\b(20\d{2})[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])\b", line)
    if not match:
        return ""
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


def infer_source_type(url: str, line: str = "") -> str:
    target = f"{url} {line}".lower()
    if "trustpilot" in target or "app store" in target or "play.google.com/store/apps" in target:
        return "review"
    if "amazon" in target or "review" in target or "bestbuy" in target or "g2.com" in target:
        return "review"
    if "reddit" in target:
        return "reddit"
    if "communityforums.atmeta.com" in target or "steamcommunity" in target or "forum" in target or "quora" in target:
        return "forum"
    if "facebook.com/ads" in target or "meta ad" in target or "creative center" in target:
        return "ad_library"
    if "support" in target or "ticket" in target:
        return "support"
    if "survey" in target:
        return "survey"
    if "tiktok" in target or "youtube" in target or "instagram" in target or "facebook" in target:
        return "social"
    if "http" in target:
        return "landing_page"
    return "other"


def _infer_pillar_tags(line: str, source_type: str, fallback: list[str] | None = None) -> list[str]:
    text = (line or "").lower()
    tags: set[str] = set()

    if any(token in text for token in ("goal", "pain", "trigger", "objection", "segment", "persona")):
        tags.add("pillar_1")
    if any(token in text for token in ("quote", "verbatim", "said", "review", "complain", "comment")):
        tags.add("pillar_2")
    if any(token in text for token in ("competitor", "vs.", "offer", "positioning", "promise", "mechanism saturation")):
        tags.add("pillar_3")
    if any(token in text for token in ("mechanism", "why problem exists", "counterweight", "uniquely works", "physics")):
        tags.add("pillar_4")
    if any(token in text for token in ("aware", "awareness", "problem_aware", "solution_aware", "product_aware")):
        tags.add("pillar_5")
    if any(token in text for token in ("emotion", "fear", "frustration", "relief", "desire", "anxiety")):
        tags.add("pillar_6")
    if any(token in text for token in ("proof", "testimonial", "authority", "story", "statistical", "warranty")):
        tags.add("pillar_7")

    if source_type in {"review", "reddit", "forum"}:
        tags.update({"pillar_1", "pillar_2"})

    if not tags and fallback:
        # Keep fallback narrow; avoid blanket all-pillar tagging.
        tags.add(fallback[0])
    if not tags:
        tags.add("pillar_1")
    return sorted(tags)


def _base_confidence(*, line: str, source_url: str, source_type: str, published_date: str) -> float:
    conf = _SOURCE_WEIGHTS.get(source_type, 0.45)
    if is_valid_http_url(source_url):
        conf += 0.10
    else:
        conf -= 0.12
    if _looks_like_quote(line):
        conf += 0.07
    if source_type == "other":
        conf -= 0.15
    if published_date:
        try:
            year = int(published_date[:4])
            age = max(0, date.today().year - year)
            if age <= 2:
                conf += 0.05
            elif age >= 7:
                conf -= 0.04
        except ValueError:
            pass
    return round(min(0.97, max(0.05, conf)), 2)


def _split_claim_fragments(line: str) -> list[str]:
    cleaned = _strip_noise(line)
    if not cleaned:
        return []
    if len(cleaned) <= 220:
        return [cleaned]

    fragments: list[str] = []
    for piece in _SENTENCE_SPLIT_RE.split(cleaned):
        token = _strip_noise(piece)
        if len(token) >= 25 or _looks_like_quote(token):
            fragments.append(token)
    return fragments or [cleaned]


def extract_seed_evidence(text: str, provider: str, pillar_tags: list[str] | None = None) -> list[EvidenceItem]:
    """Create evidence seeds from collector reports.

    P0 quality filter:
    - Drop headers/markdown scaffolding.
    - Prefer quote-bearing lines and lines with source traces.
    - Reject low-signal rows before they inflate evidence counts.
    """
    fallback_tags = list(pillar_tags or [])
    lines = [_clean_text(line) for line in (text or "").splitlines()]
    lines = [line for line in lines if line]
    recent_urls: deque[str] = deque(maxlen=5)

    seeds: list[EvidenceItem] = []
    for idx, line in enumerate(lines):
        if _is_scaffold_line(line):
            continue
        urls = _URL_RE.findall(line)
        norm_urls = [canonicalize_url(u.rstrip(").,]")) for u in urls]
        for url in norm_urls:
            if is_valid_http_url(url):
                recent_urls.append(url)
        source_url = norm_urls[0] if norm_urls else (recent_urls[-1] if recent_urls else "")

        for part_idx, fragment in enumerate(_split_claim_fragments(line)):
            if len(fragment.split()) < 8 and not _looks_like_quote(fragment):
                continue
            published_date = _extract_published_date(fragment)
            source_type = infer_source_type(source_url, fragment)
            cleaned_claim = _strip_noise(fragment)[:400]
            if not cleaned_claim or _is_scaffold_line(cleaned_claim):
                continue

            key = _norm_key(cleaned_claim[:280], source_url, str(idx), str(part_idx))
            evidence_id = f"ev_{key[:12]}"
            seeds.append(
                EvidenceItem(
                    evidence_id=evidence_id,
                    claim=cleaned_claim,
                    verbatim=_extract_verbatim(fragment),
                    source_url=canonicalize_url(source_url),
                    source_type=source_type,
                    published_date=published_date,
                    pillar_tags=_infer_pillar_tags(fragment, source_type, fallback=fallback_tags),
                    confidence=_base_confidence(
                        line=fragment,
                        source_url=source_url,
                        source_type=source_type,
                        published_date=published_date,
                    ),
                    provider=provider,
                )
            )

    return dedupe_evidence(seeds)


def dedupe_evidence(items: Iterable[EvidenceItem]) -> list[EvidenceItem]:
    deduped: list[EvidenceItem] = []
    exact_seen: dict[str, int] = {}
    materialized: list[EvidenceItem] = []

    for item in items:
        claim = _clean_text(item.claim)
        verbatim = _clean_text(item.verbatim or item.claim)
        if _is_scaffold_line(claim):
            continue
        if len(claim.split()) < 6 and not _looks_like_quote(verbatim):
            continue
        item.claim = _strip_noise(claim)[:400]
        item.verbatim = _strip_noise(verbatim)[:300]
        item.source_url = canonicalize_url(item.source_url)
        materialized.append(item)

    # Tier A: exact source-level dedupe.
    for item in materialized:
        semantic = _norm_key(item.claim, item.verbatim)
        key = _norm_key(item.source_url or f"provider:{item.provider}", semantic)
        existing_idx = exact_seen.get(key)
        if existing_idx is not None:
            if item.confidence > deduped[existing_idx].confidence:
                deduped[existing_idx] = item
            continue
        exact_seen[key] = len(deduped)
        deduped.append(item)

    semantic_support: dict[str, set[str]] = {}
    for item in deduped:
        semantic = _norm_key(item.claim, item.verbatim)
        support_key = item.source_url or f"provider:{item.provider}"
        semantic_support.setdefault(semantic, set()).add(support_key)

    if config.PHASE1_SEMANTIC_DEDUPE:
        semantic_deduped: list[EvidenceItem] = []
        semantic_index: dict[str, int] = {}
        for item in deduped:
            semantic = _norm_key(item.claim, item.verbatim)
            existing_idx = semantic_index.get(semantic)
            if existing_idx is None:
                semantic_index[semantic] = len(semantic_deduped)
                semantic_deduped.append(item)
                continue
            current = semantic_deduped[existing_idx]
            if item.confidence > current.confidence:
                current.claim = item.claim
                current.verbatim = item.verbatim
                current.source_url = item.source_url
                current.source_type = item.source_type
                current.published_date = item.published_date
                current.provider = item.provider
                current.confidence = item.confidence
            current_tags = set(current.pillar_tags)
            current_tags.update(item.pillar_tags)
            current.pillar_tags = sorted(current_tags)
        deduped = semantic_deduped

    for item in deduped:
        semantic = _norm_key(item.claim, item.verbatim)
        corroboration_count = len(semantic_support.get(semantic, set()))
        if corroboration_count > 1:
            item.confidence = round(
                min(0.97, item.confidence + min(0.2, 0.05 * (corroboration_count - 1))),
                2,
            )

    return deduped
