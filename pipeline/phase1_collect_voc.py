"""Direct VOC collector (Reddit / Amazon / Trustpilot) for Phase 1."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None

import config
from schemas.foundation_research import EvidenceItem, ResearchModelTraceEntry

logger = logging.getLogger(__name__)

_REQ_TIMEOUT = 20
_USER_AGENT = "CreativeMakerPhase1VOC/1.0"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _split_csv(value: str) -> list[str]:
    return [token.strip() for token in (value or "").split(",") if token.strip()]


def _make_evidence_id(*parts: str) -> str:
    payload = "|".join((part or "").strip().lower() for part in parts)
    return f"ev_voc_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _clean_lines(text: str) -> list[str]:
    lines = []
    for raw in (text or "").splitlines():
        token = " ".join(raw.strip().split())
        if len(token) < 24:
            continue
        lines.append(token)
    return lines


def _dedupe(items: list[EvidenceItem]) -> list[EvidenceItem]:
    keep: list[EvidenceItem] = []
    seen: set[str] = set()
    for item in items:
        key = f"{item.verbatim.lower()}|{item.source_url.lower()}"
        if key in seen:
            continue
        seen.add(key)
        keep.append(item)
    return keep


def _collect_reddit(context: dict[str, str]) -> list[EvidenceItem]:
    if not config.PHASE1_VOC_ENABLE_REDDIT:
        return []
    subreddits = _split_csv(config.PHASE1_VOC_REDDIT_SUBREDDITS)
    if not subreddits:
        return []
    brand = context.get("brand_name", "")
    product = context.get("product_name", "")
    niche = context.get("niche", "")
    query = " ".join(token for token in [brand, product, niche, "review comfort issue battery"] if token).strip()
    if not query:
        query = "vr comfort strap battery review"

    headers = {"User-Agent": _USER_AGENT}
    evidence: list[EvidenceItem] = []
    for subreddit in subreddits:
        url = (
            f"https://www.reddit.com/r/{subreddit}/search.json?q={quote_plus(query)}"
            "&restrict_sr=1&sort=top&t=year&limit=25"
        )
        try:
            response = requests.get(url, headers=headers, timeout=_REQ_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.warning("VOC collector[reddit]: %s failed (%s)", subreddit, exc)
            continue

        for child in payload.get("data", {}).get("children", []):
            data = child.get("data", {})
            title = " ".join((data.get("title") or "").split())
            body = " ".join((data.get("selftext") or "").split())
            text = f"{title} {body}".strip()
            if len(text) < 24:
                continue
            permalink = data.get("permalink") or ""
            source_url = f"https://www.reddit.com{permalink}" if permalink else ""
            if not source_url.startswith("https://www.reddit.com/"):
                continue
            evidence.append(
                EvidenceItem(
                    evidence_id=_make_evidence_id(source_url, text[:180], "reddit"),
                    claim=text[:400],
                    verbatim=text[:300],
                    source_url=source_url,
                    source_type="reddit",
                    published_date="",
                    pillar_tags=["pillar_1", "pillar_2"],
                    confidence=0.62,
                    provider="voc_api",
                )
            )
    return evidence


def _collect_trustpilot(_context: dict[str, str]) -> list[EvidenceItem]:
    if not config.PHASE1_VOC_ENABLE_TRUSTPILOT:
        return []
    domains = _split_csv(config.PHASE1_VOC_TRUSTPILOT_DOMAINS)
    if not domains:
        return []
    if BeautifulSoup is None:
        logger.warning("VOC collector[trustpilot]: bs4 unavailable; skipping")
        return []

    headers = {"User-Agent": _USER_AGENT}
    evidence: list[EvidenceItem] = []
    for domain in domains:
        for page in range(1, max(1, int(config.PHASE1_VOC_TRUSTPILOT_MAX_PAGES)) + 1):
            suffix = "" if page == 1 else f"?page={page}"
            url = f"https://www.trustpilot.com/review/{domain}{suffix}"
            try:
                response = requests.get(url, headers=headers, timeout=_REQ_TIMEOUT)
                response.raise_for_status()
            except Exception as exc:
                logger.warning("VOC collector[trustpilot]: %s page %d failed (%s)", domain, page, exc)
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            blocks = soup.select('[data-service-review-text-typography="true"]')
            if not blocks:
                blocks = soup.select("p")

            for block in blocks:
                text = " ".join(block.get_text(" ").split())
                if len(text) < 30:
                    continue
                evidence.append(
                    EvidenceItem(
                        evidence_id=_make_evidence_id(url, text[:180], "trustpilot"),
                        claim=text[:400],
                        verbatim=text[:300],
                        source_url=url,
                        source_type="review",
                        published_date="",
                        pillar_tags=["pillar_1", "pillar_2", "pillar_7"],
                        confidence=0.72,
                        provider="voc_api",
                    )
                )
    return evidence


def _collect_amazon(_context: dict[str, str]) -> list[EvidenceItem]:
    if not config.PHASE1_VOC_ENABLE_AMAZON:
        return []
    asins = _split_csv(config.PHASE1_VOC_AMAZON_ASINS)
    if not asins:
        return []
    if BeautifulSoup is None:
        logger.warning("VOC collector[amazon]: bs4 unavailable; skipping")
        return []

    headers = {"User-Agent": _USER_AGENT}
    evidence: list[EvidenceItem] = []
    for asin in asins:
        url = f"https://www.amazon.com/product-reviews/{asin}/"
        try:
            response = requests.get(url, headers=headers, timeout=_REQ_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("VOC collector[amazon]: %s failed (%s)", asin, exc)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        nodes = soup.select('[data-hook="review-body"] span')
        if not nodes:
            nodes = soup.select("span")
        for node in nodes:
            text = " ".join(node.get_text(" ").split())
            if len(text) < 30:
                continue
            if re.match(r"^\d+(\.\d+)?\s*out of 5", text.lower()):
                continue
            evidence.append(
                EvidenceItem(
                    evidence_id=_make_evidence_id(url, text[:180], "amazon"),
                    claim=text[:400],
                    verbatim=text[:300],
                    source_url=url,
                    source_type="review",
                    published_date="",
                    pillar_tags=["pillar_1", "pillar_2", "pillar_7"],
                    confidence=0.72,
                    provider="voc_api",
                )
            )
    return evidence


def collect_with_voc_api(context: dict[str, str]) -> dict:
    """Collect direct VOC evidence from first-party source adapters."""
    started = _now_iso()
    start_ts = datetime.now(timezone.utc)
    if not config.PHASE1_ENABLE_VOC_COLLECTOR:
        finished = _now_iso()
        trace = ResearchModelTraceEntry(
            stage="collector",
            provider="internal",
            model="voc-api-collector",
            status="skipped",
            started_at=started,
            finished_at=finished,
            duration_seconds=0.0,
            notes="PHASE1_ENABLE_VOC_COLLECTOR is disabled",
        )
        return {
            "success": False,
            "provider": "voc_api",
            "report": "",
            "error": "disabled",
            "trace": trace,
            "evidence": [],
        }

    evidence: list[EvidenceItem] = []
    try:
        evidence.extend(_collect_reddit(context))
        evidence.extend(_collect_trustpilot(context))
        evidence.extend(_collect_amazon(context))
        evidence = _dedupe(evidence)
        status = "success" if evidence else "failed"
        error = "" if evidence else "No direct VOC evidence collected"
    except Exception as exc:  # pragma: no cover - network dependent
        evidence = []
        status = "failed"
        error = str(exc)

    finished = _now_iso()
    end_ts = datetime.now(timezone.utc)
    trace = ResearchModelTraceEntry(
        stage="collector",
        provider="internal",
        model="voc-api-collector",
        status=status,
        started_at=started,
        finished_at=finished,
        duration_seconds=max((end_ts - start_ts).total_seconds(), 0.0),
        notes=error,
    )

    report_lines = []
    for item in evidence[:200]:
        report_lines.append(f"- {item.verbatim} ({item.source_url})")
    report = "Direct VOC evidence\n" + "\n".join(report_lines)
    return {
        "success": status == "success",
        "provider": "voc_api",
        "report": report,
        "error": error,
        "trace": trace,
        "evidence": [item.model_dump() for item in evidence],
    }
