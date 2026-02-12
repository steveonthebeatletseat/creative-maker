"""Website Intelligence Scraper — AI-powered marketing intel extraction.

Two-stage process:
  1. Fetch + Clean: httpx fetches the page HTML, BeautifulSoup strips it to text.
  2. LLM Extract: Sends cleaned text to the selected LLM to extract structured
     marketing intelligence (WebsiteIntel schema).

Runs as a pre-pipeline step before Phase 1 to give Agent 1A and 1B
richer, real-data-grounded research inputs.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
from bs4 import BeautifulSoup

from pipeline.llm import call_llm_structured
from schemas.website_intel import WebsiteIntel

logger = logging.getLogger(__name__)

# Maximum characters of cleaned page text to send to the LLM
MAX_PAGE_CHARS = 15_000

EXTRACTION_SYSTEM_PROMPT = """You are a marketing intelligence extraction specialist. Your job is to analyze a product/brand landing page and extract EVERY piece of marketing intelligence from it.

You are looking at text extracted from a real website. Extract the following with precision:

1. **Hero Headline & Subheadline** — The main H1 and supporting tagline
2. **Unique Selling Proposition** — What makes this product different, in one sentence
3. **Key Benefits** — Outcome-focused benefits the page lists (not features)
4. **Key Features** — Product specs, ingredients, technical details
5. **Testimonials** — Customer quotes, review snippets (extract VERBATIM — exact words matter for VoC mining)
6. **Social Proof Stats** — Numbers like "10,000+ sold", "4.8 stars", review counts
7. **Trust Signals** — Certifications, press mentions, "as seen in" logos, badges
8. **Price Info** — Pricing tiers, subscription options, one-time prices
9. **Guarantee** — Money-back guarantee, risk reversal offers
10. **Bonuses/Offers** — Free shipping, bundles, discounts, limited-time offers
11. **Brand Voice** — Characterize the tone (casual, clinical, playful, authoritative, etc.)
12. **Claims Made** — Specific product claims (critical for compliance — extract ALL of them)
13. **FAQ Items** — Questions and answers (these reveal top buyer objections)
14. **Page Summary** — A condensed summary of the page's overall messaging strategy

Be thorough. Extract EVERYTHING you can find. Testimonials should be copied verbatim.
Claims should be extracted exactly as stated on the page.
If a section has no data, leave it as empty string or empty list.
"""


def _fetch_and_clean(url: str) -> str:
    """Fetch a URL and return cleaned text content.

    Uses httpx for the HTTP request and BeautifulSoup to strip HTML
    down to readable text. Truncates to MAX_PAGE_CHARS.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    response = httpx.get(
        url,
        headers=headers,
        follow_redirects=True,
        timeout=30.0,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove script, style, nav, footer, and other non-content tags
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "svg", "iframe"]):
        tag.decompose()

    # Extract text with some structure preserved
    text = soup.get_text(separator="\n", strip=True)

    # Collapse multiple blank lines
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    text = "\n".join(lines)

    # Truncate to avoid context overflow
    if len(text) > MAX_PAGE_CHARS:
        text = text[:MAX_PAGE_CHARS] + "\n\n[... page text truncated ...]"

    return text


def scrape_website(
    url: str,
    provider: str = "openai",
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 8_000,
) -> dict[str, Any]:
    """Scrape a website and extract structured marketing intelligence.

    Args:
        url: The website URL to scrape.
        provider: LLM provider for extraction ("openai", "anthropic", "google").
        model: LLM model name (uses provider default if None).
        temperature: Low temp for factual extraction.
        max_tokens: Max tokens for the extraction response.

    Returns:
        Dict of extracted marketing intelligence (WebsiteIntel fields).
        Returns empty dict on failure.
    """
    logger.info("Scraping website: %s", url)

    # Stage 1: Fetch + Clean
    try:
        page_text = _fetch_and_clean(url)
        logger.info("Fetched %d chars of cleaned text from %s", len(page_text), url)
    except httpx.HTTPStatusError as e:
        logger.warning("HTTP error scraping %s: %s", url, e)
        raise ValueError(f"Website returned HTTP {e.response.status_code}") from e
    except httpx.ConnectError as e:
        logger.warning("Connection error scraping %s: %s", url, e)
        raise ValueError(f"Could not connect to {url}") from e
    except httpx.TimeoutException:
        logger.warning("Timeout scraping %s", url)
        raise ValueError(f"Timeout fetching {url} (30s limit)")
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        raise ValueError(f"Failed to fetch website: {e}") from e

    if len(page_text.strip()) < 100:
        logger.warning("Page text too short (%d chars) — skipping LLM extraction", len(page_text))
        raise ValueError("Page returned very little text content — it may be JavaScript-rendered or blocking scrapers")

    # Stage 2: LLM Extract
    user_prompt = (
        f"# WEBSITE URL\n{url}\n\n"
        f"# PAGE CONTENT\n{page_text}\n\n"
        "# YOUR TASK\n"
        "Extract ALL marketing intelligence from this page into the structured format. "
        "Be thorough — every testimonial, every claim, every benefit matters for the "
        "downstream ad creation pipeline."
    )

    result = call_llm_structured(
        system_prompt=EXTRACTION_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        response_model=WebsiteIntel,
        provider=provider,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    intel = json.loads(result.model_dump_json())
    logger.info(
        "Website intel extracted: headline=%r, %d benefits, %d testimonials, %d claims",
        intel.get("hero_headline", "")[:50],
        len(intel.get("key_benefits", [])),
        len(intel.get("testimonials", [])),
        len(intel.get("claims_made", [])),
    )

    return intel
