"""Gemini Deep Research collector for Phase 1 v2."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import config
from pipeline.llm import call_deep_research
from schemas.foundation_research import ResearchModelTraceEntry

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_prompt_template() -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / "collector_gemini.md"
    return path.read_text("utf-8").strip()


def _build_prompt(context: dict[str, Any]) -> str:
    template = _load_prompt_template()
    lines = [
        template,
        "",
        f"Brand: {context.get('brand_name', 'Unknown Brand')}",
        f"Product: {context.get('product_name', 'Unknown Product')}",
        f"Niche: {context.get('niche', '')}",
        f"Product Description: {context.get('product_description', '')}",
        f"Target Market: {context.get('target_market', '')}",
        f"Website: {context.get('website_url', '')}",
    ]
    optional_sections = [
        ("Known Competitors", context.get("competitor_info", "")),
        ("Landing Page Notes", context.get("landing_page_info", "")),
        ("Website Intel", context.get("website_intel", "")),
        ("Customer Reviews Seed", context.get("customer_reviews", "")),
        ("Previous Performance", context.get("previous_performance", "")),
        ("Additional Context", context.get("additional_context", "")),
    ]
    for label, payload in optional_sections:
        if payload not in ("", None, [], {}):
            lines.extend(["", f"{label}:", str(payload)])
    return "\n".join(lines).strip() + "\n"


def collect_with_gemini(context: dict[str, Any]) -> dict[str, Any]:
    started = _now_iso()
    start_ts = datetime.now(timezone.utc)
    prompt = _build_prompt(context)
    logger.info("Phase1 collector[gemini]: start (deep-research)")

    try:
        report = call_deep_research(prompt)
        status = "success"
        error = ""
        logger.info(
            "Phase1 collector[gemini]: success (%d chars)",
            len(report or ""),
        )
    except Exception as exc:  # pragma: no cover - network/SDK dependent
        report = ""
        status = "failed"
        error = str(exc)
        logger.error("Phase1 collector[gemini]: failed — %s", error)

    finished = _now_iso()
    end_ts = datetime.now(timezone.utc)

    trace = ResearchModelTraceEntry(
        stage="collector",
        provider="google",
        model="deep-research-pro-preview-12-2025",
        status=status,
        started_at=started,
        finished_at=finished,
        duration_seconds=max((end_ts - start_ts).total_seconds(), 0.0),
        notes=error,
    )

    return {
        "success": status == "success",
        "provider": "gemini",
        "report": report,
        "error": error,
        "trace": trace,
    }
