"""Claude Agent SDK collector for Phase 1 v2."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

import config
from pipeline.claude_agent_scout import call_claude_agent_structured
from schemas.foundation_research import ResearchModelTraceEntry

logger = logging.getLogger(__name__)


class ClaudeCollectorOutput(BaseModel):
    research_report: str = Field(..., description="Citation-backed collector report")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_prompt_template() -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / "collector_claude.md"
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
    lines.extend(["", "Return only JSON matching schema."])
    return "\n".join(lines).strip() + "\n"


def collect_with_claude(context: dict[str, Any]) -> dict[str, Any]:
    started = _now_iso()
    start_ts = datetime.now(timezone.utc)
    logger.info(
        "Phase1 collector[claude]: start (model=%s)",
        config.CREATIVE_SCOUT_MODEL,
    )

    if not config.ANTHROPIC_API_KEY:
        logger.warning("Phase1 collector[claude]: skipped — ANTHROPIC_API_KEY is not set")
        finished = _now_iso()
        end_ts = datetime.now(timezone.utc)
        trace = ResearchModelTraceEntry(
            stage="collector",
            provider="anthropic",
            model=config.CREATIVE_SCOUT_MODEL,
            status="skipped",
            started_at=started,
            finished_at=finished,
            duration_seconds=max((end_ts - start_ts).total_seconds(), 0.0),
            notes="ANTHROPIC_API_KEY is not set",
        )
        return {
            "success": False,
            "provider": "claude",
            "report": "",
            "error": "ANTHROPIC_API_KEY is not set",
            "trace": trace,
        }

    prompt = _build_prompt(context)
    try:
        output = call_claude_agent_structured(
            system_prompt="You are a precise market research collector. Return structured JSON only.",
            user_prompt=prompt,
            response_model=ClaudeCollectorOutput,
            model=config.CREATIVE_SCOUT_MODEL,
            max_turns=config.CREATIVE_SCOUT_MAX_TURNS,
            max_thinking_tokens=config.CREATIVE_SCOUT_MAX_THINKING_TOKENS,
            max_budget_usd=config.CREATIVE_SCOUT_MAX_BUDGET_USD,
        )
        report = output.research_report
        status = "success"
        error = ""
        logger.info(
            "Phase1 collector[claude]: success (%d chars)",
            len(report or ""),
        )
    except Exception as exc:  # pragma: no cover - SDK/network dependent
        report = ""
        status = "failed"
        error = str(exc)
        logger.error("Phase1 collector[claude]: failed — %s", error)

    finished = _now_iso()
    end_ts = datetime.now(timezone.utc)
    trace = ResearchModelTraceEntry(
        stage="collector",
        provider="anthropic",
        model=config.CREATIVE_SCOUT_MODEL,
        status=status,
        started_at=started,
        finished_at=finished,
        duration_seconds=max((end_ts - start_ts).total_seconds(), 0.0),
        notes=error,
    )

    return {
        "success": status == "success",
        "provider": "claude",
        "report": report,
        "error": error,
        "trace": trace,
    }
