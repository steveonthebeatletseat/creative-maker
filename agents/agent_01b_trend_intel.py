"""Agent 1B: Trend & Competitive Intel.

Cadence: runs fresh every batch.
Inputs: Agent 1A foundation brief + current ad libraries + cultural landscape.
Outputs: TrendIntelBrief → Agent 2 (Idea Generator).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_01b_system import SYSTEM_PROMPT
from schemas.trend_intel import TrendIntelBrief


class Agent01BTrendIntel(BaseAgent):
    name = "Agent 1B: Trend & Competitive Intel"
    slug = "agent_01b"
    description = (
        "Real-time competitive and cultural intelligence. "
        "Identifies trending formats, competitor ad patterns, "
        "cultural moments, and currently-working hooks."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return TrendIntelBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from inputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - niche: str (e.g. "skincare", "supplements", "SaaS")
          - foundation_brief: dict | str (Agent 1A output or summary)
          - competitor_ads: str (descriptions of current competitor ads)
          - ad_library_data: str (Meta Ad Library / TikTok Creative Center data)
          - cultural_context: str (current events, trends, memes)
          - previous_batch_learnings: str (from Agent 15B feedback loop)
          - batch_id: str
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Niche: {inputs.get('niche', 'Not specified')}")

        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                # Provide a focused summary for 1B — it doesn't need the full brief
                sections.append("\n# FOUNDATION BRIEF SUMMARY (from Agent 1A)")
                if "segments" in brief:
                    segments = brief["segments"]
                    names = [s.get("name", "?") for s in segments] if isinstance(segments, list) else []
                    sections.append(f"Segments: {', '.join(names)}")
                if "sophistication_diagnosis" in brief:
                    sd = brief["sophistication_diagnosis"]
                    sections.append(f"Market sophistication: Stage {sd.get('stage', '?')}")
                if "category_snapshot" in brief:
                    cs = brief["category_snapshot"]
                    sections.append(f"Category: {cs.get('category_definition', '?')}")
                    sections.append(f"Dominant formats: {', '.join(cs.get('dominant_formats', []))}")
            else:
                sections.append("\n# FOUNDATION BRIEF (from Agent 1A)")
                sections.append(str(brief))

        if inputs.get("competitor_ads"):
            sections.append("\n# CURRENT COMPETITOR ADS")
            sections.append(inputs["competitor_ads"])

        if inputs.get("ad_library_data"):
            sections.append("\n# AD LIBRARY / CREATIVE CENTER DATA")
            sections.append(inputs["ad_library_data"])

        if inputs.get("cultural_context"):
            sections.append("\n# CURRENT CULTURAL LANDSCAPE")
            sections.append(inputs["cultural_context"])

        if inputs.get("previous_batch_learnings"):
            sections.append("\n# LEARNINGS FROM PREVIOUS BATCHES (Agent 15B)")
            sections.append(inputs["previous_batch_learnings"])

        batch_id = inputs.get("batch_id", "")
        sections.append(
            f"\n# YOUR TASK (Batch: {batch_id})\n"
            "Produce a complete Trend Intel Brief covering:\n"
            "1. Trending formats & sounds (5-15)\n"
            "2. Competitor ad breakdowns (5-20)\n"
            "3. Cultural moments to tap (3-10)\n"
            "4. Currently working hooks in this niche (10-30)\n"
            "5. Key strategic takeaways (3-7)\n\n"
            "Be specific to THIS brand and THIS moment. "
            "Everything should be actionable for the creative team."
        )

        return "\n".join(sections)
