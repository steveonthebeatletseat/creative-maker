"""Agent 02: Idea Generator — produces 30 ad ideas across the funnel.

Inputs: Agent 1A foundation brief + Agent 1B trend intel.
Outputs: IdeaGeneratorBrief → Agent 3 (Stress Tester P1).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_02_system import SYSTEM_PROMPT
from schemas.idea_generator import IdeaGeneratorBrief


class Agent02IdeaGenerator(BaseAgent):
    name = "Agent 02: Idea Generator"
    slug = "agent_02"
    description = (
        "Creative divergence engine. Produces 30 ad ideas "
        "(10 ToF, 10 MoF, 10 BoF) from research + trend intel, "
        "with diversity rules and bold swing ideas."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return IdeaGeneratorBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A output)
          - trend_intel: dict (Agent 1B output)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Agent 1A Foundation Brief
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF (Full)")
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF")
                sections.append(str(brief))

        # Agent 1B Trend Intel
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append("\n# AGENT 1B — TREND & COMPETITIVE INTEL (Full)")
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1B — TREND & COMPETITIVE INTEL")
                sections.append(str(intel))

        sections.append(
            "\n# YOUR TASK\n"
            "Using the Foundation Research Brief and Trend Intel above, "
            "produce a complete Idea Generator Brief with:\n"
            "- 10 Top-of-Funnel ideas (Unaware → Problem Aware)\n"
            "- 10 Middle-of-Funnel ideas (Solution Aware → Product Aware)\n"
            "- 10 Bottom-of-Funnel ideas (Product Aware → Most Aware)\n\n"
            "Requirements:\n"
            "- Each idea must be grounded in the research (specific angles, segments, VoC)\n"
            "- 2-3 bold 'swing' ideas per funnel stage\n"
            "- Enforce diversity across angles, segments, emotions, formats, awareness levels\n"
            "- Include a diversity audit\n"
            "- Recommend your top 10 in priority order\n"
            "- Flag compliance risks\n"
        )

        return "\n".join(sections)
