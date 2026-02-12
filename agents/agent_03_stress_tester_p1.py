"""Agent 03: Stress Tester — Pass 1 (Strategic).

Evaluates 30 ideas against research brief, filters to 15 survivors.
Inputs: 30 ideas from Agent 02 + Agent 1A foundation brief.
Outputs: StressTesterP1Brief → Agent 04 (Copywriter).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_03_system import SYSTEM_PROMPT
from schemas.stress_tester_p1 import StressTesterP1Brief


class Agent03StressTesterP1(BaseAgent):
    name = "Agent 03: Stress Tester P1"
    slug = "agent_03"
    description = (
        "Strategic quality gate. Evaluates 30 ideas from Agent 02 against "
        "the research foundation, scoring on angle strength, differentiation, "
        "emotional resonance, compliance viability. Filters to 15 survivors."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return StressTesterP1Brief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A output)
          - idea_brief: dict (Agent 02 output)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Agent 1A Foundation Brief (the truth layer to evaluate against)
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF (Truth Layer)")
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF")
                sections.append(str(brief))

        # Agent 02 Idea Generator output (the 30 ideas to evaluate)
        if inputs.get("idea_brief"):
            ideas = inputs["idea_brief"]
            if isinstance(ideas, dict):
                sections.append("\n# AGENT 02 — 30 AD IDEAS TO EVALUATE")
                sections.append(json.dumps(ideas, indent=2, default=str))
            else:
                sections.append("\n# AGENT 02 — 30 AD IDEAS TO EVALUATE")
                sections.append(str(ideas))

        sections.append(
            "\n# YOUR TASK\n"
            "Evaluate ALL 30 ideas against the Foundation Research Brief.\n"
            "Score each on 7 dimensions (1-10).\n"
            "Select exactly 5 survivors per funnel stage (15 total).\n"
            "Document kill reasons for all 15 rejected ideas.\n\n"
            "Requirements:\n"
            "- Be ruthless but fair — no vague praise or dismissal\n"
            "- Every verdict must cite specific evidence from the research brief\n"
            "- Protect 2-3 swing ideas across all 15 survivors if they have strong fundamentals\n"
            "- Provide improvement notes for each survivor (for Agent 04)\n"
            "- Flag compliance risks for Agent 12\n"
            "- Summarize strongest angles, weakest areas, and copywriter recommendations\n"
        )

        return "\n".join(sections)
