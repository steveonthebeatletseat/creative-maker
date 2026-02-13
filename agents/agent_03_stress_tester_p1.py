"""Agent 03: Stress Tester — Pass 1 (Strategic).

Evaluates 30 creative collision ideas against the angle inventory and
research foundation, filters to 15 survivors.
Inputs: 30 ideas from Agent 02 + Angle Architect inventory + Foundation Research brief.
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
        "Strategic quality gate. Evaluates 30 creative collision ideas from "
        "Agent 02 on angle strength, collision quality, execution specificity, "
        "creative originality, and compliance. Filters to 15 survivors."
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
          - idea_brief: dict (Agent 02 output — the 30 ideas to evaluate)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Full Foundation Research Brief (truth layer for evaluation)
        # The stress tester needs the full brief to verify each idea's
        # strategic grounding: segments, VoC library, white space, awareness
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# FOUNDATION RESEARCH BRIEF (Full — for verification)\n"
                    "Use this to verify each idea's strategic grounding:\n"
                    "- Does the target_segment match a real segment?\n"
                    "- Does the core_desire match that segment's actual desires?\n"
                    "- Does the voc_anchor use real customer language from the VoC library?\n"
                    "- Does the white_space_link reference a real competitive gap?"
                )
                sections.append(json.dumps(brief, indent=2, default=str))

        # Agent 02 Idea Generator output (the 30 ideas to evaluate)
        if inputs.get("idea_brief"):
            ideas = inputs["idea_brief"]
            if isinstance(ideas, dict):
                sections.append("\n# AGENT 02 — 30 AD CONCEPTS TO EVALUATE")
                sections.append(json.dumps(ideas, indent=2, default=str))
            else:
                sections.append("\n# AGENT 02 — 30 AD CONCEPTS TO EVALUATE")
                sections.append(str(ideas))

        sections.append(
            "\n# YOUR TASK\n"
            "Evaluate ALL 30 ideas from the Creative Collision Engine.\n"
            "Score each on 8 dimensions (1-10):\n"
            "  1. Angle Strength\n"
            "  2. Differentiation\n"
            "  3. Emotional Resonance\n"
            "  4. Collision Quality (highest weight — does the trend genuinely enhance the angle?)\n"
            "  5. Execution Specificity (is this filmable?)\n"
            "  6. Creative Originality\n"
            "  7. Compliance Viability\n"
            "  8. Production Feasibility\n\n"
            "Select exactly 5 survivors per funnel stage (15 total).\n"
            "Document kill reasons for all 15 rejected ideas.\n\n"
            "Requirements:\n"
            "- Verify each idea's angle_reference against the Angle Architect inventory\n"
            "- Be ruthless but fair — no vague praise or dismissal\n"
            "- Every verdict must cite specific evidence\n"
            "- Auto-kill: collision_quality <= 3, execution_specificity <= 3, compliance <= 3\n"
            "- Protect 2-3 swing ideas across all 15 survivors if fundamentals are strong\n"
            "- Provide execution refinement notes for each survivor (for the Copywriter)\n"
            "- Identify the strongest collisions and common failure patterns\n"
        )

        return "\n".join(sections)
