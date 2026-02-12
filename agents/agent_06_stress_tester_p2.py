"""Agent 06: Stress Tester — Pass 2 (Script-Level).

Quality gate for actual scripts and hooks.
Inputs: Scripts from Agent 04 + hooks from Agent 05 + Agent 1A research brief.
Outputs: StressTesterP2Brief → Agent 07 (Versioning Engine).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_06_system import SYSTEM_PROMPT
from schemas.stress_tester_p2 import StressTesterP2Brief


class Agent06StressTesterP2(BaseAgent):
    name = "Agent 06: Stress Tester P2"
    slug = "agent_06"
    description = (
        "Script-level quality gate. Evaluates 15 scripts + hooks on "
        "hook strength, flow, persuasion, emotional arc, pacing, "
        "production readiness, and compliance. Filters to 9 winners."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return StressTesterP2Brief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A output)
          - copywriter_brief: dict (Agent 04 — 15 scripts)
          - hook_brief: dict (Agent 05 — hook variations)
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
                sections.append(
                    "\n# AGENT 1A — FOUNDATION RESEARCH BRIEF\n"
                    "(Use as the ground truth for evaluating scripts.)"
                )
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF")
                sections.append(str(brief))

        # Agent 04 Copywriter output (15 scripts)
        if inputs.get("copywriter_brief"):
            cb = inputs["copywriter_brief"]
            if isinstance(cb, dict):
                sections.append(
                    "\n# AGENT 04 — 15 SCRIPTS TO EVALUATE"
                )
                sections.append(json.dumps(cb, indent=2, default=str))
            else:
                sections.append("\n# AGENT 04 — 15 SCRIPTS")
                sections.append(str(cb))

        # Agent 05 Hook Specialist output (hook variations)
        if inputs.get("hook_brief"):
            hb = inputs["hook_brief"]
            if isinstance(hb, dict):
                sections.append(
                    "\n# AGENT 05 — HOOK VARIATIONS TO EVALUATE"
                )
                sections.append(json.dumps(hb, indent=2, default=str))
            else:
                sections.append("\n# AGENT 05 — HOOK VARIATIONS")
                sections.append(str(hb))

        sections.append(
            "\n# YOUR TASK\n"
            "Evaluate ALL 15 scripts + their hooks.\n"
            "Score each on 7 dimensions (1-10).\n"
            "Select exactly 3 winners per funnel stage (9 total).\n"
            "Document cut reasons for all 6 eliminated scripts.\n\n"
            "Requirements:\n"
            "- Rank all hook variations per script (best → worst)\n"
            "- Recommend lead hook for each winner\n"
            "- Provide compliance flags for every winner (for Agent 12)\n"
            "- Provide versioning guidance for Agent 07\n"
            "- Evaluate persuasion, emotional arc, and pacing — not just ideas\n"
        )

        return "\n".join(sections)
