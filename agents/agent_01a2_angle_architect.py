"""Agent 1A2: Angle Architect — grounded angles from the truth layer.

Cadence: runs immediately after Agent 1A (Foundation Research).
Inputs: full Foundation Research Brief from Agent 1A.
Outputs: AngleArchitectBrief → downstream agents (02, 03, 04, etc.)
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_01a2_system import SYSTEM_PROMPT
from schemas.angle_architect import AngleArchitectBrief


class Agent01A2AngleArchitect(BaseAgent):
    name = "Agent 1A2: Angle Architect"
    slug = "agent_01a2"
    description = (
        "Receives the full Foundation Research Brief and produces a "
        "comprehensive, distribution-enforced angle inventory (20-60 angles) "
        "with each angle explicitly linked to specific segments, desires, "
        "VoC phrases, and white-space hypotheses. Also produces the testing plan."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return AngleArchitectBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from the Foundation Research Brief.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - foundation_brief: dict (full Agent 1A output)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")

        # The Foundation Research Brief — the primary input
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# AGENT 1A — FOUNDATION RESEARCH BRIEF (Full)\n"
                    "This is your primary input. Every angle you produce MUST be "
                    "traceable back to specific data in this brief."
                )
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF")
                sections.append(str(brief))
        else:
            sections.append(
                "\n# WARNING: No Foundation Research Brief provided.\n"
                "Cannot produce grounded angles without the 1A research."
            )

        # Optional: trend intel for additional context
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append(
                    "\n# AGENT 1B — TREND INTEL (supplementary context)\n"
                    "Use this for additional competitive context and trending formats."
                )
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1B — TREND INTEL")
                sections.append(str(intel))

        # Extract key counts for the task section
        segment_count = 0
        white_space_count = 0
        voc_count = 0
        if isinstance(inputs.get("foundation_brief"), dict):
            fb = inputs["foundation_brief"]
            segment_count = len(fb.get("segments", []))
            comp_map = fb.get("competitor_map", {})
            if isinstance(comp_map, dict):
                white_space_count = len(comp_map.get("white_space_hypotheses", []))
            voc_count = len(fb.get("voc_library", []))

        sections.append(
            "\n# YOUR TASK\n"
            f"The research brief contains {segment_count} segments, "
            f"{white_space_count} white-space hypotheses, and {voc_count} VoC entries.\n\n"
            "Produce a complete Angle Architect Brief with:\n"
            "1. ANGLE INVENTORY (20-60 angles)\n"
            "   - Every angle linked to a specific segment, desire, white space, and VoC phrase\n"
            "   - 3-5 hook templates per angle\n"
            "   - Compliance risk rated per angle\n\n"
            "2. TESTING PLAN\n"
            "   - Test clusters grouping related angles\n"
            "   - ICE-scored hypotheses\n"
            "   - Guardrails and kill criteria\n\n"
            "3. DISTRIBUTION AUDIT\n"
            "   - Prove all distribution minimums are met:\n"
            "     • At least 3 angles per segment\n"
            "     • At least 3 angles per awareness level (5 for problem/solution aware)\n"
            "     • TOF ≥ 30%, MOF ≥ 30%, BOF ≥ 20%\n"
            "     • At least 5 distinct emotions, no emotion > 30%\n"
            "     • At least 4 distinct formats, no format > 35%\n"
            "     • No two angles share the same segment + desire + mechanism + emotion\n\n"
            "CRITICAL: Quality over quantity. Every angle must be deeply grounded in the research.\n"
            "Generic angles with no traceable research link will be rejected downstream."
        )

        return "\n".join(sections)
