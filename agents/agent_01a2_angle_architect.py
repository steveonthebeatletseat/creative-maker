"""Agent 1A2: Angle Architect — grounded angles from the truth layer + trend intel.

Cadence: runs after Agent 1A (Foundation Research) AND Agent 1B (Trend Intel).
Inputs: full Foundation Research Brief from Agent 1A + full Trend Intel Brief from Agent 1B.
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
        "Receives the Foundation Research Brief AND the Trend Intel Brief "
        "and produces a comprehensive, distribution-enforced angle inventory "
        "(20-60 angles) with trend opportunities pre-attached. Each angle is "
        "linked to specific research AND paired with 2-3 best-fit trend elements."
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
        """Build user prompt from the Foundation Research Brief and Trend Intel.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - foundation_brief: dict (full Agent 1A output)
          - trend_intel: dict (full Agent 1B output) — PRIMARY INPUT
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")

        # The Foundation Research Brief — primary input #1
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# AGENT 1A — FOUNDATION RESEARCH BRIEF (Primary Input #1)\n"
                    "This is the truth layer. Every angle you produce MUST be "
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

        # The Trend Intel Brief — primary input #2
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append(
                    "\n# AGENT 1B — TREND INTEL BRIEF (Primary Input #2)\n"
                    "This is the live market intelligence. Every angle MUST be "
                    "paired with 2-3 trend opportunities from this brief. Use the "
                    "Strategic Priority Stack to guide which trends to prioritize."
                )
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1B — TREND INTEL BRIEF")
                sections.append(str(intel))
        else:
            sections.append(
                "\n# WARNING: No Trend Intel Brief provided.\n"
                "Angles will lack trend opportunities. The Idea Generator downstream "
                "will have no trend pairings to work with — this degrades output quality."
            )

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

        trend_format_count = 0
        trend_hook_count = 0
        trend_moment_count = 0
        if isinstance(inputs.get("trend_intel"), dict):
            ti = inputs["trend_intel"]
            trend_format_count = len(ti.get("trending_formats", []))
            trend_hook_count = len(ti.get("working_hooks", []))
            trend_moment_count = len(ti.get("cultural_moments", []))

        sections.append(
            "\n# YOUR TASK\n"
            f"The research brief contains {segment_count} segments, "
            f"{white_space_count} white-space hypotheses, and {voc_count} VoC entries.\n"
            f"The trend intel contains {trend_format_count} trending formats, "
            f"{trend_hook_count} working hooks, and {trend_moment_count} cultural moments.\n\n"
            "Produce a complete Angle Architect Brief with:\n"
            "1. ANGLE INVENTORY (20-60 angles)\n"
            "   - Every angle linked to a specific segment, desire, white space, and VoC phrase\n"
            "   - 2-3 trend opportunities per angle (from 1B's trend intel)\n"
            "   - 3-5 hook templates per angle\n"
            "   - Compliance risk rated per angle\n\n"
            "2. TESTING PLAN\n"
            "   - Test clusters grouping related angles\n"
            "   - ICE-scored hypotheses\n"
            "   - Guardrails and kill criteria\n\n"
            "3. DISTRIBUTION AUDIT\n"
            "   - Prove all distribution minimums are met:\n"
            "     - At least 3 angles per segment\n"
            "     - At least 3 angles per awareness level (5 for problem/solution aware)\n"
            "     - TOF >= 30%, MOF >= 30%, BOF >= 20%\n"
            "     - At least 5 distinct emotions, no emotion > 30%\n"
            "     - At least 4 distinct formats, no format > 35%\n"
            "     - No two angles share the same segment + desire + mechanism + emotion\n\n"
            "CRITICAL: Every angle must be deeply grounded in the research AND paired "
            "with high-quality trend opportunities from 1B. Generic angles with no "
            "research link or forced trend pairings will produce weak creative concepts downstream."
        )

        return "\n".join(sections)
