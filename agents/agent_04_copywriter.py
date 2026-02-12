"""Agent 04: Copywriter — writes production-ready ad scripts.

Inputs: 15 approved concepts from Agent 03 + customer language bank from Agent 1A.
Outputs: CopywriterBrief → Agent 05 (Hook Specialist).

Deep research file: agent_04_copywriter.md
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_04_system import SYSTEM_PROMPT
from schemas.copywriter import CopywriterBrief


class Agent04Copywriter(BaseAgent):
    name = "Agent 04: Copywriter"
    slug = "agent_04"
    description = (
        "Core persuasion engine. Writes production-ready ad scripts with "
        "time-coded beat sheets, visual direction, spoken dialogue, "
        "on-screen text, SFX cues. Uses Schwartz, Halbert, Ogilvy, "
        "Bencivenga, Georgi frameworks."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return CopywriterBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A output — segments, VoC, angles)
          - stress_test_brief: dict (Agent 03 output — 15 surviving concepts)
          - trend_intel: dict (Agent 1B output — competitive intel, optional)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Agent 1A Foundation Brief (VoC language bank, segments, angles)
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# AGENT 1A — FOUNDATION RESEARCH BRIEF\n"
                    "(Use this for: customer language bank, segment details, "
                    "awareness playbook, angle inventory, compliance pre-brief)"
                )
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION RESEARCH BRIEF")
                sections.append(str(brief))

        # Agent 03 Stress Test Brief (15 surviving concepts)
        if inputs.get("stress_test_brief"):
            st = inputs["stress_test_brief"]
            if isinstance(st, dict):
                sections.append(
                    "\n# AGENT 03 — 15 APPROVED CONCEPTS\n"
                    "(These are your concept briefs. Write one script per surviving idea. "
                    "Pay attention to improvement_notes and compliance_flags.)"
                )
                sections.append(json.dumps(st, indent=2, default=str))
            else:
                sections.append("\n# AGENT 03 — 15 APPROVED CONCEPTS")
                sections.append(str(st))

        # Agent 1B Trend Intel (optional, for competitive context)
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append(
                    "\n# AGENT 1B — TREND INTEL (competitive context)"
                )
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1B — TREND INTEL")
                sections.append(str(intel))

        sections.append(
            "\n# YOUR TASK\n"
            "Write 15 production-ready ad scripts — one per surviving concept.\n\n"
            "For EACH script:\n"
            "1. Choose the right copy framework based on awareness level\n"
            "2. Write a complete time-coded beat sheet\n"
            "3. Include mechanism line, proof moment(s), objection handling\n"
            "4. Write 2-4 CTA variations\n"
            "5. Self-check all 7 quality gates\n"
            "6. Flag compliance concerns\n\n"
            "CRITICAL RULES:\n"
            "- Use VoC language from Agent 1A (verbatim phrases, not polished copy)\n"
            "- Sound like a human talking, not a copywriter writing\n"
            "- One idea per script. One core promise. No multi-message clutter.\n"
            "- Every claim needs a reason-why support line\n"
            "- Pacing: 150-160 WPM, beat changes every 2-4 seconds\n"
            "- On-screen text: 3-7 words per overlay\n"
        )

        return "\n".join(sections)
