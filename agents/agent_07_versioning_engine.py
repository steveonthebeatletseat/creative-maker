"""Agent 07: Versioning Engine — creates strategic variations for testing.

Inputs: 9 winning scripts from Agent 06 + testing priorities from Agent 15B.
Outputs: VersioningEngineBrief → Agent 08 (Screen Writer / Video Director).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_07_system import SYSTEM_PROMPT
from schemas.versioning_engine import VersioningEngineBrief


class Agent07VersioningEngine(BaseAgent):
    name = "Agent 07: Versioning Engine"
    slug = "agent_07"
    description = (
        "Creates strategic test variations of 9 winning scripts: "
        "length versions, CTA variations, tone variations, "
        "platform variations, and a testing matrix with naming conventions."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return VersioningEngineBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A — for context)
          - stress_test_p2_brief: dict (Agent 06 — 9 winners with evaluations)
          - copywriter_brief: dict (Agent 04 — full scripts for the 9 winners)
          - hook_brief: dict (Agent 05 — hook variations)
          - learning_priorities: str (optional — from Agent 15B feedback loop)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Agent 06 Stress Test P2 (9 winning scripts + evaluations)
        if inputs.get("stress_test_p2_brief"):
            st = inputs["stress_test_p2_brief"]
            if isinstance(st, dict):
                sections.append(
                    "\n# AGENT 06 — 9 WINNING SCRIPTS (Evaluations + Versioning Priorities)\n"
                    "(Use evaluations, hook rankings, and versioning_priorities to guide your work.)"
                )
                sections.append(json.dumps(st, indent=2, default=str))
            else:
                sections.append("\n# AGENT 06 — 9 WINNING SCRIPTS")
                sections.append(str(st))

        # Agent 04 Copywriter output (full scripts)
        if inputs.get("copywriter_brief"):
            cb = inputs["copywriter_brief"]
            if isinstance(cb, dict):
                sections.append(
                    "\n# AGENT 04 — FULL SCRIPTS\n"
                    "(Reference these for beat sheets, word counts, CTA blocks.)"
                )
                sections.append(json.dumps(cb, indent=2, default=str))
            else:
                sections.append("\n# AGENT 04 — FULL SCRIPTS")
                sections.append(str(cb))

        # Agent 05 Hook Specialist output
        if inputs.get("hook_brief"):
            hb = inputs["hook_brief"]
            if isinstance(hb, dict):
                sections.append(
                    "\n# AGENT 05 — HOOK VARIATIONS\n"
                    "(Reference recommended lead hooks and hook rankings.)"
                )
                sections.append(json.dumps(hb, indent=2, default=str))
            else:
                sections.append("\n# AGENT 05 — HOOK VARIATIONS")
                sections.append(str(hb))

        # Agent 1A Foundation Brief (for context)
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append("\n# AGENT 1A — FOUNDATION BRIEF (context)")
                # Only include relevant sections to save context
                summary = {}
                for key in ["brand_name", "product_name", "segments",
                            "sophistication_diagnosis", "testing_plan"]:
                    if key in brief:
                        summary[key] = brief[key]
                sections.append(json.dumps(summary, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION BRIEF")
                sections.append(str(brief))

        # Agent 15B Learning Priorities (feedback loop)
        if inputs.get("learning_priorities"):
            sections.append(
                "\n# AGENT 15B — TESTING PRIORITIES (from previous batches)\n"
                "(Incorporate these priorities into your versioning decisions.)"
            )
            sections.append(str(inputs["learning_priorities"]))

        sections.append(
            "\n# YOUR TASK\n"
            "For each of the 9 winning scripts, create strategic test variations:\n"
            "1. Length versions (2-3 per script: 15s, 30s, 60s)\n"
            "2. CTA variations (2-4 per script)\n"
            "3. Tone variations (1-3 per script where hypothesis exists)\n"
            "4. Platform variations (2-4 per script, minimum Meta + TikTok)\n\n"
            "Then build:\n"
            "5. Complete testing matrix with naming conventions\n"
            "6. Testing sequence (hooks first → CTAs → lengths/platforms)\n"
            "7. Budget allocation recommendations\n"
            "8. Production notes for Agent 08\n\n"
            "CRITICAL: Test ONE variable at a time. Name everything clearly. "
            "Agent 15A's analysis depends on clean attribution.\n"
        )

        return "\n".join(sections)
