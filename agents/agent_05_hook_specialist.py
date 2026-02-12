"""Agent 05: Hook Specialist — engineers the first 3 seconds.

Inputs: Complete scripts from Agent 04 + target awareness levels +
        platform targets + hook performance data from previous batches.
Outputs: HookSpecialistBrief → Agent 06 (Stress Tester P2).

Deep research file: agent_05_hook_specialist.md
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_05_system import SYSTEM_PROMPT
from schemas.hook_specialist import HookSpecialistBrief


class Agent05HookSpecialist(BaseAgent):
    name = "Agent 05: Hook Specialist"
    slug = "agent_05"
    description = (
        "Engineers the first 3 seconds — the highest-leverage element "
        "in the pipeline. Produces 3-5 hook variations per script with "
        "verbal + visual matched pairs, sound-on/off versions, "
        "platform variants, and testing taxonomy."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return HookSpecialistBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from upstream agent outputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A — awareness playbook, segments)
          - copywriter_brief: dict (Agent 04 — 15 production-ready scripts)
          - trend_intel: dict (Agent 1B — trending hooks, formats)
          - hook_performance_history: str (optional — from Agent 15B feedback loop)
          - platform_targets: list[str] (optional — e.g. ["meta_feed", "tiktok", "ig_reels"])
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        if inputs.get("platform_targets"):
            sections.append(
                f"Target platforms: {', '.join(inputs['platform_targets'])}"
            )
        else:
            sections.append("Target platforms: meta_feed, ig_reels, tiktok")

        # Agent 04 Copywriter output (15 scripts to engineer hooks for)
        if inputs.get("copywriter_brief"):
            cb = inputs["copywriter_brief"]
            if isinstance(cb, dict):
                sections.append(
                    "\n# AGENT 04 — 15 PRODUCTION-READY SCRIPTS\n"
                    "(Engineer 3-5 hook variations for EACH script. "
                    "Read each script's awareness_target, target_segment, "
                    "big_idea, and beats to inform hook design.)"
                )
                sections.append(json.dumps(cb, indent=2, default=str))
            else:
                sections.append("\n# AGENT 04 — 15 PRODUCTION-READY SCRIPTS")
                sections.append(str(cb))

        # Agent 1A Foundation Brief (awareness playbook, segments)
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# AGENT 1A — FOUNDATION BRIEF\n"
                    "(Use awareness playbook and segment data to match "
                    "hooks to viewer psychology.)"
                )
                sections.append(json.dumps(brief, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1A — FOUNDATION BRIEF")
                sections.append(str(brief))

        # Agent 1B Trend Intel (currently working hooks, trending formats)
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append(
                    "\n# AGENT 1B — TREND INTEL\n"
                    "(Use working_hooks and trending_formats to inform "
                    "hook design. Adapt what's working now.)"
                )
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# AGENT 1B — TREND INTEL")
                sections.append(str(intel))

        # Hook performance history (from Agent 15B feedback loop)
        if inputs.get("hook_performance_history"):
            sections.append(
                "\n# HOOK PERFORMANCE HISTORY (Agent 15B feedback loop)\n"
                "(Use this to avoid hook patterns that have fatigued "
                "and lean into patterns that perform well.)"
            )
            sections.append(str(inputs["hook_performance_history"]))

        sections.append(
            "\n# YOUR TASK\n"
            "For each of the 15 scripts, produce 3-5 hook variations.\n\n"
            "EACH hook must include:\n"
            "- Verbal + visual as a MATCHED PAIR (not separate ideas)\n"
            "- Sound-on variant (TikTok-optimized)\n"
            "- Sound-off variant (Meta Feed-optimized)\n"
            "- Platform-specific adjustments (at least Meta + TikTok)\n"
            "- Time-coded edit notes (0-0.7s, 0.7-1.5s, 1.5-3.0s)\n"
            "- Hook family classification + category tags\n"
            "- Risk flags for compliance\n"
            "- Expected hook rate tier\n\n"
            "ALSO provide:\n"
            "- Recommended lead hook per script\n"
            "- Top 5-10 hooks to test first\n"
            "- Testing methodology guidance\n"
        )

        return "\n".join(sections)
