"""Agent 02: Idea Generator (Creative Collision Engine).

Takes trend-informed angles from the Angle Architect (1A2) and collides
them with live trend intelligence from Trend Intel (1B) to produce
specific, platform-native, executable ad concepts.

Inputs: Angle Architect inventory + Trend Intel brief + slim Foundation Research extract.
Outputs: IdeaGeneratorBrief → Agent 03 (Stress Tester P1).
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
        "Creative Collision Engine. Takes trend-informed angles from the "
        "Angle Architect and collides them with live trend intelligence to "
        "produce 30 specific, filmable, platform-native ad concepts."
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
          - foundation_brief: dict (Agent 1A output — we extract slim context)
          - trend_intel: dict (Agent 1B output — full brief)
          - performance_learnings: dict (Agent 15B output — optional, for feedback loop)
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # --- Slim Foundation Research Extract ---
        # We don't pass the full 1A brief — the Angle Architect already
        # synthesized everything relevant into its angles. We only pass
        # the 3 sections the Idea Generator needs for context.
        if inputs.get("foundation_brief"):
            fb = inputs["foundation_brief"]
            if isinstance(fb, dict):
                sections.append(
                    "\n# MARKET CONTEXT (from Foundation Research)"
                )

                # Sophistication diagnosis — drives execution style
                if fb.get("sophistication_diagnosis"):
                    sections.append("\n## Market Sophistication Diagnosis")
                    sections.append(json.dumps(
                        fb["sophistication_diagnosis"], indent=2, default=str
                    ))

                # Category snapshot — seasonality, formats, channel truths
                if fb.get("category_snapshot"):
                    sections.append("\n## Category Snapshot")
                    sections.append(json.dumps(
                        fb["category_snapshot"], indent=2, default=str
                    ))

                # Compliance prebrief — global compliance landscape
                if fb.get("compliance_prebrief"):
                    sections.append("\n## Compliance Pre-Brief")
                    sections.append(json.dumps(
                        fb["compliance_prebrief"], indent=2, default=str
                    ))

        # --- Angle Architect Inventory (primary creative input) ---
        # This comes from the merged foundation_brief (1A + 1A2)
        angle_inventory = None
        if isinstance(inputs.get("foundation_brief"), dict):
            angle_inventory = inputs["foundation_brief"].get("angle_inventory")

        if angle_inventory:
            sections.append(
                "\n# ANGLE ARCHITECT INVENTORY (Primary Creative Input)\n"
                "These are your strategic angles — each grounded in research "
                "and pre-paired with trend opportunities. Select from these "
                "to build your creative collisions. Every idea must reference "
                "a specific angle_name from this inventory."
            )
            sections.append(json.dumps(angle_inventory, indent=2, default=str))
        else:
            sections.append(
                "\n# WARNING: No Angle Architect inventory found.\n"
                "Cannot produce research-grounded ideas without angles."
            )

        # --- Trend Intel (full brief — tactical input) ---
        if inputs.get("trend_intel"):
            intel = inputs["trend_intel"]
            if isinstance(intel, dict):
                sections.append(
                    "\n# TREND INTEL BRIEF (Full — Tactical Input)\n"
                    "This is the live market intelligence. Use this alongside "
                    "the angles' pre-attached trend_opportunities to build "
                    "your creative collisions. You can also draw from trends "
                    "not pre-attached to any angle for swing ideas."
                )
                sections.append(json.dumps(intel, indent=2, default=str))
            else:
                sections.append("\n# TREND INTEL BRIEF")
                sections.append(str(intel))

        # --- Performance Learnings (feedback loop — optional) ---
        if inputs.get("performance_learnings"):
            learnings = inputs["performance_learnings"]
            if isinstance(learnings, dict):
                sections.append(
                    "\n# PERFORMANCE LEARNINGS (from Previous Batches)\n"
                    "Use these to guide your exploit/explore allocation:\n"
                    "- Winning patterns: iterate on these (same principle, new execution)\n"
                    "- Anti-patterns: avoid these (tested and failed)\n"
                    "- Fatigued elements: don't reuse (worn out)\n"
                    "- Explore queue: untested hypotheses worth trying"
                )
                sections.append(json.dumps(learnings, indent=2, default=str))

        # --- Task Section ---
        angle_count = len(angle_inventory) if angle_inventory else 0
        has_learnings = bool(inputs.get("performance_learnings"))

        task = (
            "\n# YOUR TASK\n"
            f"You have {angle_count} trend-informed angles to work with.\n\n"
            "Produce a complete Idea Generator Brief with:\n"
            "- 10 Top-of-Funnel ideas (Unaware → Problem Aware)\n"
            "- 10 Middle-of-Funnel ideas (Solution Aware → Product Aware)\n"
            "- 10 Bottom-of-Funnel ideas (Product Aware → Most Aware)\n\n"
            "Requirements:\n"
            "- Every idea must reference a specific angle_name AND a specific trend element\n"
            "- Each idea must have a vivid, filmable scene concept\n"
            "- Platform-specific versions (not just duration cuts)\n"
            "- Clear proof moment in every concept\n"
            "- 2-3 swing/explore ideas per funnel stage\n"
            "- Collision audit showing unique angles and trends used\n"
            "- Top 10 priority order recommendation\n"
        )

        if has_learnings:
            task += (
                "\nPerformance learnings are available. Apply exploit/explore allocation:\n"
                "- 60-70% of ideas should iterate on winning patterns\n"
                "- 30-40% should explore new collisions and untested hypotheses\n"
                "- 0% should repeat known anti-patterns or fatigued elements\n"
            )

        sections.append(task)

        return "\n".join(sections)
