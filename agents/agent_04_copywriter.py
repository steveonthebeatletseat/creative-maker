"""Agent 04: Copywriter — writes production-ready ad scripts.

Inputs: User-selected video concepts + Foundation Research Brief.
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
        "on-screen text, SFX cues. 1 hook per script."
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
        """Build user prompt from selected video concepts + research.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - batch_id: str
          - foundation_brief: dict (Agent 1A output)
          - idea_brief: dict (Agent 02 output — angles with video concepts)
          - selected_concepts: list[dict] (user selections: [{angle_id, concept_index}])
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Foundation Research Brief (VoC language bank, segments)
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append(
                    "\n# FOUNDATION RESEARCH BRIEF\n"
                    "(Use for: customer language bank, segment details, "
                    "awareness playbook, competitive landscape)"
                )
                sections.append(json.dumps(brief, indent=2, default=str))

        # Build the selected concepts from idea_brief + selections
        idea_brief = inputs.get("idea_brief", {})
        selected = inputs.get("selected_concepts", [])
        concepts_to_write = []

        if idea_brief and isinstance(idea_brief, dict):
            angles = idea_brief.get("angles", [])

            if selected:
                # Filter to user-selected concepts only
                for sel in selected:
                    angle_id = sel.get("angle_id")
                    concept_idx = sel.get("concept_index", 0)
                    for angle in angles:
                        if isinstance(angle, dict) and angle.get("angle_id") == angle_id:
                            concepts = angle.get("video_concepts", [])
                            if concept_idx < len(concepts):
                                concepts_to_write.append({
                                    "angle": {k: v for k, v in angle.items() if k != "video_concepts"},
                                    "video_concept": concepts[concept_idx],
                                })
                            break
            else:
                # No selection — use all concepts (first option per angle)
                for angle in angles:
                    if isinstance(angle, dict):
                        concepts = angle.get("video_concepts", [])
                        if concepts:
                            concepts_to_write.append({
                                "angle": {k: v for k, v in angle.items() if k != "video_concepts"},
                                "video_concept": concepts[0],
                            })

        if concepts_to_write:
            sections.append(
                f"\n# VIDEO CONCEPTS TO WRITE SCRIPTS FOR ({len(concepts_to_write)} total)\n"
                "Write ONE production-ready script per concept below."
            )
            sections.append(json.dumps(concepts_to_write, indent=2, default=str))
        else:
            sections.append(
                "\n# WARNING: No video concepts found.\n"
                "Cannot write scripts without concept briefs."
            )

        sections.append(
            f"\n# YOUR TASK\n"
            f"Write {len(concepts_to_write)} production-ready ad scripts — "
            f"one per video concept.\n\n"
            "For EACH script:\n"
            "1. Choose the right copy framework based on awareness level\n"
            "2. Write a complete time-coded beat sheet (including 1 hook in the first 3 seconds)\n"
            "3. Include mechanism line, proof moment(s), objection handling\n"
            "4. Write 2-4 CTA variations\n"
            "5. Self-check all 7 quality gates\n"
            "6. Flag compliance concerns\n\n"
            "CRITICAL RULES:\n"
            "- Use VoC language from the Foundation Research (verbatim phrases, not polished copy)\n"
            "- Sound like a human talking, not a copywriter writing\n"
            "- One idea per script. One core promise. No multi-message clutter.\n"
            "- Every claim needs a reason-why support line\n"
            "- Pacing: 150-160 WPM, beat changes every 2-4 seconds\n"
            "- On-screen text: 3-7 words per overlay\n"
        )

        return "\n".join(sections)
