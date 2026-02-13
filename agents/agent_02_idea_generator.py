"""Agent 02: Creative Engine — 3-step ad concept generator.

Step 1: Find marketing angles from Foundation Research (structured LLM)
Step 2: Web crawl for best video styles per angle (Claude Web Search, Gemini fallback)
Step 3: Merge angles + web research into video concepts (structured LLM)

Inputs: Foundation Research Brief + funnel counts (tof_count, mof_count, bof_count).
Outputs: CreativeEngineBrief → Human selection gate → Copywriter.
"""

from __future__ import annotations

import json
import time
import logging
from typing import Any

from pydantic import BaseModel

import config
from pipeline.base_agent import BaseAgent
from pipeline.llm import call_llm_structured, call_claude_web_search, call_deep_research
from prompts.agent_02_system import STEP1_PROMPT, CREATIVE_SCOUT_PROMPT, STEP3_PROMPT
from schemas.idea_generator import (
    CreativeEngineBrief,
    Step1Output,
)

logger = logging.getLogger(__name__)


class Agent02IdeaGenerator(BaseAgent):
    name = "Agent 02: Creative Engine"
    slug = "agent_02"
    description = (
        "3-step creative engine. Step 1: finds marketing angles from "
        "Foundation Research. Step 2: Claude Web Search scouts the web "
        "for the best video styles per angle. Step 3: merges angles + "
        "research into filmable video concepts."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return STEP3_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return CreativeEngineBrief

    def run(self, inputs: dict[str, Any]) -> BaseModel:
        """3-phase execution: angles → web crawl → synthesis.

        Phase 1: Structured LLM call to find marketing angles from research
        Phase 2: Gemini Deep Research to find video styles for each angle
        Phase 3: Structured LLM call to merge angles + research into final output
        """
        self.logger.info(
            "=== %s starting [3-phase: %s/%s] ===",
            self.name, self.provider, self.model,
        )
        start = time.time()

        # ---------------------------------------------------------------
        # PHASE 1: Find marketing angles from Foundation Research
        # ---------------------------------------------------------------
        self.logger.info("Phase 1: Finding marketing angles...")
        step1_prompt = self._build_step1_prompt(inputs)
        self.logger.info("Step 1 user prompt: %d chars", len(step1_prompt))

        step1_result = call_llm_structured(
            system_prompt=STEP1_PROMPT,
            user_prompt=step1_prompt,
            response_model=Step1Output,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=16_000,
        )

        angles = step1_result.angles
        step1_elapsed = time.time() - start
        self.logger.info(
            "Phase 1 complete: %d angles found in %.1fs",
            len(angles), step1_elapsed,
        )

        # ---------------------------------------------------------------
        # PHASE 2: Web crawl for video styles (Claude Web Search → Gemini fallback)
        # ---------------------------------------------------------------
        skip_research = inputs.get("_quick_mode") or inputs.get("_skip_deep_research")

        if skip_research:
            reason = "Quick mode" if inputs.get("_quick_mode") else "Model override"
            self.logger.info("Phase 2: %s — skipping web crawl", reason)
            web_research = self._fallback_video_research(inputs, angles)
        else:
            research_prompt = self._build_research_prompt(inputs, angles)
            self.logger.info("Research prompt: %d chars", len(research_prompt))

            web_research = self._run_web_research(inputs, angles, research_prompt)

        step2_elapsed = time.time() - start
        self.logger.info("Phase 2 elapsed: %.1fs", step2_elapsed)

        # ---------------------------------------------------------------
        # PHASE 3: Merge angles + web research into video concepts
        # ---------------------------------------------------------------
        self.logger.info("Phase 3: Merging angles + research into video concepts...")
        step3_prompt = self._build_step3_prompt(inputs, angles, web_research)
        self.logger.info("Step 3 user prompt: %d chars", len(step3_prompt))

        result = call_llm_structured(
            system_prompt=STEP3_PROMPT,
            user_prompt=step3_prompt,
            response_model=CreativeEngineBrief,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        elapsed = time.time() - start
        self.logger.info("=== %s finished in %.1fs ===", self.name, elapsed)

        self._save_output(result)
        return result

    # ------------------------------------------------------------------
    # Web research (Claude Web Search → Gemini Deep Research → fallback)
    # ------------------------------------------------------------------

    def _run_web_research(
        self, inputs: dict[str, Any], angles: list, research_prompt: str
    ) -> str:
        """Execute Phase 2 web research with cascading fallback.

        Priority:
          1. Claude Web Search (if ANTHROPIC_API_KEY is set)
          2. Gemini Deep Research (if GOOGLE_API_KEY is set)
          3. Built-in knowledge fallback (always available)
        """
        # --- Attempt 1: Claude Web Search ---
        if config.ANTHROPIC_API_KEY:
            self.logger.info(
                "Phase 2: Starting Claude Web Search (creative scout)..."
            )
            try:
                report = call_claude_web_search(
                    system_prompt=CREATIVE_SCOUT_PROMPT,
                    user_prompt=research_prompt,
                )
                self.logger.info(
                    "Phase 2 complete (Claude Web Search): %d chars of research",
                    len(report),
                )
                return report
            except Exception as e:
                self.logger.warning(
                    "Phase 2 Claude Web Search failed: %s. Trying Gemini fallback...",
                    e,
                )
        else:
            self.logger.info(
                "Phase 2: No ANTHROPIC_API_KEY — skipping Claude Web Search"
            )

        # --- Attempt 2: Gemini Deep Research ---
        if config.GOOGLE_API_KEY:
            self.logger.info(
                "Phase 2: Falling back to Gemini Deep Research..."
            )
            try:
                report = call_deep_research(research_prompt)
                self.logger.info(
                    "Phase 2 complete (Gemini Deep Research): %d chars of research",
                    len(report),
                )
                return report
            except Exception as e:
                self.logger.warning(
                    "Phase 2 Gemini Deep Research also failed: %s. Using built-in fallback.",
                    e,
                )
        else:
            self.logger.info(
                "Phase 2: No GOOGLE_API_KEY — skipping Gemini Deep Research"
            )

        # --- Attempt 3: Built-in knowledge fallback ---
        self.logger.info("Phase 2: Using built-in knowledge fallback")
        return self._fallback_video_research(inputs, angles)

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _build_step1_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt for Step 1: find marketing angles."""
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")

        # Funnel counts
        tof = inputs.get("tof_count", 10)
        mof = inputs.get("mof_count", 5)
        bof = inputs.get("bof_count", 2)
        total = tof + mof + bof

        sections.append(f"\n# REQUESTED ANGLES")
        sections.append(f"Top-of-Funnel (ToF): {tof} angles")
        sections.append(f"Middle-of-Funnel (MoF): {mof} angles")
        sections.append(f"Bottom-of-Funnel (BoF): {bof} angles")
        sections.append(f"Total: {total} angles")

        # Full Foundation Research Brief
        if inputs.get("foundation_brief"):
            fb = inputs["foundation_brief"]
            if isinstance(fb, dict):
                sections.append(
                    "\n# FOUNDATION RESEARCH BRIEF (Full)\n"
                    "This is the truth layer. Every angle MUST be traceable "
                    "back to specific data in this brief."
                )
                sections.append(json.dumps(fb, indent=2, default=str))

        sections.append(
            f"\n# YOUR TASK\n"
            f"Produce exactly {total} marketing angles:\n"
            f"- {tof} ToF angles (targeting Unaware → Problem Aware)\n"
            f"- {mof} MoF angles (targeting Solution Aware → Product Aware)\n"
            f"- {bof} BoF angles (targeting Product Aware → Most Aware)\n\n"
            "Every angle must be grounded in the research. "
            "Use angle_id format: tof_01, tof_02, ..., mof_01, ..., bof_01, ..."
        )

        return "\n".join(sections)

    def _build_research_prompt(
        self, inputs: dict[str, Any], angles: list
    ) -> str:
        """Build user prompt for web research (Step 2).

        Works for both Claude Web Search and Gemini Deep Research.
        Claude will use this as the user message alongside the
        CREATIVE_SCOUT_PROMPT system prompt and decide its own search strategy.
        """
        brand = inputs.get("brand_name", "Unknown")
        product = inputs.get("product_name", "Unknown")
        niche = inputs.get("niche", "")

        angle_summaries = []
        for a in angles:
            angle_summaries.append(
                f"- {a.angle_id} ({a.funnel_stage}): {a.angle_name}\n"
                f"  Segment: {a.target_segment} | Desire: {a.core_desire}\n"
                f"  Emotion: {a.emotional_lever} | Mechanism: {a.mechanism_hint}"
            )

        return f"""# RESEARCH BRIEF

## Brand & Product
- Brand: {brand}
- Product: {product}
- Niche: {niche}

## What We Need
We have {len(angles)} marketing angles for this brand's paid social video ad campaign. For each angle, we need to know what video ad formats and creative styles are performing best RIGHT NOW for similar products, messages, and audiences.

## The Marketing Angles

{chr(10).join(angle_summaries)}

## Research Priorities

1. **What formats are converting** for products like {product} — search ad libraries, creative breakdowns, and performance reports
2. **Real examples** — find specific brands or creators whose ads match our angles
3. **Platform-specific insights** — what works on TikTok vs Meta Reels vs YouTube Shorts for this niche
4. **Emerging formats** — any new video ad styles gaining traction in 2025-2026

## Deliverable

For each angle (or cluster of similar angles), report:
- 2-3 recommended video formats with evidence from your research
- Specific examples of ads you found that match the angle's approach
- Platform recommendations
- Style notes (editing pace, tone, visual world)

Be specific. "UGC testimonial" is too vague. "Direct-to-camera testimonial with product demo insert shots and text overlay hook, 15-30s, filmed on phone for authenticity" is what we need.
"""

    def _build_step3_prompt(
        self, inputs: dict[str, Any], angles: list, web_research: str
    ) -> str:
        """Build user prompt for Step 3: merge angles + research."""
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Batch: {inputs.get('batch_id', '')}")

        # Marketing angles from Step 1
        sections.append(
            "\n# MARKETING ANGLES (from Step 1)\n"
            "These are the strategic angles. Preserve all their grounding "
            "data exactly as-is. Your job is to pair each with 1-3 video concepts."
        )
        angle_dicts = [a.model_dump() for a in angles]
        sections.append(json.dumps(angle_dicts, indent=2, default=str))

        # Web research from Step 2
        sections.append(
            "\n# VIDEO STYLE RESEARCH (from Web Crawl)\n"
            "This is what we found by searching the web for what's actually "
            "working. Use these findings to inform your video concept choices."
        )
        sections.append(web_research)

        sections.append(
            "\n# YOUR TASK\n"
            f"For each of the {len(angles)} marketing angles, produce 1-3 "
            "video concept options.\n\n"
            "Each concept must include:\n"
            "- concept_name, video_format, scene_concept\n"
            "- why_this_format (how the format serves the angle)\n"
            "- reference_examples (from the web research)\n"
            "- platform_targets, sound_music_direction\n"
            "- proof_approach, proof_description\n\n"
            "Output a complete CreativeEngineBrief with all angles and "
            "their video concepts. Preserve the angle data exactly."
        )

        return "\n".join(sections)

    def _fallback_video_research(
        self, inputs: dict[str, Any], angles: list
    ) -> str:
        """Fallback when web research is skipped — use model's built-in knowledge."""
        brand = inputs.get("brand_name", "Unknown")
        product = inputs.get("product_name", "Unknown")

        return (
            f"# VIDEO FORMAT RESEARCH (Fallback — no web crawl)\n\n"
            f"No live web research was performed. Use your built-in knowledge "
            f"of high-performing paid social video formats for {brand} / {product}.\n\n"
            f"Consider these proven formats:\n"
            f"- UGC Testimonial: real person, direct to camera\n"
            f"- Green Screen Reaction: creator reacts, split-screen breakdown\n"
            f"- ASMR / Sensory Demo: macro shots, satisfying sounds\n"
            f"- Demo / How-It-Works: product in action, mechanism visible\n"
            f"- Before/After Transformation: time-lapse, split-screen\n"
            f"- Day in the Life: lifestyle integration, product natural\n"
            f"- Founder / Expert Story: authority explains the why\n"
            f"- Comparison / Vs: head-to-head with old way\n"
            f"- Unboxing / First Impressions: discovery moment\n"
            f"- Challenge / Stress Test: product under pressure\n"
            f"- Confession / Storytime: vulnerability as hook\n"
            f"- POV / First Person: viewer's perspective\n\n"
            f"Match each format to the angle's specific persuasion goal."
        )

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Not used directly — run() handles the 3-phase flow.
        Kept for compatibility with base class."""
        return self._build_step3_prompt(inputs, [], "No research available.")
