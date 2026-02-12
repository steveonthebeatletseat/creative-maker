"""Agent 1B: Trend & Competitive Intel — Two-Phase Agent.

Uses Gemini Deep Research for real-time web intelligence, then synthesizes
the findings into structured output.

Phase 1 (Research): Gemini Deep Research agent autonomously browses the web
    for competitor ads, trending formats, cultural moments, and working hooks.

Phase 2 (Synthesis): Structured LLM call produces the TrendIntelBrief
    from the raw research data + brand context + Agent 1A foundation brief.

Cadence: runs fresh every batch.
Inputs: brand/product info, niche, competitor names, Agent 1A foundation brief.
Outputs: TrendIntelBrief → Agent 2 (Idea Generator).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_01b_system import RESEARCH_PROMPT_TEMPLATE, SYSTEM_PROMPT
from schemas.trend_intel import TrendIntelBrief

logger = logging.getLogger(__name__)


class Agent01BTrendIntel(BaseAgent):
    name = "Agent 1B: Trend & Competitive Intel"
    slug = "agent_01b"
    description = (
        "Real-time competitive and cultural intelligence. "
        "Uses Claude Agent SDK to autonomously search the web for "
        "competitor ads, trending formats, cultural moments, and "
        "working hooks, then synthesizes into structured output."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return TrendIntelBrief

    # ------------------------------------------------------------------
    # Phase 1 — Web Research via Claude Agent SDK
    # ------------------------------------------------------------------

    def _build_research_prompt(self, inputs: dict[str, Any]) -> str:
        """Build the research prompt for the Claude Agent SDK."""
        brand_name = inputs.get("brand_name", "Unknown")
        product_name = inputs.get("product_name", "Unknown")
        niche = inputs.get("niche", "general")
        year = datetime.now().year

        # Extract competitor names from various input sources
        competitor_names = []
        if inputs.get("competitor_info"):
            competitor_names.append(inputs["competitor_info"])
        if inputs.get("foundation_brief") and isinstance(inputs["foundation_brief"], dict):
            comp_map = inputs["foundation_brief"].get("competitor_map", {})
            if isinstance(comp_map, dict):
                competitors = comp_map.get("competitors", [])
                for c in competitors:
                    if isinstance(c, dict) and "name" in c:
                        competitor_names.append(c["name"])

        competitor_context = ""
        if competitor_names:
            competitor_context = f"Key competitors to research: {', '.join(competitor_names[:10])}"

        website_context = ""
        if inputs.get("website_url"):
            website_context = (
                f"Brand website: {inputs['website_url']}\n"
                "Start your research by visiting this URL to understand the brand's "
                "positioning, then search for their competitors and the broader niche."
            )

        return RESEARCH_PROMPT_TEMPLATE.format(
            brand_name=brand_name,
            product_name=product_name,
            niche=niche,
            year=year,
            competitor_context=competitor_context,
            website_context=website_context,
        )

    @staticmethod
    def _current_month_year() -> str:
        """Return e.g. 'February 2026'."""
        return datetime.now().strftime("%B %Y")

    def _run_web_research(self, inputs: dict[str, Any]) -> str:
        """Execute Phase 1 — autonomous web research using Gemini Deep Research.

        Returns the raw research text from the Deep Research agent.
        Falls back gracefully if Deep Research is unavailable.
        """
        research_prompt = self._build_research_prompt(inputs)

        try:
            from pipeline.llm import call_deep_research

            self.logger.info("Phase 1: Starting web research via Gemini Deep Research...")
            result_text = call_deep_research(research_prompt)

            if result_text:
                self.logger.info(
                    "Phase 1 complete: %d chars of web research collected",
                    len(result_text),
                )
                return result_text
            else:
                self.logger.warning("Phase 1: Deep Research returned empty, using fallback")
                return self._fallback_research(inputs)

        except Exception as e:
            self.logger.warning(
                "Phase 1 Deep Research failed: %s. Falling back to non-research mode.", e
            )
            return self._fallback_research(inputs)

    def _fallback_research(self, inputs: dict[str, Any]) -> str:
        """Fallback when SDK is unavailable — uses any user-provided data."""
        sections = []
        sections.append("# WEB RESEARCH DATA (Fallback: no live web research available)")
        sections.append(
            "NOTE: The Claude Agent SDK was not available for live web research. "
            "The following data is based on user-provided inputs only. "
            "Supplement with your training knowledge where needed, but clearly "
            "label inferred data as such."
        )

        if inputs.get("competitor_ads"):
            sections.append("\n## User-Provided Competitor Ads")
            sections.append(inputs["competitor_ads"])
        if inputs.get("competitor_info"):
            sections.append("\n## User-Provided Competitor Info")
            sections.append(inputs["competitor_info"])
        if inputs.get("ad_library_data"):
            sections.append("\n## User-Provided Ad Library Data")
            sections.append(inputs["ad_library_data"])
        if inputs.get("cultural_context"):
            sections.append("\n## User-Provided Cultural Context")
            sections.append(inputs["cultural_context"])

        if len(sections) <= 2:
            sections.append(
                "\nNo user-provided competitive data available. "
                "Use your training knowledge for the niche: "
                f"{inputs.get('niche', 'general')}"
            )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Phase 2 — Synthesis (structured output)
    # ------------------------------------------------------------------

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt for Phase 2 (synthesis).

        This is called by the base class run() method after we inject
        the web research data into inputs.
        """
        sections = []

        sections.append("# BRAND CONTEXT")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Niche: {inputs.get('niche', 'Not specified')}")
        sections.append(f"Price: {inputs.get('price_point', 'Not specified')}")
        sections.append(f"Date: {self._current_month_year()}")

        # Foundation brief summary — give the synthesizer the key strategic context
        if inputs.get("foundation_brief"):
            brief = inputs["foundation_brief"]
            if isinstance(brief, dict):
                sections.append("\n# FOUNDATION BRIEF SUMMARY (from Agent 1A)")
                if "segments" in brief:
                    segments = brief["segments"]
                    if isinstance(segments, list):
                        names = [s.get("name", "?") for s in segments]
                        sections.append(f"Segments: {', '.join(names)}")
                        # Include top objections for gap analysis
                        for seg in segments[:3]:
                            objections = seg.get("top_objections", [])
                            if objections:
                                obj_texts = [
                                    o.get("objection", "?") if isinstance(o, dict) else str(o)
                                    for o in objections[:3]
                                ]
                                sections.append(
                                    f"  {seg.get('name', '?')} objections: {'; '.join(obj_texts)}"
                                )
                if "sophistication_diagnosis" in brief:
                    sd = brief["sophistication_diagnosis"]
                    sections.append(f"Market sophistication: Stage {sd.get('stage', '?')}")
                    if sd.get("recommended_differentiation"):
                        sections.append(
                            f"Recommended differentiation: {sd['recommended_differentiation']}"
                        )
                if "category_snapshot" in brief:
                    cs = brief["category_snapshot"]
                    sections.append(f"Category: {cs.get('category_definition', '?')}")
                    sections.append(
                        f"Dominant formats: {', '.join(cs.get('dominant_formats', []))}"
                    )
                if "competitor_map" in brief:
                    cm = brief["competitor_map"]
                    if isinstance(cm, dict) and cm.get("white_space_hypotheses"):
                        ws = cm["white_space_hypotheses"]
                        if isinstance(ws, list):
                            hypotheses = [
                                h.get("hypothesis", "?") if isinstance(h, dict) else str(h)
                                for h in ws[:5]
                            ]
                            sections.append(
                                f"White space hypotheses: {'; '.join(hypotheses)}"
                            )
            else:
                sections.append("\n# FOUNDATION BRIEF (from Agent 1A)")
                sections.append(str(brief))

        # Web research data (the primary input for synthesis)
        if inputs.get("_web_research"):
            sections.append("\n# LIVE WEB RESEARCH DATA")
            sections.append(
                "(This data was gathered from real web searches — competitor ads, "
                "trending formats, cultural moments, and hooks observed in the wild. "
                "Tag findings from this section as 'observed' confidence.)"
            )
            sections.append(inputs["_web_research"])

        # Any additional user-provided data
        if inputs.get("previous_batch_learnings"):
            sections.append("\n# LEARNINGS FROM PREVIOUS BATCHES (Agent 15B)")
            sections.append(
                "Weight these heavily — they are PROVEN for this brand."
            )
            sections.append(inputs["previous_batch_learnings"])

        batch_id = inputs.get("batch_id", "")
        sections.append(
            f"\n# YOUR TASK (Batch: {batch_id})\n"
            "Synthesize the research data above into a complete Trend Intel Brief.\n\n"
            "REQUIREMENTS:\n"
            "1. Score every format, hook, and cultural moment with priority_score (1-10)\n"
            "2. Tag every item with confidence (observed / inferred / supplemented)\n"
            "3. Assess DR conversion potential for every format\n"
            "4. Write out actual brand-adapted hooks (not just suggestions)\n"
            "5. Populate the gap_analysis — what competitors are NOT doing\n"
            "6. Build the strategic_priority_stack — Agent 2 reads this FIRST\n"
            "7. Set the meta.data_quality_score honestly\n\n"
            "Be specific to THIS brand and THIS moment. "
            "Ground everything in the research data. "
            "Think like a direct response marketer — everything traces back to revenue."
        )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Override run() for the two-phase approach
    # ------------------------------------------------------------------

    def run(self, inputs: dict[str, Any]) -> BaseModel:
        """Two-phase execution: web research → structured synthesis.

        If inputs["_quick_mode"] is True, skips web research entirely and
        goes straight to synthesis using the fallback data (much faster,
        good for testing).
        """
        quick_mode = inputs.get("_quick_mode", False)

        if quick_mode:
            self.logger.info(
                "=== %s starting (QUICK MODE — no web research) [%s/%s] ===",
                self.name, self.provider, self.model,
            )
            # Skip web research, use fallback data and go straight to synthesis
            inputs["_web_research"] = self._fallback_research(inputs)
            return super().run(inputs)

        self.logger.info(
            "=== %s starting (two-phase) [%s/%s] ===",
            self.name, self.provider, self.model,
        )
        start = time.time()

        # Phase 1: Web research
        phase1_start = time.time()
        web_research = self._run_web_research(inputs)
        phase1_time = time.time() - phase1_start
        self.logger.info(
            "Phase 1 (web research) complete: %.1fs, %d chars",
            phase1_time, len(web_research),
        )

        # Inject research into inputs for Phase 2
        inputs["_web_research"] = web_research

        # Phase 2: Structured synthesis (uses base class machinery)
        phase2_start = time.time()
        user_prompt = self.build_user_prompt(inputs)
        self.logger.info("Phase 2 user prompt: %d chars", len(user_prompt))

        from pipeline.llm import call_llm_structured

        result = call_llm_structured(
            system_prompt=self.system_prompt,
            user_prompt=user_prompt,
            response_model=self.output_schema,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        phase2_time = time.time() - phase2_start
        total_time = time.time() - start
        self.logger.info(
            "Phase 2 (synthesis) complete: %.1fs", phase2_time,
        )
        self.logger.info(
            "=== %s finished in %.1fs (research: %.1fs, synthesis: %.1fs) ===",
            self.name, total_time, phase1_time, phase2_time,
        )

        self._save_output(result)

        # Also save raw research for debugging
        self._save_research_log(web_research)

        return result

    def _save_research_log(self, research: str):
        """Save raw web research to disk for debugging/auditing."""
        import config
        log_path = config.OUTPUT_DIR / f"{self.slug}_web_research.txt"
        log_path.write_text(research, encoding="utf-8")
        self.logger.info("Web research log saved: %s", log_path)
