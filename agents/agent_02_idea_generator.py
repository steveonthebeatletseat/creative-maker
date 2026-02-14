"""Agent 02: Creative Engine — 3-step ad concept generator.

Step 1: Find marketing angles from Foundation Research (structured LLM)
Step 2: Web crawl for best video styles per angle (Claude Web Search, Gemini fallback)
Step 3: Merge angles + web research into video concepts (structured LLM)

Inputs: Foundation Research Brief + funnel counts (tof_count, mof_count, bof_count).
Outputs: CreativeEngineBrief → Human selection gate → Copywriter.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import time
from typing import Any

from pydantic import BaseModel

import config
from pipeline.base_agent import BaseAgent
from pipeline.claude_agent_scout import call_claude_agent_structured
from pipeline.llm import (
    call_claude_web_search,
    call_deep_research,
    call_llm_structured,
    get_model_pricing,
    get_usage_summary,
)
from prompts.agent_02_system import STEP1_PROMPT, CREATIVE_SCOUT_PROMPT, STEP3_PROMPT
from schemas.idea_generator import (
    CreativeScoutReport,
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
        """Run Creative Engine with optional parallel stage specialization."""
        self.logger.info(
            "=== %s starting [3-phase: %s/%s] ===",
            self.name, self.provider, self.model,
        )
        start = time.time()
        engine_start_cost = get_usage_summary()["total_cost"]
        engine_budget = config.CREATIVE_ENGINE_MAX_COST_USD
        self.logger.info(
            "Creative Engine budget cap: $%.2f (model=%s, scout=%s)",
            engine_budget, self.model, config.CREATIVE_SCOUT_MODEL,
        )

        stage_counts = self._get_stage_counts(inputs)
        active_stages = [stage for stage, count in stage_counts.items() if count > 0]
        if not active_stages:
            raise RuntimeError("Creative Engine requires at least one stage count > 0")

        if config.CREATIVE_ENGINE_PARALLEL_BY_STAGE and len(active_stages) > 1:
            self.logger.info(
                "Parallel stage mode: running stage-specialized engines for %s",
                ", ".join(stage.upper() for stage in active_stages),
            )
            result = self._run_parallel_stage_engines(inputs, stage_counts)
        elif config.CREATIVE_ENGINE_PARALLEL_BY_STAGE and len(active_stages) == 1:
            stage = active_stages[0]
            requested = stage_counts[stage]
            self.logger.info(
                "Stage-specialized mode: running %s worker only (%d requested)",
                stage.upper(),
                requested,
            )
            stage_angles = self._run_stage_pipeline(
                stage,
                self._build_stage_inputs(inputs, stage, requested),
                requested,
            )
            result = CreativeEngineBrief(
                brand_name=inputs.get("brand_name", "Unknown"),
                product_name=inputs.get("product_name", "Unknown"),
                generated_date=time.strftime("%Y-%m-%d"),
                batch_id=inputs.get("batch_id", ""),
                angles=stage_angles,
            )
        else:
            self.logger.info("Monolithic mode: single Creative Engine pass")
            result = self._run_monolithic_engine(inputs, engine_start_cost)

        elapsed = time.time() - start
        total_cost = self._engine_cost_since(engine_start_cost)
        self.logger.info(
            "=== %s finished in %.1fs (cost: $%.4f / $%.2f cap) ===",
            self.name,
            elapsed,
            total_cost,
            engine_budget,
        )

        self._save_output(result)
        return result

    def _run_monolithic_engine(
        self,
        inputs: dict[str, Any],
        engine_start_cost: float,
    ) -> CreativeEngineBrief:
        """Original single-pass Creative Engine flow."""
        start = time.time()

        # ---------------------------------------------------------------
        # PHASE 1: Find marketing angles from Foundation Research
        # ---------------------------------------------------------------
        self.logger.info("Phase 1: Finding marketing angles...")
        step1_prompt = self._build_step1_prompt(inputs)
        self.logger.info("Step 1 user prompt: %d chars", len(step1_prompt))
        step1_max_tokens = self._budget_limited_max_tokens(
            model=self.model,
            remaining_budget_usd=self._remaining_engine_budget(engine_start_cost),
            prompt_chars=len(step1_prompt),
            configured_max=16_000,
        )

        step1_result = call_llm_structured(
            system_prompt=STEP1_PROMPT,
            user_prompt=step1_prompt,
            response_model=Step1Output,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=step1_max_tokens,
        )
        self._assert_engine_budget("Phase 1", engine_start_cost)

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
            if config.CREATIVE_SCOUT_SDK_ONLY:
                raise RuntimeError(
                    "Creative Engine Step 2 is configured as SDK-only "
                    "(CREATIVE_SCOUT_SDK_ONLY=true), but research was skipped "
                    "by quick mode/model override."
                )
            reason = "Quick mode" if inputs.get("_quick_mode") else "Model override"
            self.logger.info("Phase 2: %s — skipping web crawl", reason)
            web_research = self._fallback_video_research(inputs, angles)
        else:
            research_prompt = self._build_research_prompt(inputs, angles)
            self.logger.info("Research prompt: %d chars", len(research_prompt))
            remaining_before_step2 = self._remaining_engine_budget(engine_start_cost)
            if remaining_before_step2 <= 0:
                raise RuntimeError(
                    f"Creative Engine budget exhausted before Step 2 (cap: ${config.CREATIVE_ENGINE_MAX_COST_USD:.2f})"
                )

            web_research = self._run_web_research(
                inputs,
                angles,
                research_prompt,
                remaining_budget_usd=remaining_before_step2,
            )
        self._assert_engine_budget("Phase 2", engine_start_cost)

        step2_elapsed = time.time() - start
        self.logger.info("Phase 2 elapsed: %.1fs", step2_elapsed)

        # ---------------------------------------------------------------
        # PHASE 3: Merge angles + web research into video concepts
        # ---------------------------------------------------------------
        self.logger.info("Phase 3: Merging angles + research into video concepts...")
        step3_prompt = self._build_step3_prompt(inputs, angles, web_research)
        self.logger.info("Step 3 user prompt: %d chars", len(step3_prompt))
        step3_max_tokens = self._budget_limited_max_tokens(
            model=self.model,
            remaining_budget_usd=self._remaining_engine_budget(engine_start_cost),
            prompt_chars=len(step3_prompt),
            configured_max=self.max_tokens,
        )

        result = call_llm_structured(
            system_prompt=STEP3_PROMPT,
            user_prompt=step3_prompt,
            response_model=CreativeEngineBrief,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=step3_max_tokens,
        )
        self._assert_engine_budget("Phase 3", engine_start_cost)
        return result

    def _get_stage_counts(self, inputs: dict[str, Any]) -> dict[str, int]:
        """Read and normalize ToF/MoF/BoF counts from inputs."""
        defaults = {"tof": 10, "mof": 5, "bof": 2}
        counts: dict[str, int] = {}
        for stage, fallback in defaults.items():
            raw = inputs.get(f"{stage}_count", fallback)
            try:
                count = int(raw)
            except (TypeError, ValueError):
                count = fallback
            counts[stage] = max(0, count)
        return counts

    def _build_stage_inputs(
        self,
        base_inputs: dict[str, Any],
        stage: str,
        count: int,
    ) -> dict[str, Any]:
        """Build stage-specialized inputs where only one funnel stage is active."""
        stage_inputs = dict(base_inputs)
        stage_inputs["tof_count"] = count if stage == "tof" else 0
        stage_inputs["mof_count"] = count if stage == "mof" else 0
        stage_inputs["bof_count"] = count if stage == "bof" else 0
        return stage_inputs

    @staticmethod
    def _stage_key(value: Any) -> str:
        """Normalize funnel-stage enum/string to tof|mof|bof."""
        if hasattr(value, "value"):
            return str(value.value).strip().lower()
        return str(value).strip().lower()

    def _run_stage_pipeline(
        self,
        stage: str,
        stage_inputs: dict[str, Any],
        requested_count: int,
    ) -> list[Any]:
        """Run a full 3-step creative pass for one funnel stage."""
        stage_name = stage.upper()
        stage_start = time.time()
        self.logger.info("[%s] Stage worker starting (%d requested angles)", stage_name, requested_count)

        step1_prompt = self._build_step1_prompt(stage_inputs)
        self.logger.info("[%s] Step 1 prompt: %d chars", stage_name, len(step1_prompt))
        step1_result = call_llm_structured(
            system_prompt=STEP1_PROMPT,
            user_prompt=step1_prompt,
            response_model=Step1Output,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=min(16_000, int(self.max_tokens)),
        )
        angles = step1_result.angles
        if not angles:
            raise RuntimeError(f"[{stage_name}] Step 1 produced no angles")
        self.logger.info("[%s] Step 1 complete: %d angles", stage_name, len(angles))

        skip_research = stage_inputs.get("_quick_mode") or stage_inputs.get("_skip_deep_research")
        if skip_research:
            if config.CREATIVE_SCOUT_SDK_ONLY:
                raise RuntimeError(
                    f"[{stage_name}] Step 2 is SDK-only but research was skipped by quick mode/model override."
                )
            reason = "Quick mode" if stage_inputs.get("_quick_mode") else "Model override"
            self.logger.info("[%s] Step 2: %s — using fallback research", stage_name, reason)
            web_research = self._fallback_video_research(stage_inputs, angles)
        else:
            research_prompt = self._build_research_prompt(stage_inputs, angles)
            self.logger.info("[%s] Step 2 prompt: %d chars", stage_name, len(research_prompt))
            stage_budget = max(
                float(config.CREATIVE_SCOUT_MAX_BUDGET_USD or 0.0),
                0.01,
            )
            web_research = self._run_web_research(
                stage_inputs,
                angles,
                research_prompt,
                remaining_budget_usd=stage_budget,
            )

        step3_prompt = self._build_step3_prompt(stage_inputs, angles, web_research)
        self.logger.info("[%s] Step 3 prompt: %d chars", stage_name, len(step3_prompt))
        result = call_llm_structured(
            system_prompt=STEP3_PROMPT,
            user_prompt=step3_prompt,
            response_model=CreativeEngineBrief,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=int(self.max_tokens),
        )

        stage_angles = [
            angle for angle in result.angles if self._stage_key(angle.funnel_stage) == stage
        ]
        if not stage_angles:
            raise RuntimeError(f"[{stage_name}] Step 3 produced zero {stage_name} concepts")
        if len(stage_angles) > requested_count:
            stage_angles = stage_angles[:requested_count]
        if len(stage_angles) < requested_count:
            self.logger.warning(
                "[%s] Requested %d concepts, got %d",
                stage_name,
                requested_count,
                len(stage_angles),
            )

        self.logger.info(
            "[%s] Stage worker finished in %.1fs",
            stage_name,
            time.time() - stage_start,
        )
        return stage_angles

    def _run_parallel_stage_engines(
        self,
        inputs: dict[str, Any],
        stage_counts: dict[str, int],
    ) -> CreativeEngineBrief:
        """Run three stage-specialized workers in parallel and merge outputs."""
        active = [(stage, count) for stage, count in stage_counts.items() if count > 0]
        max_workers = max(1, min(len(active), int(config.CREATIVE_ENGINE_PARALLEL_MAX_WORKERS)))

        stage_angles: dict[str, list[Any]] = {}
        failures: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="agent02-stage") as pool:
            futures = {
                pool.submit(
                    self._run_stage_pipeline,
                    stage,
                    self._build_stage_inputs(inputs, stage, count),
                    count,
                ): stage
                for stage, count in active
            }
            for future in as_completed(futures):
                stage = futures[future]
                try:
                    stage_angles[stage] = future.result()
                except Exception as exc:
                    failures[stage] = str(exc)

        if failures:
            details = "; ".join(f"{stage.upper()}: {err}" for stage, err in sorted(failures.items()))
            raise RuntimeError(f"Parallel stage workers failed: {details}")

        merged_angles: list[Any] = []
        for stage in ("tof", "mof", "bof"):
            merged_angles.extend(stage_angles.get(stage, []))

        if not merged_angles:
            raise RuntimeError("Parallel stage mode produced no concepts")

        return CreativeEngineBrief(
            brand_name=inputs.get("brand_name", "Unknown"),
            product_name=inputs.get("product_name", "Unknown"),
            generated_date=time.strftime("%Y-%m-%d"),
            batch_id=inputs.get("batch_id", ""),
            angles=merged_angles,
        )

    # ------------------------------------------------------------------
    # Web research (Claude Web Search → Gemini Deep Research → fallback)
    # ------------------------------------------------------------------

    def _run_web_research(
        self,
        inputs: dict[str, Any],
        angles: list,
        research_prompt: str,
        remaining_budget_usd: float,
    ) -> str:
        """Execute Phase 2 web research with cascading fallback.

        Priority:
          1. Claude Agent SDK (structured output, Opus default)
          2. Legacy Anthropic Web Search API
          3. Gemini Deep Research (if GOOGLE_API_KEY is set)
          4. Built-in knowledge fallback (always available)
        """
        if remaining_budget_usd <= 0:
            raise RuntimeError(
                f"Creative Engine budget exhausted before Step 2 research (cap: ${config.CREATIVE_ENGINE_MAX_COST_USD:.2f})"
            )

        sdk_budget = min(
            max(0.0, remaining_budget_usd),
            config.CREATIVE_SCOUT_MAX_BUDGET_USD,
        )

        # --- Attempt 1: Claude Agent SDK ---
        if config.ANTHROPIC_API_KEY:
            self.logger.info(
                "Phase 2: Starting Claude Agent SDK scout (model=%s, budget=$%.2f)...",
                config.CREATIVE_SCOUT_MODEL,
                sdk_budget,
            )
            try:
                structured_report = call_claude_agent_structured(
                    system_prompt=CREATIVE_SCOUT_PROMPT,
                    user_prompt=research_prompt,
                    response_model=CreativeScoutReport,
                    model=config.CREATIVE_SCOUT_MODEL,
                    max_turns=config.CREATIVE_SCOUT_MAX_TURNS,
                    max_thinking_tokens=config.CREATIVE_SCOUT_MAX_THINKING_TOKENS,
                    max_budget_usd=sdk_budget,
                )
                report = self._format_structured_research(structured_report)
                self.logger.info(
                    "Phase 2 complete (Claude Agent SDK): %d chars of structured research",
                    len(report),
                )
                return report
            except Exception as e:
                if config.CREATIVE_SCOUT_SDK_ONLY:
                    raise RuntimeError(
                        "Creative Engine Step 2 SDK-only mode is enabled and "
                        f"Claude Agent SDK failed: {e}"
                    ) from e
                self.logger.warning(
                    "Phase 2 Claude Agent SDK failed: %s. Trying legacy Anthropic web search...",
                    e,
                )
        else:
            if config.CREATIVE_SCOUT_SDK_ONLY:
                raise RuntimeError(
                    "Creative Engine Step 2 SDK-only mode is enabled, but "
                    "ANTHROPIC_API_KEY is not set."
                )
            self.logger.info(
                "Phase 2: No ANTHROPIC_API_KEY — skipping Claude SDK research"
            )

        # --- Attempt 2: Legacy Claude Web Search API ---
        if config.ANTHROPIC_API_KEY:
            self.logger.info("Phase 2: Falling back to legacy Claude web_search tool...")
            try:
                report = call_claude_web_search(
                    system_prompt=CREATIVE_SCOUT_PROMPT,
                    user_prompt=research_prompt,
                    model=config.CREATIVE_SCOUT_MODEL,
                    max_uses=config.CREATIVE_SCOUT_WEB_MAX_USES,
                    max_tokens=config.CREATIVE_SCOUT_WEB_MAX_TOKENS,
                )
                self.logger.info(
                    "Phase 2 complete (Legacy Claude Web Search): %d chars",
                    len(report),
                )
                return report
            except Exception as e:
                self.logger.warning(
                    "Phase 2 legacy Claude web search failed: %s. Trying Gemini fallback...",
                    e,
                )
        else:
            self.logger.info(
                "Phase 2: No ANTHROPIC_API_KEY — skipping legacy Claude Web Search"
            )

        # --- Attempt 3: Gemini Deep Research ---
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

        # --- Attempt 4: Built-in knowledge fallback ---
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

Return a structured report with ONE entry per angle_id. For each angle:
- 2-3 recommended video formats with citation-backed evidence
- Specific examples of ads/campaigns found in research
- Platform recommendations and format-fit rationale
- Style notes (editing pace, tone, visual world)
- Confidence score and at least one source URL for each core claim

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
            "\n# VIDEO STYLE RESEARCH (Structured JSON from Step 2)\n"
            "Use this structured, citation-backed research keyed by angle_id. "
            "Do not invent evidence not present in the research payload."
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
            "Ground each concept in the provided angle_id research evidence.\n"
            "Output a complete CreativeEngineBrief with all angles and "
            "their video concepts. Preserve the angle data exactly."
        )

        return "\n".join(sections)

    def _fallback_video_research(
        self, inputs: dict[str, Any], angles: list
    ) -> str:
        """Fallback when web research is skipped — still return structured JSON."""
        report = {
            "brand_name": inputs.get("brand_name", "Unknown"),
            "product_name": inputs.get("product_name", "Unknown"),
            "generated_date": time.strftime("%Y-%m-%d"),
            "global_insights": [
                "Fallback mode: no live web research available",
                "Use broad paid-social best practices with explicit proof moments",
            ],
            "angle_research": [],
        }

        for a in angles:
            report["angle_research"].append(
                {
                    "angle_id": a.angle_id,
                    "angle_name": a.angle_name,
                    "source_count": 1,
                    "trend_signals": ["No live trend validation available"],
                    "recommended_formats": [
                        {
                            "video_format": "Direct-to-camera testimonial with demo inserts",
                            "platform_targets": ["tiktok", "meta_reels"],
                            "why_fit": "Balances trust + product proof when live evidence is unavailable.",
                            "style_notes": "Fast hook, 15-30s runtime, proof beat by second 6-10.",
                            "watchouts": ["Treat as hypothesis until validated by live research"],
                            "evidence": [
                                {
                                    "claim": "This is a fallback prior based on historical direct-response patterns.",
                                    "confidence": 0.4,
                                    "citations": [
                                        {
                                            "source_url": "fallback://built-in-prior",
                                            "source_title": "Built-in knowledge fallback",
                                            "publisher": "Creative Engine",
                                            "source_date": time.strftime("%Y-%m-%d"),
                                            "relevance_note": "No live source available in fallback mode.",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "video_format": "Mechanism-first demo walkthrough",
                            "platform_targets": ["meta_feed", "youtube_shorts"],
                            "why_fit": "Shows the how/why mechanism and lands proof clearly.",
                            "style_notes": "Tight problem-solution arc with visible transformation.",
                            "watchouts": ["Requires live-source validation before scale"],
                            "evidence": [
                                {
                                    "claim": "Mechanism-focused formats usually improve comprehension for skeptical buyers.",
                                    "confidence": 0.4,
                                    "citations": [
                                        {
                                            "source_url": "fallback://built-in-prior",
                                            "source_title": "Built-in knowledge fallback",
                                            "publisher": "Creative Engine",
                                            "source_date": time.strftime("%Y-%m-%d"),
                                            "relevance_note": "No live source available in fallback mode.",
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                }
            )

        return "# STRUCTURED_RESEARCH_JSON\n" + json.dumps(report, indent=2)

    def _format_structured_research(self, report: CreativeScoutReport) -> str:
        """Format structured Step 2 output for Step 3 ingestion."""
        return "# STRUCTURED_RESEARCH_JSON\n" + report.model_dump_json(indent=2)

    def _engine_cost_since(self, engine_start_cost: float) -> float:
        current = float(get_usage_summary().get("total_cost", 0.0))
        return max(0.0, current - float(engine_start_cost))

    def _remaining_engine_budget(self, engine_start_cost: float) -> float:
        return max(0.0, config.CREATIVE_ENGINE_MAX_COST_USD - self._engine_cost_since(engine_start_cost))

    def _assert_engine_budget(self, phase: str, engine_start_cost: float):
        spent = self._engine_cost_since(engine_start_cost)
        cap = config.CREATIVE_ENGINE_MAX_COST_USD
        if spent > cap:
            raise RuntimeError(
                f"Creative Engine budget exceeded during {phase}: ${spent:.4f} > ${cap:.2f}"
            )

    def _budget_limited_max_tokens(
        self,
        *,
        model: str,
        remaining_budget_usd: float,
        prompt_chars: int,
        configured_max: int,
    ) -> int:
        """Convert remaining budget into a conservative max_tokens cap."""
        if remaining_budget_usd <= 0:
            raise RuntimeError(
                f"Creative Engine budget exhausted before call (cap: ${config.CREATIVE_ENGINE_MAX_COST_USD:.2f})"
            )

        input_price, output_price = get_model_pricing(model)
        est_input_tokens = max(1, prompt_chars // 4)
        est_input_cost = (est_input_tokens * input_price) / 1_000_000
        remaining_for_output = remaining_budget_usd - est_input_cost
        if remaining_for_output <= 0:
            raise RuntimeError(
                f"Remaining budget (${remaining_budget_usd:.4f}) is too low for prompt/input token cost."
            )

        max_output_by_budget = int((remaining_for_output * 1_000_000) / max(output_price, 0.0001))
        return max(1, min(int(configured_max), max_output_by_budget))

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Not used directly — run() handles the 3-phase flow.
        Kept for compatibility with base class."""
        return self._build_step3_prompt(inputs, [], "No research available.")
