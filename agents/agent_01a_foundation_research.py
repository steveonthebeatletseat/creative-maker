"""Agent 1A: Foundation Research v2 — hybrid DAG + hard quality gates."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from pipeline.phase1_engine import run_phase1_engine
from schemas.foundation_research import FoundationResearchBriefV2


class Agent01AFoundationResearch(BaseAgent):
    name = "Foundation Research"
    slug = "foundation_research"
    description = (
        "Hybrid-DAG foundation research engine. Runs parallel collectors, "
        "synthesizes all 7 pillars, adjudicates consistency, and enforces "
        "hard quality gates."
    )

    @property
    def system_prompt(self) -> str:
        # Unused in v2 path (kept for BaseAgent contract).
        return "Phase 1 v2 is orchestrated in pipeline.phase1_engine"

    @property
    def output_schema(self) -> type[BaseModel]:
        return FoundationResearchBriefV2

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        # Unused in v2 path (kept for BaseAgent contract).
        return "Phase 1 v2 uses internal orchestration"

    def run(self, inputs: dict[str, Any]) -> BaseModel:
        result = run_phase1_engine(
            inputs=inputs,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            output_dir=self.output_dir,
        )
        self._save_output(result)
        return result
