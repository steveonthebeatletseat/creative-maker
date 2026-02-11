"""Base agent class — all 16 agents inherit from this.

Each agent auto-loads its provider/model/temperature/max_tokens from
config.AGENT_LLM_CONFIG, so you can assign different LLMs per agent.
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

import config
from pipeline.llm import call_llm, call_llm_structured

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Base class for all pipeline agents.

    Each agent must define:
      - name: human-readable identifier
      - slug: file-safe identifier (must match a key in AGENT_LLM_CONFIG)
      - system_prompt: the full system prompt for the LLM
      - output_schema: Pydantic model for structured output
      - build_user_prompt(): constructs the user message from inputs

    LLM config (provider, model, temperature, max_tokens) is auto-loaded
    from config.py based on the agent's slug. Override in constructor if needed.
    """

    name: str = "BaseAgent"
    slug: str = "base"
    description: str = ""

    def __init__(
        self,
        provider: str | None = None,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ):
        # Load per-agent config, then allow constructor overrides
        llm_conf = config.get_agent_llm_config(self.slug)
        self.provider = provider or llm_conf["provider"]
        self.model = model or llm_conf["model"]
        self.temperature = temperature if temperature is not None else llm_conf["temperature"]
        self.max_tokens = max_tokens if max_tokens is not None else llm_conf["max_tokens"]
        self.logger = logging.getLogger(f"agent.{self.slug}")

        self.logger.debug(
            "Config: provider=%s, model=%s, temp=%.2f, max_tokens=%d",
            self.provider, self.model, self.temperature, self.max_tokens,
        )

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """Return the full system prompt for this agent."""
        ...

    @property
    @abstractmethod
    def output_schema(self) -> type[BaseModel]:
        """Return the Pydantic model class for structured output."""
        ...

    @abstractmethod
    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build the user prompt from pipeline inputs."""
        ...

    def run(self, inputs: dict[str, Any]) -> BaseModel:
        """Execute this agent: build prompt → call LLM → parse → save → return."""
        self.logger.info(
            "=== %s starting [%s/%s] ===",
            self.name, self.provider, self.model,
        )
        start = time.time()

        user_prompt = self.build_user_prompt(inputs)
        self.logger.info("User prompt: %d chars", len(user_prompt))

        result = call_llm_structured(
            system_prompt=self.system_prompt,
            user_prompt=user_prompt,
            response_model=self.output_schema,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        elapsed = time.time() - start
        self.logger.info("=== %s finished in %.1fs ===", self.name, elapsed)

        self._save_output(result)
        return result

    def run_text(self, inputs: dict[str, Any]) -> str:
        """Execute and return raw text (for agents that don't need structured output)."""
        self.logger.info(
            "=== %s starting (text) [%s/%s] ===",
            self.name, self.provider, self.model,
        )
        start = time.time()

        user_prompt = self.build_user_prompt(inputs)
        result = call_llm(
            system_prompt=self.system_prompt,
            user_prompt=user_prompt,
            provider=self.provider,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        elapsed = time.time() - start
        self.logger.info("=== %s finished in %.1fs ===", self.name, elapsed)
        return result

    def _save_output(self, result: BaseModel) -> Path:
        """Save structured output as JSON to the outputs directory."""
        output_path = config.OUTPUT_DIR / f"{self.slug}_output.json"
        output_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        self.logger.info("Output saved: %s", output_path)
        return output_path

    def load_previous_output(self) -> dict[str, Any] | None:
        """Load this agent's most recent output from disk (if any)."""
        output_path = config.OUTPUT_DIR / f"{self.slug}_output.json"
        if output_path.exists():
            return json.loads(output_path.read_text(encoding="utf-8"))
        return None
