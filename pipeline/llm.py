"""LLM client — multi-provider support (OpenAI, Anthropic, Google).

Each agent can use a different provider + model. The config determines
which provider/model pair each agent gets.
"""

from __future__ import annotations

import json
import logging
from typing import TypeVar

from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

import config

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# ---------------------------------------------------------------------------
# Provider clients (lazy-init singletons)
# ---------------------------------------------------------------------------

_openai_client = None
_anthropic_client = None
_google_client = None


def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
    return _openai_client


def _get_anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _anthropic_client


def _get_google():
    global _google_client
    if _google_client is None:
        from google import genai
        _google_client = genai.Client(api_key=config.GOOGLE_API_KEY)
    return _google_client


# ---------------------------------------------------------------------------
# Provider-specific call implementations
# ---------------------------------------------------------------------------

def _call_openai(
    system_prompt: str,
    user_prompt: str,
    model: str,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> str:
    client = _get_openai()
    kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)
    content = response.choices[0].message.content or ""
    logger.info("OpenAI [%s]: %d chars, usage=%s", model, len(content), response.usage)
    return content


def _call_anthropic(
    system_prompt: str,
    user_prompt: str,
    model: str,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> str:
    client = _get_anthropic()

    # If json_mode, append instruction to system prompt
    effective_system = system_prompt
    if json_mode:
        effective_system += "\n\nRespond ONLY with a valid JSON object. No markdown fences, no explanation."

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=effective_system,
        messages=[{"role": "user", "content": user_prompt}],
    )
    content = response.content[0].text
    logger.info(
        "Anthropic [%s]: %d chars, in=%d out=%d",
        model, len(content), response.usage.input_tokens, response.usage.output_tokens,
    )
    return content


def _call_google(
    system_prompt: str,
    user_prompt: str,
    model: str,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> str:
    from google.genai import types

    client = _get_google()

    gen_config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=temperature,
        max_output_tokens=max_tokens,
    )
    if json_mode:
        gen_config.response_mime_type = "application/json"

    response = client.models.generate_content(
        model=model,
        contents=user_prompt,
        config=gen_config,
    )
    content = response.text or ""
    logger.info("Google [%s]: %d chars", model, len(content))
    return content


# Provider dispatch
_PROVIDERS = {
    "openai": _call_openai,
    "anthropic": _call_anthropic,
    "google": _call_google,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def call_llm(
    system_prompt: str,
    user_prompt: str,
    provider: str = "openai",
    model: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 16_000,
) -> str:
    """Call an LLM and return raw text. Provider-agnostic."""
    model = model or config.DEFAULT_MODEL
    call_fn = _PROVIDERS.get(provider)
    if not call_fn:
        raise ValueError(f"Unknown provider: {provider}. Available: {list(_PROVIDERS.keys())}")

    logger.info("LLM call: provider=%s, model=%s, temp=%.1f", provider, model, temperature)
    return call_fn(system_prompt, user_prompt, model, temperature, max_tokens, json_mode=False)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def call_llm_structured(
    system_prompt: str,
    user_prompt: str,
    response_model: type[T],
    provider: str = "openai",
    model: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 16_000,
) -> T:
    """Call an LLM and parse into a Pydantic model. Provider-agnostic.

    Injects the JSON schema into the system prompt so every provider
    knows the exact structure required.
    """
    model = model or config.DEFAULT_MODEL
    call_fn = _PROVIDERS.get(provider)
    if not call_fn:
        raise ValueError(f"Unknown provider: {provider}. Available: {list(_PROVIDERS.keys())}")

    # Inject schema into system prompt
    schema = response_model.model_json_schema()
    schema_instruction = (
        "\n\nYou MUST respond with valid JSON that conforms to this schema:\n"
        f"```json\n{json.dumps(schema, indent=2)}\n```\n"
        "Respond ONLY with the JSON object. No markdown fences, no explanation."
    )

    logger.info(
        "LLM structured call: provider=%s, model=%s, schema=%s",
        provider, model, response_model.__name__,
    )

    raw = call_fn(
        system_prompt + schema_instruction,
        user_prompt,
        model,
        temperature,
        max_tokens,
        json_mode=True,
    )

    # Strip markdown fences if present (Anthropic/Google sometimes add them)
    raw = raw.strip()
    if raw.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = raw.index("\n")
        raw = raw[first_newline + 1:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()

    # Parse and validate
    parsed = response_model.model_validate_json(raw)
    return parsed
