"""LLM client — multi-provider support (OpenAI, Anthropic, Google).

Each agent can use a different provider + model. The config determines
which provider/model pair each agent gets.

Error handling:
  - 400-level errors (bad request, auth) are NOT retried — they won't fix themselves.
  - 429 (rate limit) and 5xx (server errors) ARE retried with exponential backoff.
  - All errors are extracted into clean, readable messages.
"""

from __future__ import annotations

import json
import logging
from typing import TypeVar

from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

import config

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------

class LLMError(Exception):
    """Clean error from an LLM call with a human-readable message."""

    def __init__(self, message: str, provider: str = "", model: str = "", cause: Exception | None = None):
        self.provider = provider
        self.model = model
        self.cause = cause
        super().__init__(message)


def _is_retryable(exc: BaseException) -> bool:
    """Return True if the error is transient and worth retrying.

    We retry on:
      - Rate limits (429)
      - Server errors (500, 502, 503, 529)
      - Connection / timeout errors
    We do NOT retry on:
      - 400 Bad Request (invalid params, won't fix itself)
      - 401/403 Auth errors (key is wrong)
      - 404 (model doesn't exist)
      - Pydantic validation errors (need different approach)
    """
    # OpenAI errors
    try:
        from openai import (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        )
        if isinstance(exc, (RateLimitError, InternalServerError, APIConnectionError, APITimeoutError)):
            return True
    except ImportError:
        pass

    # Anthropic errors
    try:
        from anthropic import (
            APIConnectionError as AnthropicConnError,
            APITimeoutError as AnthropicTimeout,
            InternalServerError as AnthropicInternal,
            RateLimitError as AnthropicRateLimit,
        )
        if isinstance(exc, (AnthropicRateLimit, AnthropicInternal, AnthropicConnError, AnthropicTimeout)):
            return True
    except ImportError:
        pass

    # Generic connection / timeout
    import socket
    if isinstance(exc, (ConnectionError, TimeoutError, socket.timeout)):
        return True

    return False


def _extract_error_message(exc: Exception, provider: str, model: str) -> str:
    """Pull out a clean, human-readable error message from an API exception."""

    msg = str(exc)

    # OpenAI — extract the message from the JSON body
    try:
        from openai import BadRequestError, AuthenticationError, NotFoundError, PermissionDeniedError
        if isinstance(exc, BadRequestError):
            body = getattr(exc, "body", None)
            if isinstance(body, dict):
                inner = body.get("error", {})
                msg = inner.get("message", msg)
            return f"[{provider}/{model}] Bad request: {msg}"
        if isinstance(exc, AuthenticationError):
            return f"[{provider}] Authentication failed — check your OPENAI_API_KEY."
        if isinstance(exc, NotFoundError):
            return f"[{provider}] Model '{model}' not found. Check the model name in config.py or .env."
        if isinstance(exc, PermissionDeniedError):
            return f"[{provider}] Permission denied — your API key may not have access to '{model}'."
    except ImportError:
        pass

    # Anthropic
    try:
        from anthropic import BadRequestError as AnthropicBadReq, AuthenticationError as AnthropicAuth, NotFoundError as AnthropicNotFound
        if isinstance(exc, AnthropicBadReq):
            return f"[{provider}/{model}] Bad request: {msg}"
        if isinstance(exc, AnthropicAuth):
            return f"[{provider}] Authentication failed — check your ANTHROPIC_API_KEY."
        if isinstance(exc, AnthropicNotFound):
            return f"[{provider}] Model '{model}' not found."
    except ImportError:
        pass

    # Pydantic validation
    from pydantic import ValidationError
    if isinstance(exc, ValidationError):
        n_errors = exc.error_count()
        return f"[{provider}/{model}] Response JSON didn't match the expected schema ({n_errors} validation error{'s' if n_errors != 1 else ''}). The model may need a different prompt or more tokens."

    # Generic fallback — truncate very long messages
    if len(msg) > 300:
        msg = msg[:300] + "..."
    return f"[{provider}/{model}] {msg}"


# ---------------------------------------------------------------------------
# Provider clients (lazy-init singletons)
# ---------------------------------------------------------------------------

_openai_client = None
_anthropic_client = None
_google_client = None


def _get_openai():
    global _openai_client
    if _openai_client is None:
        if not config.OPENAI_API_KEY:
            raise LLMError(
                "OPENAI_API_KEY is not set. Add it to your .env file.",
                provider="openai",
            )
        from openai import OpenAI
        _openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
    return _openai_client


def _get_anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        if not config.ANTHROPIC_API_KEY:
            raise LLMError(
                "ANTHROPIC_API_KEY is not set. Add it to your .env file.",
                provider="anthropic",
            )
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _anthropic_client


def _get_google():
    global _google_client
    if _google_client is None:
        if not config.GOOGLE_API_KEY:
            raise LLMError(
                "GOOGLE_API_KEY is not set. Add it to your .env file.",
                provider="google",
            )
        from google import genai
        _google_client = genai.Client(api_key=config.GOOGLE_API_KEY)
    return _google_client


# ---------------------------------------------------------------------------
# Provider-specific call implementations
# ---------------------------------------------------------------------------

# Models that require max_completion_tokens instead of the legacy max_tokens.
# Newer OpenAI models (gpt-4o, gpt-4.1, gpt-5, etc.) all use the new param.
_OPENAI_NEW_TOKEN_PARAM_PREFIXES = (
    "gpt-4o", "gpt-4.1", "gpt-4.5", "gpt-5", "o1", "o3", "o4",
)


def _call_openai(
    system_prompt: str,
    user_prompt: str,
    model: str,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> str:
    client = _get_openai()

    # Determine correct token parameter name for this model
    use_new_param = any(model.startswith(p) for p in _OPENAI_NEW_TOKEN_PARAM_PREFIXES)

    kwargs: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
    }

    if use_new_param:
        kwargs["max_completion_tokens"] = max_tokens
    else:
        kwargs["max_tokens"] = max_tokens

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
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def call_llm(
    system_prompt: str,
    user_prompt: str,
    provider: str = "openai",
    model: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 16_000,
) -> str:
    """Call an LLM and return raw text. Provider-agnostic.

    Retries on transient errors (rate limits, server errors).
    Raises LLMError immediately for bad requests or auth errors.
    """
    model = model or config.DEFAULT_MODEL
    call_fn = _PROVIDERS.get(provider)
    if not call_fn:
        raise LLMError(
            f"Unknown provider: '{provider}'. Available: {list(_PROVIDERS.keys())}",
            provider=provider,
            model=model,
        )

    logger.info("LLM call: provider=%s, model=%s, temp=%.1f", provider, model, temperature)
    try:
        return call_fn(system_prompt, user_prompt, model, temperature, max_tokens, json_mode=False)
    except LLMError:
        raise
    except Exception as exc:
        clean_msg = _extract_error_message(exc, provider, model)
        logger.error("LLM call failed: %s", clean_msg)
        if _is_retryable(exc):
            raise  # let tenacity retry
        raise LLMError(clean_msg, provider=provider, model=model, cause=exc) from exc


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
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

    Retries on transient errors (rate limits, server errors).
    Raises LLMError immediately for bad requests or auth errors.
    """
    model = model or config.DEFAULT_MODEL
    call_fn = _PROVIDERS.get(provider)
    if not call_fn:
        raise LLMError(
            f"Unknown provider: '{provider}'. Available: {list(_PROVIDERS.keys())}",
            provider=provider,
            model=model,
        )

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

    try:
        raw = call_fn(
            system_prompt + schema_instruction,
            user_prompt,
            model,
            temperature,
            max_tokens,
            json_mode=True,
        )
    except LLMError:
        raise
    except Exception as exc:
        clean_msg = _extract_error_message(exc, provider, model)
        logger.error("LLM structured call failed: %s", clean_msg)
        if _is_retryable(exc):
            raise  # let tenacity retry
        raise LLMError(clean_msg, provider=provider, model=model, cause=exc) from exc

    # Strip markdown fences if present (Anthropic/Google sometimes add them)
    raw = raw.strip()
    if raw.startswith("```"):
        first_newline = raw.index("\n")
        raw = raw[first_newline + 1:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()

    # Parse and validate
    try:
        parsed = response_model.model_validate_json(raw)
    except Exception as exc:
        clean_msg = _extract_error_message(exc, provider, model)
        logger.error("Response parsing failed: %s", clean_msg)
        # Log a snippet of the raw response for debugging
        snippet = raw[:500] if raw else "(empty response)"
        logger.debug("Raw response snippet: %s", snippet)
        raise LLMError(clean_msg, provider=provider, model=model, cause=exc) from exc

    return parsed
