"""LLM client — multi-provider support (OpenAI, Anthropic, Google).

Each agent can use a different provider + model. The config determines
which provider/model pair each agent gets.

Includes built-in cost tracking: every LLM call records token usage and
calculates cost based on per-model pricing. Use reset_usage(), get_usage_log(),
and get_usage_summary() to access the accumulated data.

Error handling:
  - 400-level errors (bad request, auth) are NOT retried — they won't fix themselves.
  - 429 (rate limit) and 5xx (server errors) ARE retried with exponential backoff.
  - All errors are extracted into clean, readable messages.
"""

from __future__ import annotations

import json
import logging
import threading
import time as _time
from typing import Any, TypeVar

from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cost tracking
# ---------------------------------------------------------------------------

# Pricing per 1M tokens: { model_prefix: (input_$/1M, output_$/1M) }
# Models are matched longest-prefix-first, so "gpt-5.2-mini" matches before "gpt-5.2".
# Update these when pricing changes — they're used purely for cost estimation.
MODEL_PRICING: dict[str, tuple[float, float]] = {
    # OpenAI
    "gpt-5.2-mini":     (0.30,   1.25),
    "gpt-5.2":          (2.50,  10.00),
    "gpt-4o-mini":      (0.15,   0.60),
    "gpt-4o":           (2.50,  10.00),
    "gpt-4.1-mini":     (0.40,   1.60),
    "gpt-4.1-nano":     (0.10,   0.40),
    "gpt-4.1":          (2.00,   8.00),
    "gpt-4.5":          (7.50,  30.00),
    "o4-mini":          (1.10,   4.40),
    "o3-mini":          (1.10,   4.40),
    "o3":               (2.00,   8.00),
    # Anthropic
    "claude-opus-4":    (15.00,  75.00),
    "claude-sonnet-4":  (3.00,   15.00),
    "claude-3.5-sonnet":(3.00,   15.00),
    "claude-3-opus":    (15.00,  75.00),
    "claude-3-haiku":   (0.25,    1.25),
    # Google
    "gemini-3.0-pro":   (1.25,  10.00),
    "gemini-2.5-pro":   (1.25,  10.00),
    "gemini-2.5-flash": (0.15,   0.60),
    "gemini-2.0-flash": (0.10,   0.40),
    "gemini-1.5-pro":   (1.25,   5.00),
    "gemini-1.5-flash": (0.075,  0.30),
}

# Fallback pricing if a model isn't in the table (conservative estimate)
_FALLBACK_PRICING = (2.50, 10.00)

_usage_lock = threading.Lock()
_usage_log: list[dict[str, Any]] = []


def _get_pricing(model: str) -> tuple[float, float]:
    """Find pricing for a model by longest-prefix match."""
    best_match = ""
    for prefix in MODEL_PRICING:
        if model.startswith(prefix) and len(prefix) > len(best_match):
            best_match = prefix
    if best_match:
        return MODEL_PRICING[best_match]
    logger.warning("No pricing found for model '%s' — using fallback $%.2f/$%.2f per 1M", model, *_FALLBACK_PRICING)
    return _FALLBACK_PRICING


def _record_usage(provider: str, model: str, input_tokens: int, output_tokens: int):
    """Record a single LLM call's token usage and cost."""
    in_price, out_price = _get_pricing(model)
    cost = (input_tokens * in_price + output_tokens * out_price) / 1_000_000
    entry = {
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost": cost,
        "timestamp": _time.time(),
    }
    with _usage_lock:
        _usage_log.append(entry)
    logger.info(
        "Token usage: %s/%s — in=%d out=%d cost=$%.4f",
        provider, model, input_tokens, output_tokens, cost,
    )


def reset_usage():
    """Clear all accumulated usage data (call at pipeline start)."""
    with _usage_lock:
        _usage_log.clear()


def get_usage_log() -> list[dict[str, Any]]:
    """Return a copy of the full usage log."""
    with _usage_lock:
        return list(_usage_log)


def get_usage_summary() -> dict[str, Any]:
    """Return aggregated cost and token totals."""
    with _usage_lock:
        entries = list(_usage_log)
    total_input = sum(e["input_tokens"] for e in entries)
    total_output = sum(e["output_tokens"] for e in entries)
    total_cost = sum(e["cost"] for e in entries)
    return {
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_tokens": total_input + total_output,
        "total_cost": round(total_cost, 4),
        "calls": len(entries),
    }

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

    # Record token usage
    usage = response.usage
    if usage:
        _record_usage("openai", model, usage.prompt_tokens or 0, usage.completion_tokens or 0)
    logger.info("OpenAI [%s]: %d chars, usage=%s", model, len(content), usage)
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

    # Use streaming to avoid 10-minute timeout on long requests
    # (Anthropic requires streaming for operations > 10 min)
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=effective_system,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        response = stream.get_final_message()

    content = response.content[0].text

    # Record token usage
    in_tok = response.usage.input_tokens or 0
    out_tok = response.usage.output_tokens or 0
    _record_usage("anthropic", model, in_tok, out_tok)
    logger.info(
        "Anthropic [%s]: %d chars, in=%d out=%d",
        model, len(content), in_tok, out_tok,
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

    # Record token usage
    meta = getattr(response, "usage_metadata", None)
    if meta:
        in_tok = getattr(meta, "prompt_token_count", 0) or 0
        out_tok = getattr(meta, "candidates_token_count", 0) or 0
        _record_usage("google", model, in_tok, out_tok)
    logger.info("Google [%s]: %d chars, usage_meta=%s", model, len(content), meta)
    return content


# ---------------------------------------------------------------------------
# Gemini Deep Research (Interactions API — async, polling-based)
# ---------------------------------------------------------------------------

DEEP_RESEARCH_AGENT = "deep-research-pro-preview-12-2025"
DEEP_RESEARCH_POLL_INTERVAL = 10  # seconds between polls
DEEP_RESEARCH_MAX_WAIT = 1200  # 20 minutes max


def call_deep_research(prompt: str) -> str:
    """Run a Gemini Deep Research task and return the text report.

    Uses the Interactions REST API directly (not the Python SDK, which
    may not support it on older Python versions). The agent autonomously
    browses the web, reads sources, and produces a detailed cited report.
    This is a blocking call that polls until completion (typically 2-10 min).

    Returns the final text output from the research agent.
    Raises LLMError on failure or timeout.
    """
    import time
    import requests as req_lib

    if not config.GOOGLE_API_KEY:
        raise LLMError(
            "GOOGLE_API_KEY is not set. Required for Deep Research.",
            provider="google",
            model=DEEP_RESEARCH_AGENT,
        )

    api_key = config.GOOGLE_API_KEY
    base_url = "https://generativelanguage.googleapis.com/v1beta"

    logger.info("Deep Research: starting task (%d char prompt)", len(prompt))

    # Step 1: Start the research task
    try:
        resp = req_lib.post(
            f"{base_url}/interactions",
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            json={
                "input": prompt,
                "agent": DEEP_RESEARCH_AGENT,
                "background": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise LLMError(
            f"[google/deep-research] Failed to start: {exc}",
            provider="google",
            model=DEEP_RESEARCH_AGENT,
            cause=exc,
        ) from exc

    interaction_id = data.get("id")
    if not interaction_id:
        raise LLMError(
            f"[google/deep-research] No interaction ID returned: {data}",
            provider="google",
            model=DEEP_RESEARCH_AGENT,
        )

    logger.info("Deep Research: started interaction %s", interaction_id)

    # Step 2: Poll for completion
    elapsed = 0
    while elapsed < DEEP_RESEARCH_MAX_WAIT:
        time.sleep(DEEP_RESEARCH_POLL_INTERVAL)
        elapsed += DEEP_RESEARCH_POLL_INTERVAL

        try:
            poll_resp = req_lib.get(
                f"{base_url}/interactions/{interaction_id}",
                headers={"x-goog-api-key": api_key},
                timeout=15,
            )
            poll_resp.raise_for_status()
            poll_data = poll_resp.json()
        except Exception as exc:
            logger.warning("Deep Research: poll error (will retry): %s", exc)
            continue

        status = poll_data.get("status", "unknown")
        logger.info(
            "Deep Research: status=%s (elapsed %ds)", status, elapsed
        )

        if status == "completed":
            # Extract the final text output from outputs array
            outputs = poll_data.get("outputs", [])
            if outputs:
                # Get the last output's text
                last_output = outputs[-1]
                text = last_output.get("text", "")
                if not text:
                    # Try nested content structure
                    parts = last_output.get("parts", [])
                    for part in parts:
                        if isinstance(part, dict) and "text" in part:
                            text += part["text"]
                if text:
                    logger.info(
                        "Deep Research: completed in %ds, %d chars output",
                        elapsed, len(text),
                    )
                    return text

            raise LLMError(
                "[google/deep-research] Completed but no output text found",
                provider="google",
                model=DEEP_RESEARCH_AGENT,
            )

        if status == "failed":
            err = poll_data.get("error", "Unknown error")
            raise LLMError(
                f"[google/deep-research] Research failed: {err}",
                provider="google",
                model=DEEP_RESEARCH_AGENT,
            )

    raise LLMError(
        f"[google/deep-research] Timed out after {DEEP_RESEARCH_MAX_WAIT}s",
        provider="google",
        model=DEEP_RESEARCH_AGENT,
    )


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

    # Parse and validate — with lenient fallback for common LLM output quirks
    try:
        parsed = response_model.model_validate_json(raw)
    except Exception as exc:
        # Log the actual validation error details (not just count)
        from pydantic import ValidationError
        if isinstance(exc, ValidationError):
            for err in exc.errors():
                logger.error(
                    "Schema validation error: field=%s type=%s msg=%s",
                    " → ".join(str(loc) for loc in err["loc"]),
                    err["type"],
                    err["msg"],
                )

        # Attempt lenient re-parse: load as dict first, coerce known issues
        logger.info("Attempting lenient re-parse with coercion...")
        try:
            data = _safe_json_loads(raw)
            _coerce_llm_output(data)
            parsed = response_model.model_validate(data)
            logger.info("Lenient re-parse succeeded!")
            return parsed
        except Exception as exc2:
            # Log the second failure details too
            if isinstance(exc2, ValidationError):
                for err in exc2.errors():
                    logger.error(
                        "Lenient re-parse also failed: field=%s type=%s msg=%s",
                        " → ".join(str(loc) for loc in err["loc"]),
                        err["type"],
                        err["msg"],
                    )

            clean_msg = _extract_error_message(exc, provider, model)
            logger.error("Response parsing failed: %s", clean_msg)
            snippet = raw[:500] if raw else "(empty response)"
            logger.debug("Raw response snippet: %s", snippet)
            raise LLMError(clean_msg, provider=provider, model=model, cause=exc) from exc

    return parsed


def _safe_json_loads(raw: str) -> dict:
    """Parse JSON with fallback repair for common LLM quirks.

    Handles: unquoted numeric keys, trailing commas, markdown fences.
    """
    import re

    # Strip markdown fences if present
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        # Remove opening fence (with optional language tag)
        cleaned = re.sub(r"^```\w*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned)

    # First try: standard parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Fix 1: Quote unquoted numeric keys (e.g. {1: "value"} -> {"1": "value"})
    fixed = re.sub(r'(?<=[\{,])\s*(\d+)\s*:', r' "\1":', cleaned)

    # Fix 2: Remove trailing commas before } or ]
    fixed = re.sub(r',\s*([}\]])', r'\1', fixed)

    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Fix 3: Try to find the JSON object in the string (strip preamble/postamble)
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        try:
            candidate = match.group(0)
            candidate = re.sub(r'(?<=[\{,])\s*(\d+)\s*:', r' "\1":', candidate)
            candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # Give up — raise the original error
    return json.loads(cleaned)


def _coerce_llm_output(obj):
    """Recursively fix common LLM output quirks in-place.

    - Lowercases string values that look like enum members
    - Converts string-encoded ints to ints (for int enums like SophisticationStage)
    - Normalises dict keys that should be enum values (e.g. awareness_distribution)
    - Auto-computes swing_idea_count from actual ideas if missing or wrong
    - Trims/pads idea lists to exactly 10 per funnel stage
    """
    if isinstance(obj, dict):
        # Fix keys — some dicts expect enum-value keys (e.g. awareness_distribution)
        keys_to_fix = []
        for k in list(obj.keys()):
            lower_k = k.lower().replace(" ", "_").replace("-", "_") if isinstance(k, str) else k
            if lower_k != k:
                keys_to_fix.append((k, lower_k))
            # Recurse into values
            _coerce_llm_output(obj[k])
        for old_k, new_k in keys_to_fix:
            if new_k not in obj:
                obj[new_k] = obj.pop(old_k)
            else:
                # Both exist — keep the lowercase one, remove old
                del obj[old_k]

        # Fix specific known fields
        if "stage" in obj and isinstance(obj["stage"], str):
            try:
                obj["stage"] = int(obj["stage"])
            except (ValueError, TypeError):
                pass

        # Auto-compute swing_idea_count from actual ideas (LLMs often miscount)
        if "ideas" in obj and isinstance(obj["ideas"], list) and "swing_idea_count" in obj:
            actual_swing = sum(
                1 for idea in obj["ideas"]
                if isinstance(idea, dict) and idea.get("is_swing_idea")
            )
            if actual_swing != obj["swing_idea_count"]:
                logger.info(
                    "Fixing swing_idea_count: model said %s, actual is %d",
                    obj["swing_idea_count"], actual_swing,
                )
                obj["swing_idea_count"] = actual_swing

    elif isinstance(obj, list):
        for item in obj:
            _coerce_llm_output(item)
