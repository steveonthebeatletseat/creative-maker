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
import os
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
# Streaming progress callback — set by server to broadcast to frontend
# ---------------------------------------------------------------------------
_stream_progress_callback = None


def set_stream_progress_callback(cb):
    """Set a callback(message: str) that's called during LLM streaming."""
    global _stream_progress_callback
    _stream_progress_callback = cb


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


def get_model_pricing(model: str) -> tuple[float, float]:
    """Public helper for model pricing lookup ($/1M input, $/1M output)."""
    return _get_pricing(model)


def record_external_usage(
    provider: str,
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost: float | None = None,
    metadata: dict[str, Any] | None = None,
):
    """Record externally computed usage (e.g., Claude Agent SDK call).

    If cost is None, cost is estimated from token pricing.
    """
    if cost is None:
        in_price, out_price = _get_pricing(model)
        cost = (input_tokens * in_price + output_tokens * out_price) / 1_000_000

    entry = {
        "provider": provider,
        "model": model,
        "input_tokens": int(input_tokens or 0),
        "output_tokens": int(output_tokens or 0),
        "cost": float(cost),
        "timestamp": _time.time(),
    }
    if metadata:
        entry["metadata"] = metadata

    with _usage_lock:
        _usage_log.append(entry)

    logger.info(
        "External usage: %s/%s — in=%d out=%d cost=$%.4f metadata=%s",
        provider,
        model,
        entry["input_tokens"],
        entry["output_tokens"],
        entry["cost"],
        bool(metadata),
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


# Gemini models that require non-zero thinking budget.
_GOOGLE_THINKING_REQUIRED_PREFIXES = (
    "gemini-2.5-pro",
    "gemini-3.0-pro",
)

# Small default so reasoning is enabled but doesn't consume too much output headroom.
_GOOGLE_DEFAULT_THINKING_BUDGET = int(os.getenv("GOOGLE_THINKING_BUDGET", "2048"))


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


def _google_requires_thinking(model: str) -> bool:
    m = (model or "").lower().strip()
    return any(m.startswith(prefix) for prefix in _GOOGLE_THINKING_REQUIRED_PREFIXES)


def _google_thinking_budget(model: str, max_tokens: int) -> int:
    # Keep budget positive and comfortably below output cap.
    base = _GOOGLE_DEFAULT_THINKING_BUDGET
    if base <= 0:
        base = 1024
    upper_bound = max(1, int(max_tokens) - 1)
    return min(base, upper_bound)


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

    # Use streaming so we can log progress
    import time as _time
    _stream_start = _time.time()
    _last_progress = _stream_start
    _chunk_count = 0
    _chunks = []

    stream = client.chat.completions.create(**kwargs, stream=True, stream_options={"include_usage": True})
    _usage = None
    for chunk in stream:
        if chunk.usage:
            _usage = chunk.usage
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            _chunks.append(chunk.choices[0].delta.content)
            _chunk_count += 1
            now = _time.time()
            if now - _last_progress >= 15:
                elapsed = round(now - _stream_start)
                msg = f"Streaming... ~{_chunk_count} chunks, {elapsed}s elapsed"
                logger.info("OpenAI [%s]: %s", model, msg)
                if _stream_progress_callback:
                    try:
                        _stream_progress_callback(msg)
                    except Exception:
                        pass
                _last_progress = now

    content = "".join(_chunks)
    elapsed_total = round(_time.time() - _stream_start, 1)
    logger.info(
        "OpenAI [%s]: stream complete — %d chars in %.1fs",
        model, len(content), elapsed_total,
    )

    # Record token usage
    if _usage:
        _record_usage("openai", model, _usage.prompt_tokens or 0, _usage.completion_tokens or 0)
    logger.info("OpenAI [%s]: %d chars, usage=%s", model, len(content), _usage)
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
        effective_system += (
            "\n\nIMPORTANT: Respond ONLY with a valid JSON object. No markdown fences, no explanation, no preamble."
            " You MUST populate ALL required arrays with actual data — NEVER return empty arrays."
            " Start your response with the opening brace '{' of the JSON object immediately."
        )

    messages = [{"role": "user", "content": user_prompt}]

    # Use streaming to avoid 10-minute timeout on long requests
    # (Anthropic requires streaming for operations > 10 min)
    import time as _time
    _stream_start = _time.time()
    _last_progress = _stream_start
    _token_count = 0

    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=effective_system,
        messages=messages,
    ) as stream:
        for text in stream.text_stream:
            _token_count += 1
            now = _time.time()
            # Log progress every 15 seconds so user knows it's alive
            if now - _last_progress >= 15:
                elapsed = round(now - _stream_start)
                msg = f"Streaming... ~{_token_count} chunks, {elapsed}s elapsed"
                logger.info("Anthropic [%s]: %s", model, msg)
                if _stream_progress_callback:
                    try:
                        _stream_progress_callback(msg)
                    except Exception:
                        pass
                _last_progress = now

        response = stream.get_final_message()

    elapsed_total = round(_time.time() - _stream_start, 1)
    content = response.content[0].text

    logger.info(
        "Anthropic [%s]: stream complete — %d chars in %.1fs",
        model, len(content), elapsed_total,
    )

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
    needs_thinking = _google_requires_thinking(model)

    def _build_config(force_thinking: bool = False, disable_json_mime: bool = False):
        cfg = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=temperature,
            max_output_tokens=max_tokens,
        )

        if json_mode and not disable_json_mime:
            cfg.response_mime_type = "application/json"

        if needs_thinking or force_thinking:
            # Some Gemini Pro models reject thinking_budget=0.
            cfg.thinking_config = types.ThinkingConfig(
                thinking_budget=_google_thinking_budget(model, max_tokens)
            )
        elif json_mode:
            # Keep non-thinking models deterministic in JSON mode.
            cfg.thinking_config = types.ThinkingConfig(thinking_budget=0)

        return cfg

    def _stream_once(cfg):
        import time as _time

        stream_start = _time.time()
        last_progress = stream_start
        chunk_count = 0
        chunks: list[str] = []
        last_chunk = None

        for last_chunk in client.models.generate_content_stream(
            model=model,
            contents=user_prompt,
            config=cfg,
        ):
            if last_chunk.text:
                chunks.append(last_chunk.text)
                chunk_count += 1
                now = _time.time()
                if now - last_progress >= 15:
                    elapsed = round(now - stream_start)
                    msg = f"Streaming... ~{chunk_count} chunks, {elapsed}s elapsed"
                    logger.info("Google [%s]: %s", model, msg)
                    if _stream_progress_callback:
                        try:
                            _stream_progress_callback(msg)
                        except Exception:
                            pass
                    last_progress = now

        content = "".join(chunks)
        elapsed_total = round(_time.time() - stream_start, 1)
        logger.info(
            "Google [%s]: stream complete — %d chars in %.1fs",
            model, len(content), elapsed_total,
        )
        usage_meta = getattr(last_chunk, "usage_metadata", None) if last_chunk is not None else None
        return content, usage_meta

    gen_config = _build_config()
    thinking_cfg = getattr(gen_config, "thinking_config", None)
    thinking_budget = getattr(thinking_cfg, "thinking_budget", None) if thinking_cfg else None
    logger.info(
        "Google [%s]: json_mode=%s thinking_budget=%s max_output_tokens=%d",
        model, json_mode, thinking_budget, max_tokens,
    )

    try:
        content, meta = _stream_once(gen_config)
    except Exception as exc:
        msg = str(exc)
        if needs_thinking and ("Budget 0 is invalid" in msg or "only works in thinking mode" in msg):
            retry_budget = _google_thinking_budget(model, max_tokens)
            logger.warning(
                "Google [%s]: thinking-mode error, retrying with forced budget=%d",
                model, retry_budget,
            )
            try:
                content, meta = _stream_once(_build_config(force_thinking=True))
            except Exception:
                if not json_mode:
                    raise
                # Last resort for SDK/API quirks: disable explicit JSON mime and rely on prompt schema.
                logger.warning(
                    "Google [%s]: retrying without response_mime_type due thinking/json compatibility issue",
                    model,
                )
                content, meta = _stream_once(_build_config(force_thinking=True, disable_json_mime=True))
        else:
            raise

    # Record token usage from the last chunk's usage_metadata
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


def call_deep_research(prompt: str, is_cancelled=None) -> str:
    """Run a Gemini Deep Research task and return the text report.

    Uses the Interactions REST API directly (not the Python SDK, which
    may not support it on older Python versions). The agent autonomously
    browses the web, reads sources, and produces a detailed cited report.
    This is a blocking call that polls until completion (typically 2-10 min).

    Returns the final text output from the research agent.
    Raises LLMError on failure, timeout, or cancellation.
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

    if callable(is_cancelled) and is_cancelled():
        raise LLMError(
            "[google/deep-research] Cancelled before start",
            provider="google",
            model=DEEP_RESEARCH_AGENT,
        )

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
        if callable(is_cancelled) and is_cancelled():
            raise LLMError(
                "[google/deep-research] Cancelled by user",
                provider="google",
                model=DEEP_RESEARCH_AGENT,
            )

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


# ---------------------------------------------------------------------------
# Claude Web Search (Anthropic Messages API + web_search tool)
# ---------------------------------------------------------------------------

CLAUDE_WEB_SEARCH_MODEL = config.CREATIVE_SCOUT_MODEL
CLAUDE_WEB_SEARCH_MAX_USES = config.CREATIVE_SCOUT_WEB_MAX_USES
CLAUDE_WEB_SEARCH_MAX_TOKENS = config.CREATIVE_SCOUT_WEB_MAX_TOKENS


def call_claude_web_search(
    system_prompt: str,
    user_prompt: str,
    model: str = CLAUDE_WEB_SEARCH_MODEL,
    max_uses: int = CLAUDE_WEB_SEARCH_MAX_USES,
    max_tokens: int = CLAUDE_WEB_SEARCH_MAX_TOKENS,
) -> str:
    """Run a Claude web-search-powered research task and return the text report.

    Uses Anthropic's built-in web_search_20250305 tool. Claude autonomously
    decides what to search for, reads results, and synthesises a report.
    Unlike Gemini Deep Research, Claude controls the search strategy — it
    can fire multiple targeted queries and iteratively refine.

    Returns the final text output (with citations inline).
    Raises LLMError on failure.
    """
    import time as _t

    client = _get_anthropic()

    logger.info(
        "Claude Web Search: starting [%s, max_uses=%d] (%d char prompt)",
        model, max_uses, len(user_prompt),
    )

    tools = [
        {
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": max_uses,
        }
    ]

    # We may need to handle pause_turn (long-running turns that get paused)
    messages = [{"role": "user", "content": user_prompt}]
    search_count = 0
    total_in_tokens = 0
    total_out_tokens = 0
    start = _t.time()

    # Loop to handle pause_turn continuations
    for iteration in range(5):  # safety limit on continuations
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=messages,
                tools=tools,
            )
        except Exception as exc:
            elapsed = round(_t.time() - start, 1)
            clean_msg = _extract_error_message(exc, "anthropic", model)
            logger.error("Claude Web Search failed after %.1fs: %s", elapsed, clean_msg)
            raise LLMError(clean_msg, provider="anthropic", model=model, cause=exc) from exc

        # Count searches from usage
        usage = response.usage
        total_in_tokens += getattr(usage, "input_tokens", 0) or 0
        total_out_tokens += getattr(usage, "output_tokens", 0) or 0

        server_tool_use = getattr(usage, "server_tool_use", None)
        if server_tool_use:
            search_count += getattr(server_tool_use, "web_search_requests", 0) or 0

        elapsed = round(_t.time() - start, 1)
        logger.info(
            "Claude Web Search: iteration %d — %d searches so far, %.1fs elapsed, stop=%s",
            iteration + 1, search_count, elapsed, response.stop_reason,
        )

        # If the turn is paused (long-running), continue it
        if response.stop_reason == "pause_turn":
            logger.info("Claude Web Search: pause_turn — continuing...")
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": [{"type": "text", "text": "Continue your research."}]})
            continue

        # Turn complete (end_turn or tool_use exhausted)
        break

    # Extract all text blocks from the final response
    text_parts = []
    for block in response.content:
        if hasattr(block, "text"):
            text_parts.append(block.text)

    report = "\n".join(text_parts)

    elapsed = round(_t.time() - start, 1)
    logger.info(
        "Claude Web Search: complete — %d searches, %d chars, %.1fs, in=%d out=%d",
        search_count, len(report), elapsed, total_in_tokens, total_out_tokens,
    )

    # Record token usage
    _record_usage("anthropic", model, total_in_tokens, total_out_tokens)

    # Record search costs separately (~$0.01 per search)
    if search_count > 0:
        search_cost = search_count * 0.01  # $10 per 1000 searches
        with _usage_lock:
            _usage_log.append({
                "provider": "anthropic",
                "model": f"{model}/web_search",
                "input_tokens": 0,
                "output_tokens": 0,
                "cost": search_cost,
                "timestamp": _t.time(),
            })
        logger.info(
            "Claude Web Search: %d searches × $0.01 = $%.2f search cost",
            search_count, search_cost,
        )

    if not report.strip():
        raise LLMError(
            "[anthropic/web-search] Claude returned an empty report",
            provider="anthropic",
            model=model,
        )

    return report


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
        "Respond ONLY with the JSON object. No markdown fences, no explanation.\n"
        "CRITICAL: All required arrays MUST contain actual populated items — NEVER return empty arrays."
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

            # Final salvage attempt: ask the model to repair malformed JSON.
            try:
                logger.info("Attempting LLM JSON repair pass...")
                parsed = _attempt_llm_json_repair(
                    call_fn=call_fn,
                    provider=provider,
                    model=model,
                    response_model=response_model,
                    raw=raw,
                    max_tokens=max_tokens,
                )
                logger.info("LLM JSON repair pass succeeded!")
                return parsed
            except Exception as exc3:
                logger.warning("LLM JSON repair pass failed: %s", exc3)

                clean_msg = _extract_error_message(exc, provider, model)
                logger.error("Response parsing failed: %s", clean_msg)
                snippet = raw[:500] if raw else "(empty response)"
                logger.debug("Raw response snippet: %s", snippet)
                raise LLMError(clean_msg, provider=provider, model=model, cause=exc) from exc

    return parsed


def _attempt_llm_json_repair(
    *,
    call_fn,
    provider: str,
    model: str,
    response_model: type[T],
    raw: str,
    max_tokens: int,
) -> T:
    """Ask the model to repair malformed JSON into valid schema-conforming JSON."""
    if not raw or len(raw) < 20:
        raise ValueError("No JSON payload available for repair")

    # Keep repair requests bounded so huge malformed payloads do not blow context.
    max_chars = 160_000
    if len(raw) > max_chars:
        raise ValueError(
            f"Repair payload too large ({len(raw)} chars) — skipping repair pass"
        )

    schema_json = json.dumps(response_model.model_json_schema(), indent=2)
    repair_system = (
        "You are a strict JSON repair engine.\n"
        "Fix malformed JSON so it is valid and conforms to the provided schema.\n"
        "Return ONLY a single JSON object and preserve original meaning.\n"
    )
    repair_user = (
        "Schema:\n"
        f"```json\n{schema_json}\n```\n\n"
        "Malformed JSON to repair:\n"
        f"```json\n{raw}\n```\n"
    )

    repair_max_tokens = min(max(4_000, max_tokens), 32_000)
    repaired_raw = call_fn(
        repair_system,
        repair_user,
        model,
        0.0,
        repair_max_tokens,
        json_mode=True,
    )

    repaired_raw = repaired_raw.strip()
    if repaired_raw.startswith("```"):
        first_newline = repaired_raw.index("\n")
        repaired_raw = repaired_raw[first_newline + 1:]
    if repaired_raw.endswith("```"):
        repaired_raw = repaired_raw[:-3]
    repaired_raw = repaired_raw.strip()

    repaired_data = _safe_json_loads(repaired_raw)
    _coerce_llm_output(repaired_data)
    return response_model.model_validate(repaired_data)


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
