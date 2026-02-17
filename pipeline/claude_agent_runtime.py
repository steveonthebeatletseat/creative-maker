"""Generic Claude Agent SDK structured runner.

Used by Phase 3 v2 for optional per-step Claude SDK execution.
Unlike the scout helper, this module is provider-agnostic in purpose and
defaults to no web tools.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import logging
import time
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

import config
from pipeline.llm import LLMError, get_model_pricing, record_external_usage

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def _strip_markdown_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1 :]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _coerce_result_payload(raw_result: Any) -> dict[str, Any]:
    if isinstance(raw_result, dict):
        return raw_result
    if isinstance(raw_result, BaseModel):
        return raw_result.model_dump()
    if hasattr(raw_result, "model_dump") and callable(raw_result.model_dump):
        return raw_result.model_dump()
    if raw_result is None:
        raise ValueError("SDK result payload is empty")
    if isinstance(raw_result, str):
        cleaned = _strip_markdown_fences(raw_result)
        if not cleaned:
            raise ValueError("SDK result payload is empty")
        data = json.loads(cleaned)
        if not isinstance(data, dict):
            raise ValueError("SDK JSON result must be an object")
        return data
    raise ValueError(f"Unsupported SDK result type: {type(raw_result).__name__}")


def _extract_usage_metrics(usage: Any) -> tuple[int, int]:
    if not usage:
        return 0, 0
    data = usage
    if hasattr(usage, "model_dump") and callable(usage.model_dump):
        data = usage.model_dump()
    elif not isinstance(usage, dict):
        try:
            data = dict(usage)
        except Exception:
            return 0, 0
    return int(data.get("input_tokens", 0) or 0), int(data.get("output_tokens", 0) or 0)


async def _run_query_async(
    *,
    system_prompt: str,
    user_prompt: str,
    response_model: type[T],
    model: str,
    allowed_tools: list[str] | None,
    max_turns: int,
    max_thinking_tokens: int,
    max_budget_usd: float | None,
    cwd: Path,
) -> T:
    try:
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient
    except ImportError as exc:
        raise LLMError(
            "claude-agent-sdk is not installed. Install dependencies with: pip install -r requirements.txt",
            provider="anthropic",
            model=model,
            cause=exc,
        ) from exc

    option_kwargs: dict[str, Any] = {
        "model": model,
        "system_prompt": system_prompt,
        "max_turns": max_turns,
        "max_thinking_tokens": max_thinking_tokens,
        "permission_mode": "bypassPermissions",
        "cwd": str(cwd),
        "output_format": {
            "type": "json_schema",
            "schema": response_model.model_json_schema(),
        },
    }
    if allowed_tools is not None:
        option_kwargs["allowed_tools"] = list(allowed_tools)
    if max_budget_usd is not None:
        option_kwargs["max_budget_usd"] = float(max_budget_usd)

    options = ClaudeAgentOptions(**option_kwargs)

    started_at = time.time()
    event_count = 0
    result_message = None

    logger.info(
        "Claude Agent SDK runtime: start model=%s turns=%d tools=%s",
        model,
        max_turns,
        allowed_tools if allowed_tools is not None else "default",
    )

    async def _heartbeat():
        while True:
            await asyncio.sleep(15)
            logger.info(
                "Claude Agent SDK runtime: still running... %ds elapsed events=%d",
                int(time.time() - started_at),
                event_count,
            )

    heartbeat_task = asyncio.create_task(_heartbeat())
    async with ClaudeSDKClient(options=options) as client:
        try:
            await client.query(user_prompt)
            async for message in client.receive_response():
                event_count += 1
                if message.__class__.__name__ == "ResultMessage":
                    result_message = message
                    break
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    if result_message is None:
        raise LLMError(
            "Claude Agent SDK returned no ResultMessage.",
            provider="anthropic",
            model=model,
        )

    subtype = getattr(result_message, "subtype", "") or ""
    if subtype != "success":
        raise LLMError(
            f"Claude Agent SDK failed with subtype='{subtype}'",
            provider="anthropic",
            model=model,
        )

    raw_result = getattr(result_message, "structured_output", None)
    if raw_result is None:
        raw_result = getattr(result_message, "result", None)
    if raw_result is None:
        raise LLMError(
            "Claude Agent SDK returned an empty result.",
            provider="anthropic",
            model=model,
        )

    try:
        payload = _coerce_result_payload(raw_result)
        parsed = response_model.model_validate(payload)
    except Exception as exc:
        raise LLMError(
            f"Claude Agent SDK output did not match {response_model.__name__}: {exc}",
            provider="anthropic",
            model=model,
            cause=exc,
        ) from exc

    usage = getattr(result_message, "usage", None)
    input_tokens, output_tokens = _extract_usage_metrics(usage)
    total_cost_usd = float(getattr(result_message, "total_cost_usd", 0.0) or 0.0)
    explicit_cost = total_cost_usd if total_cost_usd > 0 else None
    if explicit_cost is None and (input_tokens or output_tokens):
        in_price, out_price = get_model_pricing(model)
        explicit_cost = ((input_tokens * in_price) + (output_tokens * out_price)) / 1_000_000

    record_external_usage(
        provider="anthropic",
        model=f"{model}/claude-agent-sdk",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost=explicit_cost,
        metadata={
            "source": "phase3_v2_runtime",
            "duration_ms": int(getattr(result_message, "duration_ms", 0) or 0),
            "num_turns": int(getattr(result_message, "num_turns", 0) or 0),
        },
    )

    return parsed


def call_claude_agent_structured(
    *,
    system_prompt: str,
    user_prompt: str,
    response_model: type[T],
    model: str = config.ANTHROPIC_FRONTIER,
    allowed_tools: list[str] | None = None,
    max_turns: int = 6,
    max_thinking_tokens: int = 8_000,
    max_budget_usd: float | None = None,
    cwd: Path | None = None,
) -> T:
    """Run Claude Agent SDK and parse output into a typed Pydantic model.

    `allowed_tools=None` means SDK defaults. Pass `[]` for no tools (default for
    Phase 3 v2 generation).
    """
    coro = _run_query_async(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        response_model=response_model,
        model=model,
        allowed_tools=allowed_tools,
        max_turns=max_turns,
        max_thinking_tokens=max_thinking_tokens,
        max_budget_usd=max_budget_usd,
        cwd=cwd or config.ROOT_DIR,
    )
    try:
        return asyncio.run(coro)
    except RuntimeError as exc:
        if "asyncio.run() cannot be called from a running event loop" not in str(exc):
            raise
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
