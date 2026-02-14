"""Prompt Lab runner for Agent 02 (Creative Engine).

Stepwise harness for prompt iteration with full artifact capture.
This module is intentionally decoupled from server routes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

import config
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from pipeline.claude_agent_scout import call_claude_agent_structured
from pipeline.llm import LLMError, call_llm_structured, get_usage_log, get_usage_summary
from prompts.agent_02_system import CREATIVE_SCOUT_PROMPT, STEP1_PROMPT, STEP3_PROMPT
from schemas.idea_generator import CreativeEngineBrief, CreativeScoutReport, Step1Output

logger = logging.getLogger(__name__)

RAW_SENTINEL = "RAW_NOT_CAPTURED_IN_THIS_PATH"
DEFAULT_OUTPUT_ROOT = config.OUTPUT_DIR / "prompt_lab"
DEFAULT_PROMPT_DIR = config.ROOT_DIR / "prompts" / "prompt_lab"

STEP_NAMES = ("step1", "step2", "step3")
STEP_TO_SCHEMA: dict[str, type[BaseModel]] = {
    "step1": Step1Output,
    "step2": CreativeScoutReport,
    "step3": CreativeEngineBrief,
}
STEP_TO_PROMPT_FILE = {
    "step1": "agent_02_step1_system.md",
    "step2": "agent_02_step2_system.md",
    "step3": "agent_02_step3_system.md",
}
STEP_TO_DEFAULT_PROMPT = {
    "step1": STEP1_PROMPT,
    "step2": CREATIVE_SCOUT_PROMPT,
    "step3": STEP3_PROMPT,
}


class PromptLabError(RuntimeError):
    """Base exception for prompt-lab failures."""

    exit_code = 1


class InputDataError(PromptLabError):
    exit_code = 2


class PromptResolutionError(PromptLabError):
    exit_code = 3


class LLMCallExecutionError(PromptLabError):
    exit_code = 4


class SchemaValidationFailure(PromptLabError):
    exit_code = 5


@dataclass
class PromptLabSettings:
    """Runtime settings for prompt lab execution."""

    run_label: str = "prompt-lab"
    out_root: Path = DEFAULT_OUTPUT_ROOT
    prompt_dir: Path = DEFAULT_PROMPT_DIR
    provider: str | None = None
    model: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    strict_sdk_only: bool = True
    force: bool = False


@dataclass
class StepExecution:
    """Execution metadata for one step."""

    name: str
    schema_name: str
    status: str
    duration_seconds: float
    artifacts: dict[str, Path] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class RunExecution:
    """Execution metadata for one prompt-lab run."""

    run_id: str
    created_at: str
    run_dir: Path
    settings: PromptLabSettings
    steps: dict[str, StepExecution] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def _slugify(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"[^a-z0-9_-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "prompt_lab"


def create_run_directory(settings: PromptLabSettings) -> tuple[str, str, Path]:
    """Create a unique run directory and return run metadata."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    created_at = datetime.now().isoformat(timespec="seconds")
    run_name = f"{run_id}_{_slugify(settings.run_label)}"
    run_dir = settings.out_root / run_name

    if run_dir.exists() and not settings.force:
        raise InputDataError(
            f"Run directory already exists: {run_dir}. Use --force to overwrite."
        )

    run_dir.mkdir(parents=True, exist_ok=True)
    return run_id, created_at, run_dir


def read_json_file(path: Path) -> dict[str, Any]:
    """Read and parse JSON file with actionable errors."""
    if not path.exists():
        raise InputDataError(f"Input file does not exist: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InputDataError(f"Malformed JSON in {path}: {exc}") from exc
    except OSError as exc:
        raise InputDataError(f"Failed reading {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise InputDataError(f"Expected JSON object in {path}, got {type(data).__name__}")
    return data


def load_step1_output(path: Path) -> Step1Output:
    """Load and validate Step1Output artifact."""
    data = read_json_file(path)
    try:
        return Step1Output.model_validate(data)
    except ValidationError as exc:
        raise SchemaValidationFailure(
            f"{path} is not valid Step1Output: {exc}"
        ) from exc


def load_step2_output(path: Path) -> CreativeScoutReport:
    """Load and validate CreativeScoutReport artifact."""
    data = read_json_file(path)
    try:
        return CreativeScoutReport.model_validate(data)
    except ValidationError as exc:
        raise SchemaValidationFailure(
            f"{path} is not valid CreativeScoutReport: {exc}"
        ) from exc


def _write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]):
    _write_text(path, json.dumps(payload, indent=2, default=str))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _usage_delta(before_summary: dict[str, Any], before_log_len: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    after_summary = get_usage_summary()
    after_log = get_usage_log()
    new_entries = after_log[before_log_len:]
    delta = {
        "input_tokens": int(after_summary["total_input_tokens"] - before_summary["total_input_tokens"]),
        "output_tokens": int(after_summary["total_output_tokens"] - before_summary["total_output_tokens"]),
        "tokens": int(after_summary["total_tokens"] - before_summary["total_tokens"]),
        "cost": round(float(after_summary["total_cost"] - before_summary["total_cost"]), 6),
        "calls": int(after_summary["calls"] - before_summary["calls"]),
    }
    return delta, new_entries


def _resolve_system_prompt(step: str, prompt_dir: Path) -> tuple[str, str]:
    if step not in STEP_TO_PROMPT_FILE:
        raise PromptResolutionError(f"Unknown prompt step: {step}")

    override_path = prompt_dir / STEP_TO_PROMPT_FILE[step]
    if override_path.exists():
        try:
            text = override_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise PromptResolutionError(f"Failed reading prompt override {override_path}: {exc}") from exc
        if not text:
            raise PromptResolutionError(f"Prompt override is empty: {override_path}")
        return text, str(override_path)

    default_prompt = STEP_TO_DEFAULT_PROMPT[step].strip()
    if not default_prompt:
        raise PromptResolutionError(f"Default prompt for {step} is empty")
    return default_prompt, f"prompts.agent_02_system.{step.upper()}"


def _agent_for_lab(settings: PromptLabSettings) -> Agent02IdeaGenerator:
    return Agent02IdeaGenerator(
        provider=settings.provider,
        model=settings.model,
        temperature=settings.temperature,
        max_tokens=settings.max_tokens,
    )


def _validate_step1_input(input_data: dict[str, Any]):
    if not isinstance(input_data.get("foundation_brief"), dict):
        raise InputDataError(
            "step1 requires a 'foundation_brief' object in --input-json"
        )


def _step_dir(run_dir: Path, step: str) -> Path:
    step_path = run_dir / step
    step_path.mkdir(parents=True, exist_ok=True)
    return step_path


def _write_step_artifacts(
    *,
    step: str,
    run_dir: Path,
    schema_name: str,
    system_prompt: str,
    user_prompt: str,
    parsed: BaseModel,
    raw_response: str,
    meta: dict[str, Any],
) -> dict[str, Path]:
    step_path = _step_dir(run_dir, step)
    files: dict[str, Path] = {
        "system_prompt": step_path / "system_prompt.md",
        "user_prompt": step_path / "user_prompt.md",
        "response_parsed": step_path / "response_parsed.json",
        "response_raw": step_path / "response_raw.txt",
        "validation": step_path / "validation.json",
        "meta": step_path / "meta.json",
    }

    _write_text(files["system_prompt"], system_prompt)
    _write_text(files["user_prompt"], user_prompt)
    _write_text(files["response_parsed"], parsed.model_dump_json(indent=2))
    _write_text(files["response_raw"], raw_response or RAW_SENTINEL)
    _write_json(
        files["validation"],
        {
            "schema": schema_name,
            "pass": True,
            "errors": [],
        },
    )
    _write_json(files["meta"], meta)
    return files


def _extract_sdk_metadata(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in entries:
        metadata = entry.get("metadata")
        if isinstance(metadata, dict) and metadata.get("source") == "claude-agent-sdk":
            return metadata
    return None


def _extract_json_payload(text: str) -> dict[str, Any]:
    body = text.strip()
    marker = "# STRUCTURED_RESEARCH_JSON"
    if body.startswith(marker):
        body = body[len(marker):].strip()

    # Best effort when the payload contains prose around JSON.
    if not body.startswith("{"):
        start = body.find("{")
        end = body.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise SchemaValidationFailure("Step 2 response did not contain a JSON object")
        body = body[start : end + 1]

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SchemaValidationFailure(f"Step 2 JSON parsing failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise SchemaValidationFailure("Step 2 JSON payload must be an object")
    return payload


def run_step1(
    *,
    input_data: dict[str, Any],
    run_dir: Path,
    settings: PromptLabSettings,
) -> tuple[StepExecution, Step1Output]:
    """Run Prompt Lab Step 1 and write artifacts."""
    _validate_step1_input(input_data)

    step = "step1"
    schema = STEP_TO_SCHEMA[step]
    agent = _agent_for_lab(settings)
    system_prompt, system_source = _resolve_system_prompt(step, settings.prompt_dir)
    user_prompt = agent._build_step1_prompt(input_data)

    start = time.time()
    usage_before = get_usage_summary()
    usage_len_before = len(get_usage_log())

    max_tokens = settings.max_tokens if settings.max_tokens is not None else 16_000

    try:
        result = call_llm_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=Step1Output,
            provider=agent.provider,
            model=agent.model,
            temperature=agent.temperature,
            max_tokens=max_tokens,
        )
    except (LLMError, Exception) as exc:
        raise LLMCallExecutionError(f"Step 1 failed: {exc}") from exc

    duration = round(time.time() - start, 3)
    usage, entries = _usage_delta(usage_before, usage_len_before)

    meta = {
        "provider": agent.provider,
        "model": agent.model,
        "temperature": agent.temperature,
        "max_tokens": max_tokens,
        "duration_seconds": duration,
        "system_prompt_source": system_source,
        "user_prompt_chars": len(user_prompt),
        "usage_delta": usage,
        "usage_entries_count": len(entries),
    }

    files = _write_step_artifacts(
        step=step,
        run_dir=run_dir,
        schema_name=schema.__name__,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        parsed=result,
        raw_response=RAW_SENTINEL,
        meta=meta,
    )

    execution = StepExecution(
        name=step,
        schema_name=schema.__name__,
        status="completed",
        duration_seconds=duration,
        artifacts=files,
        meta=meta,
    )
    return execution, result


def run_step2(
    *,
    step1_output: Step1Output,
    run_dir: Path,
    settings: PromptLabSettings,
    base_inputs: dict[str, Any] | None = None,
) -> tuple[StepExecution, CreativeScoutReport]:
    """Run Prompt Lab Step 2 and write artifacts."""
    step = "step2"
    schema = STEP_TO_SCHEMA[step]
    base_inputs = dict(base_inputs or {})
    base_inputs.setdefault("brand_name", step1_output.brand_name)
    base_inputs.setdefault("product_name", step1_output.product_name)

    agent = _agent_for_lab(settings)
    system_prompt, system_source = _resolve_system_prompt(step, settings.prompt_dir)
    user_prompt = agent._build_research_prompt(base_inputs, list(step1_output.angles))

    start = time.time()
    usage_before = get_usage_summary()
    usage_len_before = len(get_usage_log())
    raw_response = RAW_SENTINEL

    try:
        if settings.strict_sdk_only:
            if not config.ANTHROPIC_API_KEY:
                raise LLMCallExecutionError(
                    "Step 2 strict SDK-only mode requires ANTHROPIC_API_KEY"
                )
            result = call_claude_agent_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=CreativeScoutReport,
                model=config.CREATIVE_SCOUT_MODEL,
                max_turns=config.CREATIVE_SCOUT_MAX_TURNS,
                max_thinking_tokens=config.CREATIVE_SCOUT_MAX_THINKING_TOKENS,
                max_budget_usd=config.CREATIVE_SCOUT_MAX_BUDGET_USD,
            )
        else:
            report_text = agent._run_web_research(
                base_inputs,
                list(step1_output.angles),
                user_prompt,
                remaining_budget_usd=max(config.CREATIVE_SCOUT_MAX_BUDGET_USD, 0.01),
            )
            raw_response = report_text
            payload = _extract_json_payload(report_text)
            result = CreativeScoutReport.model_validate(payload)
    except LLMCallExecutionError:
        raise
    except ValidationError as exc:
        raise SchemaValidationFailure(f"Step 2 schema validation failed: {exc}") from exc
    except LLMError as exc:
        raise LLMCallExecutionError(f"Step 2 failed: {exc}") from exc
    except Exception as exc:
        if settings.strict_sdk_only:
            raise LLMCallExecutionError(f"Step 2 SDK-only failure: {exc}") from exc
        raise

    duration = round(time.time() - start, 3)
    usage, entries = _usage_delta(usage_before, usage_len_before)

    meta = {
        "provider": "anthropic",
        "model": config.CREATIVE_SCOUT_MODEL,
        "temperature": 0.0,
        "max_tokens": None,
        "duration_seconds": duration,
        "system_prompt_source": system_source,
        "user_prompt_chars": len(user_prompt),
        "strict_sdk_only": settings.strict_sdk_only,
        "usage_delta": usage,
        "usage_entries_count": len(entries),
        "sdk_metadata": _extract_sdk_metadata(entries),
    }

    files = _write_step_artifacts(
        step=step,
        run_dir=run_dir,
        schema_name=schema.__name__,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        parsed=result,
        raw_response=raw_response,
        meta=meta,
    )

    execution = StepExecution(
        name=step,
        schema_name=schema.__name__,
        status="completed",
        duration_seconds=duration,
        artifacts=files,
        meta=meta,
    )
    return execution, result


def run_step3(
    *,
    input_data: dict[str, Any],
    step1_output: Step1Output,
    step2_output: CreativeScoutReport,
    run_dir: Path,
    settings: PromptLabSettings,
) -> tuple[StepExecution, CreativeEngineBrief]:
    """Run Prompt Lab Step 3 and write artifacts."""
    step = "step3"
    schema = STEP_TO_SCHEMA[step]
    agent = _agent_for_lab(settings)

    merged_inputs = dict(input_data)
    merged_inputs.setdefault("brand_name", step1_output.brand_name)
    merged_inputs.setdefault("product_name", step1_output.product_name)
    merged_inputs.setdefault("batch_id", merged_inputs.get("batch_id", ""))

    system_prompt, system_source = _resolve_system_prompt(step, settings.prompt_dir)
    web_research = agent._format_structured_research(step2_output)
    user_prompt = agent._build_step3_prompt(merged_inputs, list(step1_output.angles), web_research)

    start = time.time()
    usage_before = get_usage_summary()
    usage_len_before = len(get_usage_log())

    max_tokens = settings.max_tokens if settings.max_tokens is not None else agent.max_tokens

    try:
        result = call_llm_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=CreativeEngineBrief,
            provider=agent.provider,
            model=agent.model,
            temperature=agent.temperature,
            max_tokens=max_tokens,
        )
    except (LLMError, Exception) as exc:
        raise LLMCallExecutionError(f"Step 3 failed: {exc}") from exc

    duration = round(time.time() - start, 3)
    usage, entries = _usage_delta(usage_before, usage_len_before)

    meta = {
        "provider": agent.provider,
        "model": agent.model,
        "temperature": agent.temperature,
        "max_tokens": max_tokens,
        "duration_seconds": duration,
        "system_prompt_source": system_source,
        "user_prompt_chars": len(user_prompt),
        "usage_delta": usage,
        "usage_entries_count": len(entries),
    }

    files = _write_step_artifacts(
        step=step,
        run_dir=run_dir,
        schema_name=schema.__name__,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        parsed=result,
        raw_response=RAW_SENTINEL,
        meta=meta,
    )

    execution = StepExecution(
        name=step,
        schema_name=schema.__name__,
        status="completed",
        duration_seconds=duration,
        artifacts=files,
        meta=meta,
    )
    return execution, result


def run_chain(
    *,
    input_data: dict[str, Any],
    run_dir: Path,
    settings: PromptLabSettings,
) -> RunExecution:
    """Run step1 -> step2 -> step3 in sequence."""
    run = RunExecution(
        run_id=run_dir.name.split("_")[0],
        created_at=datetime.now().isoformat(timespec="seconds"),
        run_dir=run_dir,
        settings=settings,
    )

    s1_exec, s1_out = run_step1(input_data=input_data, run_dir=run_dir, settings=settings)
    run.steps[s1_exec.name] = s1_exec

    s2_exec, s2_out = run_step2(
        step1_output=s1_out,
        run_dir=run_dir,
        settings=settings,
        base_inputs=input_data,
    )
    run.steps[s2_exec.name] = s2_exec

    s3_exec, _ = run_step3(
        input_data=input_data,
        step1_output=s1_out,
        step2_output=s2_out,
        run_dir=run_dir,
        settings=settings,
    )
    run.steps[s3_exec.name] = s3_exec

    return run


def _render_settings(settings: PromptLabSettings) -> dict[str, Any]:
    return {
        "run_label": settings.run_label,
        "out_root": str(settings.out_root),
        "prompt_dir": str(settings.prompt_dir),
        "provider": settings.provider,
        "model": settings.model,
        "temperature": settings.temperature,
        "max_tokens": settings.max_tokens,
        "strict_sdk_only": settings.strict_sdk_only,
        "force": settings.force,
    }


def _artifact_manifest(artifacts: dict[str, Path]) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    for name, path in artifacts.items():
        entry = {"path": str(path)}
        if path.exists() and path.is_file():
            entry["sha256"] = _sha256(path)
        payload[name] = entry
    return payload


def write_run_artifacts(
    *,
    run: RunExecution,
    inputs_snapshot: dict[str, Any],
) -> dict[str, Path]:
    """Write run-level manifest, summary, and input snapshot."""
    run_files = {
        "manifest": run.run_dir / "manifest.json",
        "summary": run.run_dir / "summary.md",
        "inputs_snapshot": run.run_dir / "inputs_snapshot.json",
    }

    _write_json(run_files["inputs_snapshot"], inputs_snapshot)

    step_status = {name: step.status for name, step in run.steps.items()}
    durations = {name: step.duration_seconds for name, step in run.steps.items()}
    artifacts = {name: _artifact_manifest(step.artifacts) for name, step in run.steps.items()}

    manifest = {
        "run_id": run.run_id,
        "created_at": run.created_at,
        "settings": _render_settings(run.settings),
        "step_status": step_status,
        "artifacts": artifacts,
        "durations": durations,
        "errors": run.errors,
    }
    _write_json(run_files["manifest"], manifest)

    lines = [
        "# Agent 02 Prompt Lab Summary",
        "",
        f"- Run ID: `{run.run_id}`",
        f"- Created: `{run.created_at}`",
        f"- Run Dir: `{run.run_dir}`",
        "",
        "## Step Status",
    ]
    for step in STEP_NAMES:
        if step in run.steps:
            s = run.steps[step]
            lines.append(f"- `{step}`: **{s.status}** ({s.duration_seconds:.3f}s)")
        else:
            lines.append(f"- `{step}`: not-run")

    lines.append("")
    lines.append("## Errors")
    if run.errors:
        for err in run.errors:
            lines.append(f"- {err}")
    else:
        lines.append("- none")

    _write_text(run_files["summary"], "\n".join(lines) + "\n")
    return run_files
