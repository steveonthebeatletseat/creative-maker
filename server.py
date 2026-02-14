"""Creative Maker — Web Server.

FastAPI backend that serves the dashboard and exposes API routes
for running the pipeline, checking status, viewing outputs, and
browsing run history (SQLite-backed).

Usage:
    python server.py
    # Then open http://localhost:8000
"""

from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import queue as queue_mod
import shutil
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from agents.agent_01a_foundation_research import Agent01AFoundationResearch
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist

# ---------------------------------------------------------------------------
# Branch storage (brand-scoped)
# ---------------------------------------------------------------------------

def _brand_output_dir(brand_slug: str) -> Path:
    """Return the output directory for a brand."""
    d = config.OUTPUT_DIR / brand_slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _brand_branches_dir(brand_slug: str) -> Path:
    """Return the branches directory for a brand."""
    return _brand_output_dir(brand_slug) / "branches"


def _brand_branches_manifest(brand_slug: str) -> Path:
    return _brand_branches_dir(brand_slug) / "manifest.json"


def _load_branches(brand_slug: str | None = None) -> list[dict]:
    """Load all branches from a brand's manifest file."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    if not brand_slug:
        return []
    manifest = _brand_branches_manifest(brand_slug)
    if manifest.exists():
        try:
            return json.loads(manifest.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_branches(brand_slug: str, branches: list[dict]):
    """Save branches to a brand's manifest file."""
    bdir = _brand_branches_dir(brand_slug)
    bdir.mkdir(parents=True, exist_ok=True)
    _brand_branches_manifest(brand_slug).write_text(json.dumps(branches, indent=2), "utf-8")


def _get_branch(branch_id: str, brand_slug: str | None = None) -> dict | None:
    """Get a branch by ID."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    for b in _load_branches(brand_slug):
        if b["id"] == branch_id:
            return b
    return None


def _update_branch(branch_id: str, updates: dict, brand_slug: str | None = None):
    """Update a branch's fields and save."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    branches = _load_branches(brand_slug)
    for b in branches:
        if b["id"] == branch_id:
            b.update(updates)
            break
    _save_branches(brand_slug, branches)


def _clear_all_branches(brand_slug: str):
    """Remove all branches and their output directories. Called when Phase 1 starts (new pipeline)."""
    for b in _load_branches(brand_slug):
        bdir = _brand_branches_dir(brand_slug) / b["id"]
        if bdir.exists():
            shutil.rmtree(bdir, ignore_errors=True)
    _save_branches(brand_slug, [])
    logger.info("Cleared all branches for brand %s", brand_slug)


def _branch_output_dir(brand_slug: str, branch_id: str) -> Path:
    """Return the output directory for a branch within a brand."""
    return _brand_branches_dir(brand_slug) / branch_id


def _load_branch_output(brand_slug: str, branch_id: str, slug: str) -> dict | None:
    """Load an agent output from a specific branch directory."""
    path = _branch_output_dir(brand_slug, branch_id) / f"{slug}_output.json"
    if path.exists():
        return json.loads(path.read_text("utf-8"))
    return None


def _migrate_flat_outputs_to_brand():
    """One-time migration: move flat output files from outputs/ into a brand directory.

    Detects flat agent_*_output.json in the root outputs/ dir (old layout) and
    moves them into outputs/{brand-slug}/ (new layout). Idempotent.
    """
    flat_files = list(config.OUTPUT_DIR.glob("agent_*_output.json"))
    if not flat_files:
        return  # Nothing to migrate

    # Try to determine brand from Foundation Research output
    brand_name = "legacy"
    foundation_path = config.OUTPUT_DIR / "agent_01a_output.json"
    if foundation_path.exists():
        try:
            data = json.loads(foundation_path.read_text("utf-8"))
            brand_name = data.get("brand_name", "legacy") or "legacy"
        except Exception:
            pass

    brand_slug = _slugify(brand_name)
    brand_dir = config.OUTPUT_DIR / brand_slug
    brand_dir.mkdir(parents=True, exist_ok=True)

    # Move flat output files
    moved = 0
    for f in flat_files:
        dest = brand_dir / f.name
        if not dest.exists():
            shutil.move(str(f), str(dest))
            moved += 1

    # Move branches directory if it exists at root level
    old_branches = config.OUTPUT_DIR / "branches"
    new_branches = brand_dir / "branches"
    if old_branches.exists() and not new_branches.exists():
        shutil.move(str(old_branches), str(new_branches))

    # Create the brand record in SQLite
    from pipeline.storage import get_brand as _get_brand_db, create_brand as _create_brand_db
    if not _get_brand_db(brand_slug):
        _create_brand_db(brand_name, "", {})

    logger.info("Migrated %d flat outputs to brand directory: %s", moved, brand_slug)


from pipeline.llm import reset_usage, get_usage_summary
from pipeline.scraper import scrape_website
from pipeline.storage import (
    init_db,
    create_run,
    complete_run,
    fail_run,
    save_agent_output,
    list_runs,
    get_run,
    update_run_label,
    delete_run,
    # Brand system
    get_or_create_brand,
    get_brand,
    list_brands,
    touch_brand,
    delete_brand as storage_delete_brand,
    _slugify,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _check_api_keys() -> list[str]:
    """Check which LLM provider API keys are configured. Returns list of warnings."""
    warnings = []
    if not config.OPENAI_API_KEY:
        warnings.append("OPENAI_API_KEY is not set")
    if not config.ANTHROPIC_API_KEY:
        warnings.append("ANTHROPIC_API_KEY is not set")
    if not config.GOOGLE_API_KEY:
        warnings.append("GOOGLE_API_KEY is not set")

    # Check if the default provider has a key
    provider = config.DEFAULT_PROVIDER
    key_map = {
        "openai": config.OPENAI_API_KEY,
        "anthropic": config.ANTHROPIC_API_KEY,
        "google": config.GOOGLE_API_KEY,
    }
    if not key_map.get(provider):
        warnings.insert(0, f"DEFAULT_PROVIDER is '{provider}' but {provider.upper()}_API_KEY is not set — pipeline will fail!")

    return warnings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    _migrate_flat_outputs_to_brand()
    key_warnings = _check_api_keys()
    if key_warnings:
        logger.warning("=" * 60)
        logger.warning("API KEY WARNINGS:")
        for w in key_warnings:
            logger.warning("  • %s", w)
        logger.warning("Copy .env.example to .env and add your keys:")
        logger.warning("  cp .env.example .env")
        logger.warning("=" * 60)
    else:
        logger.info("API keys: all providers configured")

    # Install WebSocket log handler so server logs stream to the frontend
    ws_handler = _WSLogHandler()
    ws_handler.setLevel(logging.INFO)
    ws_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(ws_handler)

    # Start background log broadcaster
    broadcaster_task = asyncio.create_task(_log_broadcaster())

    yield

    # Shutdown
    broadcaster_task.cancel()
    logging.getLogger().removeHandler(ws_handler)


app = FastAPI(title="Creative Maker Pipeline", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

pipeline_state: dict[str, Any] = {
    "running": False,
    "abort_requested": False,
    "abort_generation": 0,  # monotonic abort token for cancelling background thread work
    "pipeline_task": None,  # asyncio.Task reference for cancellation
    "current_phase": None,
    "current_agent": None,
    "completed_agents": [],
    "failed_agents": [],
    "start_time": None,
    "log": [],
    "run_id": None,  # current SQLite run_id
    "phase_gate": None,  # asyncio.Event — set when user approves next phase
    "waiting_for_approval": False,  # True while paused between phases
    "selected_concepts": [],  # user-selected video concepts from Phase 2
    "active_branch": None,  # currently running branch ID (None = main pipeline)
    "active_brand_slug": None,  # current brand slug (scopes outputs + branches)
    "copywriter_failed_jobs": [],  # failed per-concept jobs from parallel Agent 04
    "copywriter_parallel_context": None,  # context for rewriting only failed jobs
    "copywriter_rewrite_in_progress": False,  # guard to block Continue during rewrite
}

ws_clients: list[WebSocket] = []


# ---------------------------------------------------------------------------
# WebSocket broadcast
# ---------------------------------------------------------------------------

async def broadcast(msg: dict):
    """Send a message to all connected WebSocket clients."""
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)


def _add_log(message: str, level: str = "info"):
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "level": level,
        "message": message,
    }
    pipeline_state["log"].append(entry)
    if len(pipeline_state["log"]) > 200:
        pipeline_state["log"] = pipeline_state["log"][-200:]


# ---------------------------------------------------------------------------
# Live server-log streaming (captures Python logs → WebSocket)
# ---------------------------------------------------------------------------

_log_queue: queue_mod.Queue = queue_mod.Queue(maxsize=1000)
_recent_server_logs: deque[str] = deque(maxlen=500)


class _WSLogHandler(logging.Handler):
    """Captures pipeline log messages and queues them for WebSocket streaming."""

    _SKIP_LOGGERS = frozenset({
        "uvicorn.access", "uvicorn.error", "uvicorn", "websockets",
        "websockets.server", "websockets.protocol",
    })

    def emit(self, record: logging.LogRecord):
        try:
            if record.name in self._SKIP_LOGGERS:
                return
            msg = self.format(record)
            # Skip HTTP access lines and WS connection noise
            if ("HTTP/1.1" in msg and '- "' in msg):
                return
            if any(skip in msg for skip in ("WebSocket /ws", "connection open", "connection closed")):
                return
            _recent_server_logs.append(msg)
            _log_queue.put_nowait(msg)
        except (queue_mod.Full, Exception):
            pass


async def _log_broadcaster():
    """Background task: drain log queue and broadcast to WS clients."""
    while True:
        batch: list[str] = []
        try:
            while len(batch) < 30:
                batch.append(_log_queue.get_nowait())
        except queue_mod.Empty:
            pass

        if batch and ws_clients:
            await broadcast({"type": "server_log", "lines": batch})

        await asyncio.sleep(0.5)


def _reset_server_log_stream():
    """Clear buffered server log tail/queue so each run starts with a clean terminal."""
    _recent_server_logs.clear()
    try:
        while True:
            _log_queue.get_nowait()
    except queue_mod.Empty:
        pass


# ---------------------------------------------------------------------------
# Agent runner helpers (sync, called in thread)
# ---------------------------------------------------------------------------

AGENT_CLASSES = {
    "agent_01a": Agent01AFoundationResearch,
    "agent_02": Agent02IdeaGenerator,
    "agent_04": Agent04Copywriter,
    "agent_05": Agent05HookSpecialist,
}

AGENT_META = {
    "agent_01a": {"name": "Foundation Research", "phase": 1, "icon": "🔬"},
    "agent_02": {"name": "Creative Engine", "phase": 2, "icon": "💡"},
    "agent_04": {"name": "Copywriter", "phase": 3, "icon": "✍️"},
    "agent_05": {"name": "Hook Specialist", "phase": 3, "icon": "🎣"},
}

MODEL_LABELS = {
    "gpt-5.2": "GPT 5.2",
    "gpt-5.2-mini": "GPT 5.2 Mini",
    "gemini-3.0-pro": "Gemini 3.0 Pro",
    "gemini-2.5-pro": "Gemini 2.5 Pro",
    "gemini-2.5-flash": "Gemini 2.5 Flash",
    "claude-opus-4-6": "Claude Opus 4.6",
}


def _friendly_model_label(model: str) -> str:
    return MODEL_LABELS.get(model, model)


def _load_output(slug: str, brand_slug: str | None = None) -> dict | None:
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    if brand_slug:
        base = _brand_output_dir(brand_slug)
    else:
        base = config.OUTPUT_DIR
    path = base / f"{slug}_output.json"
    if path.exists():
        return json.loads(path.read_text("utf-8"))
    return None


def _run_agent_sync(
    slug: str,
    inputs: dict,
    provider: str | None = None,
    model: str | None = None,
    skip_deep_research: bool = False,
    output_dir: Path | None = None,
    temperature: float | None = None,
    abort_check=None,
) -> dict | None:
    """Run a single agent synchronously. Returns the output dict or None."""
    cls = AGENT_CLASSES.get(slug)
    if not cls:
        return None
    agent = cls(provider=provider, model=model, output_dir=output_dir, temperature=temperature)
    # Use a shallow copy so per-agent flags don't leak to other agents
    agent_inputs = dict(inputs)
    if skip_deep_research:
        agent_inputs["_skip_deep_research"] = True
    if abort_check:
        agent_inputs["_abort_check"] = abort_check
    result = agent.run(agent_inputs)
    return json.loads(result.model_dump_json())


def _auto_load_upstream(inputs: dict, needed: list[str], sync_foundation_identity: bool = False, brand_slug: str | None = None):
    """Load upstream agent outputs from disk into the inputs dict.

    If ``sync_foundation_identity`` is True, brand/product are only hydrated
    from Foundation Research when those fields are currently missing.
    """
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    mapping = {
        "foundation_brief": "agent_01a",
        "idea_brief": "agent_02",
        "copywriter_brief": "agent_04",
        "hook_brief": "agent_05",
    }
    for key in needed:
        if key not in inputs or inputs[key] is None:
            slug = mapping.get(key)
            if slug:
                data = _load_output(slug, brand_slug=brand_slug)
                if data:
                    inputs[key] = data

                    # Optional identity hydration from Foundation Research.
                    # Never overwrite explicit values from the current brief.
                    if sync_foundation_identity and key == "foundation_brief":
                        if not inputs.get("brand_name") and data.get("brand_name"):
                            inputs["brand_name"] = data["brand_name"]
                        if not inputs.get("product_name") and data.get("product_name"):
                            inputs["product_name"] = data["product_name"]


def _norm_identity_value(value: Any) -> str:
    """Normalise text for stable brand/product identity comparison."""
    return " ".join(str(value or "").strip().lower().split())


def _validate_foundation_context(inputs: dict[str, Any]) -> str | None:
    """Validate that Foundation Research exists and matches current brief identity."""
    foundation = inputs.get("foundation_brief")
    if not isinstance(foundation, dict):
        return (
            "Creative Engine requires Foundation Research output. "
            "Run Foundation Research (Phase 1) first."
        )

    foundation_brand = str(foundation.get("brand_name") or "").strip()
    foundation_product = str(foundation.get("product_name") or "").strip()
    if not foundation_brand or not foundation_product:
        return (
            "Saved Foundation Research is missing brand/product identity. "
            "Rerun Foundation Research (Phase 1)."
        )

    request_brand = str(inputs.get("brand_name") or "").strip()
    request_product = str(inputs.get("product_name") or "").strip()

    if request_brand and _norm_identity_value(request_brand) != _norm_identity_value(foundation_brand):
        return (
            f"Foundation Research is for brand '{foundation_brand}', but the current brief brand "
            f"is '{request_brand}'. Run Foundation Research again for this brief."
        )
    if request_product and _norm_identity_value(request_product) != _norm_identity_value(foundation_product):
        return (
            f"Foundation Research is for product '{foundation_product}', but the current brief product "
            f"is '{request_product}'. Run Foundation Research again for this brief."
        )

    # Fill missing identity from Foundation Research only when blank.
    if not request_brand:
        inputs["brand_name"] = foundation_brand
    if not request_product:
        inputs["product_name"] = foundation_product

    return None


def _ensure_foundation_for_creative_engine(inputs: dict[str, Any], brand_slug: str | None = None) -> str | None:
    """Load and validate Foundation Research context before Creative Engine runs."""
    _auto_load_upstream(inputs, ["foundation_brief"], sync_foundation_identity=False, brand_slug=brand_slug)
    return _validate_foundation_context(inputs)


def _slugify_job_key(raw: str, fallback: str) -> str:
    text = (raw or "").strip()
    if not text:
        text = fallback
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in text)
    cleaned = cleaned.strip("_")
    return cleaned[:80] or fallback


def _resolve_copywriter_jobs(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    """Resolve one copywriter job per selected concept (or first concept per angle)."""
    idea_brief = inputs.get("idea_brief")
    if not isinstance(idea_brief, dict):
        return []

    angles = idea_brief.get("angles", [])
    if not isinstance(angles, list):
        return []

    selected = inputs.get("selected_concepts", [])
    jobs: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _append_job(angle: dict[str, Any], concept: dict[str, Any], concept_index: int, fallback_idx: int):
        angle_id = str(angle.get("angle_id") or f"angle_{fallback_idx + 1}")
        base_key = _slugify_job_key(f"{angle_id}_{concept_index}", f"job_{fallback_idx + 1}")
        job_key = base_key
        suffix = 2
        while job_key in seen_keys:
            job_key = f"{base_key}_{suffix}"
            suffix += 1
        seen_keys.add(job_key)

        jobs.append({
            "job_index": len(jobs),
            "job_key": job_key,
            "angle_id": angle_id,
            "concept_index": concept_index,
            "angle": {k: v for k, v in angle.items() if k != "video_concepts"},
            "video_concept": concept,
        })

    if isinstance(selected, list) and selected:
        for i, sel in enumerate(selected):
            if not isinstance(sel, dict):
                continue
            angle_id = str(sel.get("angle_id") or "").strip()
            try:
                concept_index = int(sel.get("concept_index", 0))
            except (TypeError, ValueError):
                concept_index = 0

            for angle in angles:
                if not isinstance(angle, dict):
                    continue
                if str(angle.get("angle_id") or "") != angle_id:
                    continue
                concepts = angle.get("video_concepts", [])
                if isinstance(concepts, list) and 0 <= concept_index < len(concepts):
                    concept = concepts[concept_index]
                    if isinstance(concept, dict):
                        _append_job(angle, concept, concept_index, i)
                break
    else:
        for i, angle in enumerate(angles):
            if not isinstance(angle, dict):
                continue
            concepts = angle.get("video_concepts", [])
            if not isinstance(concepts, list) or not concepts:
                continue
            concept = concepts[0]
            if not isinstance(concept, dict):
                continue
            _append_job(angle, concept, 0, i)

    return jobs


def _build_copywriter_output(inputs: dict[str, Any], scripts: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the aggregated Agent 04 output from per-script results."""
    scripts_by_funnel: dict[str, int] = {}
    scripts_by_framework: dict[str, int] = {}
    wpm_values: list[float] = []
    quality_gate_failures: list[str] = []

    for script in scripts:
        if not isinstance(script, dict):
            continue
        funnel = str(script.get("funnel_stage", "unknown"))
        framework = str(script.get("copy_framework", "unknown"))
        scripts_by_funnel[funnel] = scripts_by_funnel.get(funnel, 0) + 1
        scripts_by_framework[framework] = scripts_by_framework.get(framework, 0) + 1

        wpm = script.get("words_per_minute")
        if isinstance(wpm, (int, float)):
            wpm_values.append(float(wpm))

        gates = script.get("quality_gates_passed")
        if isinstance(gates, dict):
            failed = [k for k, v in gates.items() if v is False]
            if failed:
                script_id = str(script.get("script_id") or script.get("concept_id") or "script")
                quality_gate_failures.append(f"{script_id}: {', '.join(failed)}")

    avg_wpm = round(sum(wpm_values) / len(wpm_values), 1) if wpm_values else 0.0

    return {
        "brand_name": str(inputs.get("brand_name") or ""),
        "product_name": str(inputs.get("product_name") or ""),
        "generated_date": date.today().isoformat(),
        "batch_id": str(inputs.get("batch_id") or ""),
        "scripts": scripts,
        "total_scripts": len(scripts),
        "scripts_by_funnel": scripts_by_funnel,
        "scripts_by_framework": scripts_by_framework,
        "average_wpm": avg_wpm,
        "quality_gate_failures": quality_gate_failures,
    }


async def _run_copywriter_jobs_parallel(
    jobs: list[dict[str, Any]],
    base_inputs: dict[str, Any],
    loop,
    provider: str,
    model: str,
    jobs_dir: Path,
    max_parallel: int = 4,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run one Agent 04 call per concept in parallel, with bounded concurrency."""
    jobs_dir.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(max_parallel)
    lock = asyncio.Lock()
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    done = 0
    total = len(jobs)

    async def _run_one(job: dict[str, Any]):
        nonlocal done
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        async with sem:
            if pipeline_state["abort_requested"]:
                raise PipelineAborted("Pipeline aborted by user")

            job_started = time.time()
            job_inputs = dict(base_inputs)
            angle = dict(job.get("angle") or {})
            angle["video_concepts"] = [job.get("video_concept", {})]
            job_inputs["idea_brief"] = {"angles": [angle]}
            job_inputs["selected_concepts"] = [{
                "angle_id": job.get("angle_id"),
                "concept_index": 0,
            }]

            run_tag = str(int(time.time() * 1000))
            job_dir = jobs_dir / f"{job.get('job_key', 'job')}_{run_tag}"
            job_dir.mkdir(parents=True, exist_ok=True)

            status_payload: dict[str, Any]
            try:
                result = await loop.run_in_executor(
                    None,
                    _run_agent_sync,
                    "agent_04",
                    job_inputs,
                    provider,
                    model,
                    False,
                    job_dir,
                    None,
                )
                scripts = result.get("scripts") if isinstance(result, dict) else None
                if not isinstance(scripts, list) or not scripts:
                    raise ValueError("Copywriter job returned no script")

                script = scripts[0]
                success = {
                    "job_index": int(job.get("job_index", 0)),
                    "job_key": str(job.get("job_key", "")),
                    "script": script,
                    "elapsed": round(time.time() - job_started, 1),
                }
                successes.append(success)
                status_payload = {
                    "status": "success",
                    "job": job,
                    "elapsed": success["elapsed"],
                    "script_id": script.get("script_id"),
                }
            except PipelineAborted:
                raise
            except Exception as exc:
                failure = {
                    "job_index": int(job.get("job_index", 0)),
                    "job_key": str(job.get("job_key", "")),
                    "angle_id": job.get("angle_id"),
                    "concept_index": job.get("concept_index"),
                    "angle": job.get("angle"),
                    "video_concept": job.get("video_concept"),
                    "error": str(exc),
                    "elapsed": round(time.time() - job_started, 1),
                }
                failures.append(failure)
                status_payload = {
                    "status": "failed",
                    "job": job,
                    "elapsed": failure["elapsed"],
                    "error": failure["error"],
                }

            try:
                (job_dir / "job_result.json").write_text(
                    json.dumps(status_payload, indent=2),
                    encoding="utf-8",
                )
            except OSError:
                pass

            async with lock:
                done += 1
                ok_count = len(successes)
                fail_count = len(failures)
                progress_msg = (
                    f"Parallel scripts {done}/{total} complete "
                    f"({ok_count} ok, {fail_count} failed)"
                )

            await broadcast({
                "type": "stream_progress",
                "slug": "agent_04",
                "message": progress_msg,
            })

    tasks = [asyncio.create_task(_run_one(job)) for job in jobs]
    await asyncio.gather(*tasks)

    successes.sort(key=lambda x: x["job_index"])
    failures.sort(key=lambda x: x["job_index"])
    return successes, failures


async def _run_copywriter_parallel_async(
    inputs: dict[str, Any],
    loop,
    run_id: int,
    provider: str | None = None,
    model: str | None = None,
    output_dir: Path | None = None,
) -> dict | None:
    """Run Agent 04 as one parallel job per selected concept (max concurrency 4)."""
    slug = "agent_04"
    meta = AGENT_META[slug]

    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    overrides = pipeline_state.get("model_overrides", {})
    agent_provider = provider
    agent_model = model
    if slug in overrides:
        agent_provider = overrides[slug].get("provider", provider)
        agent_model = overrides[slug].get("model", model)

    default_conf = config.get_agent_llm_config(slug)
    final_provider = agent_provider or default_conf["provider"]
    final_model = agent_model or default_conf["model"]
    model_label = _friendly_model_label(final_model)

    jobs = _resolve_copywriter_jobs(inputs)
    total_jobs = len(jobs)
    if total_jobs == 0:
        err = "No selected video concepts found for Copywriter."
        pipeline_state["failed_agents"].append(slug)
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        await broadcast({
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": 0.0,
        })
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=0.0,
            error=err,
        )
        return None

    max_parallel = 4
    base_output_dir = output_dir or config.OUTPUT_DIR
    jobs_dir = base_output_dir / "agent_04_jobs" / f"run_{run_id}"

    pipeline_state["current_agent"] = slug
    _add_log(
        f"Starting {meta['icon']} {meta['name']} [{model_label}] — "
        f"{total_jobs} scripts in parallel (max {max_parallel})..."
    )
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
        "model": model_label,
        "provider": final_provider,
    })

    started = time.time()
    successes, failures = await _run_copywriter_jobs_parallel(
        jobs=jobs,
        base_inputs=inputs,
        loop=loop,
        provider=final_provider,
        model=final_model,
        jobs_dir=jobs_dir,
        max_parallel=max_parallel,
    )
    elapsed = time.time() - started

    success_scripts = [s["script"] for s in successes]
    fail_count = len(failures)
    ok_count = len(success_scripts)

    pipeline_state["copywriter_failed_jobs"] = failures
    pipeline_state["copywriter_parallel_context"] = {
        "run_id": run_id,
        "output_dir": str(base_output_dir),
        "jobs_dir": str(jobs_dir),
        "provider": final_provider,
        "model": final_model,
        "base_inputs": {
            "brand_name": inputs.get("brand_name"),
            "product_name": inputs.get("product_name"),
            "batch_id": inputs.get("batch_id"),
            "foundation_brief": inputs.get("foundation_brief"),
        },
    }

    try:
        (base_output_dir / "agent_04_parallel_meta.json").write_text(
            json.dumps({
                "generated_at": datetime.now().isoformat(),
                "run_id": run_id,
                "total_jobs": total_jobs,
                "succeeded_jobs": ok_count,
                "failed_jobs": fail_count,
                "failures": failures,
                "model": {"provider": final_provider, "model": final_model},
            }, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass

    if ok_count == 0:
        err = f"Copywriter failed for all {total_jobs} scripts."
        pipeline_state["failed_agents"].append(slug)
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        await broadcast({
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": round(elapsed, 1),
        })
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=elapsed,
            error=err,
        )
        return None

    result = _build_copywriter_output(inputs, success_scripts)
    output_path = base_output_dir / "agent_04_output.json"
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("Output saved: %s", output_path)

    pipeline_state["completed_agents"].append(slug)
    cost_summary = get_usage_summary()
    cost_str = (
        f"${cost_summary['total_cost']:.2f}"
        if cost_summary["total_cost"] >= 0.01
        else f"${cost_summary['total_cost']:.4f}"
    )
    fail_suffix = f", {fail_count} failed" if fail_count else ""
    _add_log(
        f"Completed {meta['icon']} {meta['name']} in {elapsed:.1f}s — "
        f"{ok_count}/{total_jobs} scripts succeeded{fail_suffix} — running total: {cost_str}",
        "success" if fail_count == 0 else "warning",
    )
    await broadcast({
        "type": "agent_complete",
        "slug": slug,
        "name": meta["name"],
        "elapsed": round(elapsed, 1),
        "cost": cost_summary,
        "parallel_jobs": total_jobs,
        "failed_jobs": fail_count,
    })

    save_agent_output(
        run_id=run_id,
        agent_slug=slug,
        agent_name=meta["name"],
        output=result,
        elapsed=elapsed,
    )

    return result


# ---------------------------------------------------------------------------
# Pipeline execution (runs in background task)
# ---------------------------------------------------------------------------

class PipelineAborted(Exception):
    """Raised when the pipeline is aborted by the user."""
    pass


async def _run_single_agent_async(slug: str, inputs: dict, loop, run_id: int, provider: str | None = None, model: str | None = None, output_dir: Path | None = None, temperature: float | None = None) -> dict | None:
    """Run agent in thread pool and broadcast progress. Saves to SQLite."""
    # Check abort flag before starting this agent
    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    # Resolve per-agent model overrides (takes priority over global override)
    overrides = pipeline_state.get("model_overrides", {})
    agent_provider = provider
    agent_model = model
    skip_deep_research = False
    if slug in overrides:
        agent_provider = overrides[slug].get("provider", provider)
        agent_model = overrides[slug].get("model", model)
        # Agents 1A and 1B default to deep research — if user picked a
        # different model, skip deep research and use that model directly
        if slug == "agent_01a":
            skip_deep_research = True

    # Resolve the final model label for broadcast
    from config import get_agent_llm_config, AGENT_LLM_CONFIG
    default_conf = get_agent_llm_config(slug)
    # If agent_provider/model were explicitly set (override or global), use those;
    # otherwise fall back to the per-agent defaults from config.py
    final_provider = agent_provider or default_conf["provider"]
    final_model = agent_model or default_conf["model"]
    # For deep research agents, label them correctly
    if slug == "agent_01a" and not skip_deep_research:
        model_label = "Deep Research"
    else:
        model_label = _friendly_model_label(final_model)

    meta = AGENT_META[slug]
    pipeline_state["current_agent"] = slug
    _add_log(f"Starting {meta['icon']} {meta['name']} [{model_label}]...")
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
        "model": model_label,
        "provider": final_provider,
    })

    # Set up streaming progress callback to broadcast to frontend
    from pipeline.llm import set_stream_progress_callback

    def _on_stream_progress(msg):
        """Called from LLM thread during streaming — fire-and-forget broadcast."""
        try:
            asyncio.run_coroutine_threadsafe(
                broadcast({"type": "stream_progress", "slug": slug, "message": msg}),
                loop,
            )
        except Exception:
            pass

    set_stream_progress_callback(_on_stream_progress)

    start = time.time()
    try:
        run_abort_generation = int(pipeline_state.get("abort_generation", 0))

        def _abort_check() -> bool:
            return bool(
                pipeline_state.get("abort_requested")
                or int(pipeline_state.get("abort_generation", 0)) != run_abort_generation
            )

        result = await loop.run_in_executor(
            None,
            _run_agent_sync,
            slug,
            inputs,
            agent_provider,
            agent_model,
            skip_deep_research,
            output_dir,
            temperature,
            _abort_check,
        )
        elapsed = time.time() - start
        pipeline_state["completed_agents"].append(slug)

        # Get running cost totals
        cost_summary = get_usage_summary()
        cost_str = f"${cost_summary['total_cost']:.2f}" if cost_summary['total_cost'] >= 0.01 else f"${cost_summary['total_cost']:.4f}"
        _add_log(f"Completed {meta['icon']} {meta['name']} in {elapsed:.1f}s — running total: {cost_str}", "success")
        await broadcast({
            "type": "agent_complete",
            "slug": slug,
            "name": meta["name"],
            "elapsed": round(elapsed, 1),
            "cost": cost_summary,
        })

        # Save to SQLite
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=result,
            elapsed=elapsed,
        )

        return result
    except Exception as e:
        elapsed = time.time() - start
        pipeline_state["failed_agents"].append(slug)
        err = str(e)
        _add_log(f"Failed {meta['icon']} {meta['name']}: {err}", "error")
        logger.exception("Agent %s failed", slug)
        await broadcast({
            "type": "agent_error",
            "slug": slug,
            "name": meta["name"],
            "error": err,
            "elapsed": round(elapsed, 1),
        })

        # Save failure to SQLite
        save_agent_output(
            run_id=run_id,
            agent_slug=slug,
            agent_name=meta["name"],
            output=None,
            elapsed=elapsed,
            error=err,
        )

        return None
    finally:
        set_stream_progress_callback(None)


async def _wait_for_agent_gate(completed_slug: str, next_slug: str, next_name: str, show_concept_selection: bool = False, phase: int = 0):
    """Emit a phase gate for user to review, pick model, and continue."""
    gate_msg = f"{AGENT_META[completed_slug]['name']} complete"
    if show_concept_selection:
        gate_msg += " — select concepts and choose model for Copywriter."
    else:
        gate_msg += f" — review, choose model for {next_name}, then continue."
    _add_log(gate_msg)
    # Once an agent has completed and we're at a manual gate, there is no
    # active running agent. Clearing this avoids stale "running" state after
    # browser refresh/state sync.
    pipeline_state["current_agent"] = None
    copywriter_failed_count = 0
    if completed_slug == "agent_04":
        copywriter_failed_count = len(pipeline_state.get("copywriter_failed_jobs", []))
    pipeline_state["waiting_for_approval"] = True
    pipeline_state["phase_gate"] = asyncio.Event()
    await broadcast({
        "type": "phase_gate",
        "completed_agent": completed_slug,
        "next_agent": next_slug,
        "next_agent_name": next_name,
        "phase": phase,
        "show_concept_selection": show_concept_selection,
        "copywriter_failed_count": copywriter_failed_count,
    })
    await pipeline_state["phase_gate"].wait()
    pipeline_state["waiting_for_approval"] = False

    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    # Apply any per-agent model override the user picked at the gate
    override = pipeline_state.pop("next_agent_override", None)
    if override and isinstance(override, dict) and override.get("provider"):
        pipeline_state["model_overrides"][next_slug] = override
        logger.info("User selected model override for %s: %s", next_slug, override)

    await broadcast({"type": "phase_gate_cleared"})


async def run_pipeline_phases(phases: list[int], inputs: dict, provider: str | None = None, model: str | None = None, model_overrides: dict | None = None, brand_slug: str | None = None):
    """Execute requested pipeline phases sequentially, gating between every agent."""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["abort_requested"] = False
    pipeline_state["model_overrides"] = model_overrides or {}
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []
    pipeline_state["copywriter_failed_jobs"] = []
    pipeline_state["copywriter_parallel_context"] = None
    pipeline_state["copywriter_rewrite_in_progress"] = False
    pipeline_state["selected_concepts"] = []
    pipeline_state["active_brand_slug"] = brand_slug

    # Brand-scoped output directory
    output_dir = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR

    # Keep live terminal scoped to this run only.
    _reset_server_log_stream()

    # Reset the LLM cost tracker for this run
    reset_usage()

    # New pipeline (Phase 1 included) → clear stale branches from previous runs
    if 1 in phases and brand_slug:
        _clear_all_branches(brand_slug)

    # Reuse existing run if continuing later phases (e.g. Phase 3 after Phase 1+2)
    # Only create a new run if Phase 1 is included or no prior run exists
    existing_run_id = pipeline_state.get("run_id")
    if 1 not in phases and existing_run_id:
        run_id = existing_run_id
        logger.info("Continuing existing run #%d with phases %s", run_id, phases)
    elif 1 not in phases:
        # Server may have restarted — find the most recent run from DB
        recent = list_runs(limit=1)
        if recent:
            run_id = recent[0]["id"]
            pipeline_state["run_id"] = run_id
            logger.info("Resuming most recent run #%d with phases %s", run_id, phases)
        else:
            run_id = create_run(phases, inputs, brand_slug=brand_slug or "")
            pipeline_state["run_id"] = run_id
    else:
        run_id = create_run(phases, inputs, brand_slug=brand_slug or "")
        pipeline_state["run_id"] = run_id

    await broadcast({"type": "pipeline_start", "phases": phases, "run_id": run_id, "brand_slug": brand_slug or ""})

    try:
        # Pre-step: Scrape website if URL provided
        website_url = inputs.get("website_url")
        if website_url and not pipeline_state["abort_requested"]:
            _add_log(f"🌐 Scraping website: {website_url}")
            await broadcast({"type": "phase_start", "phase": 0})
            try:
                scrape_result = await loop.run_in_executor(
                    None, scrape_website, website_url, provider or "openai", model,
                )
                inputs["website_intel"] = scrape_result

                # Count what we extracted for the log
                n_testimonials = len(scrape_result.get("testimonials", []))
                n_benefits = len(scrape_result.get("key_benefits", []))
                n_claims = len(scrape_result.get("claims_made", []))
                headline = scrape_result.get("hero_headline", "")
                parts = []
                if headline:
                    parts.append(f"headline found")
                if n_testimonials:
                    parts.append(f"{n_testimonials} testimonials")
                if n_benefits:
                    parts.append(f"{n_benefits} benefits")
                if n_claims:
                    parts.append(f"{n_claims} claims")
                summary = ", ".join(parts) if parts else "basic info extracted"
                _add_log(f"✅ Website scraped — {summary}", "success")
            except Exception as e:
                _add_log(f"⚠️ Website scrape failed: {e} — continuing without it", "warning")
                logger.warning("Website scrape failed for %s: %s", website_url, e)

        # ===================================================================
        # Phase 1 — Research (Agent 1A: Foundation Research)
        # ===================================================================
        if 1 in phases:
            pipeline_state["current_phase"] = 1
            _add_log("═══ PHASE 1 — RESEARCH ═══")
            await broadcast({"type": "phase_start", "phase": 1})

            r1a = await _run_single_agent_async("agent_01a", inputs, loop, run_id, provider, model, output_dir=output_dir)

            if not r1a:
                error_detail = "Agent 1A failed (unknown reason)"
                for entry in reversed(pipeline_state["log"]):
                    if "Agent 1" in entry.get("message", "") and entry.get("level") == "error":
                        error_detail = entry["message"]
                        break
                _add_log("Phase 1 failed — Agent 1A is required", "error")
                await broadcast({"type": "pipeline_error", "message": error_detail})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            inputs["foundation_brief"] = r1a

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Agent 1A → before Agent 02 ---
        if 1 in phases and 2 in phases:
            await _wait_for_agent_gate("agent_01a", "agent_02", "Creative Engine", phase=1)

        # ===================================================================
        # Phase 2 — Ideation (Agent 02: Creative Engine)
        # ===================================================================
        if 2 in phases:
            pipeline_state["current_phase"] = 2
            _add_log("═══ PHASE 2 — IDEATION ═══")
            await broadcast({"type": "phase_start", "phase": 2})

            foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
            if foundation_err:
                _add_log(f"Phase 2 blocked — {foundation_err}", "error")
                await broadcast({"type": "pipeline_error", "message": foundation_err})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            r02 = await _run_single_agent_async("agent_02", inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r02:
                _add_log("Phase 2 failed — Creative Engine is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["idea_brief"] = r02

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Agent 02 → before Agent 04 (with concept selection) ---
        if 2 in phases and 3 in phases:
            await _wait_for_agent_gate("agent_02", "agent_04", "Copywriter", show_concept_selection=True, phase=2)

        # ===================================================================
        # Phase 3 — Scripting (one agent at a time: 04 → 05 → 07)
        # ===================================================================
        if 3 in phases:
            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3})

            _auto_load_upstream(
                inputs,
                ["foundation_brief", "idea_brief"],
                sync_foundation_identity=True,
                brand_slug=brand_slug,
            )

            # Apply user's concept selections (from the Phase 2→3 gate)
            selected = pipeline_state.get("selected_concepts", [])
            if selected and inputs.get("idea_brief"):
                inputs["selected_concepts"] = selected
                _add_log(f"User selected {len(selected)} video concepts")

            # --- Agent 04: Copywriter ---
            r04 = await _run_copywriter_parallel_async(inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["copywriter_brief"] = r04

            # --- GATE: After Agent 04 → before Agent 05 ---
            await _wait_for_agent_gate("agent_04", "agent_05", "Hook Specialist", phase=3)

            # --- Agent 05: Hook Specialist ---
            r05 = await _run_single_agent_async("agent_05", inputs, loop, run_id, provider, model, output_dir=output_dir)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["hook_brief"] = r05

        total = time.time() - pipeline_state["start_time"]
        complete_run(run_id, total)
        final_cost = get_usage_summary()
        cost_str = f"${final_cost['total_cost']:.2f}" if final_cost['total_cost'] >= 0.01 else f"${final_cost['total_cost']:.4f}"
        _add_log(f"Pipeline complete in {total:.1f}s — total cost: {cost_str}", "success")
        await broadcast({
            "type": "pipeline_complete",
            "elapsed": round(total, 1),
            "run_id": run_id,
            "cost": final_cost,
            "brand_slug": brand_slug or "",
        })

    except (PipelineAborted, asyncio.CancelledError):
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        abort_cost = get_usage_summary()
        _add_log(f"🛑 Pipeline aborted by user — cost so far: ${abort_cost['total_cost']:.4f}", "warning")
        logger.info("Pipeline aborted by user after %.1fs", total)
        # Shield the broadcast so it sends even though the task is cancelled
        try:
            await asyncio.shield(broadcast({
                "type": "pipeline_error",
                "message": "Pipeline aborted by user",
                "aborted": True,
                "cost": abort_cost,
            }))
        except asyncio.CancelledError:
            pass  # broadcast already sent or task fully cancelled
    except Exception as e:
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        err_cost = get_usage_summary()
        _add_log(f"Pipeline error: {e}", "error")
        logger.exception("Pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost})
    finally:
        pipeline_state["running"] = False
        pipeline_state["abort_requested"] = False
        pipeline_state["pipeline_task"] = None
        pipeline_state["current_phase"] = None
        pipeline_state["current_agent"] = None
        pipeline_state["run_id"] = None
        pipeline_state["copywriter_rewrite_in_progress"] = False


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    phases: list[int] = [1, 2, 3]
    inputs: dict = {}
    quick_mode: bool = False  # Skip web research in Phase 1 (fast testing)
    model_overrides: dict = {}  # Per-agent: {"agent_01a": {"provider": "openai", "model": "gpt-5.2"}}


@app.post("/api/run")
async def api_run(req: RunRequest):
    """Kick off pipeline phases."""
    if pipeline_state["running"]:
        return JSONResponse(
            {"error": "Pipeline is already running"}, status_code=409
        )

    inputs = {k: v for k, v in req.inputs.items() if v}

    # Brand name is required only when starting Phase 1 from brief.
    # Phase 2/3 can hydrate identity from saved Foundation Research.
    needs_brand = 1 in req.phases
    if needs_brand and not inputs.get("brand_name"):
        return JSONResponse(
            {"error": "Brand name is required"}, status_code=400
        )

    # Create or update the brand
    brand_slug = ""
    if inputs.get("brand_name"):
        brand_slug = get_or_create_brand(
            inputs["brand_name"],
            inputs.get("product_name", ""),
            inputs,
        )
        # Save brief.json to the brand's output directory
        brand_dir = _brand_output_dir(brand_slug)
        (brand_dir / "brief.json").write_text(json.dumps(inputs, indent=2, default=str), "utf-8")

    # Starting at Phase 2 without Phase 1 requires valid saved Foundation Research.
    if 2 in req.phases and 1 not in req.phases:
        preflight_inputs = dict(inputs)
        foundation_err = _ensure_foundation_for_creative_engine(preflight_inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Quick mode — use Gemini 2.5 Flash (fast, cheap, 65K output tokens)
    # and skip web research in Agent 1B
    override_provider = None
    override_model = None
    if req.quick_mode:
        inputs["_quick_mode"] = True
        override_provider = "google"
        override_model = "gemini-2.5-flash"

    model_overrides = req.model_overrides if not req.quick_mode else {}
    task = asyncio.create_task(run_pipeline_phases(req.phases, inputs, override_provider, override_model, model_overrides, brand_slug=brand_slug))
    pipeline_state["pipeline_task"] = task
    return {"status": "started", "phases": req.phases, "quick_mode": req.quick_mode, "brand_slug": brand_slug}


@app.post("/api/abort")
async def api_abort():
    """Abort the currently running pipeline immediately."""
    if not pipeline_state["running"]:
        return JSONResponse({"error": "No pipeline is running"}, status_code=409)

    pipeline_state["abort_generation"] = int(pipeline_state.get("abort_generation", 0)) + 1
    pipeline_state["abort_requested"] = True
    _add_log("🛑 Abort requested — stopping pipeline now...", "warning")
    await broadcast({
        "type": "pipeline_aborting",
        "message": "Stopping pipeline...",
    })

    # Cancel the asyncio task — this interrupts the current await immediately
    task = pipeline_state.get("pipeline_task")
    if task and not task.done():
        task.cancel()

    return {"status": "aborting"}


class RerunRequest(BaseModel):
    slug: str
    inputs: dict = {}
    quick_mode: bool = False
    provider: Optional[str] = None   # Optional model override for rerun
    model: Optional[str] = None


@app.post("/api/rerun")
async def api_rerun(req: RerunRequest):
    """Rerun a single agent independently.

    Auto-loads upstream outputs from disk so the agent has the context it needs.
    Useful for retrying a failed agent without restarting the entire pipeline.
    """
    if req.slug not in AGENT_CLASSES:
        return JSONResponse(
            {"error": f"Unknown agent: {req.slug}"}, status_code=400
        )

    inputs = {k: v for k, v in req.inputs.items() if v}

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Model overrides: explicit provider/model > quick_mode > defaults
    override_provider = req.provider
    override_model = req.model
    skip_deep_research = False
    if req.quick_mode and not override_provider:
        inputs["_quick_mode"] = True
        override_provider = "google"
        override_model = "gemini-2.5-flash"
    # If user explicitly picked a non-default model for deep research agents,
    # skip deep research and use the model directly
    if override_provider and req.slug == "agent_01a":
        skip_deep_research = True

    # Auto-load upstream outputs from disk (brand-scoped)
    brand_slug = pipeline_state.get("active_brand_slug") or ""
    needed = ["foundation_brief", "idea_brief",
              "copywriter_brief", "hook_brief"]
    _auto_load_upstream(inputs, needed, sync_foundation_identity=True, brand_slug=brand_slug)

    if req.slug == "agent_02":
        foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    # Run the single agent in a thread pool
    rerun_output_dir = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    loop = asyncio.get_event_loop()
    start = time.time()

    try:
        result = await loop.run_in_executor(
            None,
            lambda: _run_agent_sync(
                req.slug, inputs, override_provider, override_model,
                skip_deep_research, output_dir=rerun_output_dir,
            ),
        )
        elapsed = round(time.time() - start, 1)

        if result is None:
            return JSONResponse(
                {"error": f"Agent {req.slug} returned no output"}, status_code=500
            )

        # Get cost data
        from pipeline.llm import get_usage_summary
        cost = get_usage_summary()

        # Save to SQLite so it persists across refreshes
        # Use current pipeline run_id, or fall back to most recent run
        run_id = pipeline_state.get("run_id")
        if not run_id:
            recent_runs = list_runs(limit=1)
            if recent_runs:
                run_id = recent_runs[0]["id"]
        if run_id:
            meta = AGENT_META.get(req.slug, {})
            save_agent_output(
                run_id=run_id,
                agent_slug=req.slug,
                agent_name=meta.get("name", req.slug),
                output=result,
                elapsed=elapsed,
            )
            logger.info("Rerun output saved to SQLite (run_id=%d)", run_id)

        return {
            "status": "completed",
            "slug": req.slug,
            "elapsed": elapsed,
            "cost": cost,
        }
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.exception("Rerun failed for %s", req.slug)
        return JSONResponse(
            {"error": str(e), "elapsed": elapsed}, status_code=500
        )


# ---------------------------------------------------------------------------
# Chat with agent output
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str    # "user" or "assistant"
    content: str

class ChatRequest(BaseModel):
    slug: str
    message: str
    history: list[ChatMessage] = []
    provider: Optional[str] = None
    model: Optional[str] = None

_CHAT_SYSTEM_TEMPLATE = """You are an AI assistant helping a user analyze and refine the output of a creative advertising pipeline agent called "{agent_name}".

## Current Agent Output (JSON)
```json
{output_json}
```

## Your Role
1. **Answer questions** about this output — summarize, explain, highlight key points.
2. **Make changes** when the user asks — add, remove, or modify content in the output.

## When Making Changes
When the user asks you to change the output, do BOTH:
- Briefly explain what you changed
- Return the FULL modified JSON wrapped in <modified_output> and </modified_output> tags

IMPORTANT: The JSON inside <modified_output> must be the COMPLETE valid output — not a partial diff. Only include these tags when the user explicitly asks for changes.

## Style
Be concise and direct. No fluff."""


@app.post("/api/chat")
async def api_chat(req: ChatRequest):
    """Chat with an agent's output — ask questions or request modifications."""
    from pipeline.llm import call_llm

    # Load the agent's output
    output = _load_output(req.slug)
    if not output:
        return JSONResponse({"error": f"No output found for {req.slug}"}, status_code=404)

    meta = AGENT_META.get(req.slug, {"name": req.slug})
    output_json = json.dumps(output, indent=2)

    # Truncate if extremely large (keep under ~80K chars for context)
    if len(output_json) > 80000:
        output_json = output_json[:80000] + "\n... (truncated)"

    system_prompt = _CHAT_SYSTEM_TEMPLATE.format(
        agent_name=meta["name"],
        output_json=output_json,
    )

    # Build conversation into user prompt (since call_llm only takes system + user)
    conversation_parts = []
    for msg in req.history[-20:]:  # Keep last 20 messages for context
        role_label = "User" if msg.role == "user" else "Assistant"
        conversation_parts.append(f"{role_label}: {msg.content}")
    conversation_parts.append(f"User: {req.message}")
    user_prompt = "\n\n".join(conversation_parts)

    # Default to Gemini 3.0 Pro for chat (1M context + strong reasoning)
    provider = req.provider or "google"
    model = req.model or "gemini-3.0-pro"

    loop = asyncio.get_event_loop()
    try:
        response_text = await loop.run_in_executor(
            None,
            lambda: call_llm(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                provider=provider,
                model=model,
                temperature=0.5,
                max_tokens=16_000,
            ),
        )
    except Exception as e:
        logger.exception("Chat LLM call failed for %s", req.slug)
        return JSONResponse({"error": str(e)}, status_code=500)

    # Check if the response contains a modified output
    modified_output = None
    display_text = response_text

    if "<modified_output>" in response_text and "</modified_output>" in response_text:
        # Extract the modified JSON
        start = response_text.index("<modified_output>") + len("<modified_output>")
        end = response_text.index("</modified_output>")
        json_str = response_text[start:end].strip()

        # Strip markdown fences if present
        if json_str.startswith("```"):
            first_nl = json_str.index("\n")
            json_str = json_str[first_nl + 1:]
        if json_str.endswith("```"):
            json_str = json_str[:-3].strip()

        try:
            modified_output = json.loads(json_str)
            # Remove the raw JSON block from the display text
            display_text = response_text[:response_text.index("<modified_output>")].strip()
            if not display_text:
                display_text = "Changes applied."
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse modified output JSON: %s", e)
            # Leave the full response as-is if parsing fails
            display_text = response_text

    result = {
        "response": display_text,
        "has_changes": modified_output is not None,
    }

    if modified_output is not None:
        result["modified_output"] = modified_output

    return result


class ChatApplyRequest(BaseModel):
    slug: str
    output: dict


@app.post("/api/chat/apply")
async def api_chat_apply(req: ChatApplyRequest):
    """Apply a modified output from a chat session — saves to disk."""
    if req.slug not in AGENT_META:
        return JSONResponse({"error": f"Unknown agent: {req.slug}"}, status_code=400)
    if not req.output:
        return JSONResponse({"error": "No output provided"}, status_code=400)

    brand_slug = pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    path = base / f"{req.slug}_output.json"
    path.write_text(json.dumps(req.output, indent=2), encoding="utf-8")
    logger.info("Chat: applied modified output for %s (%d chars, brand=%s)", req.slug, len(json.dumps(req.output)), brand_slug)

    return {"ok": True, "slug": req.slug}


class ConceptSelectionRequest(BaseModel):
    """Selected video concepts to pass to the Copywriter."""
    selected: list[dict] = []  # [{angle_id, concept_index}, ...]
    model_override: dict = {}  # {"provider": "openai", "model": "gpt-5.2"}


@app.post("/api/select-concepts")
async def api_select_concepts(req: ConceptSelectionRequest):
    """Save user's video concept selections and continue pipeline."""
    pipeline_state["selected_concepts"] = req.selected
    logger.info("User selected %d video concepts", len(req.selected))

    # Store model override for the Copywriter (next agent)
    if req.model_override:
        pipeline_state["next_agent_override"] = req.model_override
        logger.info("Copywriter model override: %s", req.model_override)

    # Also trigger the phase gate continue
    if not pipeline_state.get("waiting_for_approval"):
        return JSONResponse(
            {"error": "Pipeline is not waiting for approval"}, status_code=409
        )

    gate = pipeline_state.get("phase_gate")
    if gate:
        gate.set()
    return {"status": "continued", "selected": len(req.selected)}


# ---------------------------------------------------------------------------
# Branch API Routes
# ---------------------------------------------------------------------------

class CreateBranchRequest(BaseModel):
    label: str = ""
    tof_count: int = 10
    mof_count: int = 5
    bof_count: int = 2
    temperature: Optional[float] = None  # Custom temperature for Creative Engine
    model_overrides: dict = {}
    brand: str = ""


@app.get("/api/branches")
async def api_list_branches(brand: str = ""):
    """List all branches with their status."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    branches = _load_branches(brand_slug)
    # Enrich each branch with output availability
    for b in branches:
        bdir = _branch_output_dir(brand_slug, b["id"])
        available_agents = []
        for slug in ["agent_02", "agent_04", "agent_05"]:
            if (bdir / f"{slug}_output.json").exists():
                available_agents.append(slug)
        b["available_agents"] = available_agents
    return branches


@app.post("/api/branches")
async def api_create_branch(req: CreateBranchRequest, brand: str = ""):
    """Create a new creative branch (Phase 2+ direction)."""
    brand_slug = (brand or req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branches = _load_branches(brand_slug)
    branch_num = len(branches) + 1
    branch_id = f"b{branch_num}_{int(time.time())}"

    label = req.label.strip() or f"Branch {branch_num}"

    branch = {
        "id": branch_id,
        "label": label,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "inputs": {
            "tof_count": req.tof_count,
            "mof_count": req.mof_count,
            "bof_count": req.bof_count,
        },
        "temperature": req.temperature,
        "model_overrides": req.model_overrides,
        "status": "pending",
        "completed_agents": [],
        "failed_agents": [],
    }
    branches.append(branch)
    _save_branches(brand_slug, branches)
    _branch_output_dir(brand_slug, branch_id).mkdir(parents=True, exist_ok=True)

    logger.info("Created branch %s: %s (brand: %s)", branch_id, label, brand_slug)
    await broadcast({"type": "branch_created", "branch": branch})
    return branch


@app.delete("/api/branches/{branch_id}")
async def api_delete_branch(branch_id: str, brand: str = ""):
    """Delete a branch and its outputs."""
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branches = _load_branches(brand_slug)
    found = False
    branches = [b for b in branches if b["id"] != branch_id or not (found := True)]
    if not found:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    _save_branches(brand_slug, branches)

    # Remove output directory
    import shutil
    bdir = _branch_output_dir(brand_slug, branch_id)
    if bdir.exists():
        shutil.rmtree(bdir, ignore_errors=True)

    logger.info("Deleted branch %s (brand: %s)", branch_id, brand_slug)
    await broadcast({"type": "branch_deleted", "branch_id": branch_id})
    return {"ok": True, "deleted": branch_id}


class RenameBranchRequest(BaseModel):
    label: str
    brand: str = ""


@app.patch("/api/branches/{branch_id}")
async def api_rename_branch(branch_id: str, body: RenameBranchRequest, brand: str = ""):
    """Rename a branch."""
    brand_slug = (brand or body.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)
    _update_branch(branch_id, {"label": body.label.strip()}, brand_slug)
    return {"ok": True, "branch_id": branch_id, "label": body.label.strip()}


@app.get("/api/branches/{branch_id}/outputs/{slug}")
async def api_get_branch_output(branch_id: str, slug: str, brand: str = ""):
    """Get a specific agent's output from a branch."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    data = _load_branch_output(brand_slug, branch_id, slug)
    if not data:
        return JSONResponse({"error": f"No output for {slug} in branch {branch_id}"}, status_code=404)
    meta = AGENT_META.get(slug, {"name": slug, "phase": 0, "icon": ""})
    return {
        "slug": slug,
        "name": meta["name"],
        "phase": meta["phase"],
        "data": data,
        "branch_id": branch_id,
    }


class RunBranchRequest(BaseModel):
    phases: list[int] = [2]  # Which phases to run (2, or [2, 3])
    inputs: dict = {}  # Brief inputs (brand, product, etc.)
    model_overrides: dict = {}
    brand: str = ""


@app.post("/api/branches/{branch_id}/run")
async def api_run_branch(branch_id: str, req: RunBranchRequest, brand: str = ""):
    """Run Phase 2+ for a specific branch."""
    if pipeline_state["running"]:
        return JSONResponse(
            {"error": "Pipeline is already running"}, status_code=409
        )

    brand_slug = (brand or req.brand or pipeline_state.get("active_brand_slug") or "").strip()
    if not brand_slug:
        return JSONResponse({"error": "No active brand selected"}, status_code=400)
    branch = _get_branch(branch_id, brand_slug)
    if not branch:
        return JSONResponse({"error": "Branch not found"}, status_code=404)

    inputs = {k: v for k, v in req.inputs.items() if v}

    if not inputs.get("batch_id"):
        inputs["batch_id"] = f"batch_{date.today().isoformat()}"

    # Merge branch-level funnel counts into inputs
    branch_inputs = branch.get("inputs", {})
    inputs["tof_count"] = branch_inputs.get("tof_count", 10)
    inputs["mof_count"] = branch_inputs.get("mof_count", 5)
    inputs["bof_count"] = branch_inputs.get("bof_count", 2)

    if 2 in req.phases:
        preflight_inputs = dict(inputs)
        foundation_err = _ensure_foundation_for_creative_engine(preflight_inputs, brand_slug=brand_slug)
        if foundation_err:
            return JSONResponse({"error": foundation_err}, status_code=400)

    # Model overrides: request-level > branch-level > none
    model_overrides = req.model_overrides or branch.get("model_overrides", {})

    phases = req.phases

    task = asyncio.create_task(
        run_branch_pipeline(branch_id, phases, inputs, model_overrides, brand_slug=brand_slug)
    )
    pipeline_state["pipeline_task"] = task
    return {"status": "started", "branch_id": branch_id, "phases": phases}


async def run_branch_pipeline(
    branch_id: str,
    phases: list[int],
    inputs: dict,
    model_overrides: dict | None = None,
    brand_slug: str | None = None,
):
    """Execute Phase 2+ for a specific branch, saving outputs to the branch directory."""
    if not brand_slug:
        brand_slug = pipeline_state.get("active_brand_slug") or ""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["abort_requested"] = False
    pipeline_state["model_overrides"] = model_overrides or {}
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []
    pipeline_state["active_branch"] = branch_id
    pipeline_state["active_brand_slug"] = brand_slug
    pipeline_state["copywriter_failed_jobs"] = []
    pipeline_state["copywriter_parallel_context"] = None
    pipeline_state["copywriter_rewrite_in_progress"] = False
    pipeline_state["selected_concepts"] = []

    # Keep live terminal scoped to this branch run only.
    _reset_server_log_stream()

    reset_usage()

    output_dir = _branch_output_dir(brand_slug, branch_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a DB run record
    run_id = create_run(phases, inputs, brand_slug=brand_slug)
    pipeline_state["run_id"] = run_id

    _update_branch(branch_id, {"status": "running", "completed_agents": [], "failed_agents": []}, brand_slug)

    await broadcast({
        "type": "pipeline_start",
        "phases": phases,
        "run_id": run_id,
        "branch_id": branch_id,
        "brand_slug": brand_slug,
    })

    try:
        # Phase 2 — Ideation (Creative Engine)
        if 2 in phases:
            pipeline_state["current_phase"] = 2
            _add_log(f"═══ PHASE 2 — IDEATION (Branch: {_get_branch(branch_id, brand_slug)['label']}) ═══")
            await broadcast({"type": "phase_start", "phase": 2, "branch_id": branch_id})

            # Always load/validate Phase 1 from the shared output directory
            foundation_err = _ensure_foundation_for_creative_engine(inputs, brand_slug=brand_slug)
            if foundation_err:
                _add_log(f"Phase 2 blocked — {foundation_err}", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["agent_02"]}, brand_slug)
                await broadcast({"type": "pipeline_error", "message": foundation_err, "branch_id": branch_id})
                return

            # Use branch-specific temperature for Creative Engine (if set)
            branch_data = _get_branch(branch_id, brand_slug)
            branch_temp = branch_data.get("temperature") if branch_data else None

            r02 = await _run_single_agent_async("agent_02", inputs, loop, run_id, output_dir=output_dir, temperature=branch_temp)
            if not r02:
                _add_log("Phase 2 failed — Creative Engine is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": ["agent_02"]}, brand_slug)
                return
            inputs["idea_brief"] = r02

            branch_completed = ["agent_02"]
            _update_branch(branch_id, {"completed_agents": branch_completed}, brand_slug)

        # Abort check
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- GATE: After Agent 02 → before Agent 04 (with concept selection) ---
        if 2 in phases and 3 in phases:
            await _wait_for_agent_gate("agent_02", "agent_04", "Copywriter", show_concept_selection=True, phase=2)

        # Phase 3 — Scripting (one agent at a time with gates)
        if 3 in phases:
            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3, "branch_id": branch_id})

            # Load upstream: Phase 1 from shared, Phase 2 from branch
            _auto_load_upstream(inputs, ["foundation_brief"], sync_foundation_identity=True, brand_slug=brand_slug)
            if "idea_brief" not in inputs or inputs["idea_brief"] is None:
                data = _load_branch_output(brand_slug, branch_id, "agent_02")
                if data:
                    inputs["idea_brief"] = data

            selected = pipeline_state.get("selected_concepts", [])
            if selected and inputs.get("idea_brief"):
                inputs["selected_concepts"] = selected
                _add_log(f"User selected {len(selected)} video concepts")

            # --- Agent 04: Copywriter ---
            r04 = await _run_copywriter_parallel_async(inputs, loop, run_id, output_dir=output_dir)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": pipeline_state["failed_agents"]}, brand_slug)
                return
            inputs["copywriter_brief"] = r04

            # --- GATE: After Agent 04 → before Agent 05 ---
            await _wait_for_agent_gate("agent_04", "agent_05", "Hook Specialist", phase=3)

            # --- Agent 05: Hook Specialist ---
            r05 = await _run_single_agent_async("agent_05", inputs, loop, run_id, output_dir=output_dir)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                _update_branch(branch_id, {"status": "failed", "failed_agents": pipeline_state["failed_agents"]}, brand_slug)
                return
            inputs["hook_brief"] = r05

        total = time.time() - pipeline_state["start_time"]
        complete_run(run_id, total)
        final_cost = get_usage_summary()
        cost_str = f"${final_cost['total_cost']:.2f}" if final_cost['total_cost'] >= 0.01 else f"${final_cost['total_cost']:.4f}"
        _add_log(f"Branch pipeline complete in {total:.1f}s — total cost: {cost_str}", "success")

        _update_branch(branch_id, {
            "status": "completed",
            "completed_agents": pipeline_state["completed_agents"],
        }, brand_slug)

        await broadcast({
            "type": "pipeline_complete",
            "elapsed": round(total, 1),
            "run_id": run_id,
            "cost": final_cost,
            "branch_id": branch_id,
            "brand_slug": brand_slug,
        })

    except (PipelineAborted, asyncio.CancelledError):
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        _update_branch(branch_id, {"status": "failed"}, brand_slug)
        abort_cost = get_usage_summary()
        _add_log(f"Branch pipeline aborted — cost so far: ${abort_cost['total_cost']:.4f}", "warning")
        try:
            await asyncio.shield(broadcast({
                "type": "pipeline_error",
                "message": "Pipeline aborted by user",
                "aborted": True,
                "cost": abort_cost,
                "branch_id": branch_id,
            }))
        except asyncio.CancelledError:
            pass
    except Exception as e:
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        _update_branch(branch_id, {"status": "failed"}, brand_slug)
        err_cost = get_usage_summary()
        _add_log(f"Branch pipeline error: {e}", "error")
        logger.exception("Branch pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e), "cost": err_cost, "branch_id": branch_id})
    finally:
        pipeline_state["running"] = False
        pipeline_state["abort_requested"] = False
        pipeline_state["pipeline_task"] = None
        pipeline_state["current_phase"] = None
        pipeline_state["current_agent"] = None
        pipeline_state["run_id"] = None
        pipeline_state["active_branch"] = None
        pipeline_state["copywriter_rewrite_in_progress"] = False


class ContinueRequest(BaseModel):
    model_override: dict = {}  # {"provider": "openai", "model": "gpt-5.2"}


@app.post("/api/continue")
async def api_continue(req: ContinueRequest = None):
    """Approve the current phase gate and continue to the next agent."""
    if req is None:
        req = ContinueRequest()
    if pipeline_state.get("copywriter_rewrite_in_progress"):
        return JSONResponse(
            {"error": "Rewrite is in progress. Wait for it to finish before continuing."},
            status_code=409,
        )
    if not pipeline_state["waiting_for_approval"]:
        return JSONResponse(
            {"error": "Pipeline is not waiting for approval"}, status_code=409
        )

    # Store the model override for the next agent
    if req.model_override:
        pipeline_state["next_agent_override"] = req.model_override
        logger.info("Continue with model override: %s", req.model_override)

    gate = pipeline_state.get("phase_gate")
    if gate:
        gate.set()
    return {"status": "continued"}


class RewriteFailedCopywriterRequest(BaseModel):
    model_override: dict = {}  # optional: {"provider": "...", "model": "..."}


@app.post("/api/rewrite-failed-copywriter")
async def api_rewrite_failed_copywriter(req: RewriteFailedCopywriterRequest = None):
    """Retry only failed parallel Agent 04 jobs while paused at the phase gate."""
    if req is None:
        req = RewriteFailedCopywriterRequest()

    if not pipeline_state.get("waiting_for_approval"):
        return JSONResponse(
            {"error": "Pipeline is not paused at a phase gate."},
            status_code=409,
        )

    if pipeline_state.get("copywriter_rewrite_in_progress"):
        return JSONResponse(
            {"error": "Rewrite already in progress."},
            status_code=409,
        )

    failed_jobs = list(pipeline_state.get("copywriter_failed_jobs") or [])
    if not failed_jobs:
        return {"status": "nothing_to_rewrite", "rewritten": 0, "remaining_failed": 0}

    ctx = pipeline_state.get("copywriter_parallel_context") or {}
    base_inputs = dict(ctx.get("base_inputs") or {})
    brand_slug = pipeline_state.get("active_brand_slug") or ""
    if not base_inputs.get("foundation_brief"):
        _auto_load_upstream(base_inputs, ["foundation_brief"], sync_foundation_identity=True, brand_slug=brand_slug)

    base_output_dir = Path(ctx.get("output_dir") or (_brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR))
    run_id = int(ctx.get("run_id") or pipeline_state.get("run_id") or 0)
    jobs_dir = Path(ctx.get("jobs_dir") or (base_output_dir / "agent_04_jobs" / f"run_{run_id}"))

    provider = str(ctx.get("provider") or config.get_agent_llm_config("agent_04")["provider"])
    model = str(ctx.get("model") or config.get_agent_llm_config("agent_04")["model"])

    if req.model_override:
        provider = req.model_override.get("provider", provider)
        model = req.model_override.get("model", model)

    loop = asyncio.get_event_loop()
    started = time.time()
    pipeline_state["copywriter_rewrite_in_progress"] = True
    pipeline_state["current_agent"] = "agent_04"

    model_label = _friendly_model_label(model)
    _add_log(
        f"Retrying {len(failed_jobs)} failed Copywriter scripts [{model_label}]...",
        "info",
    )
    await broadcast({
        "type": "agent_start",
        "slug": "agent_04",
        "name": AGENT_META["agent_04"]["name"],
        "model": model_label,
        "provider": provider,
    })

    try:
        successes, remaining_failures = await _run_copywriter_jobs_parallel(
            jobs=failed_jobs,
            base_inputs=base_inputs,
            loop=loop,
            provider=provider,
            model=model,
            jobs_dir=jobs_dir,
            max_parallel=4,
        )

        new_scripts = [s["script"] for s in successes]
        out_path = base_output_dir / "agent_04_output.json"
        existing_output = {}
        existing_scripts: list[dict[str, Any]] = []
        if out_path.exists():
            try:
                existing_output = json.loads(out_path.read_text("utf-8"))
                data_scripts = existing_output.get("scripts", [])
                if isinstance(data_scripts, list):
                    existing_scripts = [s for s in data_scripts if isinstance(s, dict)]
            except Exception:
                existing_output = {}
                existing_scripts = []

        merged_scripts = existing_scripts + new_scripts
        merged_inputs = {
            "brand_name": existing_output.get("brand_name") or base_inputs.get("brand_name"),
            "product_name": existing_output.get("product_name") or base_inputs.get("product_name"),
            "batch_id": existing_output.get("batch_id") or base_inputs.get("batch_id"),
        }
        merged_output = _build_copywriter_output(merged_inputs, merged_scripts)
        out_path.write_text(json.dumps(merged_output, indent=2), encoding="utf-8")
        logger.info("Output saved: %s", out_path)

        elapsed = time.time() - started
        pipeline_state["copywriter_failed_jobs"] = remaining_failures
        pipeline_state["copywriter_parallel_context"] = {
            **ctx,
            "provider": provider,
            "model": model,
            "output_dir": str(base_output_dir),
            "jobs_dir": str(jobs_dir),
            "run_id": run_id,
            "base_inputs": base_inputs,
        }

        if run_id:
            save_agent_output(
                run_id=run_id,
                agent_slug="agent_04",
                agent_name=AGENT_META["agent_04"]["name"],
                output=merged_output,
                elapsed=elapsed,
            )

        cost = get_usage_summary()
        rewritten = len(new_scripts)
        remaining = len(remaining_failures)
        _add_log(
            f"Copywriter rewrite finished — {rewritten} recovered, {remaining} still failed.",
            "success" if remaining == 0 else "warning",
        )
        await broadcast({
            "type": "agent_complete",
            "slug": "agent_04",
            "name": AGENT_META["agent_04"]["name"],
            "elapsed": round(elapsed, 1),
            "cost": cost,
            "parallel_jobs": len(failed_jobs),
            "failed_jobs": remaining,
        })
        return {
            "status": "completed",
            "rewritten": rewritten,
            "remaining_failed": remaining,
            "cost": cost,
        }
    except Exception as e:
        logger.exception("Failed rewriting copywriter jobs")
        await broadcast({
            "type": "agent_error",
            "slug": "agent_04",
            "name": AGENT_META["agent_04"]["name"],
            "error": str(e),
            "elapsed": round(time.time() - started, 1),
        })
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        pipeline_state["copywriter_rewrite_in_progress"] = False
        pipeline_state["current_agent"] = None


@app.get("/api/status")
async def api_status():
    """Get current pipeline status."""
    return {
        "running": pipeline_state["running"],
        "current_phase": pipeline_state["current_phase"],
        "current_agent": pipeline_state["current_agent"],
        "completed_agents": pipeline_state["completed_agents"],
        "failed_agents": pipeline_state["failed_agents"],
        "elapsed": (
            round(time.time() - pipeline_state["start_time"], 1)
            if pipeline_state["start_time"] and pipeline_state["running"]
            else None
        ),
        "log": pipeline_state["log"][-50:],
        "server_log_tail": list(_recent_server_logs)[-150:],
        "active_brand_slug": pipeline_state.get("active_brand_slug"),
    }


@app.get("/api/outputs")
async def api_list_outputs(brand: str = ""):
    """List all available agent outputs (from disk — brand-scoped)."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    outputs = []
    for slug in AGENT_META:
        path = base / f"{slug}_output.json"
        meta = AGENT_META[slug]
        entry = {
            "slug": slug,
            "name": meta["name"],
            "phase": meta["phase"],
            "icon": meta["icon"],
            "available": path.exists(),
        }
        if path.exists():
            stat = path.stat()
            entry["size_kb"] = round(stat.st_size / 1024, 1)
            entry["modified"] = datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        outputs.append(entry)
    return outputs


@app.get("/api/outputs/{slug}")
async def api_get_output(slug: str, brand: str = ""):
    """Get a specific agent's output (from disk — brand-scoped)."""
    brand_slug = brand or pipeline_state.get("active_brand_slug") or ""
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    path = base / f"{slug}_output.json"
    if not path.exists():
        return JSONResponse({"error": f"No output for {slug}"}, status_code=404)
    data = json.loads(path.read_text("utf-8"))
    meta = AGENT_META.get(slug, {"name": slug, "phase": 0, "icon": ""})
    return {
        "slug": slug,
        "name": meta["name"],
        "phase": meta["phase"],
        "data": data,
    }


@app.get("/api/agent-models")
async def api_agent_models():
    """Return the default model config for each active agent."""
    result = {}
    for slug in AGENT_META:
        conf = config.get_agent_llm_config(slug)
        model_name = conf["model"]
        # Agent 1A uses Deep Research by default
        if slug == "agent_01a":
            label = "Deep Research"
        else:
            label = _friendly_model_label(model_name)
        result[slug] = {
            "provider": conf["provider"],
            "model": model_name,
            "label": label,
        }
    return result


@app.get("/api/health")
async def api_health():
    """Check system health — API keys, config, etc."""
    providers = {
        "openai": bool(config.OPENAI_API_KEY),
        "anthropic": bool(config.ANTHROPIC_API_KEY),
        "google": bool(config.GOOGLE_API_KEY),
    }
    default_ok = providers.get(config.DEFAULT_PROVIDER, False)
    warnings = _check_api_keys()

    return {
        "ok": default_ok,
        "default_provider": config.DEFAULT_PROVIDER,
        "default_model": config.DEFAULT_MODEL,
        "providers": providers,
        "any_provider_configured": any(providers.values()),
        "warnings": warnings,
    }


@app.get("/api/sample-input")
async def api_sample_input(name: str = "animus"):
    """Return a sample input JSON. Use ?name=animus or ?name=nord."""
    filename_map = {
        "animus": "sample_input.json",
        "nord": "sample_input_nord.json",
    }
    filename = filename_map.get(name, "sample_input.json")
    path = Path(filename)
    if path.exists():
        return json.loads(path.read_text())
    return {}


@app.delete("/api/outputs")
async def api_clear_outputs(brand: str = ""):
    """Clear all agent outputs from disk (brand-scoped)."""
    brand_slug = (brand or pipeline_state.get("active_brand_slug") or "").strip()
    base = _brand_output_dir(brand_slug) if brand_slug else config.OUTPUT_DIR
    count = 0
    for slug in AGENT_META:
        path = base / f"{slug}_output.json"
        if path.exists():
            path.unlink()
            count += 1
    return {"cleared": count}


# ---------------------------------------------------------------------------
# Brand API
# ---------------------------------------------------------------------------

@app.get("/api/brands")
async def api_list_brands_endpoint(limit: int = 50):
    """List all brands, most recently opened first."""
    brands = list_brands(limit=limit)
    # Enrich with agent availability
    for b in brands:
        bdir = config.OUTPUT_DIR / b["slug"]
        available_agents = [
            slug for slug in AGENT_META
            if (bdir / f"{slug}_output.json").exists()
        ]
        b["available_agents"] = available_agents
    return brands


@app.get("/api/brands/{brand_slug_param}")
async def api_get_brand_endpoint(brand_slug_param: str):
    """Get a brand with its brief and available agents."""
    brand = get_brand(brand_slug_param)
    if not brand:
        return JSONResponse({"error": "Brand not found"}, status_code=404)
    # Enrich with agent availability
    bdir = config.OUTPUT_DIR / brand_slug_param
    brand["available_agents"] = [
        slug for slug in AGENT_META
        if (bdir / f"{slug}_output.json").exists()
    ]
    # Load branches for this brand
    brand["branches"] = _load_branches(brand_slug_param)
    return brand


@app.post("/api/brands/{brand_slug_param}/open")
async def api_open_brand(brand_slug_param: str):
    """Touch the brand's last_opened_at and set it as the active brand."""
    brand = get_brand(brand_slug_param)
    if not brand:
        return JSONResponse({"error": "Brand not found"}, status_code=404)
    touch_brand(brand_slug_param)
    pipeline_state["active_brand_slug"] = brand_slug_param
    # Enrich with agent availability
    bdir = config.OUTPUT_DIR / brand_slug_param
    brand["available_agents"] = [
        slug for slug in AGENT_META
        if (bdir / f"{slug}_output.json").exists()
    ]
    brand["branches"] = _load_branches(brand_slug_param)
    return brand


@app.delete("/api/brands/{brand_slug_param}")
async def api_delete_brand_endpoint(brand_slug_param: str):
    """Delete a brand, its outputs directory, and all runs."""
    found = storage_delete_brand(brand_slug_param)
    if not found:
        return JSONResponse({"error": "Brand not found"}, status_code=404)
    brand_dir = config.OUTPUT_DIR / brand_slug_param
    if brand_dir.exists():
        shutil.rmtree(brand_dir, ignore_errors=True)
    if pipeline_state.get("active_brand_slug") == brand_slug_param:
        pipeline_state["active_brand_slug"] = None
    return {"ok": True, "deleted": brand_slug_param}


# ---------------------------------------------------------------------------
# Run History API (SQLite-backed)
# ---------------------------------------------------------------------------

@app.get("/api/runs")
async def api_list_runs(limit: int = 50):
    """List past pipeline runs, newest first."""
    return list_runs(limit=limit)


@app.get("/api/runs/{run_id}")
async def api_get_run(run_id: int):
    """Get a specific run with all its agent outputs."""
    run = get_run(run_id)
    if not run:
        return JSONResponse({"error": f"Run #{run_id} not found"}, status_code=404)
    return run


class LabelUpdate(BaseModel):
    label: str


@app.patch("/api/runs/{run_id}")
async def api_update_run(run_id: int, body: LabelUpdate):
    """Update a run's label."""
    update_run_label(run_id, body.label)
    return {"ok": True, "run_id": run_id, "label": body.label.strip()}


@app.delete("/api/runs/{run_id}")
async def api_delete_run(run_id: int):
    """Delete a run and all its agent outputs."""
    found = delete_run(run_id)
    if not found:
        return JSONResponse({"error": f"Run #{run_id} not found"}, status_code=404)
    return {"ok": True, "deleted": run_id}


# ---------------------------------------------------------------------------
# WebSocket for real-time updates
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    try:
        # Build gate info if pipeline is paused at a phase gate
        gate_info = None
        if pipeline_state.get("waiting_for_approval"):
            current_phase = pipeline_state.get("current_phase", 1)
            # Try to reconstruct agent-level gate info
            last_completed = pipeline_state.get("completed_agents", [])
            completed_slug = last_completed[-1] if last_completed else "agent_01a"
            # Map completed agent to next agent
            _next_agent_map = {
                "agent_01a": ("agent_02", "Creative Engine"),
                "agent_02": ("agent_04", "Copywriter"),
                "agent_04": ("agent_05", "Hook Specialist"),
            }
            next_slug, next_name = _next_agent_map.get(completed_slug, ("unknown", "Next Agent"))
            copywriter_failed_count = 0
            if completed_slug == "agent_04":
                copywriter_failed_count = len(pipeline_state.get("copywriter_failed_jobs", []))
            gate_info = {
                "completed_agent": completed_slug,
                "next_agent": next_slug,
                "next_agent_name": next_name,
                "phase": current_phase,
                "show_concept_selection": completed_slug == "agent_02",
                "copywriter_failed_count": copywriter_failed_count,
            }

        await ws.send_json({
            "type": "state_sync",
            "running": pipeline_state["running"],
            "current_phase": pipeline_state["current_phase"],
            "current_agent": pipeline_state["current_agent"],
            "completed_agents": pipeline_state["completed_agents"],
            "log": pipeline_state["log"][-50:],
            "server_log_tail": list(_recent_server_logs)[-150:],
            "waiting_for_approval": pipeline_state.get("waiting_for_approval", False),
            "gate_info": gate_info,
            "active_branch": pipeline_state.get("active_branch"),
            "active_brand_slug": pipeline_state.get("active_brand_slug"),
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)


# ---------------------------------------------------------------------------
# Static files & SPA fallback
# ---------------------------------------------------------------------------

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(static_dir / "index.html"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    print("\n  Creative Maker Pipeline Dashboard")
    print("  http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
