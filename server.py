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
import json
import logging
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
from agents.agent_01a2_angle_architect import Agent01A2AngleArchitect
from agents.agent_01b_trend_intel import Agent01BTrendIntel
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_03_stress_tester_p1 import Agent03StressTesterP1
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from agents.agent_06_stress_tester_p2 import Agent06StressTesterP2
from agents.agent_07_versioning_engine import Agent07VersioningEngine
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
    yield
    # Shutdown (nothing to do)


app = FastAPI(title="Creative Maker Pipeline", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

pipeline_state: dict[str, Any] = {
    "running": False,
    "abort_requested": False,
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
# Agent runner helpers (sync, called in thread)
# ---------------------------------------------------------------------------

AGENT_CLASSES = {
    "agent_01a": Agent01AFoundationResearch,
    "agent_01a2": Agent01A2AngleArchitect,
    "agent_01b": Agent01BTrendIntel,
    "agent_02": Agent02IdeaGenerator,
    "agent_03": Agent03StressTesterP1,
    "agent_04": Agent04Copywriter,
    "agent_05": Agent05HookSpecialist,
    "agent_06": Agent06StressTesterP2,
    "agent_07": Agent07VersioningEngine,
}

AGENT_META = {
    "agent_01a": {"name": "Foundation Research", "phase": 1, "icon": "🔬"},
    "agent_01a2": {"name": "Angle Architect", "phase": 1, "icon": "📐"},
    "agent_01b": {"name": "Trend & Competitive Intel", "phase": 1, "icon": "📡"},
    "agent_02": {"name": "Idea Generator", "phase": 2, "icon": "💡"},
    "agent_03": {"name": "Stress Tester P1", "phase": 2, "icon": "🔍"},
    "agent_04": {"name": "Copywriter", "phase": 3, "icon": "✍️"},
    "agent_05": {"name": "Hook Specialist", "phase": 3, "icon": "🎣"},
    "agent_06": {"name": "Stress Tester P2", "phase": 3, "icon": "🔍"},
    "agent_07": {"name": "Versioning Engine", "phase": 3, "icon": "🔀"},
}


def _load_output(slug: str) -> dict | None:
    path = config.OUTPUT_DIR / f"{slug}_output.json"
    if path.exists():
        return json.loads(path.read_text("utf-8"))
    return None


def _run_agent_sync(slug: str, inputs: dict, provider: str | None = None, model: str | None = None) -> dict | None:
    """Run a single agent synchronously. Returns the output dict or None."""
    cls = AGENT_CLASSES.get(slug)
    if not cls:
        return None
    agent = cls(provider=provider, model=model)
    result = agent.run(inputs)
    return json.loads(result.model_dump_json())


def _auto_load_upstream(inputs: dict, needed: list[str]):
    """Load upstream agent outputs from disk into inputs dict.

    For foundation_brief, merges 1A (research) + 1A2 (angles) if both exist.
    """
    mapping = {
        "foundation_brief": "agent_01a",
        "trend_intel": "agent_01b",
        "idea_brief": "agent_02",
        "stress_test_brief": "agent_03",
        "copywriter_brief": "agent_04",
        "hook_brief": "agent_05",
        "stress_test_p2_brief": "agent_06",
    }
    for key in needed:
        if key not in inputs or inputs[key] is None:
            if key == "foundation_brief":
                # Merge 1A + 1A2 outputs
                fb = _load_output("agent_01a")
                if fb:
                    angles = _load_output("agent_01a2")
                    if angles:
                        fb["angle_inventory"] = angles.get("angle_inventory", [])
                        fb["testing_plan"] = angles.get("testing_plan", {})
                        fb["distribution_audit"] = angles.get("distribution_audit", {})
                    inputs[key] = fb
            else:
                slug = mapping.get(key)
                if slug:
                    data = _load_output(slug)
                    if data:
                        inputs[key] = data


# ---------------------------------------------------------------------------
# Pipeline execution (runs in background task)
# ---------------------------------------------------------------------------

class PipelineAborted(Exception):
    """Raised when the pipeline is aborted by the user."""
    pass


async def _run_single_agent_async(slug: str, inputs: dict, loop, run_id: int, provider: str | None = None, model: str | None = None) -> dict | None:
    """Run agent in thread pool and broadcast progress. Saves to SQLite."""
    # Check abort flag before starting this agent
    if pipeline_state["abort_requested"]:
        raise PipelineAborted("Pipeline aborted by user")

    meta = AGENT_META[slug]
    pipeline_state["current_agent"] = slug
    _add_log(f"Starting {meta['icon']} {meta['name']}...")
    await broadcast({
        "type": "agent_start",
        "slug": slug,
        "name": meta["name"],
    })

    start = time.time()
    try:
        result = await loop.run_in_executor(None, _run_agent_sync, slug, inputs, provider, model)
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


async def run_pipeline_phases(phases: list[int], inputs: dict, provider: str | None = None, model: str | None = None):
    """Execute requested pipeline phases sequentially."""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["abort_requested"] = False
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []

    # Reset the LLM cost tracker for this run
    reset_usage()

    # Create a DB run record
    run_id = create_run(phases, inputs)
    pipeline_state["run_id"] = run_id

    await broadcast({"type": "pipeline_start", "phases": phases, "run_id": run_id})

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

        # Phase 1 — Research (1A + 1B parallel, then 1A2 sequential)
        if 1 in phases:
            pipeline_state["current_phase"] = 1
            _add_log("═══ PHASE 1 — RESEARCH ═══")
            await broadcast({"type": "phase_start", "phase": 1})

            # Step 1: Run 1A and 1B in parallel
            results = await asyncio.gather(
                _run_single_agent_async("agent_01a", inputs, loop, run_id, provider, model),
                _run_single_agent_async("agent_01b", inputs, loop, run_id, provider, model),
            )

            if not results[0]:
                # Collect the actual error messages from the failed agents
                errors = []
                for slug in ["agent_01a", "agent_01b"]:
                    if slug in pipeline_state["failed_agents"]:
                        for entry in reversed(pipeline_state["log"]):
                            if slug.replace("agent_0", "Agent ") in entry.get("message", "") and entry.get("level") == "error":
                                errors.append(entry["message"])
                                break

                error_detail = errors[0] if errors else "Agent 1A failed (unknown reason)"
                _add_log("Phase 1 failed — Agent 1A is required", "error")
                await broadcast({"type": "pipeline_error", "message": error_detail})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

            # Feed 1A output for 1A2
            inputs["foundation_brief"] = results[0]
            if results[1]:
                inputs["trend_intel"] = results[1]

            # Step 2: Run 1A2 (Angle Architect) sequentially after 1A
            _add_log("Running Angle Architect (1A2) with Foundation Research...")
            r1a2 = await _run_single_agent_async("agent_01a2", inputs, loop, run_id, provider, model)

            # Merge 1A + 1A2 into a single foundation_brief for downstream
            if r1a2:
                merged = dict(results[0])
                merged["angle_inventory"] = r1a2.get("angle_inventory", [])
                merged["testing_plan"] = r1a2.get("testing_plan", {})
                merged["distribution_audit"] = r1a2.get("distribution_audit", {})
                inputs["foundation_brief"] = merged
            else:
                _add_log(
                    "Agent 1A2 (Angle Architect) failed — downstream agents will "
                    "not have angle inventory", "warning"
                )

        # Abort check between phases
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- PHASE GATE: Wait for user approval before Phase 2+ ---
        if 1 in phases and (2 in phases or 3 in phases):
            _add_log("Phase 1 complete — review the research outputs, then click Continue when ready.")
            pipeline_state["waiting_for_approval"] = True
            pipeline_state["phase_gate"] = asyncio.Event()
            await broadcast({
                "type": "phase_gate",
                "completed_phase": 1,
                "next_phase": 2,
                "message": "Phase 1 complete. Review the research, then continue when satisfied.",
            })
            # Wait until user clicks Continue (or aborts)
            await pipeline_state["phase_gate"].wait()
            pipeline_state["waiting_for_approval"] = False

            if pipeline_state["abort_requested"]:
                raise PipelineAborted("Pipeline aborted by user")

            _add_log("Approval received — continuing to Phase 2...")
            await broadcast({"type": "phase_gate_cleared"})

        # Phase 2 — Ideation (serial: 02 → 03)
        if 2 in phases:
            pipeline_state["current_phase"] = 2
            _add_log("═══ PHASE 2 — IDEATION ═══")
            await broadcast({"type": "phase_start", "phase": 2})

            _auto_load_upstream(inputs, ["foundation_brief", "trend_intel"])

            r02 = await _run_single_agent_async("agent_02", inputs, loop, run_id, provider, model)
            if not r02:
                _add_log("Phase 2 failed — Agent 02 is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["idea_brief"] = r02

            r03 = await _run_single_agent_async("agent_03", inputs, loop, run_id, provider, model)
            if not r03:
                _add_log("Phase 2 failed — Agent 03 is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["stress_test_brief"] = r03

        # Abort check between phases
        if pipeline_state["abort_requested"]:
            raise PipelineAborted("Pipeline aborted by user")

        # --- PHASE GATE: Wait for user approval before Phase 3 ---
        if 2 in phases and 3 in phases:
            _add_log("Phase 2 complete — review the ideas, then click Continue when ready.")
            pipeline_state["waiting_for_approval"] = True
            pipeline_state["phase_gate"] = asyncio.Event()
            await broadcast({
                "type": "phase_gate",
                "completed_phase": 2,
                "next_phase": 3,
                "message": "Phase 2 complete. Review the ideas, then continue when satisfied.",
            })
            await pipeline_state["phase_gate"].wait()
            pipeline_state["waiting_for_approval"] = False

            if pipeline_state["abort_requested"]:
                raise PipelineAborted("Pipeline aborted by user")

            _add_log("Approval received — continuing to Phase 3...")
            await broadcast({"type": "phase_gate_cleared"})

        # Phase 3 — Scripting (serial: 04 → 05 → 06 → 07)
        if 3 in phases:
            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3})

            _auto_load_upstream(inputs, [
                "foundation_brief", "trend_intel", "stress_test_brief",
            ])

            r04 = await _run_single_agent_async("agent_04", inputs, loop, run_id, provider, model)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["copywriter_brief"] = r04

            r05 = await _run_single_agent_async("agent_05", inputs, loop, run_id, provider, model)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["hook_brief"] = r05

            r06 = await _run_single_agent_async("agent_06", inputs, loop, run_id, provider, model)
            if not r06:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["stress_test_p2_brief"] = r06

            await _run_single_agent_async("agent_07", inputs, loop, run_id, provider, model)

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


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    phases: list[int] = [1, 2, 3]
    inputs: dict = {}
    quick_mode: bool = False  # Skip web research in Phase 1 (fast testing)


@app.post("/api/run")
async def api_run(req: RunRequest):
    """Kick off pipeline phases."""
    if pipeline_state["running"]:
        return JSONResponse(
            {"error": "Pipeline is already running"}, status_code=409
        )

    inputs = {k: v for k, v in req.inputs.items() if v}

    if not inputs.get("brand_name"):
        return JSONResponse(
            {"error": "Brand name is required"}, status_code=400
        )

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

    task = asyncio.create_task(run_pipeline_phases(req.phases, inputs, override_provider, override_model))
    pipeline_state["pipeline_task"] = task
    return {"status": "started", "phases": req.phases, "quick_mode": req.quick_mode}


@app.post("/api/abort")
async def api_abort():
    """Abort the currently running pipeline immediately."""
    if not pipeline_state["running"]:
        return JSONResponse({"error": "No pipeline is running"}, status_code=409)

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

    # Quick mode overrides
    override_provider = None
    override_model = None
    if req.quick_mode:
        inputs["_quick_mode"] = True
        override_provider = "google"
        override_model = "gemini-2.5-flash"

    # Auto-load upstream outputs from disk
    needed = ["foundation_brief", "trend_intel", "idea_brief",
              "stress_test_brief", "copywriter_brief", "hook_brief",
              "stress_test_p2_brief"]
    _auto_load_upstream(inputs, needed)

    # Run the single agent in a thread pool
    loop = asyncio.get_event_loop()
    start = time.time()

    try:
        result = await loop.run_in_executor(
            None,
            _run_agent_sync,
            req.slug,
            inputs,
            override_provider,
            override_model,
        )
        elapsed = round(time.time() - start, 1)

        if result is None:
            return JSONResponse(
                {"error": f"Agent {req.slug} returned no output"}, status_code=500
            )

        # Get cost data
        from pipeline.llm import get_usage_summary
        cost = get_usage_summary()

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


@app.post("/api/continue")
async def api_continue():
    """Approve the current phase gate and continue to the next phase."""
    if not pipeline_state["waiting_for_approval"]:
        return JSONResponse(
            {"error": "Pipeline is not waiting for approval"}, status_code=409
        )

    gate = pipeline_state.get("phase_gate")
    if gate:
        gate.set()
    return {"status": "continued"}


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
    }


@app.get("/api/outputs")
async def api_list_outputs():
    """List all available agent outputs (from disk — latest run)."""
    outputs = []
    for slug in AGENT_META:
        path = config.OUTPUT_DIR / f"{slug}_output.json"
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
async def api_get_output(slug: str):
    """Get a specific agent's output (from disk — latest run)."""
    path = config.OUTPUT_DIR / f"{slug}_output.json"
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
async def api_clear_outputs():
    """Clear all agent outputs from disk."""
    count = 0
    for slug in AGENT_META:
        path = config.OUTPUT_DIR / f"{slug}_output.json"
        if path.exists():
            path.unlink()
            count += 1
    return {"cleared": count}


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
        await ws.send_json({
            "type": "state_sync",
            "running": pipeline_state["running"],
            "current_phase": pipeline_state["current_phase"],
            "current_agent": pipeline_state["current_agent"],
            "completed_agents": pipeline_state["completed_agents"],
            "log": pipeline_state["log"][-50:],
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
