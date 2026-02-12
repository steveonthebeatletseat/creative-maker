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
from typing import Any

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from agents.agent_01a_foundation_research import Agent01AFoundationResearch
from agents.agent_01b_trend_intel import Agent01BTrendIntel
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_03_stress_tester_p1 import Agent03StressTesterP1
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from agents.agent_06_stress_tester_p2 import Agent06StressTesterP2
from agents.agent_07_versioning_engine import Agent07VersioningEngine
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    yield
    # Shutdown (nothing to do)


app = FastAPI(title="Creative Maker Pipeline", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

pipeline_state: dict[str, Any] = {
    "running": False,
    "current_phase": None,
    "current_agent": None,
    "completed_agents": [],
    "failed_agents": [],
    "start_time": None,
    "log": [],
    "run_id": None,  # current SQLite run_id
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


def _run_agent_sync(slug: str, inputs: dict) -> dict | None:
    """Run a single agent synchronously. Returns the output dict or None."""
    cls = AGENT_CLASSES.get(slug)
    if not cls:
        return None
    agent = cls()
    result = agent.run(inputs)
    return json.loads(result.model_dump_json())


def _auto_load_upstream(inputs: dict, needed: list[str]):
    """Load upstream agent outputs from disk into inputs dict."""
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
            slug = mapping.get(key)
            if slug:
                data = _load_output(slug)
                if data:
                    inputs[key] = data


# ---------------------------------------------------------------------------
# Pipeline execution (runs in background task)
# ---------------------------------------------------------------------------

async def _run_single_agent_async(slug: str, inputs: dict, loop, run_id: int) -> dict | None:
    """Run agent in thread pool and broadcast progress. Saves to SQLite."""
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
        result = await loop.run_in_executor(None, _run_agent_sync, slug, inputs)
        elapsed = time.time() - start
        pipeline_state["completed_agents"].append(slug)
        _add_log(f"Completed {meta['icon']} {meta['name']} in {elapsed:.1f}s", "success")
        await broadcast({
            "type": "agent_complete",
            "slug": slug,
            "name": meta["name"],
            "elapsed": round(elapsed, 1),
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


async def run_pipeline_phases(phases: list[int], inputs: dict):
    """Execute requested pipeline phases sequentially."""
    loop = asyncio.get_event_loop()
    pipeline_state["running"] = True
    pipeline_state["completed_agents"] = []
    pipeline_state["failed_agents"] = []
    pipeline_state["start_time"] = time.time()
    pipeline_state["log"] = []

    # Create a DB run record
    run_id = create_run(phases, inputs)
    pipeline_state["run_id"] = run_id

    await broadcast({"type": "pipeline_start", "phases": phases, "run_id": run_id})

    try:
        # Phase 1 — Research (parallel)
        if 1 in phases:
            pipeline_state["current_phase"] = 1
            _add_log("═══ PHASE 1 — RESEARCH ═══")
            await broadcast({"type": "phase_start", "phase": 1})

            results = await asyncio.gather(
                _run_single_agent_async("agent_01a", inputs, loop, run_id),
                _run_single_agent_async("agent_01b", inputs, loop, run_id),
            )
            if results[0]:
                inputs["foundation_brief"] = results[0]
            if results[1]:
                inputs["trend_intel"] = results[1]

            if not results[0]:
                _add_log("Phase 1 failed — Agent 1A is required", "error")
                await broadcast({"type": "pipeline_error", "message": "Agent 1A failed"})
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return

        # Phase 2 — Ideation (serial: 02 → 03)
        if 2 in phases:
            pipeline_state["current_phase"] = 2
            _add_log("═══ PHASE 2 — IDEATION ═══")
            await broadcast({"type": "phase_start", "phase": 2})

            _auto_load_upstream(inputs, ["foundation_brief", "trend_intel"])

            r02 = await _run_single_agent_async("agent_02", inputs, loop, run_id)
            if not r02:
                _add_log("Phase 2 failed — Agent 02 is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["idea_brief"] = r02

            r03 = await _run_single_agent_async("agent_03", inputs, loop, run_id)
            if not r03:
                _add_log("Phase 2 failed — Agent 03 is required", "error")
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["stress_test_brief"] = r03

        # Phase 3 — Scripting (serial: 04 → 05 → 06 → 07)
        if 3 in phases:
            pipeline_state["current_phase"] = 3
            _add_log("═══ PHASE 3 — SCRIPTING ═══")
            await broadcast({"type": "phase_start", "phase": 3})

            _auto_load_upstream(inputs, [
                "foundation_brief", "trend_intel", "stress_test_brief",
            ])

            r04 = await _run_single_agent_async("agent_04", inputs, loop, run_id)
            if not r04:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["copywriter_brief"] = r04

            r05 = await _run_single_agent_async("agent_05", inputs, loop, run_id)
            if not r05:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["hook_brief"] = r05

            r06 = await _run_single_agent_async("agent_06", inputs, loop, run_id)
            if not r06:
                total = time.time() - pipeline_state["start_time"]
                fail_run(run_id, total)
                return
            inputs["stress_test_p2_brief"] = r06

            await _run_single_agent_async("agent_07", inputs, loop, run_id)

        total = time.time() - pipeline_state["start_time"]
        complete_run(run_id, total)
        _add_log(f"Pipeline complete in {total:.1f}s", "success")
        await broadcast({"type": "pipeline_complete", "elapsed": round(total, 1), "run_id": run_id})

    except Exception as e:
        total = time.time() - pipeline_state["start_time"]
        fail_run(run_id, total)
        _add_log(f"Pipeline error: {e}", "error")
        logger.exception("Pipeline failed")
        await broadcast({"type": "pipeline_error", "message": str(e)})
    finally:
        pipeline_state["running"] = False
        pipeline_state["current_phase"] = None
        pipeline_state["current_agent"] = None
        pipeline_state["run_id"] = None


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    phases: list[int] = [1, 2, 3]
    inputs: dict = {}


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

    asyncio.create_task(run_pipeline_phases(req.phases, inputs))
    return {"status": "started", "phases": req.phases}


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


@app.get("/api/sample-input")
async def api_sample_input():
    """Return the sample input JSON."""
    path = Path("sample_input.json")
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
