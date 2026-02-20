"""SQLite storage for pipeline run history and brand management.

Stores brands, pipeline runs, and agent outputs so you can
browse past results, switch between brands, and never lose data.

Uses Python's built-in sqlite3 — zero dependencies.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import config

logger = logging.getLogger(__name__)

DB_PATH = config.ROOT_DIR / "creative_maker.db"

# Thread-local connections (sqlite3 objects can't be shared across threads)
_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


def init_db():
    """Create tables if they don't exist. Call once at startup."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS brands (
            slug            TEXT    PRIMARY KEY,
            brand_name      TEXT    NOT NULL,
            product_name    TEXT    NOT NULL DEFAULT '',
            brief_json      TEXT    NOT NULL DEFAULT '{}',
            created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            last_opened_at  TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            phases          TEXT    NOT NULL DEFAULT '1,2,3',
            status          TEXT    NOT NULL DEFAULT 'running',
            inputs_json     TEXT    NOT NULL DEFAULT '{}',
            elapsed_seconds REAL,
            label           TEXT    DEFAULT '',
            brand_slug      TEXT    DEFAULT '',
            total_cost_usd  REAL    NOT NULL DEFAULT 0.0
        );

        CREATE TABLE IF NOT EXISTS agent_outputs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            agent_slug      TEXT    NOT NULL,
            agent_name      TEXT    NOT NULL DEFAULT '',
            status          TEXT    NOT NULL DEFAULT 'running',
            output_json     TEXT,
            error_message   TEXT,
            elapsed_seconds REAL,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_agent_outputs_run
            ON agent_outputs(run_id);

        CREATE TABLE IF NOT EXISTS video_runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            video_run_id      TEXT    NOT NULL UNIQUE,
            brand_slug        TEXT    NOT NULL,
            branch_id         TEXT    NOT NULL,
            phase3_run_id     TEXT    NOT NULL,
            status            TEXT    NOT NULL DEFAULT 'active',
            workflow_state    TEXT    NOT NULL DEFAULT 'draft',
            voice_preset_id   TEXT    NOT NULL,
            reviewer_role     TEXT    NOT NULL DEFAULT 'operator',
            drive_folder_url  TEXT    NOT NULL DEFAULT '',
            parallelism       INTEGER NOT NULL DEFAULT 1,
            error             TEXT    NOT NULL DEFAULT '',
            metrics_json      TEXT    NOT NULL DEFAULT '{}',
            created_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at        TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            completed_at      TEXT    NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS video_clips (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            clip_id               TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            scene_unit_id         TEXT    NOT NULL DEFAULT '',
            scene_line_id         TEXT    NOT NULL DEFAULT '',
            brief_unit_id         TEXT    NOT NULL DEFAULT '',
            hook_id               TEXT    NOT NULL DEFAULT '',
            arm                   TEXT    NOT NULL DEFAULT '',
            script_line_id        TEXT    NOT NULL DEFAULT '',
            mode                  TEXT    NOT NULL DEFAULT 'b_roll',
            status                TEXT    NOT NULL DEFAULT 'pending',
            current_revision_index INTEGER NOT NULL DEFAULT 1,
            line_index            INTEGER NOT NULL DEFAULT 0,
            narration_text        TEXT    NOT NULL DEFAULT '',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS video_clip_revisions (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            revision_id           TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            clip_id               TEXT    NOT NULL REFERENCES video_clips(clip_id) ON DELETE CASCADE,
            revision_index        INTEGER NOT NULL,
            status                TEXT    NOT NULL DEFAULT 'pending',
            created_by            TEXT    NOT NULL DEFAULT '',
            operator_note         TEXT    NOT NULL DEFAULT '',
            input_snapshot_json   TEXT    NOT NULL DEFAULT '{}',
            provenance_json       TEXT    NOT NULL DEFAULT '{}',
            qc_json               TEXT    NOT NULL DEFAULT '{}',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            UNIQUE(clip_id, revision_index)
        );

        CREATE TABLE IF NOT EXISTS video_assets (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id              TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            clip_id               TEXT    NOT NULL DEFAULT '',
            revision_id           TEXT    NOT NULL DEFAULT '',
            asset_type            TEXT    NOT NULL DEFAULT '',
            storage_path          TEXT    NOT NULL DEFAULT '',
            source_url            TEXT    NOT NULL DEFAULT '',
            file_name             TEXT    NOT NULL DEFAULT '',
            mime_type             TEXT    NOT NULL DEFAULT '',
            byte_size             INTEGER NOT NULL DEFAULT 0,
            checksum_sha256       TEXT    NOT NULL DEFAULT '',
            metadata_json         TEXT    NOT NULL DEFAULT '{}',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS video_provider_calls (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_call_id      TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            clip_id               TEXT    NOT NULL DEFAULT '',
            revision_id           TEXT    NOT NULL DEFAULT '',
            provider_name         TEXT    NOT NULL DEFAULT '',
            operation             TEXT    NOT NULL DEFAULT '',
            idempotency_key       TEXT    NOT NULL UNIQUE,
            status                TEXT    NOT NULL DEFAULT 'submitted',
            request_json          TEXT    NOT NULL DEFAULT '{}',
            response_json         TEXT    NOT NULL DEFAULT '{}',
            error                 TEXT    NOT NULL DEFAULT '',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS video_validation_reports (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id             TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            status                TEXT    NOT NULL DEFAULT 'failed',
            folder_url            TEXT    NOT NULL DEFAULT '',
            summary_json          TEXT    NOT NULL DEFAULT '{}',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS video_validation_items (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id             TEXT    NOT NULL REFERENCES video_validation_reports(report_id) ON DELETE CASCADE,
            filename              TEXT    NOT NULL DEFAULT '',
            file_role             TEXT    NOT NULL DEFAULT '',
            required              INTEGER NOT NULL DEFAULT 1,
            status                TEXT    NOT NULL DEFAULT 'ok',
            issue_code            TEXT    NOT NULL DEFAULT '',
            message               TEXT    NOT NULL DEFAULT '',
            remediation           TEXT    NOT NULL DEFAULT '',
            asset_json            TEXT    NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS video_operator_actions (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            action_id             TEXT    NOT NULL UNIQUE,
            video_run_id          TEXT    NOT NULL REFERENCES video_runs(video_run_id) ON DELETE CASCADE,
            clip_id               TEXT    NOT NULL DEFAULT '',
            revision_id           TEXT    NOT NULL DEFAULT '',
            action_type           TEXT    NOT NULL DEFAULT '',
            actor                 TEXT    NOT NULL DEFAULT '',
            action_payload_json   TEXT    NOT NULL DEFAULT '{}',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_video_runs_branch
            ON video_runs(brand_slug, branch_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_clips_run
            ON video_clips(video_run_id, line_index);
        CREATE INDEX IF NOT EXISTS idx_video_revisions_clip
            ON video_clip_revisions(clip_id, revision_index DESC);
        CREATE INDEX IF NOT EXISTS idx_video_assets_run
            ON video_assets(video_run_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_provider_calls_run
            ON video_provider_calls(video_run_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_validation_reports_run
            ON video_validation_reports(video_run_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_actions_run
            ON video_operator_actions(video_run_id, created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_video_runs_single_active
            ON video_runs((1))
            WHERE status='active';
    """)

    # Migration: add brand_slug column if missing (existing DBs)
    try:
        conn.execute("ALTER TABLE pipeline_runs ADD COLUMN brand_slug TEXT DEFAULT ''")
        conn.commit()
        logger.info("Migrated pipeline_runs: added brand_slug column")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Migration: add total_cost_usd column if missing (existing DBs)
    try:
        conn.execute("ALTER TABLE pipeline_runs ADD COLUMN total_cost_usd REAL NOT NULL DEFAULT 0.0")
        conn.commit()
        logger.info("Migrated pipeline_runs: added total_cost_usd column")
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    logger.info("SQLite database initialized: %s", DB_PATH)


# ---------------------------------------------------------------------------
# Brand slug helper
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    """Convert brand name to a URL/filesystem-safe slug."""
    slug = name.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug or "unnamed"


# ---------------------------------------------------------------------------
# Brands
# ---------------------------------------------------------------------------

def create_brand(brand_name: str, product_name: str, brief_inputs: dict) -> str:
    """Create a new brand. Returns the slug."""
    conn = _get_conn()
    slug = _slugify(brand_name)

    # Handle collision: append -2, -3, etc.
    base_slug = slug
    counter = 1
    while conn.execute("SELECT 1 FROM brands WHERE slug=?", (slug,)).fetchone():
        counter += 1
        slug = f"{base_slug}-{counter}"

    conn.execute(
        "INSERT INTO brands (slug, brand_name, product_name, brief_json) VALUES (?,?,?,?)",
        (slug, brand_name.strip(), (product_name or "").strip(), json.dumps(brief_inputs, default=str)),
    )
    conn.commit()
    logger.info("Created brand: %s (%s)", slug, brand_name)
    return slug


def get_brand(slug: str) -> dict | None:
    """Get a brand by slug."""
    conn = _get_conn()
    row = conn.execute("SELECT * FROM brands WHERE slug=?", (slug,)).fetchone()
    if not row:
        return None

    brief = {}
    try:
        brief = json.loads(row["brief_json"])
    except Exception:
        pass

    return {
        "slug": row["slug"],
        "brand_name": row["brand_name"],
        "product_name": row["product_name"],
        "brief": brief,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_opened_at": row["last_opened_at"],
    }


def list_brands(limit: int = 50) -> list[dict]:
    """List brands, most recently opened first."""
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT slug, brand_name, product_name, created_at, updated_at, last_opened_at
        FROM brands
        ORDER BY last_opened_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    return [
        {
            "slug": r["slug"],
            "brand_name": r["brand_name"],
            "product_name": r["product_name"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "last_opened_at": r["last_opened_at"],
        }
        for r in rows
    ]


def update_brand_brief(slug: str, brief_inputs: dict):
    """Update a brand's brief and touch updated_at."""
    conn = _get_conn()
    # Also update brand_name/product_name if they changed in the brief
    brand_name = brief_inputs.get("brand_name", "").strip()
    product_name = brief_inputs.get("product_name", "").strip()
    conn.execute(
        """
        UPDATE brands
        SET brief_json=?, updated_at=datetime('now','localtime'),
            brand_name=CASE WHEN ?!='' THEN ? ELSE brand_name END,
            product_name=CASE WHEN ?!='' THEN ? ELSE product_name END
        WHERE slug=?
        """,
        (json.dumps(brief_inputs, default=str), brand_name, brand_name, product_name, product_name, slug),
    )
    conn.commit()


def touch_brand(slug: str):
    """Update a brand's last_opened_at timestamp."""
    conn = _get_conn()
    conn.execute(
        "UPDATE brands SET last_opened_at=datetime('now','localtime') WHERE slug=?",
        (slug,),
    )
    conn.commit()


def delete_brand(slug: str) -> bool:
    """Delete a brand. Returns True if found."""
    conn = _get_conn()
    # Also delete associated pipeline runs
    conn.execute("DELETE FROM pipeline_runs WHERE brand_slug=?", (slug,))
    cur = conn.execute("DELETE FROM brands WHERE slug=?", (slug,))
    conn.commit()
    return cur.rowcount > 0


def get_or_create_brand(brand_name: str, product_name: str, brief_inputs: dict) -> str:
    """If a brand with this name exists, update its brief and return its slug.
    Otherwise create a new brand. Returns the brand slug."""
    slug = _slugify(brand_name)
    existing = get_brand(slug)
    if existing:
        update_brand_brief(slug, brief_inputs)
        touch_brand(slug)
        return slug
    return create_brand(brand_name, product_name, brief_inputs)


# ---------------------------------------------------------------------------
# Pipeline runs
# ---------------------------------------------------------------------------

def create_run(phases: list[int], inputs: dict, brand_slug: str = "") -> int:
    """Insert a new pipeline run. Returns the run_id."""
    conn = _get_conn()
    cur = conn.execute(
        """
        INSERT INTO pipeline_runs (phases, status, inputs_json, brand_slug)
        VALUES (?, 'running', ?, ?)
        """,
        (",".join(str(p) for p in phases), json.dumps(inputs, default=str), brand_slug),
    )
    conn.commit()
    run_id = cur.lastrowid
    logger.info("Created pipeline run #%d (phases=%s, brand=%s)", run_id, phases, brand_slug)
    return run_id


def complete_run(run_id: int, elapsed: float):
    """Mark a run as completed."""
    conn = _get_conn()
    conn.execute(
        "UPDATE pipeline_runs SET status='completed', elapsed_seconds=? WHERE id=?",
        (round(elapsed, 1), run_id),
    )
    conn.commit()


def update_run_cost(run_id: int, total_cost_usd: float):
    """Persist running/final cost for a run."""
    conn = _get_conn()
    conn.execute(
        "UPDATE pipeline_runs SET total_cost_usd=? WHERE id=?",
        (round(float(total_cost_usd or 0.0), 6), run_id),
    )
    conn.commit()


def fail_run(run_id: int, elapsed: float):
    """Mark a run as failed."""
    conn = _get_conn()
    conn.execute(
        "UPDATE pipeline_runs SET status='failed', elapsed_seconds=? WHERE id=?",
        (round(elapsed, 1), run_id),
    )
    conn.commit()


def update_run_label(run_id: int, label: str):
    """Update a run's label."""
    conn = _get_conn()
    conn.execute(
        "UPDATE pipeline_runs SET label=? WHERE id=?",
        (label.strip(), run_id),
    )
    conn.commit()


def delete_run(run_id: int) -> bool:
    """Delete a run and its agent outputs. Returns True if found."""
    conn = _get_conn()
    # CASCADE will handle agent_outputs
    cur = conn.execute("DELETE FROM pipeline_runs WHERE id=?", (run_id,))
    conn.commit()
    return cur.rowcount > 0


def list_runs(limit: int = 50) -> list[dict]:
    """List recent runs, newest first."""
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT
            r.id,
            r.created_at,
            r.phases,
            r.status,
            r.elapsed_seconds,
            r.label,
            r.brand_slug,
            r.total_cost_usd,
            (SELECT COUNT(*) FROM agent_outputs ao WHERE ao.run_id = r.id AND ao.status='completed') AS agent_count
        FROM pipeline_runs r
        ORDER BY r.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    results = []
    for row in rows:
        # Extract brand name from inputs for display
        brand = ""
        try:
            inputs_data = json.loads(
                conn.execute("SELECT inputs_json FROM pipeline_runs WHERE id=?", (row["id"],)).fetchone()["inputs_json"]
            )
            brand = inputs_data.get("brand_name", "")
        except Exception:
            pass

        results.append({
            "id": row["id"],
            "created_at": row["created_at"],
            "phases": row["phases"],
            "status": row["status"],
            "elapsed_seconds": row["elapsed_seconds"],
            "label": row["label"] or "",
            "agent_count": row["agent_count"],
            "brand_name": brand,
            "brand_slug": row["brand_slug"] or "",
            "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
        })
    return results


def get_run(run_id: int) -> dict | None:
    """Get a single run with all its agent outputs."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE id=?", (run_id,)
    ).fetchone()
    if not row:
        return None

    agents = conn.execute(
        """
        SELECT agent_slug, agent_name, status, output_json, error_message, elapsed_seconds, created_at
        FROM agent_outputs
        WHERE run_id=?
        ORDER BY id
        """,
        (run_id,),
    ).fetchall()

    agent_list = []
    for a in agents:
        entry: dict[str, Any] = {
            "agent_slug": a["agent_slug"],
            "agent_name": a["agent_name"],
            "status": a["status"],
            "elapsed_seconds": a["elapsed_seconds"],
            "created_at": a["created_at"],
        }
        if a["output_json"]:
            entry["data"] = json.loads(a["output_json"])
        if a["error_message"]:
            entry["error"] = a["error_message"]
        agent_list.append(entry)

    inputs = {}
    try:
        inputs = json.loads(row["inputs_json"])
    except Exception:
        pass

    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "phases": row["phases"],
        "status": row["status"],
        "elapsed_seconds": row["elapsed_seconds"],
        "label": row["label"] or "",
        "inputs": inputs,
        "agents": agent_list,
        "brand_slug": row["brand_slug"] or "",
        "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
    }


def get_latest_run_cost(brand_slug: str = "") -> float | None:
    """Return the latest run cost, optionally scoped to a brand."""
    conn = _get_conn()
    brand = str(brand_slug or "").strip()
    if brand:
        row = conn.execute(
            """
            SELECT total_cost_usd
            FROM pipeline_runs
            WHERE brand_slug=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (brand,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT total_cost_usd
            FROM pipeline_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    return round(float(row["total_cost_usd"] or 0.0), 6)


# ---------------------------------------------------------------------------
# Agent outputs
# ---------------------------------------------------------------------------

def save_agent_output(
    run_id: int,
    agent_slug: str,
    agent_name: str,
    output: dict | None,
    elapsed: float,
    error: str | None = None,
):
    """Save a completed or failed agent output."""
    conn = _get_conn()
    status = "completed" if output else "failed"
    conn.execute(
        """
        INSERT INTO agent_outputs (run_id, agent_slug, agent_name, status, output_json, error_message, elapsed_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            agent_slug,
            agent_name,
            status,
            json.dumps(output) if output else None,
            error,
            round(elapsed, 1),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Phase 4 video runs
# ---------------------------------------------------------------------------

def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str)


def _json_loads(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _row_to_video_run(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "video_run_id": row["video_run_id"],
        "brand_slug": row["brand_slug"],
        "branch_id": row["branch_id"],
        "phase3_run_id": row["phase3_run_id"],
        "status": row["status"],
        "workflow_state": row["workflow_state"],
        "voice_preset_id": row["voice_preset_id"],
        "reviewer_role": row["reviewer_role"],
        "drive_folder_url": row["drive_folder_url"] or "",
        "parallelism": int(row["parallelism"] or 1),
        "error": row["error"] or "",
        "metrics": _json_loads(row["metrics_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "completed_at": row["completed_at"] or "",
    }


def create_video_run(
    *,
    video_run_id: str,
    brand_slug: str,
    branch_id: str,
    phase3_run_id: str,
    voice_preset_id: str,
    reviewer_role: str = "operator",
    status: str = "active",
    workflow_state: str = "draft",
    parallelism: int = 1,
    drive_folder_url: str = "",
    metrics: dict[str, Any] | None = None,
) -> dict:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_runs (
            video_run_id,
            brand_slug,
            branch_id,
            phase3_run_id,
            status,
            workflow_state,
            voice_preset_id,
            reviewer_role,
            drive_folder_url,
            parallelism,
            metrics_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        """,
        (
            str(video_run_id),
            str(brand_slug),
            str(branch_id),
            str(phase3_run_id),
            str(status),
            str(workflow_state),
            str(voice_preset_id),
            str(reviewer_role or "operator"),
            str(drive_folder_url or ""),
            max(1, int(parallelism or 1)),
            _json_dumps(metrics or {}),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM video_runs WHERE video_run_id=?",
        (str(video_run_id),),
    ).fetchone()
    result = _row_to_video_run(row)
    if not result:
        raise RuntimeError("Failed to create video run")
    return result


def list_video_runs_for_branch(brand_slug: str, branch_id: str, limit: int = 100) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT *
        FROM video_runs
        WHERE brand_slug=? AND branch_id=?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (str(brand_slug), str(branch_id), int(limit)),
    ).fetchall()
    return [_row_to_video_run(row) for row in rows if row]


def get_video_run(video_run_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM video_runs WHERE video_run_id=?",
        (str(video_run_id),),
    ).fetchone()
    return _row_to_video_run(row)


def update_video_run(
    video_run_id: str,
    *,
    status: str | None = None,
    workflow_state: str | None = None,
    drive_folder_url: str | None = None,
    error: str | None = None,
    completed_at: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> dict | None:
    conn = _get_conn()
    updates: list[str] = ["updated_at=datetime('now','localtime')"]
    values: list[Any] = []
    if status is not None:
        updates.append("status=?")
        values.append(str(status))
    if workflow_state is not None:
        updates.append("workflow_state=?")
        values.append(str(workflow_state))
    if drive_folder_url is not None:
        updates.append("drive_folder_url=?")
        values.append(str(drive_folder_url))
    if error is not None:
        updates.append("error=?")
        values.append(str(error))
    if completed_at is not None:
        updates.append("completed_at=?")
        values.append(str(completed_at))
    if metrics is not None:
        updates.append("metrics_json=?")
        values.append(_json_dumps(metrics))
    values.append(str(video_run_id))
    conn.execute(
        f"UPDATE video_runs SET {', '.join(updates)} WHERE video_run_id=?",
        tuple(values),
    )
    conn.commit()
    return get_video_run(video_run_id)


def _row_to_video_clip(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "clip_id": row["clip_id"],
        "video_run_id": row["video_run_id"],
        "scene_unit_id": row["scene_unit_id"] or "",
        "scene_line_id": row["scene_line_id"] or "",
        "brief_unit_id": row["brief_unit_id"] or "",
        "hook_id": row["hook_id"] or "",
        "arm": row["arm"] or "",
        "script_line_id": row["script_line_id"] or "",
        "mode": row["mode"] or "b_roll",
        "status": row["status"] or "pending",
        "current_revision_index": int(row["current_revision_index"] or 1),
        "line_index": int(row["line_index"] or 0),
        "narration_text": row["narration_text"] or "",
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def create_video_clip(
    *,
    clip_id: str,
    video_run_id: str,
    scene_unit_id: str,
    scene_line_id: str,
    brief_unit_id: str,
    hook_id: str,
    arm: str,
    script_line_id: str,
    mode: str,
    line_index: int,
    narration_text: str = "",
    status: str = "pending",
    current_revision_index: int = 1,
) -> dict:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_clips (
            clip_id, video_run_id, scene_unit_id, scene_line_id, brief_unit_id, hook_id, arm,
            script_line_id, mode, status, current_revision_index, line_index, narration_text, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        """,
        (
            str(clip_id),
            str(video_run_id),
            str(scene_unit_id or ""),
            str(scene_line_id or ""),
            str(brief_unit_id or ""),
            str(hook_id or ""),
            str(arm or ""),
            str(script_line_id or ""),
            str(mode or "b_roll"),
            str(status or "pending"),
            max(1, int(current_revision_index or 1)),
            max(0, int(line_index or 0)),
            str(narration_text or ""),
        ),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM video_clips WHERE clip_id=?", (str(clip_id),)).fetchone()
    result = _row_to_video_clip(row)
    if not result:
        raise RuntimeError("Failed to create clip")
    return result


def list_video_clips(video_run_id: str) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT *
        FROM video_clips
        WHERE video_run_id=?
        ORDER BY line_index ASC, created_at ASC
        """,
        (str(video_run_id),),
    ).fetchall()
    return [_row_to_video_clip(row) for row in rows if row]


def get_video_clip(clip_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM video_clips WHERE clip_id=?", (str(clip_id),)).fetchone()
    return _row_to_video_clip(row)


def update_video_clip(
    clip_id: str,
    *,
    status: str | None = None,
    current_revision_index: int | None = None,
    narration_text: str | None = None,
) -> dict | None:
    conn = _get_conn()
    updates: list[str] = ["updated_at=datetime('now','localtime')"]
    values: list[Any] = []
    if status is not None:
        updates.append("status=?")
        values.append(str(status))
    if current_revision_index is not None:
        updates.append("current_revision_index=?")
        values.append(max(1, int(current_revision_index)))
    if narration_text is not None:
        updates.append("narration_text=?")
        values.append(str(narration_text))
    values.append(str(clip_id))
    conn.execute(f"UPDATE video_clips SET {', '.join(updates)} WHERE clip_id=?", tuple(values))
    conn.commit()
    return get_video_clip(clip_id)


def _row_to_video_revision(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "revision_id": row["revision_id"],
        "video_run_id": row["video_run_id"],
        "clip_id": row["clip_id"],
        "revision_index": int(row["revision_index"] or 1),
        "status": row["status"] or "pending",
        "created_by": row["created_by"] or "",
        "operator_note": row["operator_note"] or "",
        "input_snapshot": _json_loads(row["input_snapshot_json"], {}),
        "provenance": _json_loads(row["provenance_json"], {}),
        "qc_report": _json_loads(row["qc_json"], {}),
        "created_at": row["created_at"],
    }


def create_video_clip_revision(
    *,
    revision_id: str,
    video_run_id: str,
    clip_id: str,
    revision_index: int,
    status: str = "pending",
    created_by: str = "",
    operator_note: str = "",
    input_snapshot: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    qc_report: dict[str, Any] | None = None,
) -> dict:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_clip_revisions (
            revision_id, video_run_id, clip_id, revision_index, status,
            created_by, operator_note, input_snapshot_json, provenance_json, qc_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(revision_id),
            str(video_run_id),
            str(clip_id),
            max(1, int(revision_index or 1)),
            str(status or "pending"),
            str(created_by or ""),
            str(operator_note or ""),
            _json_dumps(input_snapshot or {}),
            _json_dumps(provenance or {}),
            _json_dumps(qc_report or {}),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM video_clip_revisions WHERE revision_id=?",
        (str(revision_id),),
    ).fetchone()
    result = _row_to_video_revision(row)
    if not result:
        raise RuntimeError("Failed to create clip revision")
    return result


def get_video_clip_revision(revision_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM video_clip_revisions WHERE revision_id=?",
        (str(revision_id),),
    ).fetchone()
    return _row_to_video_revision(row)


def get_video_clip_revision_by_index(clip_id: str, revision_index: int) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM video_clip_revisions WHERE clip_id=? AND revision_index=?",
        (str(clip_id), max(1, int(revision_index))),
    ).fetchone()
    return _row_to_video_revision(row)


def get_latest_video_clip_revision(clip_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        """
        SELECT *
        FROM video_clip_revisions
        WHERE clip_id=?
        ORDER BY revision_index DESC, created_at DESC
        LIMIT 1
        """,
        (str(clip_id),),
    ).fetchone()
    return _row_to_video_revision(row)


def list_video_clip_revisions(clip_id: str) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT *
        FROM video_clip_revisions
        WHERE clip_id=?
        ORDER BY revision_index ASC
        """,
        (str(clip_id),),
    ).fetchall()
    return [_row_to_video_revision(row) for row in rows if row]


def update_video_clip_revision(
    revision_id: str,
    *,
    status: str | None = None,
    created_by: str | None = None,
    operator_note: str | None = None,
    input_snapshot: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    qc_report: dict[str, Any] | None = None,
) -> dict | None:
    conn = _get_conn()
    updates: list[str] = []
    values: list[Any] = []
    if status is not None:
        updates.append("status=?")
        values.append(str(status))
    if created_by is not None:
        updates.append("created_by=?")
        values.append(str(created_by))
    if operator_note is not None:
        updates.append("operator_note=?")
        values.append(str(operator_note))
    if input_snapshot is not None:
        updates.append("input_snapshot_json=?")
        values.append(_json_dumps(input_snapshot))
    if provenance is not None:
        updates.append("provenance_json=?")
        values.append(_json_dumps(provenance))
    if qc_report is not None:
        updates.append("qc_json=?")
        values.append(_json_dumps(qc_report))
    if not updates:
        return get_video_clip_revision(revision_id)
    values.append(str(revision_id))
    conn.execute(
        f"UPDATE video_clip_revisions SET {', '.join(updates)} WHERE revision_id=?",
        tuple(values),
    )
    conn.commit()
    return get_video_clip_revision(revision_id)


def _row_to_video_asset(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "asset_id": row["asset_id"],
        "video_run_id": row["video_run_id"],
        "clip_id": row["clip_id"] or "",
        "revision_id": row["revision_id"] or "",
        "asset_type": row["asset_type"] or "",
        "storage_path": row["storage_path"] or "",
        "source_url": row["source_url"] or "",
        "file_name": row["file_name"] or "",
        "mime_type": row["mime_type"] or "",
        "byte_size": int(row["byte_size"] or 0),
        "checksum_sha256": row["checksum_sha256"] or "",
        "metadata": _json_loads(row["metadata_json"], {}),
        "created_at": row["created_at"],
    }


def create_video_asset(
    *,
    asset_id: str,
    video_run_id: str,
    clip_id: str = "",
    revision_id: str = "",
    asset_type: str,
    storage_path: str,
    source_url: str = "",
    file_name: str = "",
    mime_type: str = "",
    byte_size: int = 0,
    checksum_sha256: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_assets (
            asset_id, video_run_id, clip_id, revision_id, asset_type, storage_path,
            source_url, file_name, mime_type, byte_size, checksum_sha256, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(asset_id),
            str(video_run_id),
            str(clip_id or ""),
            str(revision_id or ""),
            str(asset_type or ""),
            str(storage_path or ""),
            str(source_url or ""),
            str(file_name or ""),
            str(mime_type or ""),
            max(0, int(byte_size or 0)),
            str(checksum_sha256 or ""),
            _json_dumps(metadata or {}),
        ),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM video_assets WHERE asset_id=?", (str(asset_id),)).fetchone()
    result = _row_to_video_asset(row)
    if not result:
        raise RuntimeError("Failed to create asset")
    return result


def get_video_asset(asset_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM video_assets WHERE asset_id=?", (str(asset_id),)).fetchone()
    return _row_to_video_asset(row)


def list_video_assets(
    video_run_id: str,
    *,
    clip_id: str = "",
    revision_id: str = "",
) -> list[dict]:
    conn = _get_conn()
    query = "SELECT * FROM video_assets WHERE video_run_id=?"
    values: list[Any] = [str(video_run_id)]
    if clip_id:
        query += " AND clip_id=?"
        values.append(str(clip_id))
    if revision_id:
        query += " AND revision_id=?"
        values.append(str(revision_id))
    query += " ORDER BY created_at ASC"
    rows = conn.execute(query, tuple(values)).fetchall()
    return [_row_to_video_asset(row) for row in rows if row]


def find_video_asset_by_filename(video_run_id: str, file_name: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        """
        SELECT *
        FROM video_assets
        WHERE video_run_id=? AND file_name=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (str(video_run_id), str(file_name)),
    ).fetchone()
    return _row_to_video_asset(row)


def _row_to_provider_call(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "provider_call_id": row["provider_call_id"],
        "video_run_id": row["video_run_id"],
        "clip_id": row["clip_id"] or "",
        "revision_id": row["revision_id"] or "",
        "provider_name": row["provider_name"] or "",
        "operation": row["operation"] or "",
        "idempotency_key": row["idempotency_key"] or "",
        "status": row["status"] or "submitted",
        "request_payload": _json_loads(row["request_json"], {}),
        "response_payload": _json_loads(row["response_json"], {}),
        "error": row["error"] or "",
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def create_or_get_video_provider_call(
    *,
    provider_call_id: str,
    video_run_id: str,
    clip_id: str,
    revision_id: str,
    provider_name: str,
    operation: str,
    idempotency_key: str,
    request_payload: dict[str, Any] | None = None,
    status: str = "submitted",
) -> dict:
    conn = _get_conn()
    existing = conn.execute(
        "SELECT * FROM video_provider_calls WHERE idempotency_key=?",
        (str(idempotency_key),),
    ).fetchone()
    if existing:
        return _row_to_provider_call(existing) or {}

    conn.execute(
        """
        INSERT INTO video_provider_calls (
            provider_call_id, video_run_id, clip_id, revision_id, provider_name, operation,
            idempotency_key, status, request_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        """,
        (
            str(provider_call_id),
            str(video_run_id),
            str(clip_id or ""),
            str(revision_id or ""),
            str(provider_name or ""),
            str(operation or ""),
            str(idempotency_key),
            str(status or "submitted"),
            _json_dumps(request_payload or {}),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM video_provider_calls WHERE provider_call_id=?",
        (str(provider_call_id),),
    ).fetchone()
    result = _row_to_provider_call(row)
    if not result:
        raise RuntimeError("Failed to create provider call row")
    return result


def update_video_provider_call(
    idempotency_key: str,
    *,
    status: str | None = None,
    response_payload: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict | None:
    conn = _get_conn()
    updates: list[str] = ["updated_at=datetime('now','localtime')"]
    values: list[Any] = []
    if status is not None:
        updates.append("status=?")
        values.append(str(status))
    if response_payload is not None:
        updates.append("response_json=?")
        values.append(_json_dumps(response_payload))
    if error is not None:
        updates.append("error=?")
        values.append(str(error))
    values.append(str(idempotency_key))
    conn.execute(
        f"UPDATE video_provider_calls SET {', '.join(updates)} WHERE idempotency_key=?",
        tuple(values),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM video_provider_calls WHERE idempotency_key=?",
        (str(idempotency_key),),
    ).fetchone()
    return _row_to_provider_call(row)


def list_video_provider_calls(
    video_run_id: str,
    *,
    clip_id: str = "",
    revision_id: str = "",
) -> list[dict]:
    conn = _get_conn()
    query = "SELECT * FROM video_provider_calls WHERE video_run_id=?"
    values: list[Any] = [str(video_run_id)]
    if clip_id:
        query += " AND clip_id=?"
        values.append(str(clip_id))
    if revision_id:
        query += " AND revision_id=?"
        values.append(str(revision_id))
    query += " ORDER BY created_at ASC"
    rows = conn.execute(query, tuple(values)).fetchall()
    return [_row_to_provider_call(row) for row in rows if row]


def save_video_validation_report(
    *,
    report_id: str,
    video_run_id: str,
    status: str,
    folder_url: str,
    summary: dict[str, Any],
    items: list[dict[str, Any]],
) -> None:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_validation_reports (report_id, video_run_id, status, folder_url, summary_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(report_id),
            str(video_run_id),
            str(status),
            str(folder_url or ""),
            _json_dumps(summary or {}),
        ),
    )
    for item in items:
        payload = dict(item or {})
        asset_json = payload.get("matched_asset")
        conn.execute(
            """
            INSERT INTO video_validation_items (
                report_id, filename, file_role, required, status, issue_code, message, remediation, asset_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(report_id),
                str(payload.get("filename") or ""),
                str(payload.get("file_role") or ""),
                1 if bool(payload.get("required", True)) else 0,
                str(payload.get("status") or ""),
                str(payload.get("issue_code") or ""),
                str(payload.get("message") or ""),
                str(payload.get("remediation") or ""),
                _json_dumps(asset_json or {}),
            ),
        )
    conn.commit()


def get_latest_video_validation_report(video_run_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        """
        SELECT *
        FROM video_validation_reports
        WHERE video_run_id=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (str(video_run_id),),
    ).fetchone()
    if not row:
        return None
    return {
        "report_id": row["report_id"],
        "video_run_id": row["video_run_id"],
        "status": row["status"],
        "folder_url": row["folder_url"] or "",
        "summary": _json_loads(row["summary_json"], {}),
        "created_at": row["created_at"],
    }


def list_video_validation_items(report_id: str) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT *
        FROM video_validation_items
        WHERE report_id=?
        ORDER BY id ASC
        """,
        (str(report_id),),
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        matched_asset = _json_loads(row["asset_json"], {})
        if not isinstance(matched_asset, dict) or not matched_asset:
            matched_asset = None
        out.append(
            {
                "filename": row["filename"] or "",
                "file_role": row["file_role"] or "",
                "required": bool(int(row["required"] or 0)),
                "status": row["status"] or "",
                "issue_code": row["issue_code"] or "",
                "message": row["message"] or "",
                "remediation": row["remediation"] or "",
                "matched_asset": matched_asset,
            }
        )
    return out


def create_video_operator_action(
    *,
    action_id: str,
    video_run_id: str,
    clip_id: str = "",
    revision_id: str = "",
    action_type: str,
    actor: str = "",
    payload: dict[str, Any] | None = None,
) -> dict:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO video_operator_actions (
            action_id, video_run_id, clip_id, revision_id, action_type, actor, action_payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(action_id),
            str(video_run_id),
            str(clip_id or ""),
            str(revision_id or ""),
            str(action_type or ""),
            str(actor or ""),
            _json_dumps(payload or {}),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM video_operator_actions WHERE action_id=?",
        (str(action_id),),
    ).fetchone()
    if not row:
        raise RuntimeError("Failed to create operator action")
    return {
        "action_id": row["action_id"],
        "video_run_id": row["video_run_id"],
        "clip_id": row["clip_id"] or "",
        "revision_id": row["revision_id"] or "",
        "action_type": row["action_type"] or "",
        "actor": row["actor"] or "",
        "payload": _json_loads(row["action_payload_json"], {}),
        "created_at": row["created_at"],
    }


def list_video_operator_actions(video_run_id: str, clip_id: str = "") -> list[dict]:
    conn = _get_conn()
    if clip_id:
        rows = conn.execute(
            """
            SELECT *
            FROM video_operator_actions
            WHERE video_run_id=? AND clip_id=?
            ORDER BY created_at ASC
            """,
            (str(video_run_id), str(clip_id)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM video_operator_actions
            WHERE video_run_id=?
            ORDER BY created_at ASC
            """,
            (str(video_run_id),),
        ).fetchall()
    return [
        {
            "action_id": row["action_id"],
            "video_run_id": row["video_run_id"],
            "clip_id": row["clip_id"] or "",
            "revision_id": row["revision_id"] or "",
            "action_type": row["action_type"] or "",
            "actor": row["actor"] or "",
            "payload": _json_loads(row["action_payload_json"], {}),
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def reset_storage_connection_for_tests() -> None:
    """Close thread-local connection so tests can swap DB_PATH cleanly."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    _local.conn = None
