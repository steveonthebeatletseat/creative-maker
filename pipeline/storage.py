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
            brand_slug      TEXT    DEFAULT ''
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
    """)

    # Migration: add brand_slug column if missing (existing DBs)
    try:
        conn.execute("ALTER TABLE pipeline_runs ADD COLUMN brand_slug TEXT DEFAULT ''")
        conn.commit()
        logger.info("Migrated pipeline_runs: added brand_slug column")
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
    }


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
