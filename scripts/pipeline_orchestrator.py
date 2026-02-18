#!/usr/bin/env python3
"""LLM-backed review-first orchestrator for pipeline council checks.

Usage:
    python3 scripts/pipeline_orchestrator.py review --change "Added new step"
    python3 scripts/pipeline_orchestrator.py apply
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from pipeline.llm import call_llm

SERVER = ROOT / "server.py"
UI_APP = ROOT / "static" / "app.js"
ARCH_DOC = ROOT / "PIPELINE_ARCHITECTURE.md"
STRATEGY_DOC = ROOT / "prompts" / "architecture_council" / "strategy_designer.md"
SCENE_ENGINE = ROOT / "pipeline" / "phase3_v2_scene_engine.py"
SCENE_ARCH_DOC = ROOT / "docs" / "architecture" / "PHASE3_SCENE_WRITER_ARCHITECTURE_DECISION_SOURCE.md"
SCENE_HEAD_DOC = ROOT / "docs" / "architecture" / "PHASE3_SCENE_WRITER_HEAD_TO_HEAD_DECISION_SOURCE.md"
HOOK_ENGINE = ROOT / "pipeline" / "phase3_v2_hook_engine.py"
PHASE1_ENGINE = ROOT / "pipeline" / "phase1_engine.py"
HOOK_ARCH_DOC = ROOT / "docs" / "architecture" / "PHASE3_HOOK_GENERATOR_ARCHITECTURE_DECISION_SOURCE.md"
REPORT_DIR = ROOT / "outputs" / "orchestrator"
REPORT_JSON = REPORT_DIR / "last_review.json"
REPORT_MD = REPORT_DIR / "last_review.md"

TARGET_PROFILES: dict[str, dict[str, Any]] = {
    "pipeline_core": {
        "description": "Core pipeline architecture/map/logs/strategy sync.",
        "allowed_files": [
            str(SERVER.resolve()),
            str(UI_APP.resolve()),
            str(ARCH_DOC.resolve()),
            str(STRATEGY_DOC.resolve()),
        ],
    },
    "scene_writer": {
        "description": "Phase 3C v2 Scene Writer architecture/map/logs/strategy sync.",
        "allowed_files": [
            str(SERVER.resolve()),
            str(UI_APP.resolve()),
            str(ARCH_DOC.resolve()),
            str(STRATEGY_DOC.resolve()),
            str(SCENE_ENGINE.resolve()),
            str(SCENE_ARCH_DOC.resolve()),
            str(SCENE_HEAD_DOC.resolve()),
        ],
    },
    "hook_foundation_research": {
        "description": "Phase 3B Hook Generator + Phase 1 Foundation Research architecture/map/logs/strategy sync.",
        "allowed_files": [
            str(SERVER.resolve()),
            str(UI_APP.resolve()),
            str(ARCH_DOC.resolve()),
            str(STRATEGY_DOC.resolve()),
            str(HOOK_ENGINE.resolve()),
            str(PHASE1_ENGINE.resolve()),
            str(HOOK_ARCH_DOC.resolve()),
        ],
    },
}


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _extract_python_literal_variable(path: Path, var_name: str) -> Any:
    source = path.read_text(encoding="utf-8")
    module = ast.parse(source, filename=str(path))
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    return ast.literal_eval(node.value)
    raise ValueError(f"{var_name} not found in {path}")


def _extract_js_const_block(text: str, const_name: str) -> str:
    marker = f"const {const_name} = "
    start = text.find(marker)
    if start < 0:
        raise ValueError(f"{const_name} not found in UI file")
    open_brace = text.find("{", start)
    if open_brace < 0:
        raise ValueError(f"{const_name} has no opening brace")
    close = text.find("};", open_brace)
    if close < 0:
        raise ValueError(f"{const_name} has no closing brace")
    return text[start : close + 2]


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text).strip()
        text = re.sub(r"\n?```$", "", text).strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    first = text.find("{")
    last = text.rfind("}")
    if first >= 0 and last > first:
        data = json.loads(text[first : last + 1])
        if isinstance(data, dict):
            return data
    raise ValueError("LLM response was not valid JSON object")


def _safe_file(path: str, allowed_files: set[str]) -> bool:
    try:
        resolved = str(Path(path).resolve())
    except OSError:
        return False
    return resolved in allowed_files


def _snip_server_log_block(server_text: str) -> str:
    m = re.search(
        r"(# Live server-log streaming .*?class _WSLogHandler\(logging\.Handler\):.*?def emit\(self, record: logging\.LogRecord\):.*?^\s*except Exception:\s*pass)",
        server_text,
        flags=re.DOTALL | re.MULTILINE,
    )
    if m:
        return m.group(1)
    start = server_text.find("class _WSLogHandler(logging.Handler):")
    if start >= 0:
        return server_text[start : min(len(server_text), start + 2200)]
    return server_text[:2200]


def _grep_excerpt(path: Path, pattern: str, *, radius: int = 1, max_lines: int = 120) -> str:
    if not path.exists():
        return ""
    rx = re.compile(pattern, re.IGNORECASE)
    rows = path.read_text(encoding="utf-8").splitlines()
    keep: set[int] = set()
    for idx, line in enumerate(rows):
        if not rx.search(line):
            continue
        start = max(0, idx - radius)
        end = min(len(rows), idx + radius + 1)
        keep.update(range(start, end))
    out: list[str] = []
    for i in sorted(keep):
        out.append(f"{i+1}: {rows[i]}")
        if len(out) >= max_lines:
            break
    return "\n".join(out)


def _agent_context(change: str, target: str) -> dict[str, Any]:
    server_meta = _extract_python_literal_variable(SERVER, "AGENT_META")
    ui_text = UI_APP.read_text(encoding="utf-8")
    server_text = SERVER.read_text(encoding="utf-8")
    context = {
        "change": change,
        "target": target,
        "server_agent_meta": server_meta,
        "ui_agent_names_block": _extract_js_const_block(ui_text, "AGENT_NAMES"),
        "ui_agent_slugs_block": _extract_js_const_block(ui_text, "AGENT_SLUGS"),
        "arch_doc_text": ARCH_DOC.read_text(encoding="utf-8"),
        "strategy_doc_text": STRATEGY_DOC.read_text(encoding="utf-8") if STRATEGY_DOC.exists() else "",
        "server_log_block": _snip_server_log_block(server_text),
    }
    if target == "scene_writer":
        context.update(
            {
                "scene_engine_excerpt": _grep_excerpt(
                    SCENE_ENGINE,
                    r"scene writer|scene|logger\.(info|warning|error|exception)|phase3_v2",
                    radius=1,
                    max_lines=220,
                ),
                "server_scene_excerpt": _grep_excerpt(
                    SERVER,
                    r"scenes/|scene_writer|phase3_v2_scene|scene_stage|scene_handoff|scene_plan|logger\.",
                    radius=0,
                    max_lines=260,
                ),
                "ui_scene_excerpt": _grep_excerpt(
                    UI_APP,
                    r"phase3-v2-scenes|scene writer|scene_stage|scene_plan|scene_handoff",
                    radius=1,
                    max_lines=240,
                ),
                "arch_scene_excerpt": _grep_excerpt(
                    ARCH_DOC,
                    r"scene writer|phase 3c|scene_handoff|scene plans|production_handoff",
                    radius=1,
                    max_lines=220,
                ),
                "scene_architecture_doc": SCENE_ARCH_DOC.read_text(encoding="utf-8") if SCENE_ARCH_DOC.exists() else "",
                "scene_head_to_head_doc": SCENE_HEAD_DOC.read_text(encoding="utf-8") if SCENE_HEAD_DOC.exists() else "",
            }
        )
    if target == "hook_foundation_research":
        context.update(
            {
                "hook_engine_excerpt": _grep_excerpt(
                    HOOK_ENGINE,
                    r"hook|phase3_v2|logger\.(info|warning|error|exception)|gate|repair|rank",
                    radius=1,
                    max_lines=260,
                ),
                "phase1_engine_excerpt": _grep_excerpt(
                    PHASE1_ENGINE,
                    r"foundation|phase1|collector|quality|gate|checkpoint|logger\.",
                    radius=1,
                    max_lines=260,
                ),
                "server_hook_foundation_excerpt": _grep_excerpt(
                    SERVER,
                    r"foundation_research|collectors|phase1|hook|phase3_v2_hook|hooks/|logger\.",
                    radius=0,
                    max_lines=320,
                ),
                "ui_hook_foundation_excerpt": _grep_excerpt(
                    UI_APP,
                    r"foundation_research|collector|phase1|hook|phase3-v2-hooks",
                    radius=1,
                    max_lines=260,
                ),
                "arch_hook_foundation_excerpt": _grep_excerpt(
                    ARCH_DOC,
                    r"foundation research|phase 1|collector|hook generator|phase 3b|hook",
                    radius=1,
                    max_lines=260,
                ),
                "hook_architecture_doc": HOOK_ARCH_DOC.read_text(encoding="utf-8") if HOOK_ARCH_DOC.exists() else "",
            }
        )
    return context


def _call_subagent(agent_slug: str, system_prompt: str, user_payload: dict[str, Any]) -> dict[str, Any]:
    llm_conf = config.get_agent_llm_config(agent_slug)
    raw = call_llm(
        system_prompt=system_prompt,
        user_prompt=json.dumps(user_payload, indent=2),
        provider=str(llm_conf["provider"]),
        model=str(llm_conf["model"]),
        temperature=float(llm_conf.get("temperature", 0.2)),
        max_tokens=int(llm_conf.get("max_tokens", 8000)),
    )
    out = _extract_json_object(raw)
    out["_model"] = {"provider": llm_conf["provider"], "model": llm_conf["model"]}
    return out


def _validate_agent_output(agent_name: str, payload: dict[str, Any], *, allowed_files: set[str]) -> dict[str, Any]:
    findings = payload.get("findings")
    edits = payload.get("edits")
    if not isinstance(findings, list):
        findings = []
    if not isinstance(edits, list):
        edits = []
    safe_edits: list[dict[str, Any]] = []
    for edit in edits:
        if not isinstance(edit, dict):
            continue
        op = str(edit.get("op") or "").strip().lower()
        file_path = str(edit.get("file") or "").strip()
        if not op or not file_path or not _safe_file(file_path, allowed_files):
            continue
        if op not in {"replace", "insert_after", "append"}:
            continue
        safe_edits.append(edit)
    return {
        "agent": agent_name,
        "summary": str(payload.get("summary") or "").strip(),
        "findings": findings,
        "edits": safe_edits,
        "model": payload.get("_model", {}),
    }


def _run_llm_review(change: str, target: str) -> dict[str, Any]:
    profile = TARGET_PROFILES.get(target)
    if not profile:
        raise ValueError(f"Unknown target: {target}")
    ctx = _agent_context(change, target)
    allowed_files = set(profile["allowed_files"])

    shared_rules = (
        "You are a code-maintenance sub-agent. Return ONLY JSON.\n"
        "JSON shape: {summary:string, findings:[{severity,file,message}], edits:[...]}.\n"
        f"Target: {target}\n"
        f"Target description: {profile['description']}\n"
        "Allowed files:\n"
        + "".join(f"- {Path(p)}\\n" for p in sorted(allowed_files))
        + "Edits schema supports:\n"
        "1) {op:'replace', file, search, replace, all?:bool}\n"
        "2) {op:'insert_after', file, anchor, content}\n"
        "3) {op:'append', file, content}\n"
        "Use edits only when highly confident and the edit is deterministic.\n"
        "If no update is required, return edits: []."
    )

    doc_payload = {
        "change": ctx["change"],
        "target": target,
        "server_agent_meta": ctx["server_agent_meta"],
        "pipeline_architecture_text": ctx["arch_doc_text"],
        "scene_architecture_excerpt": ctx.get("arch_scene_excerpt", ""),
        "scene_architecture_decision_source": ctx.get("scene_architecture_doc", ""),
        "hook_foundation_architecture_excerpt": ctx.get("arch_hook_foundation_excerpt", ""),
        "hook_architecture_decision_source": ctx.get("hook_architecture_doc", ""),
    }
    map_payload = {
        "change": ctx["change"],
        "target": target,
        "server_agent_meta": ctx["server_agent_meta"],
        "ui_agent_names_block": ctx["ui_agent_names_block"],
        "ui_agent_slugs_block": ctx["ui_agent_slugs_block"],
        "ui_scene_excerpt": ctx.get("ui_scene_excerpt", ""),
        "ui_hook_foundation_excerpt": ctx.get("ui_hook_foundation_excerpt", ""),
    }
    obs_payload = {
        "change": ctx["change"],
        "target": target,
        "server_log_block": ctx["server_log_block"],
        "scene_engine_excerpt": ctx.get("scene_engine_excerpt", ""),
        "server_scene_excerpt": ctx.get("server_scene_excerpt", ""),
        "hook_engine_excerpt": ctx.get("hook_engine_excerpt", ""),
        "phase1_engine_excerpt": ctx.get("phase1_engine_excerpt", ""),
        "server_hook_foundation_excerpt": ctx.get("server_hook_foundation_excerpt", ""),
    }
    ux_payload = {
        "change": ctx["change"],
        "target": target,
        "strategy_doc_text": ctx["strategy_doc_text"],
        "pipeline_architecture_excerpt": ctx.get("arch_scene_excerpt", ""),
        "ui_scene_excerpt": ctx.get("ui_scene_excerpt", ""),
        "scene_head_to_head_doc": ctx.get("scene_head_to_head_doc", ""),
        "pipeline_architecture_hook_foundation_excerpt": ctx.get("arch_hook_foundation_excerpt", ""),
        "ui_hook_foundation_excerpt": ctx.get("ui_hook_foundation_excerpt", ""),
        "hook_architecture_doc": ctx.get("hook_architecture_doc", ""),
    }

    doc_raw = _call_subagent(
        "pipeline_doc_agent",
        shared_rules
        + "\nYou are DocAgent. Keep pipeline architecture docs up to date for the target. "
        "For target=scene_writer, ensure Phase 3C Scene Writer sections accurately match implementation and artifacts. "
        "For target=hook_foundation_research, ensure Phase 1 Foundation Research + Phase 3B Hook Generator sections are current.",
        doc_payload,
    )
    map_raw = _call_subagent(
        "pipeline_map_agent",
        shared_rules
        + "\nYou are MapAgent. Keep pipeline map UI and related map constants current for the target. "
        "For target=scene_writer, focus on Scene Writer panel/states/progress wiring in static/app.js. "
        "For target=hook_foundation_research, focus on Foundation collector review UI + Hook Generator panel/states wiring.",
        map_payload,
    )
    obs_raw = _call_subagent(
        "pipeline_observability_agent",
        shared_rules
        + "\nYou are ObservabilityAgent. Ensure live logs are useful and up to date for the target. "
        "For target=scene_writer, verify scene start/progress/failure/success logs in scene engine and server lifecycle. "
        "For target=hook_foundation_research, verify foundation collector/gate logs and hook generation stage logs.",
        obs_payload,
    )
    ux_raw = _call_subagent(
        "pipeline_strategy_ux_agent",
        shared_rules
        + "\nYou are StrategyUXAgent. Keep strategy_designer and target UX behavior aligned. "
        "For target=scene_writer, ensure strategy expectations do not conflict with current Scene Writer UX flow. "
        "For target=hook_foundation_research, ensure strategy expectations align with foundation + hook UX flow and approvals.",
        ux_payload,
    )

    agents = [
        _validate_agent_output("DocAgent", doc_raw, allowed_files=allowed_files),
        _validate_agent_output("MapAgent", map_raw, allowed_files=allowed_files),
        _validate_agent_output("ObservabilityAgent", obs_raw, allowed_files=allowed_files),
        _validate_agent_output("StrategyUXAgent", ux_raw, allowed_files=allowed_files),
    ]
    findings_count = sum(len(a["findings"]) for a in agents)
    edits_count = sum(len(a["edits"]) for a in agents)
    return {
        "timestamp_utc": _utc_now(),
        "change": change,
        "target": target,
        "allowed_files": sorted(allowed_files),
        "total_findings": findings_count,
        "total_proposed_edits": edits_count,
        "agents": agents,
    }


def _apply_one_edit(edit: dict[str, Any], *, allowed_files: set[str]) -> tuple[bool, str]:
    op = str(edit.get("op") or "").strip().lower()
    file_path = Path(str(edit.get("file") or "")).resolve()
    if str(file_path) not in allowed_files:
        return False, "file_not_allowed"
    try:
        old = file_path.read_text(encoding="utf-8")
    except OSError:
        return False, "read_failed"

    new = old
    if op == "replace":
        search = str(edit.get("search") or "")
        replacement = str(edit.get("replace") or "")
        replace_all = bool(edit.get("all", False))
        if not search:
            return False, "empty_search"
        if search not in old:
            return False, "search_not_found"
        if replace_all:
            new = old.replace(search, replacement)
        else:
            new = old.replace(search, replacement, 1)
    elif op == "insert_after":
        anchor = str(edit.get("anchor") or "")
        content = str(edit.get("content") or "")
        if not anchor:
            return False, "empty_anchor"
        idx = old.find(anchor)
        if idx < 0:
            return False, "anchor_not_found"
        pos = idx + len(anchor)
        new = old[:pos] + content + old[pos:]
    elif op == "append":
        content = str(edit.get("content") or "")
        if not content:
            return False, "empty_content"
        new = old.rstrip() + "\n" + content
        if not new.endswith("\n"):
            new += "\n"
    else:
        return False, "unknown_op"

    if new == old:
        return False, "no_change"
    file_path.write_text(new, encoding="utf-8")
    return True, "applied"


def _write_report(review: dict[str, Any]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(review, indent=2), encoding="utf-8")
    lines = [
        "# Pipeline Orchestrator Review",
        "",
        f"- Timestamp (UTC): {review['timestamp_utc']}",
        f"- Change: {review['change']}",
        f"- Target: {review.get('target', 'pipeline_core')}",
        f"- Total findings: {review['total_findings']}",
        f"- Total proposed edits: {review['total_proposed_edits']}",
        "",
    ]
    for block in review.get("agents", []):
        lines.append(f"## {block.get('agent', 'Agent')}")
        lines.append("")
        model = block.get("model", {})
        lines.append(f"- Model: `{model.get('provider', '?')}/{model.get('model', '?')}`")
        lines.append(f"- Summary: {block.get('summary', '').strip() or 'n/a'}")
        lines.append(f"- Findings: {len(block.get('findings', []))}")
        lines.append(f"- Proposed edits: {len(block.get('edits', []))}")
        lines.append("")
    lines.append("## Next")
    lines.append("")
    lines.append("- Apply fixes: `python3 scripts/pipeline_orchestrator.py apply`")
    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")


def cmd_review(change: str, target: str) -> int:
    review = _run_llm_review(change=change, target=target)
    _write_report(review)
    print(f"Review complete. Findings: {review['total_findings']}")
    print(f"Proposed edits: {review['total_proposed_edits']}")
    print(f"Report: {REPORT_MD}")
    return 0


def cmd_apply() -> int:
    if not REPORT_JSON.exists():
        print("No review report found. Run review first.")
        return 1
    review = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    agents = review.get("agents") or []
    allowed_files = set(str(Path(p).resolve()) for p in (review.get("allowed_files") or []))
    if not allowed_files:
        allowed_files = set(TARGET_PROFILES["pipeline_core"]["allowed_files"])
    if not isinstance(agents, list):
        print("Invalid review report format.")
        return 1

    applied = 0
    failed = 0
    for agent in agents:
        for edit in agent.get("edits") or []:
            ok, reason = _apply_one_edit(edit, allowed_files=allowed_files)
            if ok:
                applied += 1
            else:
                failed += 1
                print(f"Skipped edit ({reason}): {edit}")

    print(f"Apply done. Applied edits: {applied}. Skipped edits: {failed}.")
    print("Running post-apply review...")
    return cmd_review(
        change=f"Post-apply verification for: {review.get('change', '')}",
        target=str(review.get("target") or "pipeline_core"),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LLM-backed review-first orchestrator for pipeline council checks.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_review = sub.add_parser("review", help="Run all 4 LLM sub-agents and write a report.")
    p_review.add_argument("--change", required=True, help="Short description of what changed.")
    p_review.add_argument(
        "--target",
        choices=tuple(sorted(TARGET_PROFILES.keys())),
        default="pipeline_core",
        help="What component scope to audit.",
    )

    sub.add_parser("apply", help="Apply proposed edits from the latest review.")

    p_dogs = sub.add_parser(
        "council-of-dogs",
        help="Alias command for the same flow.",
    )
    p_dogs.add_argument(
        "--mode",
        choices=("review", "apply"),
        required=True,
        help="Run review or apply.",
    )
    p_dogs.add_argument(
        "--change",
        default="Council of Dogs requested review",
        help="Required only for --mode review.",
    )
    p_dogs.add_argument(
        "--target",
        choices=tuple(sorted(TARGET_PROFILES.keys())),
        default="pipeline_core",
        help="What component scope to audit.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.cmd == "review":
        return cmd_review(change=args.change, target=args.target)
    if args.cmd == "apply":
        return cmd_apply()
    if args.cmd == "council-of-dogs":
        if args.mode == "review":
            return cmd_review(change=args.change, target=args.target)
        return cmd_apply()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
