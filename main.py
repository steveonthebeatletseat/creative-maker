"""Creative Maker Pipeline — Entry Point.

Usage:
    # Run full Phase 1 (Research)
    python main.py phase1 --input sample_input.json

    # Run full Phase 2 (Ideation)
    python main.py phase2 --input sample_input.json

    # Run full Phase 3 (Scripting)
    python main.py phase3 --input sample_input.json

    # Run Phases 1-3 sequentially
    python main.py run --input sample_input.json

    # Run only a single agent
    python main.py agent 01a --input sample_input.json
    python main.py agent 04 --input sample_input.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel

import config
from agents.agent_01a_foundation_research import Agent01AFoundationResearch
from agents.agent_01b_trend_intel import Agent01BTrendIntel
from agents.agent_02_idea_generator import Agent02IdeaGenerator
from agents.agent_03_stress_tester_p1 import Agent03StressTesterP1
from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from agents.agent_06_stress_tester_p2 import Agent06StressTesterP2
from agents.agent_07_versioning_engine import Agent07VersioningEngine
from pipeline.orchestrator import Pipeline

console = Console()


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def load_inputs(args: argparse.Namespace) -> dict:
    """Build inputs dict from CLI args or JSON file."""
    if args.input:
        path = Path(args.input)
        if not path.exists():
            console.print(f"[red]Input file not found: {path}[/red]")
            sys.exit(1)
        return json.loads(path.read_text())

    # Build from CLI args
    inputs = {
        "brand_name": args.brand or "Unknown Brand",
        "product_name": args.product or "Unknown Product",
        "product_description": args.description or "",
        "target_market": args.market or "",
        "price_point": args.price or "",
        "niche": args.niche or "",
        "compliance_category": args.compliance or "",
        "batch_id": args.batch or f"batch_{date.today().isoformat()}",
    }

    # Load optional text files
    if args.reviews:
        inputs["customer_reviews"] = Path(args.reviews).read_text()
    if args.competitors:
        inputs["competitor_info"] = Path(args.competitors).read_text()
    if args.landing_page:
        inputs["landing_page_info"] = Path(args.landing_page).read_text()
    if args.context:
        inputs["additional_context"] = Path(args.context).read_text()

    return inputs


def _load_agent_output(slug: str) -> dict | None:
    """Load a previous agent output from disk."""
    output_path = config.OUTPUT_DIR / f"{slug}_output.json"
    if output_path.exists():
        return json.loads(output_path.read_text(encoding="utf-8"))
    return None


def run_phase1(inputs: dict) -> dict[str, object]:
    """Run Phase 1 — Research (Agents 1A and 1B in parallel)."""
    pipeline = Pipeline()
    pipeline.register(Agent01AFoundationResearch())
    pipeline.register(Agent01BTrendIntel())

    console.print(
        Panel(
            "[bold cyan]PHASE 1 — RESEARCH[/bold cyan]\n"
            "Running Agent 1A (Foundation Research) and Agent 1B (Trend Intel) in parallel",
            border_style="bright_blue",
        )
    )

    # Run both agents in parallel
    results = pipeline.run_parallel(
        slugs=["agent_01a", "agent_01b"],
        inputs=inputs,
    )

    pipeline.print_summary()

    # Report what was saved
    for slug, result in results.items():
        if result:
            output_path = config.OUTPUT_DIR / f"{slug}_output.json"
            console.print(f"  [green]Output saved:[/green] {output_path}")
        else:
            console.print(f"  [red]{slug} failed — no output saved[/red]")

    return results


def run_phase2(inputs: dict) -> dict[str, object]:
    """Run Phase 2 — Ideation (Agent 02 → Agent 03).

    Loads Phase 1 outputs from disk if not already in inputs.
    """
    console.print(
        Panel(
            "[bold cyan]PHASE 2 — IDEATION[/bold cyan]\n"
            "Agent 02 (Idea Generator) → Agent 03 (Stress Tester P1)",
            border_style="bright_blue",
        )
    )

    # Load Phase 1 outputs if not already available
    if "foundation_brief" not in inputs:
        fb = _load_agent_output("agent_01a")
        if fb:
            inputs["foundation_brief"] = fb
            console.print("  [dim]Loaded Agent 1A output from disk[/dim]")
        else:
            console.print("[red]Agent 1A output not found — run phase1 first[/red]")
            sys.exit(1)

    if "trend_intel" not in inputs:
        ti = _load_agent_output("agent_01b")
        if ti:
            inputs["trend_intel"] = ti
            console.print("  [dim]Loaded Agent 1B output from disk[/dim]")
        else:
            console.print("[red]Agent 1B output not found — run phase1 first[/red]")
            sys.exit(1)

    pipeline = Pipeline()
    results = {}

    # Agent 02 — Idea Generator
    agent_02 = Agent02IdeaGenerator()
    pipeline.register(agent_02)
    result_02 = pipeline.run_agent("agent_02", inputs)
    results["agent_02"] = result_02

    if not result_02:
        console.print("[red]Agent 02 failed — cannot continue to Agent 03[/red]")
        pipeline.print_summary()
        return results

    # Feed Agent 02 output into inputs for Agent 03
    inputs["idea_brief"] = json.loads(result_02.model_dump_json())

    # Agent 03 — Stress Tester P1
    agent_03 = Agent03StressTesterP1()
    pipeline.register(agent_03)
    result_03 = pipeline.run_agent("agent_03", inputs)
    results["agent_03"] = result_03

    pipeline.print_summary()

    for slug, result in results.items():
        if result:
            output_path = config.OUTPUT_DIR / f"{slug}_output.json"
            console.print(f"  [green]Output saved:[/green] {output_path}")
        else:
            console.print(f"  [red]{slug} failed — no output saved[/red]")

    return results


def run_phase3(inputs: dict) -> dict[str, object]:
    """Run Phase 3 — Scripting (Agent 04 → Agent 05 → Agent 06 → Agent 07).

    Loads Phase 1 + Phase 2 outputs from disk if not already in inputs.
    """
    console.print(
        Panel(
            "[bold cyan]PHASE 3 — SCRIPTING[/bold cyan]\n"
            "Agent 04 (Copywriter) → Agent 05 (Hook Specialist) → "
            "Agent 06 (Stress Tester P2) → Agent 07 (Versioning Engine)",
            border_style="bright_blue",
        )
    )

    # Load upstream outputs if not already available
    if "foundation_brief" not in inputs:
        fb = _load_agent_output("agent_01a")
        if fb:
            inputs["foundation_brief"] = fb
            console.print("  [dim]Loaded Agent 1A output from disk[/dim]")
        else:
            console.print("[red]Agent 1A output not found — run phase1 first[/red]")
            sys.exit(1)

    if "trend_intel" not in inputs:
        ti = _load_agent_output("agent_01b")
        if ti:
            inputs["trend_intel"] = ti
            console.print("  [dim]Loaded Agent 1B output from disk[/dim]")

    if "stress_test_brief" not in inputs:
        st = _load_agent_output("agent_03")
        if st:
            inputs["stress_test_brief"] = st
            console.print("  [dim]Loaded Agent 03 output from disk[/dim]")
        else:
            console.print("[red]Agent 03 output not found — run phase2 first[/red]")
            sys.exit(1)

    pipeline = Pipeline()
    results = {}

    # Agent 04 — Copywriter
    agent_04 = Agent04Copywriter()
    pipeline.register(agent_04)
    result_04 = pipeline.run_agent("agent_04", inputs)
    results["agent_04"] = result_04

    if not result_04:
        console.print("[red]Agent 04 failed — cannot continue[/red]")
        pipeline.print_summary()
        return results

    inputs["copywriter_brief"] = json.loads(result_04.model_dump_json())

    # Agent 05 — Hook Specialist
    agent_05 = Agent05HookSpecialist()
    pipeline.register(agent_05)
    result_05 = pipeline.run_agent("agent_05", inputs)
    results["agent_05"] = result_05

    if not result_05:
        console.print("[red]Agent 05 failed — cannot continue[/red]")
        pipeline.print_summary()
        return results

    inputs["hook_brief"] = json.loads(result_05.model_dump_json())

    # Agent 06 — Stress Tester P2
    agent_06 = Agent06StressTesterP2()
    pipeline.register(agent_06)
    result_06 = pipeline.run_agent("agent_06", inputs)
    results["agent_06"] = result_06

    if not result_06:
        console.print("[red]Agent 06 failed — cannot continue[/red]")
        pipeline.print_summary()
        return results

    inputs["stress_test_p2_brief"] = json.loads(result_06.model_dump_json())

    # Agent 07 — Versioning Engine
    agent_07 = Agent07VersioningEngine()
    pipeline.register(agent_07)
    result_07 = pipeline.run_agent("agent_07", inputs)
    results["agent_07"] = result_07

    pipeline.print_summary()

    for slug, result in results.items():
        if result:
            output_path = config.OUTPUT_DIR / f"{slug}_output.json"
            console.print(f"  [green]Output saved:[/green] {output_path}")
        else:
            console.print(f"  [red]{slug} failed — no output saved[/red]")

    return results


def run_full_pipeline(inputs: dict):
    """Run Phases 1-3 sequentially."""
    console.print(
        Panel(
            "[bold magenta]FULL PIPELINE RUN — PHASES 1-3[/bold magenta]\n"
            "Research → Ideation → Scripting",
            border_style="bright_magenta",
        )
    )

    # Phase 1 — Research
    p1_results = run_phase1(inputs)
    if not p1_results.get("agent_01a") or not p1_results.get("agent_01b"):
        console.print("[red]Phase 1 failed — stopping pipeline[/red]")
        return

    # Inject Phase 1 outputs into inputs for Phase 2
    inputs["foundation_brief"] = json.loads(
        p1_results["agent_01a"].model_dump_json()
    )
    inputs["trend_intel"] = json.loads(
        p1_results["agent_01b"].model_dump_json()
    )

    # Phase 2 — Ideation
    p2_results = run_phase2(inputs)
    if not p2_results.get("agent_03"):
        console.print("[red]Phase 2 failed — stopping pipeline[/red]")
        return

    # Inject Phase 2 outputs for Phase 3
    inputs["stress_test_brief"] = json.loads(
        p2_results["agent_03"].model_dump_json()
    )

    # Phase 3 — Scripting
    run_phase3(inputs)

    console.print(
        Panel(
            "[bold green]PIPELINE COMPLETE — PHASES 1-3[/bold green]\n"
            "Check outputs/ directory for all agent outputs.",
            border_style="green",
        )
    )


def run_single_agent(agent_slug: str, inputs: dict):
    """Run a single agent by slug."""
    agent_map = {
        "01a": Agent01AFoundationResearch,
        "01b": Agent01BTrendIntel,
        "02": Agent02IdeaGenerator,
        "03": Agent03StressTesterP1,
        "04": Agent04Copywriter,
        "05": Agent05HookSpecialist,
        "06": Agent06StressTesterP2,
        "07": Agent07VersioningEngine,
    }

    agent_cls = agent_map.get(agent_slug)
    if not agent_cls:
        console.print(f"[red]Unknown agent: {agent_slug}[/red]")
        console.print(f"Available: {', '.join(sorted(agent_map.keys()))}")
        sys.exit(1)

    # Auto-load upstream outputs from disk for downstream agents
    if agent_slug in ("02", "03", "04", "05", "06", "07"):
        if "foundation_brief" not in inputs:
            fb = _load_agent_output("agent_01a")
            if fb:
                inputs["foundation_brief"] = fb
                console.print("  [dim]Auto-loaded Agent 1A output[/dim]")

    if agent_slug in ("02",):
        if "trend_intel" not in inputs:
            ti = _load_agent_output("agent_01b")
            if ti:
                inputs["trend_intel"] = ti
                console.print("  [dim]Auto-loaded Agent 1B output[/dim]")

    if agent_slug == "03":
        if "idea_brief" not in inputs:
            ib = _load_agent_output("agent_02")
            if ib:
                inputs["idea_brief"] = ib
                console.print("  [dim]Auto-loaded Agent 02 output[/dim]")

    if agent_slug in ("04", "05", "06"):
        if "stress_test_brief" not in inputs:
            st = _load_agent_output("agent_03")
            if st:
                inputs["stress_test_brief"] = st
                console.print("  [dim]Auto-loaded Agent 03 output[/dim]")

    if agent_slug in ("05", "06"):
        if "copywriter_brief" not in inputs:
            cb = _load_agent_output("agent_04")
            if cb:
                inputs["copywriter_brief"] = cb
                console.print("  [dim]Auto-loaded Agent 04 output[/dim]")

    if agent_slug == "06":
        if "hook_brief" not in inputs:
            hb = _load_agent_output("agent_05")
            if hb:
                inputs["hook_brief"] = hb
                console.print("  [dim]Auto-loaded Agent 05 output[/dim]")

    if agent_slug == "07":
        if "stress_test_p2_brief" not in inputs:
            st2 = _load_agent_output("agent_06")
            if st2:
                inputs["stress_test_p2_brief"] = st2
                console.print("  [dim]Auto-loaded Agent 06 output[/dim]")
        if "copywriter_brief" not in inputs:
            cb = _load_agent_output("agent_04")
            if cb:
                inputs["copywriter_brief"] = cb
                console.print("  [dim]Auto-loaded Agent 04 output[/dim]")
        if "hook_brief" not in inputs:
            hb = _load_agent_output("agent_05")
            if hb:
                inputs["hook_brief"] = hb
                console.print("  [dim]Auto-loaded Agent 05 output[/dim]")
        if "trend_intel" not in inputs:
            ti = _load_agent_output("agent_01b")
            if ti:
                inputs["trend_intel"] = ti
                console.print("  [dim]Auto-loaded Agent 1B output[/dim]")

    pipeline = Pipeline()
    agent = agent_cls()
    pipeline.register(agent)

    result = pipeline.run_agent(agent.slug, inputs)
    pipeline.print_summary()

    if result:
        output_path = config.OUTPUT_DIR / f"{agent.slug}_output.json"
        console.print(f"\n[green]Output saved:[/green] {output_path}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Creative Maker — 16-Agent Ad Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # -- phase1 command --
    p1 = subparsers.add_parser("phase1", help="Run Phase 1 (Research): Agents 1A + 1B")
    _add_input_args(p1)

    # -- phase2 command --
    p2 = subparsers.add_parser("phase2", help="Run Phase 2 (Ideation): Agent 02 → 03")
    _add_input_args(p2)

    # -- phase3 command --
    p3 = subparsers.add_parser(
        "phase3", help="Run Phase 3 (Scripting): Agent 04 → 05 → 06 → 07"
    )
    _add_input_args(p3)

    # -- run command (full pipeline) --
    run_cmd = subparsers.add_parser("run", help="Run full pipeline (Phases 1-3)")
    _add_input_args(run_cmd)

    # -- agent command --
    ag = subparsers.add_parser("agent", help="Run a single agent")
    ag.add_argument(
        "agent_slug",
        help="Agent slug (e.g. 01a, 01b, 02, 03, 04, 05, 06, 07)",
    )
    _add_input_args(ag)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    setup_logging()

    console.print(
        Panel(
            "[bold]CREATIVE MAKER PIPELINE[/bold]\n"
            "16-Agent Automated Ad Creation",
            border_style="bright_magenta",
        )
    )

    inputs = load_inputs(args)

    if args.command == "phase1":
        run_phase1(inputs)
    elif args.command == "phase2":
        run_phase2(inputs)
    elif args.command == "phase3":
        run_phase3(inputs)
    elif args.command == "run":
        run_full_pipeline(inputs)
    elif args.command == "agent":
        run_single_agent(args.agent_slug, inputs)


def _add_input_args(parser: argparse.ArgumentParser):
    """Add common input arguments to a subparser."""
    parser.add_argument("--input", "-i", help="Path to JSON input file")
    parser.add_argument("--brand", "-b", help="Brand name")
    parser.add_argument("--product", "-p", help="Product name")
    parser.add_argument("--description", "-d", help="Product description")
    parser.add_argument("--market", "-m", help="Target market")
    parser.add_argument("--price", help="Price point")
    parser.add_argument("--niche", "-n", help="Niche (e.g. skincare, supplements)")
    parser.add_argument("--compliance", help="Compliance category")
    parser.add_argument("--batch", help="Batch identifier")
    parser.add_argument("--reviews", help="Path to customer reviews text file")
    parser.add_argument("--competitors", help="Path to competitor info text file")
    parser.add_argument("--landing-page", help="Path to landing page info text file")
    parser.add_argument("--context", help="Path to additional context text file")


if __name__ == "__main__":
    main()
