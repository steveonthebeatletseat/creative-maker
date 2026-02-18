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
    python main.py agent foundation_research --input sample_input.json
    python main.py agent copywriter --input sample_input.json

    # Run architecture council (no implementation, decision workflow only)
    python main.py architecture-council --source PIPELINE_ARCHITECTURE.md

    # Run dino council (parallel council namespace; keeps architecture-council intact)
    python main.py dino --source PIPELINE_ARCHITECTURE.md
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
from agents.agent_02_idea_generator import Agent02IdeaGenerator

from agents.agent_04_copywriter import Agent04Copywriter
from agents.agent_05_hook_specialist import Agent05HookSpecialist
from pipeline.architecture_council import run_architecture_council
from pipeline.orchestrator import Pipeline

console = Console()

LEGACY_AGENT_ALIASES = {
    "01a": "foundation_research",
    "agent_01a": "foundation_research",
    "02": "creative_engine",
    "agent_02": "creative_engine",
    "04": "copywriter",
    "agent_04": "copywriter",
    "05": "hook_specialist",
    "agent_05": "hook_specialist",
}
CANONICAL_TO_LEGACY = {
    "foundation_research": "agent_01a",
    "creative_engine": "agent_02",
    "copywriter": "agent_04",
    "hook_specialist": "agent_05",
}

DEFAULT_COUNCIL_GOAL = (
    "Design the highest-quality possible Matrix-Only Phase 2 architecture for "
    "building and validating an evidence-grounded matrix with Awareness on the "
    "X-axis (hard 5 levels) and Emotion on the Y-axis (dynamic per brand), "
    "including per-cell brief quantity planning, traceability, quality gates, "
    "and mandatory human approval. Exclude angle/concept/script generation."
)


def _canonical_agent_slug(slug: str) -> str:
    key = str(slug or "").strip().lower()
    return LEGACY_AGENT_ALIASES.get(key, key)


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
    canonical = _canonical_agent_slug(slug)
    candidates = [canonical]
    legacy = CANONICAL_TO_LEGACY.get(canonical)
    if legacy:
        candidates.append(legacy)
    for candidate in candidates:
        output_path = config.OUTPUT_DIR / f"{candidate}_output.json"
        if output_path.exists():
            return json.loads(output_path.read_text(encoding="utf-8"))
    return None


def run_phase1(inputs: dict) -> dict[str, object]:
    """Run Phase 1 — Research (Foundation Research)."""
    pipeline = Pipeline()
    pipeline.register(Agent01AFoundationResearch())

    console.print(
        Panel(
            "[bold cyan]PHASE 1 — RESEARCH[/bold cyan]\n"
            "Foundation Research",
            border_style="bright_blue",
        )
    )

    # Run 1A
    result = pipeline.run_agent("foundation_research", inputs)
    results = {"foundation_research": result}

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
    """Run Phase 2 — Ideation (Creative Engine).

    Loads Phase 1 outputs from disk if not already in inputs.
    """
    if config.PHASE2_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE2_DISABLED_MESSAGE}[/red]")
        return {}

    console.print(
        Panel(
            "[bold cyan]PHASE 2 — IDEATION[/bold cyan]\n"
            "Creative Engine",
            border_style="bright_blue",
        )
    )

    # Load Phase 1 outputs if not already available
    if "foundation_brief" not in inputs:
        fb = _load_agent_output("foundation_research")
        if fb:
            inputs["foundation_brief"] = fb
            console.print("  [dim]Loaded Foundation Research output from disk[/dim]")
        else:
            console.print("[red]Foundation Research output not found — run phase1 first[/red]")
            sys.exit(1)

    pipeline = Pipeline()
    results = {}

    # Creative Engine
    agent_02 = Agent02IdeaGenerator()
    pipeline.register(agent_02)
    result_02 = pipeline.run_agent("creative_engine", inputs)
    results["creative_engine"] = result_02

    if not result_02:
        console.print("[red]Creative Engine failed[/red]")
        pipeline.print_summary()
        return results

    inputs["idea_brief"] = json.loads(result_02.model_dump_json())

    pipeline.print_summary()

    for slug, result in results.items():
        if result:
            output_path = config.OUTPUT_DIR / f"{slug}_output.json"
            console.print(f"  [green]Output saved:[/green] {output_path}")
        else:
            console.print(f"  [red]{slug} failed — no output saved[/red]")

    return results


def run_phase3(inputs: dict) -> dict[str, object]:
    """Run Phase 3 — Scripting (Copywriter → Hook Specialist).

    Loads Phase 1 + Phase 2 outputs from disk if not already in inputs.
    """
    if config.PHASE3_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE3_DISABLED_MESSAGE}[/red]")
        return {}

    console.print(
        Panel(
            "[bold cyan]PHASE 3 — SCRIPTING[/bold cyan]\n"
            "Copywriter → Hook Specialist",
            border_style="bright_blue",
        )
    )

    # Load upstream outputs if not already available
    if "foundation_brief" not in inputs:
        fb = _load_agent_output("foundation_research")
        if fb:
            inputs["foundation_brief"] = fb
            console.print("  [dim]Loaded Foundation Research output from disk[/dim]")
        else:
            console.print("[red]Foundation Research output not found — run phase1 first[/red]")
            sys.exit(1)

    if "idea_brief" not in inputs:
        ib = _load_agent_output("creative_engine")
        if ib:
            inputs["idea_brief"] = ib
            console.print("  [dim]Loaded Creative Engine output from disk[/dim]")

    pipeline = Pipeline()
    results = {}

    # Copywriter
    agent_04 = Agent04Copywriter()
    pipeline.register(agent_04)
    result_04 = pipeline.run_agent("copywriter", inputs)
    results["copywriter"] = result_04

    if not result_04:
        console.print("[red]Copywriter failed — cannot continue[/red]")
        pipeline.print_summary()
        return results

    inputs["copywriter_brief"] = json.loads(result_04.model_dump_json())

    # Hook Specialist
    agent_05 = Agent05HookSpecialist()
    pipeline.register(agent_05)
    result_05 = pipeline.run_agent("hook_specialist", inputs)
    results["hook_specialist"] = result_05

    if not result_05:
        console.print("[red]Hook Specialist failed — cannot continue[/red]")
        pipeline.print_summary()
        return results

    inputs["hook_brief"] = json.loads(result_05.model_dump_json())

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
    if config.PHASE2_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE2_DISABLED_MESSAGE}[/red]")
        return
    if config.PHASE3_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE3_DISABLED_MESSAGE}[/red]")
        return

    console.print(
        Panel(
            "[bold magenta]FULL PIPELINE RUN — PHASES 1-3[/bold magenta]\n"
            "Research → Ideation → Scripting",
            border_style="bright_magenta",
        )
    )

    # Phase 1 — Research
    p1_results = run_phase1(inputs)
    if not p1_results.get("foundation_research"):
        console.print("[red]Phase 1 failed — stopping pipeline[/red]")
        return

    # Feed Phase 1 output to downstream
    inputs["foundation_brief"] = json.loads(
        p1_results["foundation_research"].model_dump_json()
    )

    # Phase 2 — Ideation
    p2_results = run_phase2(inputs)
    if not p2_results.get("creative_engine"):
        console.print("[red]Phase 2 failed — stopping pipeline[/red]")
        return

    # Inject Phase 2 outputs for Phase 3
    inputs["idea_brief"] = json.loads(
        p2_results["creative_engine"].model_dump_json()
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
    agent_slug = _canonical_agent_slug(agent_slug)
    if agent_slug == "creative_engine" and config.PHASE2_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE2_DISABLED_MESSAGE}[/red]")
        return
    if agent_slug in {"copywriter", "hook_specialist"} and config.PHASE3_TEMPORARILY_DISABLED:
        console.print(f"[red]{config.PHASE3_DISABLED_MESSAGE}[/red]")
        return

    agent_map = {
        "foundation_research": Agent01AFoundationResearch,
        "creative_engine": Agent02IdeaGenerator,
        "copywriter": Agent04Copywriter,
        "hook_specialist": Agent05HookSpecialist,
    }

    agent_cls = agent_map.get(agent_slug)
    if not agent_cls:
        console.print(f"[red]Unknown agent: {agent_slug}[/red]")
        console.print(f"Available: {', '.join(sorted(agent_map.keys()))}")
        sys.exit(1)

    # Auto-load upstream outputs from disk for downstream agents
    if agent_slug in ("creative_engine", "copywriter", "hook_specialist"):
        if "foundation_brief" not in inputs:
            fb = _load_agent_output("foundation_research")
            if fb:
                inputs["foundation_brief"] = fb
                console.print("  [dim]Auto-loaded Foundation Research output[/dim]")

    if agent_slug in ("copywriter", "hook_specialist"):
        if "idea_brief" not in inputs:
            ib = _load_agent_output("creative_engine")
            if ib:
                inputs["idea_brief"] = ib
                console.print("  [dim]Auto-loaded Creative Engine output[/dim]")

    if agent_slug == "hook_specialist":
        if "copywriter_brief" not in inputs:
            cb = _load_agent_output("copywriter")
            if cb:
                inputs["copywriter_brief"] = cb
                console.print("  [dim]Auto-loaded Copywriter output[/dim]")

    pipeline = Pipeline()
    agent = agent_cls()
    pipeline.register(agent)

    result = pipeline.run_agent(agent.slug, inputs)
    pipeline.print_summary()

    if result:
        output_path = config.OUTPUT_DIR / f"{agent.slug}_output.json"
        console.print(f"\n[green]Output saved:[/green] {output_path}")

    return result


def run_architecture_council_cmd(args: argparse.Namespace):
    """Run multi-agent architecture council on a source architecture document."""
    source_path = Path(args.source)
    output_dir = Path(args.output_dir) if args.output_dir else (config.OUTPUT_DIR / "architecture_council")

    if not source_path.exists():
        console.print(f"[red]Source file not found: {source_path}[/red]")
        sys.exit(1)

    console.print(
        Panel(
            "[bold cyan]ARCHITECTURE COUNCIL[/bold cyan]\n"
            "Multi-agent decision workflow for Phase 1 quality optimization",
            border_style="bright_blue",
        )
    )

    run = run_architecture_council(
        source_path=source_path,
        goal=args.goal,
        output_dir=output_dir,
    )

    winner = run.decision_report
    console.print(f"  [green]Winner:[/green] {winner.winner_option_id} — {winner.winner_name}")
    console.print(f"  [green]Confidence:[/green] {winner.confidence:.2f}")
    console.print(f"  [green]Artifacts saved:[/green] {output_dir}")


def run_dino_council_cmd(args: argparse.Namespace):
    """Run multi-agent dino council on a source architecture document."""
    source_path = Path(args.source)
    output_dir = Path(args.output_dir) if args.output_dir else (config.OUTPUT_DIR / "dino_council")

    if not source_path.exists():
        console.print(f"[red]Source file not found: {source_path}[/red]")
        sys.exit(1)

    console.print(
        Panel(
            "[bold green]DINO COUNCIL[/bold green]\n"
            "Architecture decision workflow (parallel council namespace)",
            border_style="green",
        )
    )

    run = run_architecture_council(
        source_path=source_path,
        goal=args.goal,
        output_dir=output_dir,
    )

    winner = run.decision_report
    console.print(f"  [green]Winner:[/green] {winner.winner_option_id} — {winner.winner_name}")
    console.print(f"  [green]Confidence:[/green] {winner.confidence:.2f}")
    console.print(f"  [green]Artifacts saved:[/green] {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Creative Maker — Ad Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # -- phase1 command --
    p1 = subparsers.add_parser("phase1", help="Run Phase 1 (Research): Foundation Research")
    _add_input_args(p1)

    # -- phase2 command --
    p2 = subparsers.add_parser("phase2", help="Run Phase 2 (Ideation): Creative Engine")
    _add_input_args(p2)

    # -- phase3 command --
    p3 = subparsers.add_parser(
        "phase3", help="Run Phase 3 (Scripting): Copywriter → Hook Specialist"
    )
    _add_input_args(p3)

    # -- run command (full pipeline) --
    run_cmd = subparsers.add_parser("run", help="Run full pipeline (Phases 1-3)")
    _add_input_args(run_cmd)

    # -- agent command --
    ag = subparsers.add_parser("agent", help="Run a single agent")
    ag.add_argument(
        "agent_slug",
        help="Agent slug (foundation_research, creative_engine, copywriter, hook_specialist). Legacy numeric aliases also supported.",
    )
    _add_input_args(ag)

    # -- architecture council command --
    council = subparsers.add_parser(
        "architecture-council",
        help="Run multi-agent architecture council against PIPELINE_ARCHITECTURE.md",
    )
    council.add_argument(
        "--source",
        default="PIPELINE_ARCHITECTURE.md",
        help="Path to architecture source document (default: PIPELINE_ARCHITECTURE.md)",
    )
    council.add_argument(
        "--goal",
        default=DEFAULT_COUNCIL_GOAL,
        help="Primary decision goal for the council run",
    )
    council.add_argument(
        "--output-dir",
        help="Directory for council artifacts (default: outputs/architecture_council)",
    )

    # -- dino council command (keeps architecture-council unchanged) --
    dino = subparsers.add_parser(
        "dino-council",
        aliases=["dino"],
        help="Run Dino Council (separate namespace for architecture decisions)",
    )
    dino.add_argument(
        "--source",
        default="PIPELINE_ARCHITECTURE.md",
        help="Path to architecture source document (default: PIPELINE_ARCHITECTURE.md)",
    )
    dino.add_argument(
        "--goal",
        default=DEFAULT_COUNCIL_GOAL,
        help="Primary decision goal for the dino council run",
    )
    dino.add_argument(
        "--output-dir",
        help="Directory for dino council artifacts (default: outputs/dino_council)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    setup_logging()

    console.print(
        Panel(
            "[bold]CREATIVE MAKER PIPELINE[/bold]\n"
            "Automated Ad Creation",
            border_style="bright_magenta",
        )
    )

    inputs: dict = {}
    if args.command in {"phase1", "phase2", "phase3", "run", "agent"}:
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
    elif args.command == "architecture-council":
        run_architecture_council_cmd(args)
    elif args.command in {"dino-council", "dino"}:
        run_dino_council_cmd(args)


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
