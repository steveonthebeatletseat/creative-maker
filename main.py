"""Creative Maker Pipeline — Entry Point.

Usage:
    # Run full Phase 1 (Research)
    python main.py phase1 --brand "BrandName" --product "ProductName" ...

    # Run only Agent 1A
    python main.py agent 01a --brand "BrandName" --product "ProductName" ...

    # Run from a JSON input file
    python main.py phase1 --input inputs.json
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


def run_phase1(inputs: dict):
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


def run_single_agent(agent_slug: str, inputs: dict):
    """Run a single agent by slug."""
    agent_map = {
        "01a": Agent01AFoundationResearch,
        "01b": Agent01BTrendIntel,
    }

    agent_cls = agent_map.get(agent_slug)
    if not agent_cls:
        console.print(f"[red]Unknown agent: {agent_slug}[/red]")
        console.print(f"Available: {', '.join(agent_map.keys())}")
        sys.exit(1)

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
    p1 = subparsers.add_parser("phase1", help="Run Phase 1 (Research)")
    _add_input_args(p1)

    # -- agent command --
    ag = subparsers.add_parser("agent", help="Run a single agent")
    ag.add_argument("agent_slug", help="Agent slug (e.g. 01a, 01b)")
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
