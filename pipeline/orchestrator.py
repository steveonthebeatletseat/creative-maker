"""Pipeline orchestrator — chains agents in dependency order."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pipeline.base_agent import BaseAgent

logger = logging.getLogger(__name__)
console = Console()


class PipelineResult:
    """Collects all agent outputs from a pipeline run."""

    def __init__(self):
        self.outputs: dict[str, BaseModel] = {}
        self.timings: dict[str, float] = {}
        self.errors: dict[str, str] = {}

    def add(self, agent_slug: str, output: BaseModel, elapsed: float):
        self.outputs[agent_slug] = output
        self.timings[agent_slug] = elapsed

    def add_error(self, agent_slug: str, error: str):
        self.errors[agent_slug] = error

    def get(self, agent_slug: str) -> BaseModel | None:
        return self.outputs.get(agent_slug)

    def summary(self) -> str:
        lines = ["Pipeline Run Summary", "=" * 40]
        for slug, elapsed in self.timings.items():
            status = "OK" if slug not in self.errors else f"ERROR: {self.errors[slug]}"
            lines.append(f"  {slug}: {elapsed:.1f}s [{status}]")
        if self.errors:
            lines.append(f"\n{len(self.errors)} agent(s) failed.")
        else:
            lines.append(f"\nAll {len(self.outputs)} agent(s) completed successfully.")
        return "\n".join(lines)


class Pipeline:
    """Orchestrates the 16-agent pipeline."""

    def __init__(self):
        self.agents: dict[str, BaseAgent] = {}
        self.result = PipelineResult()

    def register(self, agent: BaseAgent):
        """Register an agent in the pipeline."""
        self.agents[agent.slug] = agent
        logger.info("Registered: %s (%s)", agent.name, agent.slug)

    def run_agent(
        self,
        slug: str,
        inputs: dict[str, Any],
    ) -> BaseModel | None:
        """Run a single agent by slug."""
        agent = self.agents.get(slug)
        if not agent:
            logger.error("Agent not found: %s", slug)
            return None

        console.print(
            Panel(
                f"[bold]{agent.name}[/bold]\n{agent.description}",
                title=f"Running {slug}",
                border_style="cyan",
            )
        )

        start = time.time()
        try:
            output = agent.run(inputs)
            elapsed = time.time() - start
            self.result.add(slug, output, elapsed)
            console.print(f"  [green]Completed in {elapsed:.1f}s[/green]")
            return output
        except Exception as e:
            elapsed = time.time() - start
            self.result.add_error(slug, str(e))
            self.result.timings[slug] = elapsed
            console.print(f"  [red]Failed: {e}[/red]")
            logger.exception("Agent %s failed", slug)
            return None

    def run_parallel(
        self,
        slugs: list[str],
        inputs: dict[str, Any],
    ) -> dict[str, BaseModel | None]:
        """Run multiple agents in parallel (threaded)."""
        import concurrent.futures

        results: dict[str, BaseModel | None] = {}

        console.print(
            Panel(
                f"Running in parallel: {', '.join(slugs)}",
                border_style="yellow",
            )
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(slugs)) as executor:
            futures = {
                executor.submit(self.run_agent, slug, inputs): slug
                for slug in slugs
            }
            for future in concurrent.futures.as_completed(futures):
                slug = futures[future]
                try:
                    results[slug] = future.result()
                except Exception as e:
                    results[slug] = None
                    logger.exception("Parallel agent %s failed", slug)

        return results

    def print_summary(self):
        """Pretty-print the pipeline result."""
        table = Table(title="Pipeline Results")
        table.add_column("Agent", style="cyan")
        table.add_column("Time", style="green")
        table.add_column("Status", style="bold")

        for slug in self.agents:
            elapsed = self.result.timings.get(slug)
            if elapsed is not None:
                time_str = f"{elapsed:.1f}s"
                if slug in self.result.errors:
                    status = f"[red]FAILED: {self.result.errors[slug][:50]}[/red]"
                else:
                    status = "[green]OK[/green]"
            else:
                time_str = "-"
                status = "[dim]skipped[/dim]"
            table.add_row(slug, time_str, status)

        console.print(table)
