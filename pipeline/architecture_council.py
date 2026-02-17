"""Multi-agent workflow for selecting the best Phase 1 architecture-mining strategy."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import config
from pydantic import BaseModel
from pipeline.llm import call_llm_structured
from schemas.architecture_council import (
    ArchitectureCouncilRun,
    BlueprintReport,
    DecisionReport,
    RedTeamReport,
    RequirementsBrief,
    StrategyOption,
    StrategyPortfolio,
)

logger = logging.getLogger(__name__)

PROMPT_DIR = Path(config.ROOT_DIR) / "prompts" / "architecture_council"
DEFAULT_GOAL = (
    "Design the highest-quality possible Matrix-Only Phase 2 architecture for "
    "building and validating an evidence-grounded matrix with Awareness on the "
    "X-axis (hard 5 levels) and Emotion on the Y-axis (dynamic per brand), "
    "including per-cell brief quantity planning, traceability, quality gates, "
    "and mandatory human approval. Exclude angle/concept/script generation."
)

REQUIREMENTS_MINER_SLUG = "architecture_requirements_miner"
STRATEGY_DESIGNER_SLUG = "architecture_strategy_designer"
RED_TEAM_SLUG = "architecture_red_team"
JUDGE_SLUG = "architecture_judge"
BLUEPRINT_WRITER_SLUG = "architecture_blueprint_writer"


def _prompt(name: str) -> str:
    path = PROMPT_DIR / name
    return path.read_text("utf-8").strip()


def _agent_call(*, slug: str, system_prompt: str, user_prompt: str, response_model):
    conf = config.get_agent_llm_config(slug)
    logger.info(
        "Architecture council agent=%s provider=%s model=%s",
        slug,
        conf["provider"],
        conf["model"],
    )
    return call_llm_structured(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        response_model=response_model,
        provider=conf["provider"],
        model=conf["model"],
        temperature=conf["temperature"],
        max_tokens=conf["max_tokens"],
    )


def _to_json(data) -> str:
    return json.dumps(_dump_payload(data), indent=2, default=str)


def _dump_payload(data):
    if isinstance(data, BaseModel):
        return data.model_dump()
    if isinstance(data, list):
        return [_dump_payload(item) for item in data]
    if isinstance(data, dict):
        return {key: _dump_payload(value) for key, value in data.items()}
    return data


def _write_model(path: Path, model_obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(_dump_payload(model_obj), indent=2, default=str)
    path.write_text(payload, "utf-8")


def _run_red_team_one(*, goal: str, requirements: RequirementsBrief, option: StrategyOption) -> RedTeamReport:
    user_prompt = (
        "Primary goal:\n"
        f"{goal}\n\n"
        "Requirements brief:\n"
        f"{_to_json(requirements)}\n\n"
        "Strategy option under critique:\n"
        f"{_to_json(option)}\n"
    )
    report = _agent_call(
        slug=RED_TEAM_SLUG,
        system_prompt=_prompt("red_team.md"),
        user_prompt=user_prompt,
        response_model=RedTeamReport,
    )
    # Keep the report pinned to the option we asked about.
    report.option_id = option.option_id
    return report


def _run_red_team_reports(
    *,
    goal: str,
    requirements: RequirementsBrief,
    options: list[StrategyOption],
    parallel_red_team: bool,
) -> list[RedTeamReport]:
    if not parallel_red_team:
        return [
            _run_red_team_one(goal=goal, requirements=requirements, option=option)
            for option in options
        ]

    reports_by_id: dict[str, RedTeamReport] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(options))) as pool:
        futures = {
            pool.submit(
                _run_red_team_one,
                goal=goal,
                requirements=requirements,
                option=option,
            ): option.option_id
            for option in options
        }
        for future in as_completed(futures):
            option_id = futures[future]
            reports_by_id[option_id] = future.result()

    return [reports_by_id[option.option_id] for option in options]


def run_architecture_council(
    *,
    source_path: Path,
    goal: str = DEFAULT_GOAL,
    output_dir: Path | None = None,
    parallel_red_team: bool = True,
) -> ArchitectureCouncilRun:
    """Run the architecture council and persist all stage artifacts."""
    if not source_path.exists():
        raise FileNotFoundError(f"Source document not found: {source_path}")

    source_text = source_path.read_text("utf-8").strip()
    if not source_text:
        raise ValueError(f"Source document is empty: {source_path}")

    output_dir = output_dir or (config.OUTPUT_DIR / "architecture_council")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Architecture council: start source=%s", source_path)

    requirements_user_prompt = (
        "Primary goal:\n"
        f"{goal}\n\n"
        "Source document path:\n"
        f"{source_path}\n\n"
        "Source document content:\n"
        f"{source_text}\n"
    )
    requirements = _agent_call(
        slug=REQUIREMENTS_MINER_SLUG,
        system_prompt=_prompt("requirements_miner.md"),
        user_prompt=requirements_user_prompt,
        response_model=RequirementsBrief,
    )

    strategy_user_prompt = (
        "Primary goal:\n"
        f"{goal}\n\n"
        "Requirements brief:\n"
        f"{_to_json(requirements)}\n"
    )
    strategy_portfolio = _agent_call(
        slug=STRATEGY_DESIGNER_SLUG,
        system_prompt=_prompt("strategy_designer.md"),
        user_prompt=strategy_user_prompt,
        response_model=StrategyPortfolio,
    )

    red_team_reports = _run_red_team_reports(
        goal=goal,
        requirements=requirements,
        options=strategy_portfolio.options,
        parallel_red_team=parallel_red_team,
    )

    decision_user_prompt = (
        "Primary goal:\n"
        f"{goal}\n\n"
        "Requirements brief:\n"
        f"{_to_json(requirements)}\n\n"
        "Strategy options:\n"
        f"{_to_json(strategy_portfolio)}\n\n"
        "Red-team reports:\n"
        f"{_to_json(red_team_reports)}\n"
    )
    decision_report = _agent_call(
        slug=JUDGE_SLUG,
        system_prompt=_prompt("judge.md"),
        user_prompt=decision_user_prompt,
        response_model=DecisionReport,
    )

    blueprint_user_prompt = (
        "Primary goal:\n"
        f"{goal}\n\n"
        "Requirements brief:\n"
        f"{_to_json(requirements)}\n\n"
        "Strategy options:\n"
        f"{_to_json(strategy_portfolio)}\n\n"
        "Red-team reports:\n"
        f"{_to_json(red_team_reports)}\n\n"
        "Decision report:\n"
        f"{_to_json(decision_report)}\n"
    )
    blueprint_report = _agent_call(
        slug=BLUEPRINT_WRITER_SLUG,
        system_prompt=_prompt("blueprint_writer.md"),
        user_prompt=blueprint_user_prompt,
        response_model=BlueprintReport,
    )

    run = ArchitectureCouncilRun(
        source_path=str(source_path),
        generated_at=datetime.now(timezone.utc).isoformat(),
        requirements_brief=requirements,
        strategy_portfolio=strategy_portfolio,
        red_team_reports=red_team_reports,
        decision_report=decision_report,
        blueprint_report=blueprint_report,
    )

    _write_model(output_dir / "architecture_requirements_brief.json", requirements)
    _write_model(output_dir / "architecture_strategy_portfolio.json", strategy_portfolio)
    _write_model(output_dir / "architecture_red_team_reports.json", red_team_reports)
    _write_model(output_dir / "architecture_decision_report.json", decision_report)
    _write_model(output_dir / "architecture_blueprint_report.json", blueprint_report)
    _write_model(output_dir / "architecture_council_run.json", run)

    logger.info(
        "Architecture council complete: winner=%s (%s)",
        run.decision_report.winner_option_id,
        run.decision_report.winner_name,
    )

    return run
